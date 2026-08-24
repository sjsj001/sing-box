//go:build with_naive_outbound

package naive

import (
	"context"
	"encoding/pem"
	"errors"
	"fmt"
	"net"
	"net/http"
	"strings"

	"github.com/sagernet/cronet-go"
	_ "github.com/sagernet/cronet-go/all"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/adapter/outbound"
	"github.com/sagernet/sing-box/common/dialer"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/dns"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common"
	E "github.com/sagernet/sing/common/exceptions"
	"github.com/sagernet/sing/common/logger"
	M "github.com/sagernet/sing/common/metadata"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/common/uot"
	"github.com/sagernet/sing/service"
	"github.com/sagernet/sing/service/filemanager"

	mDNS "github.com/miekg/dns"
)

func RegisterOutbound(registry *outbound.Registry) {
	outbound.Register[option.NaiveOutboundOptions](registry, C.TypeNaive, NewOutbound)
}

type Outbound struct {
	outbound.Adapter
	ctx         context.Context
	logger      logger.ContextLogger
	client      *cronet.NaiveClient
	uotClient   *uot.Client
	concurrency int
}

// Pools reports how many isolated connection pools streams are spread over, so
// a caller that wants none of them cold knows how many connections to open.
func (h *Outbound) Pools() int {
	return h.concurrency
}

func NewOutbound(ctx context.Context, router adapter.Router, logger log.ContextLogger, tag string, options option.NaiveOutboundOptions) (adapter.Outbound, error) {
	if options.TLS == nil || !options.TLS.Enabled {
		return nil, C.ErrTLSRequired
	}
	if options.TLS.DisableSNI {
		return nil, E.New("disable_sni is not supported on naive outbound")
	}
	if options.TLS.Insecure {
		return nil, E.New("insecure is not supported on naive outbound")
	}
	if options.TLS.CertificateServerName != "" {
		return nil, E.New("certificate_server_name is not supported on naive outbound")
	}
	if len(options.TLS.ALPN) > 0 {
		return nil, E.New("alpn is not supported on naive outbound")
	}
	if options.TLS.MinVersion != "" {
		return nil, E.New("min_version is not supported on naive outbound")
	}
	if options.TLS.MaxVersion != "" {
		return nil, E.New("max_version is not supported on naive outbound")
	}
	if len(options.TLS.CipherSuites) > 0 {
		return nil, E.New("cipher_suites is not supported on naive outbound")
	}
	if len(options.TLS.CurvePreferences) > 0 {
		return nil, E.New("curve_preferences is not supported on naive outbound")
	}
	if len(options.TLS.ClientCertificate) > 0 || options.TLS.ClientCertificatePath != "" {
		return nil, E.New("client_certificate is not supported on naive outbound")
	}
	if len(options.TLS.ClientKey) > 0 || options.TLS.ClientKeyPath != "" {
		return nil, E.New("client_key is not supported on naive outbound")
	}
	if options.TLS.Fragment || options.TLS.RecordFragment {
		return nil, E.New("fragment is not supported on naive outbound")
	}
	if options.TLS.KernelTx || options.TLS.KernelRx {
		return nil, E.New("kernel TLS is not supported on naive outbound")
	}
	if options.TLS.UTLS != nil && options.TLS.UTLS.Enabled {
		return nil, E.New("uTLS is not supported on naive outbound")
	}
	if options.TLS.Reality != nil && options.TLS.Reality.Enabled {
		return nil, E.New("reality is not supported on naive outbound")
	}

	serverAddress := options.ServerOptions.Build()

	var serverName string
	if options.TLS.ServerName != "" {
		serverName = options.TLS.ServerName
	} else {
		serverName = serverAddress.AddrString()
	}

	outboundDialer, err := dialer.NewWithOptions(dialer.Options{
		Context:          ctx,
		Options:          options.DialerOptions,
		RemoteIsDomain:   true,
		ResolverOnDetour: true,
		NewDialer:        true,
	})
	if err != nil {
		return nil, err
	}

	var trustedRootCertificates string
	if len(options.TLS.Certificate) > 0 {
		trustedRootCertificates = strings.Join(options.TLS.Certificate, "\n")
	} else if options.TLS.CertificatePath != "" {
		content, err := filemanager.ReadFile(ctx, options.TLS.CertificatePath)
		if err != nil {
			return nil, E.Cause(err, "read certificate")
		}
		trustedRootCertificates = string(content)
	}

	extraHeaders := make(map[string]string)
	for key, values := range options.ExtraHeaders.Build() {
		if len(values) > 0 {
			extraHeaders[key] = values[0]
		}
	}
	// Announce that this client can handle a response held back until the
	// destination dial has settled. Servers that do not know the extension
	// ignore the header and answer immediately, as they always did.
	extraHeaders[headerSmart] = "1"

	dnsRouter := service.FromContext[adapter.DNSRouter](ctx)
	var dnsResolver cronet.DNSResolverFunc
	if dnsRouter != nil {
		dnsResolver = func(dnsContext context.Context, request *mDNS.Msg) *mDNS.Msg {
			response, err := dnsRouter.Exchange(dnsContext, request, outboundDialer.(dialer.ResolveDialer).QueryOptions())
			if err != nil {
				logger.Error("DNS exchange failed: ", err)
				return dns.FixedResponseStatus(request, mDNS.RcodeServerFailure)
			}
			return response
		}
	}

	var echEnabled bool
	var echConfigList []byte
	var echQueryServerName string
	if options.TLS.ECH != nil && options.TLS.ECH.Enabled {
		echEnabled = true
		echQueryServerName = options.TLS.ECH.QueryServerName
		var echConfig []byte
		if len(options.TLS.ECH.Config) > 0 {
			echConfig = []byte(strings.Join(options.TLS.ECH.Config, "\n"))
		} else if options.TLS.ECH.ConfigPath != "" {
			content, err := filemanager.ReadFile(ctx, options.TLS.ECH.ConfigPath)
			if err != nil {
				return nil, E.Cause(err, "read ECH config")
			}
			echConfig = content
		}
		if len(echConfig) > 0 {
			block, rest := pem.Decode(echConfig)
			if block == nil || block.Type != "ECH CONFIGS" || len(rest) > 0 {
				return nil, E.New("invalid ECH configs pem")
			}
			echConfigList = block.Bytes
		}
	}
	var quicCongestionControl cronet.QUICCongestionControl
	switch options.QUICCongestionControl {
	case "":
		quicCongestionControl = cronet.QUICCongestionControlDefault
	case "bbr":
		quicCongestionControl = cronet.QUICCongestionControlBBR
	case "bbr2":
		quicCongestionControl = cronet.QUICCongestionControlBBRv2
	case "cubic":
		quicCongestionControl = cronet.QUICCongestionControlCubic
	case "reno":
		quicCongestionControl = cronet.QUICCongestionControlReno
	default:
		return nil, E.New("unknown quic congestion control: ", options.QUICCongestionControl)
	}
	client, err := cronet.NewNaiveClient(cronet.NaiveClientOptions{
		Context:                  ctx,
		Logger:                   logger,
		ServerAddress:            serverAddress,
		ServerName:               serverName,
		Username:                 options.Username,
		Password:                 options.Password,
		InsecureConcurrency:      options.InsecureConcurrency,
		ExtraHeaders:             extraHeaders,
		ReceiveWindow:            options.ReceiveWindow.Value(),
		TrustedRootCertificates:  trustedRootCertificates,
		Dialer:                   outboundDialer,
		DNSResolver:              dnsResolver,
		ECHEnabled:               echEnabled,
		ECHConfigList:            echConfigList,
		ECHQueryServerName:       echQueryServerName,
		QUIC:                     options.QUIC,
		QUICCongestionControl:    quicCongestionControl,
		QUICSessionReceiveWindow: options.QUICSessionReceiveWindow.Value(),
	})
	if err != nil {
		return nil, err
	}
	var uotClient *uot.Client
	uotOptions := common.PtrValueOrDefault(options.UDPOverTCP)
	if uotOptions.Enabled {
		uotClient = &uot.Client{
			Dialer:  &naiveDialer{client},
			Version: uotOptions.Version,
		}
	}
	var networks []string
	if uotClient != nil {
		networks = []string{N.NetworkTCP, N.NetworkUDP}
	} else {
		networks = []string{N.NetworkTCP}
	}
	concurrency := options.InsecureConcurrency
	if concurrency < 1 {
		concurrency = 1
	}
	return &Outbound{
		Adapter:     outbound.NewAdapterWithDialerOptions(C.TypeNaive, tag, networks, options.DialerOptions),
		ctx:         ctx,
		logger:      logger,
		client:      client,
		uotClient:   uotClient,
		concurrency: concurrency,
	}, nil
}

func (h *Outbound) Start(stage adapter.StartStage) error {
	if stage != adapter.StartStateStart {
		return nil
	}
	err := h.client.Start()
	if err != nil {
		return err
	}
	h.logger.Info("NaiveProxy started, version: ", h.client.Engine().Version())
	return nil
}

func (h *Outbound) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	switch N.NetworkName(network) {
	case N.NetworkTCP:
		h.logger.InfoContext(ctx, "outbound connection to ", destination)
		conn, err := dialEarly(ctx, h.client, destination)
		if err != nil {
			return nil, err
		}
		logMeasurement(h.ctx, h.logger, conn, destination)
		return conn, nil
	case N.NetworkUDP:
		if h.uotClient == nil {
			return nil, E.New("UDP is not supported unless UDP over TCP is enabled")
		}
		h.logger.InfoContext(ctx, "outbound UoT packet connection to ", destination)
		return h.uotClient.DialContext(ctx, network, destination)
	default:
		return nil, E.Extend(N.ErrUnknownNetwork, network)
	}
}

func (h *Outbound) ListenPacket(ctx context.Context, destination M.Socksaddr) (net.PacketConn, error) {
	if h.uotClient == nil {
		return nil, E.New("UDP is not supported unless UDP over TCP is enabled")
	}
	return h.uotClient.ListenPacket(ctx, destination)
}

func (h *Outbound) InterfaceUpdated(ctx context.Context) {
	h.client.CloseAllConnections()
}

func (h *Outbound) Close() error {
	return h.client.Close()
}

func (h *Outbound) Client() *cronet.NaiveClient {
	return h.client
}

type naiveDialer struct {
	*cronet.NaiveClient
}

func (d *naiveDialer) DialContext(ctx context.Context, network string, destination M.Socksaddr) (net.Conn, error) {
	return dialEarly(ctx, d.NaiveClient, destination)
}

// dialEarly opens a tunnel and hands back a connection that can report the
// smart-extension measurements. A failure here means this client never got an
// answer out of the proxy at all, which is a problem with the hop rather than
// with the destination — the distinction decides whether a client drops one
// destination or the whole node.
func dialEarly(ctx context.Context, client *cronet.NaiveClient, destination M.Socksaddr) (net.Conn, error) {
	conn, err := client.DialEarly(ctx, destination)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrNextHopUnreachable, err)
	}
	return &dialConn{NaiveConn: conn}, nil
}

// logMeasurement reports the path split for one connection once the proxy has
// answered. It runs detached because the answer arrives after DialEarly returns
// — waiting for it inline would hold back the payload the caller is about to
// write.
func logMeasurement(ctx context.Context, logger logger.ContextLogger, conn net.Conn, destination M.Socksaddr) {
	measured, isMeasured := conn.(MeasuredConn)
	if !isMeasured {
		return
	}
	go func() {
		measurement, err := measured.Measure(ctx)
		if err != nil {
			if errors.Is(err, ErrClosedLocally) || errors.Is(err, context.Canceled) {
				// Nobody is waiting for this reading any more, by the two routes
				// that produce that: the tunnel was closed from this side, or
				// the caller stopped waiting. Neither says anything about the
				// path, and this line reaches an operator alongside real
				// failures — where it has already been read as one. A heartbeat
				// closing its own probe leaves this goroutine holding
				// ErrClosedLocally, so during an outage the trail filled with
				// "closed by this end" beside the verdict that was reached on a
				// deadline, and an audit concluded the verdict had been
				// swallowed. Same line the ranking draws: see
				// naive.ErrClosedLocally.
				return
			}
			logger.DebugContext(ctx, "measure connection to ", destination, ": ", err)
			return
		}
		// The three legs, not just the two that used to be here. Working out
		// where a relay's time went meant subtracting span and remote from the
		// round trip by hand, which is how an interior that had grown to three
		// seconds went unnoticed while the line said local=132ms.
		chain, hasChain := measurement.ChainSpan()
		logger.DebugContext(ctx, "measured ", destination,
			" setup_us=", measurement.Setup.Microseconds(),
			" rtt_us=", measurement.RoundTrip.Microseconds(),
			" span_us=", measurement.ServerSpan.Microseconds(),
			" remote_us=", measurement.RemoteDial.Microseconds(),
			" near_us=", measurement.NearHop().Microseconds(),
			" chain_us=", chain.Microseconds(),
			" has_chain=", hasChain,
			" has_remote=", measurement.HasRemote)
	}()
}

// dialConn exposes the smart-extension accessors on top of a naive connection:
// the innermost dial duration the proxy reported, and the locally measured
// setup/round-trip split. Together they let a caller separate the client-to-proxy
// leg from the proxy-to-destination leg, which is the whole point of the
// extension.
type dialConn struct {
	cronet.NaiveConn
}

// localClose re-labels the transport's "this end closed it" as this package's,
// so that the code which has to recognise it can do so without importing the
// transport — which is behind a build tag that code is not. Everything else is
// passed through untouched, including the classification the callers below then
// put on it: a local close still has to reach a relay as a hop-level failure,
// because an error that arrives unclassified is answered with the status that
// downstream reads as a licence to replay the payload.
func localClose(err error) error {
	if err != nil && errors.Is(err, cronet.ErrClosedLocally) {
		return ErrClosedLocally
	}
	return err
}

func (c *dialConn) Measure(ctx context.Context) (ConnMeasurement, error) {
	err := localClose(c.HandshakeContext(ctx))
	if err != nil {
		return ConnMeasurement{}, classifyHandshakeError(err)
	}
	var measurement ConnMeasurement
	if timing, hasTiming := c.Timing(); hasTiming {
		measurement.Setup = timing.Setup
		measurement.RoundTrip = timing.RoundTrip
	}
	value, _ := c.ResponseHeader(ConnectAckHeader)
	ack, hasAck := ParseConnectAck(value)
	if !hasAck {
		// A plain naiveproxy server, or one built before the extension. It
		// tunnels fine, but nothing here can be split into legs, and the caller
		// must not read the absence as a measurement.
		return measurement, nil
	}
	measurement.ServerSpan = ack.Total
	measurement.HasSpan = true
	// What reaching the destination cost, the connect and the lookup together.
	// Why they are summed rather than one subtracted, and the one comparison
	// where overstating it still pays, are on ConnectAck.DestinationLeg.
	measurement.RemoteDial, measurement.HasRemote = ack.DestinationLeg()
	if measurement.HasRemote {
		measurement.Resolve, measurement.HasResolve = ack.Resolve, ack.HasResolve
	}
	return measurement.Validated(), nil
}

func (c *dialConn) RemoteAck(ctx context.Context) (ConnectAck, bool, error) {
	if err := localClose(c.HandshakeContext(ctx)); err != nil {
		return ConnectAck{}, false, classifyHandshakeError(err)
	}
	value, _ := c.ResponseHeader(ConnectAckHeader)
	ack, hasAck := ParseConnectAck(value)
	return ack, hasAck, nil
}

// classifyHandshakeError decides whom to blame for a refused CONNECT. Only a
// StatusBadGateway from the next hop means the destination itself was
// unreachable; anything else — including no answer at all — means the hop is
// the part that is broken.
func classifyHandshakeError(err error) error {
	var handshakeError *cronet.HandshakeError
	if !errors.As(err, &handshakeError) {
		// No status came back at all: a deadline, a dropped route, a session
		// that died. Carries no ErrProxyAnswered, which is what lets a probe
		// tell this from the proxy refusing something — see the sentinel.
		return fmt.Errorf("%w: %w", ErrNextHopUnreachable, err)
	}
	if handshakeError.StatusCode == http.StatusBadGateway {
		return fmt.Errorf("%w: %w: %w", ErrDestinationUnreachable, ErrProxyAnswered, err)
	}
	return fmt.Errorf("%w: %w: %w", ErrNextHopUnreachable, ErrProxyAnswered, err)
}

func (c *dialConn) WaitReady(ctx context.Context) error {
	return localClose(c.NaiveConn.WaitReady(ctx))
}

func (c *dialConn) Upstream() any           { return c.NaiveConn }
func (c *dialConn) ReaderReplaceable() bool { return true }
func (c *dialConn) WriterReplaceable() bool { return true }
