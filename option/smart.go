package option

import "github.com/sagernet/sing/common/json/badoption"

// SmartOutboundOptions configures the "smart" per-destination selector group.
// All fields are optional with defaults; the minimal config only needs the
// embedded GroupCommonOption (outbounds/providers). See the design doc
// (tingly-riding-parrot-final.md) for the meaning of each knob.
type SmartOutboundOptions struct {
	GroupCommonOption

	// Probing.
	ProbeInterval badoption.Duration `json:"probe_interval,omitempty"` // active-target refresh base, default 45s
	PingInterval  badoption.Duration `json:"ping_interval,omitempty"`  // local keepalive, default 5s
	IdleTimeout   badoption.Duration `json:"idle_timeout,omitempty"`   // stop refreshing idle targets, default 30m

	// Decision (all real-millisecond quantities).
	SwitchMargin uint16             `json:"switch_margin,omitempty"` // takeover margin percent, default 15
	MinDwell     badoption.Duration `json:"min_dwell,omitempty"`     // sustained-lead window before switching, default 30s
	Tolerance    uint16             `json:"tolerance,omitempty"`     // req.2 "close total" band ms + margin_ms floor, default 20
	CDNThreshold uint16             `json:"cdn_threshold,omitempty"` // CDN enter threshold ms, default 25 (exit = +10)
	CDNBand      uint16             `json:"cdn_band,omitempty"`      // CDN in-band width ms, default 15
	CDNQuorum    int                `json:"cdn_quorum,omitempty"`    // nodes below threshold to call CDN, default 2

	// Failure detection.
	QueueDelayThreshold     uint16             `json:"queue_delay_threshold,omitempty"`     // live-witness absolute inflation ms, default 200
	CongestSuppressMax      uint16             `json:"congest_suppress_max,omitempty"`      // suppressed rounds before handshake verdict, default 3
	HandshakeVerdictTimeout badoption.Duration `json:"handshake_verdict_timeout,omitempty"` // generous new-TLS verdict timeout, default 8s

	// Nodes / regions.
	Nodes   []SmartNodeOptions   `json:"nodes,omitempty"`
	Regions []SmartRegionOptions `json:"regions,omitempty"`

	InterruptExistConnections bool `json:"interrupt_exist_connections,omitempty"` // hard-error localDown interrupts that node's live conns
}

// SmartNodeOptions carries per-node tuning: clean_priority (ordinal, smaller =
// cleaner, default 100 = no preference) and bias_ms (additive offset, clamped to
// ±100ms at startup).
type SmartNodeOptions struct {
	Tag           string `json:"tag"`
	CleanPriority uint16 `json:"clean_priority,omitempty"`
	BiasMs        int    `json:"bias_ms,omitempty"`
}

// SmartRegionOptions groups nodes by region. Mode "prefer" uses backups only
// when the primary is down; "equivalent" treats all members as interchangeable.
type SmartRegionOptions struct {
	Name    string   `json:"name"`
	Members []string `json:"members"`
	Mode    string   `json:"mode,omitempty"` // prefer | equivalent
	Primary string   `json:"primary,omitempty"`
}
