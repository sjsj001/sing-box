package naive

import (
	stdlog "log"
	"testing"

	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing/common/logger"

	"github.com/stretchr/testify/require"
)

type levelLogger struct {
	logger.Logger
	errors, debugs []string
}

func (l *levelLogger) Error(args ...any) { l.errors = append(l.errors, args[0].(string)) }
func (l *levelLogger) Debug(args ...any) { l.debugs = append(l.debugs, args[0].(string)) }

// What a scanner leaves goes to Debug; what an operator needs to see stays an
// error, as it was on stderr.
func TestTheHTTPServerLogKeepsItsErrorsErrors(t *testing.T) {
	recorder := &levelLogger{Logger: log.NewNOPFactory().Logger()}
	errorLog := stdlog.New(debugWriter{recorder}, "", 0)
	errorLog.Print("http: TLS handshake error from 192.0.2.1:4242: EOF")
	errorLog.Print("http: panic serving 192.0.2.1:4242: boom")
	errorLog.Print("http2: panic serving 192.0.2.1:4242: boom")
	errorLog.Print("http: Accept error: too many open files; retrying in 5ms")
	require.Equal(t, []string{"http: TLS handshake error from 192.0.2.1:4242: EOF"}, recorder.debugs)
	require.Len(t, recorder.errors, 3)
}
