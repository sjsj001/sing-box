//go:build go1.27

package naive

import (
	"net/http"
	"testing"
)

// Tripwire for the go1.27 scheduler flip: the first CI run on a go1.27
// toolchain compiles this file, and with it the claim that the pin still
// exists and still lands on the field x/net reads. Failing to compile is the
// alarm working, not a test to delete.
func TestKeepRoundRobinWriteScheduler(t *testing.T) {
	var srv http.Server
	keepRoundRobinWriteScheduler(&srv)
	if !srv.DisableClientPriority {
		t.Fatal("DisableClientPriority not pinned; x/net on go1.27+ would default to the RFC 9218 scheduler")
	}
}
