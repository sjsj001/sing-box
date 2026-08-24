//go:build !go1.27

package naive

import "net/http"

// Before go1.27 x/net's h2 write scheduler is round-robin unconditionally, and
// the knob this would set does not exist yet. The go1.27 twin of this file is
// where the pinning happens.
func keepRoundRobinWriteScheduler(_ *http.Server) {}
