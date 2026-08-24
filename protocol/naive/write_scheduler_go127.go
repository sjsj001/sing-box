//go:build go1.27

package naive

import "net/http"

// From go1.27 x/net's h2 server defaults to the RFC 9218 priority scheduler
// (go.dev/issue/75500). For clients that never send a priority header —
// cronet's CONNECT streams among them — that scheduler still degrades to
// round-robin, but the round-robin every stream on this proxy depends on then
// hangs on the client staying priority-silent. A cronet rebase could change
// that without anyone noticing, and a bulk stream would be written to
// completion while probes queue behind it. Pin the behavior instead of the
// luck. The field lives on net/http's Server — the h2c path fishes that server
// out of the request context — so this is the right place despite the h2
// subject. If this file stops compiling, Go moved the field: re-check what the
// x/net default became before renaming anything.
func keepRoundRobinWriteScheduler(srv *http.Server) {
	srv.DisableClientPriority = true
}
