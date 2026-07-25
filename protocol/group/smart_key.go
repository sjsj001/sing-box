package group

import (
	"github.com/sagernet/sing-box/adapter"
)

// destKey buckets a request by host. Keeping it a named struct rather than a
// bare string is deliberate: pickNode/onDialError take it alongside a `network`
// string, and a distinct type stops the two from being swapped silently.
//
// The bucket is per-HOST on purpose (tingly-riding-parrot-final.md §0.1-0.2) —
// do NOT switch it to loadbalance's getKey (eTLD+1), or a multi-region provider
// under one domain (every leaseweb location under leaseweb.net) collapses onto a
// single node and loses per-location optimality, which is a validated
// requirement of this group.
type destKey struct {
	host string
}

func keyOf(metadata *adapter.InboundContext) destKey {
	if metadata == nil {
		return destKey{}
	}
	if metadata.Destination.IsDomain() {
		return destKey{host: metadata.Destination.Fqdn}
	}
	if metadata.SniffHost != "" {
		return destKey{host: metadata.SniffHost}
	}
	if metadata.Domain != "" {
		return destKey{host: metadata.Domain}
	}
	addr := metadata.Destination.Addr
	if len(metadata.DestinationAddresses) > 0 {
		addr = metadata.DestinationAddresses[0]
	}
	if addr.IsValid() {
		return destKey{host: addr.String()}
	}
	return destKey{}
}
