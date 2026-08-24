package option

import (
	"github.com/sagernet/sing/common/byteformats"
	"github.com/sagernet/sing/common/json/badoption"
)

type SelectorOutboundOptions struct {
	GroupCommonOption
	Default                   string `json:"default,omitempty" reference:"outbound"`
	InterruptExistConnections bool   `json:"interrupt_exist_connections,omitempty"`
}

type URLTestOutboundOptions struct {
	GroupCommonOption
	URL                       string                 `json:"url,omitempty"`
	Interval                  badoption.Duration     `json:"interval,omitempty"`
	Tolerance                 uint16                 `json:"tolerance,omitempty"`
	IdleTimeout               badoption.Duration     `json:"idle_timeout,omitempty"`
	InterruptExistConnections bool                   `json:"interrupt_exist_connections,omitempty"`
	Fallback                  URLTestFallbackOptions `json:"fallback,omitempty"`
}

type SmartOutboundOptions struct {
	// Alpha discounts the client-to-proxy leg when ranking, so that a shorter
	// proxy-to-destination leg wins when totals are close. 1.0 ranks purely by
	// total latency; the default is 0.7.
	//
	// A pointer because 0 is a meaningful setting — rank on the destination leg
	// alone — and indistinguishable from "not set" otherwise.
	Alpha *float64 `json:"alpha,omitempty"`
	// AuditPath turns on a durable record of every routing decision and the
	// measurements behind it, written as one JSON object per line. It is off
	// unless set, because it is the only place destination names reach the disk
	// in the clear — the snapshot cache stores digests.
	AuditPath string `json:"audit_path,omitempty"`
	// AuditSize is how large the trail grows before it is rotated, keeping one
	// previous generation. Default 64 MiB.
	AuditSize *byteformats.MemoryBytes `json:"audit_size,omitempty"`
	Groups    []SmartGroupOptions      `json:"groups"`
}

type SmartGroupOptions struct {
	Tag string `json:"tag"`
	// Outbounds must all be naive: the ranking depends on the destination dial
	// duration that only naive reports.
	Outbounds []string `json:"outbounds"`
	// Bonus credits a group with latency the user is willing to spend to use
	// it, for the one property latency cannot measure — whether its exit
	// address is a good one to be seen from. It decides the outcome for CDN and
	// anycast destinations, where every group's destination leg is a millisecond
	// or two, and is drowned out elsewhere.
	Bonus badoption.Duration `json:"bonus,omitempty"`
	// Select picks the member to use within the group: "first" follows the
	// configured order and only moves on failure, "fastest" follows the measured
	// client-to-proxy latency. Exactly one member is used either way, so the
	// same destination always leaves from the same address.
	Select string `json:"select,omitempty"`
}

type GroupCommonOption struct {
	Outbounds       []string          `json:"outbounds" reference:"outbound"`
	Providers       []string          `json:"providers" reference:"provider"`
	Exclude         *badoption.Regexp `json:"exclude,omitempty"`
	Include         *badoption.Regexp `json:"include,omitempty"`
	UseAllProviders bool              `json:"use_all_providers,omitempty"`
}

type URLTestFallbackOptions struct {
	Enabled  bool               `json:"enabled,omitempty"`
	MaxDelay badoption.Duration `json:"max_delay,omitempty"`
}

type LoadBalanceOutboundOptions struct {
	GroupCommonOption
	URL         string             `json:"url,omitempty"`
	Interval    badoption.Duration `json:"interval,omitempty"`
	IdleTimeout badoption.Duration `json:"idle_timeout,omitempty"`
	TTL         badoption.Duration `json:"ttl,omitempty"`
	Strategy    string             `json:"strategy,omitempty"`
}
