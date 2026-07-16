package option

import "github.com/sagernet/sing/common/json/badoption"

type FailoverOutboundOptions struct {
	Outbounds                 []string             `json:"outbounds"`
	Strategy                  string               `json:"strategy,omitempty"` // order (default) | auto
	HealthCheck               string               `json:"health_check,omitempty"`
	Cooldown                  SmartCooldownOptions `json:"cooldown,omitempty"`
	HedgeDelay                badoption.Duration   `json:"hedge_delay,omitempty"`
	InterruptExistConnections bool                 `json:"interrupt_exist_connections,omitempty"`
}
