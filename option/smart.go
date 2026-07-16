package option

import "github.com/sagernet/sing/common/json/badoption"

type SmartOutboundOptions struct {
	Outbounds                 []string             `json:"outbounds"`
	Preferred                 []string             `json:"preferred,omitempty"`
	HealthCheck               string               `json:"health_check,omitempty"`
	AnycastThreshold          badoption.Duration   `json:"anycast_threshold,omitempty"`
	Tolerance                 badoption.Duration   `json:"tolerance,omitempty"`
	Switch                    SmartSwitchOptions   `json:"switch,omitempty"`
	Cooldown                  SmartCooldownOptions `json:"cooldown,omitempty"`
	Probe                     SmartProbeOptions    `json:"probe,omitempty"`
	ExploreInterval           *int                 `json:"explore_interval,omitempty"`
	MaxTargets                int                  `json:"max_targets,omitempty"`
	RecordTTL                 badoption.Duration   `json:"record_ttl,omitempty"`
	CachePath                 string               `json:"cache_path,omitempty"`
	InterruptExistConnections bool                 `json:"interrupt_exist_connections,omitempty"`
}

type SmartSwitchOptions struct {
	ImproveRatio  float64            `json:"improve_ratio,omitempty"`
	ImproveMin    badoption.Duration `json:"improve_min,omitempty"`
	Dwell         badoption.Duration `json:"dwell,omitempty"`
	Confirmations int                `json:"confirmations,omitempty"`
}

type SmartCooldownOptions struct {
	FailLimit int                `json:"fail_limit,omitempty"`
	Base      badoption.Duration `json:"base,omitempty"`
	Max       badoption.Duration `json:"max,omitempty"`
}

type SmartProbeOptions struct {
	Concurrency int                `json:"concurrency,omitempty"`
	SampleTTL   badoption.Duration `json:"sample_ttl,omitempty"`
	Timeout     badoption.Duration `json:"timeout,omitempty"`
}
