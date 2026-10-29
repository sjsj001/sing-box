package route

import (
	"context"
	"testing"

	"github.com/sagernet/sing-box/adapter"
	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	"github.com/sagernet/sing/common/logger"
	N "github.com/sagernet/sing/common/network"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

func TestRoutingPreservesSmartDestinationSelection(t *testing.T) {
	member := &testFlowOutbound{tag: "node", outboundType: C.TypeNaive}
	manager := &testL3OutboundManager{outbounds: map[string]adapter.Outbound{"node": member}}
	ctx := service.ContextWith[adapter.OutboundManager](context.Background(), manager)
	smart, err := group.NewSmart(ctx, nil, logger.NOP(), "smart", option.SmartOutboundOptions{
		Groups: []option.SmartGroupOptions{{Tag: "region", Outbounds: []string{"node"}}},
	})
	require.NoError(t, err)
	scope := adapter.NewScope(ctx, logger.NOP())
	require.NoError(t, smart.(adapter.Lifecycle).Start(adapter.StartStateStart, scope))
	t.Cleanup(func() { require.NoError(t, scope.Close()) })
	outer := &testOutboundGroup{Outbound: &testFlowOutbound{tag: "selector"}, selected: smart}

	for _, network := range []string{N.NetworkTCP, N.NetworkUDP} {
		t.Run(network, func(t *testing.T) {
			metadata := adapter.InboundContext{Network: network}
			require.Same(t, member, smart.(adapter.OutboundGroup).Selected(network), "the dashboard still shows a member")
			for _, outbound := range []adapter.Outbound{smart, outer} {
				chain, err := resolveOutbound(outbound, network, &metadata)
				require.NoError(t, err)
				require.Same(t, smart, chain[len(chain)-1], "routing must call smart's race, replay and recovery paths")
				chain, action := new(Router).selectPreMatchOutbound(&metadata, outbound, 0)
				require.Empty(t, chain, "TUN forwarding must not bypass smart through its displayed member")
				require.Equal(t, adapter.PreMatchContinue, action)
			}
		})
	}
}
