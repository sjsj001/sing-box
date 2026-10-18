package option

import (
	"testing"
	"time"

	"github.com/sagernet/sing/common/json"

	"github.com/stretchr/testify/require"
)

func TestNaiveInboundSendQueueTargetIsOffUnlessSet(t *testing.T) {
	for content, want := range map[string]time.Duration{
		`{}`:                           0,
		`{"send_queue_target":"40ms"}`: 40 * time.Millisecond,
	} {
		var options NaiveInboundOptions
		require.NoError(t, json.Unmarshal([]byte(content), &options), content)
		require.Equal(t, want, options.SendQueueTarget.Build(), content)
	}
}
