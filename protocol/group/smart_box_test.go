package group_test

import (
	"context"
	"fmt"
	"testing"

	box "github.com/sagernet/sing-box"
	"github.com/sagernet/sing-box/include"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/json"
	"github.com/sagernet/sing/service"

	"github.com/stretchr/testify/require"
)

// newSmartBox builds a box the way `sing-box check` and `run` do: one parsed
// configuration, one box context per instance over a shared global one.
func newSmartBox(t *testing.T, global context.Context, mainTrail string, usTrail string) error {
	options, err := json.UnmarshalExtendedContext[option.Options](global, []byte(fmt.Sprintf(`{
		"log": {"disabled": true},
		"outbounds": [
			{"type": "direct", "tag": "hk"},
			{"type": "direct", "tag": "us"},
			{"type": "smart", "tag": "out-main", "audit_path": %q, "groups": [{"tag": "hk", "outbounds": ["hk"]}]},
			{"type": "smart", "tag": "out-us", "audit_path": %q, "groups": [{"tag": "us", "outbounds": ["us"]}]}
		]
	}`, mainTrail, usTrail)))
	require.NoError(t, err)
	instance, err := box.New(box.Options{Context: service.ExtendContext(global), Options: options})
	if err == nil {
		t.Cleanup(func() { instance.Close() })
	}
	return err
}

func TestASharedAuditFileIsRefusedByCheck(t *testing.T) {
	t.Parallel()
	// Refused only at start, a shared trail passed `sing-box check`, and a
	// reload — check, close the running instance, start the new one — ended
	// with the process exiting.
	global := include.Context(context.Background())
	dir := t.TempDir()
	require.ErrorContains(t, newSmartBox(t, global, dir+"/a.jsonl", dir+"/a.jsonl"), "share audit_path")
	require.ErrorContains(t, newSmartBox(t, global, dir+"/a.jsonl", dir+"/sub/../a.jsonl"), "share audit_path",
		"two spellings of one file are one file")

	// A reload checks the new configuration while the running instance, with
	// the very same trails, is still alive: each box answers for its own.
	require.NoError(t, newSmartBox(t, global, dir+"/main.jsonl", dir+"/us.jsonl"))
	require.NoError(t, newSmartBox(t, global, dir+"/main.jsonl", dir+"/us.jsonl"))
}
