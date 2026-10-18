//go:build unix

package group

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/sagernet/sing/common/logger"

	"github.com/stretchr/testify/require"
)

// A state dump is a single batch of a line per destination, up to the cache
// size. Checked per batch, the close budget let that one batch run past it on
// a slow disk: six seconds of a three-second budget.
func TestTheCloseBudgetHoldsWithinABatch(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), "trail.jsonl")
	require.NoError(t, syscall.Mkfifo(path, 0o600))
	audit := newSmartAudit(context.Background(), logger.NOP(), path, auditDefaultSize)
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		// The disk: 4KiB every 20ms, and gone when the test ends.
		reader, err := os.OpenFile(path, os.O_RDONLY, 0)
		if err != nil {
			return
		}
		defer reader.Close()
		buffer := make([]byte, 4096)
		for {
			select {
			case <-stop:
				return
			case <-time.After(20 * time.Millisecond):
			}
			if _, err := reader.Read(buffer); err != nil {
				return
			}
		}
	}()
	line := []byte(`{"type":"state","key":"` + strings.Repeat("x", 200) + "\"}\n")
	lines := make([][]byte, 20000)
	for i := range lines {
		lines[i] = line
	}
	audit.enqueue(lines...)
	started := time.Now()
	require.NoError(t, audit.Close())
	require.Less(t, time.Since(started), auditCloseBudget+time.Second, "the budget ran out inside a batch and was not kept")
}
