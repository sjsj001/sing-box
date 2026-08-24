package cachefile

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sagernet/bbolt"
	"github.com/sagernet/sing-box/adapter"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing/common/logger"
	"github.com/stretchr/testify/require"
)

func TestStartupRemovesUnknownBucketsUnderACacheID(t *testing.T) {
	t.Parallel()
	// The nested pass deleted by the cache ID bucket's own name rather than by
	// the child it was looking at, so with a cache ID configured it removed
	// nothing at all and unknown buckets accumulated for the life of the file.
	// The root pass, which is what runs without one, always worked — both are
	// checked here so a fix to one cannot quietly undo the other.
	path := filepath.Join(t.TempDir(), "cache.db")
	ctx := context.Background()

	cache := New(ctx, logger.NOP(), option.CacheFileOptions{Path: path, CacheID: "profile"})
	require.NoError(t, cache.Start(adapter.StartStateInitialize))
	require.NoError(t, cache.DB.Update(func(tx *bbolt.Tx) error {
		scoped, err := tx.CreateBucketIfNotExists(cache.cacheID)
		if err != nil {
			return err
		}
		if _, err = scoped.CreateBucketIfNotExists([]byte("left_over")); err != nil {
			return err
		}
		if _, err = scoped.CreateBucketIfNotExists(bucketSelected); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists([]byte("left_over")); err != nil {
			return err
		}
		if _, err = tx.CreateBucketIfNotExists(bucketFakeIP); err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists(bucketSmart)
		return err
	}))
	require.NoError(t, cache.Close())

	restarted := New(ctx, logger.NOP(), option.CacheFileOptions{Path: path, CacheID: "profile"})
	require.NoError(t, restarted.Start(adapter.StartStateInitialize))
	t.Cleanup(func() { restarted.Close() })

	require.NoError(t, restarted.DB.View(func(tx *bbolt.Tx) error {
		scoped := tx.Bucket(restarted.cacheID)
		require.NotNil(t, scoped)
		require.Nil(t, scoped.Bucket([]byte("left_over")), "an unknown bucket under the cache ID has to go")
		require.NotNil(t, scoped.Bucket(bucketSelected), "and a known one has to stay")
		require.Nil(t, tx.Bucket([]byte("left_over")), "the root pass keeps working")
		require.NotNil(t, tx.Bucket(bucketFakeIP), "fake-IP buckets are spared by prefix")
		require.NotNil(t, tx.Bucket(bucketSmart))
		return nil
	}))
}
