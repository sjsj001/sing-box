package group

import (
	"encoding/json"
	"os"
	"path/filepath"
	"time"
)

const smartSnapshotVersion = 1

// smartSnapshotSampleCap bounds persisted samples per (target, member); the
// full in-memory windows are summarized, not dumped.
const smartSnapshotSampleCap = 8

type smartSnapshotStats struct {
	Samples    []float64 `json:"samples,omitempty"`
	LastSample time.Time `json:"last_sample,omitempty"`
	TTFBMedian float64   `json:"ttfb_median,omitempty"`
	TTFBCount  int       `json:"ttfb_count,omitempty"`
}

type smartSnapshotTarget struct {
	AggregateKey string                         `json:"aggregate_key,omitempty"`
	ProbeHost    string                         `json:"probe_host,omitempty"`
	Port         uint16                         `json:"port,omitempty"`
	Current      string                         `json:"current,omitempty"`
	CurrentWhy   string                         `json:"current_why,omitempty"`
	CurrentAt    time.Time                      `json:"current_at,omitempty"`
	LastUsed     time.Time                      `json:"last_used,omitempty"`
	Switches     int                            `json:"switches,omitempty"`
	Affinity     string                         `json:"affinity,omitempty"`
	Members      map[string]*smartSnapshotStats `json:"members,omitempty"`
}

type smartSnapshot struct {
	Version    int                             `json:"version"`
	SavedAt    time.Time                       `json:"saved_at"`
	Baselines  map[string][]float64            `json:"baselines,omitempty"`
	LegFactors map[string]float64              `json:"leg_factors,omitempty"`
	Targets    map[string]*smartSnapshotTarget `json:"targets,omitempty"`
}

func smartSnapshotFromTable(table *smartTable, members []*smartMember) *smartSnapshot {
	snapshot := &smartSnapshot{
		Version:    smartSnapshotVersion,
		SavedAt:    time.Now(),
		Baselines:  make(map[string][]float64),
		LegFactors: make(map[string]float64),
		Targets:    make(map[string]*smartSnapshotTarget),
	}
	for _, member := range members {
		member.mu.Lock()
		snapshot.Baselines[member.tag] = member.baseline.Values()
		snapshot.LegFactors[member.tag] = member.legFactor
		member.mu.Unlock()
	}
	for _, target := range table.all() {
		target.mu.Lock()
		entry := &smartSnapshotTarget{
			ProbeHost:  target.probeHost,
			Port:       target.port,
			Current:    target.current,
			CurrentWhy: target.currentWhy,
			CurrentAt:  target.currentAt,
			LastUsed:   target.lastUsed,
			Switches:   target.switches,
			Affinity:   target.affinity,
			Members:    make(map[string]*smartSnapshotStats),
		}
		if target.aggregate != nil {
			entry.AggregateKey = target.aggregate.key
		}
		for tag, stats := range target.members {
			values := stats.window.Values()
			if len(values) > smartSnapshotSampleCap {
				values = values[len(values)-smartSnapshotSampleCap:]
			}
			saved := &smartSnapshotStats{
				Samples:    values,
				LastSample: stats.lastSample,
				TTFBCount:  stats.ttfb.Count(),
			}
			if median, ok := stats.ttfb.Median(); ok {
				saved.TTFBMedian = median
			}
			entry.Members[tag] = saved
		}
		snapshot.Targets[target.key] = entry
		target.mu.Unlock()
	}
	return snapshot
}

// apply rebuilds the table from a snapshot: aggregate entries are linked by
// key, unknown member tags are dropped, entries past the record TTL are
// skipped. TTFB windows are re-seeded from their persisted median so
// affinity conclusions survive but must re-earn full sample counts.
func (s *smartSnapshot) apply(table *smartTable, knownTags map[string]bool, recordTTL time.Duration, now time.Time) {
	for key, entry := range s.Targets {
		if !entry.LastUsed.IsZero() && now.Sub(entry.LastUsed) > recordTTL {
			continue
		}
		target := &smartTarget{
			key:        key,
			probeHost:  entry.ProbeHost,
			port:       entry.Port,
			currentAt:  entry.CurrentAt,
			currentWhy: entry.CurrentWhy,
			lastUsed:   entry.LastUsed,
			switches:   entry.Switches,
			members:    make(map[string]*smartTargetStats),
		}
		if knownTags[entry.Current] {
			target.current = entry.Current
		}
		if knownTags[entry.Affinity] {
			target.affinity = entry.Affinity
		}
		for tag, saved := range entry.Members {
			if !knownTags[tag] {
				continue
			}
			stats := newSmartTargetStats()
			for _, sample := range saved.Samples {
				stats.window.Push(sample)
			}
			stats.lastSample = saved.LastSample
			if saved.TTFBMedian > 0 && saved.TTFBCount > 0 {
				seed := saved.TTFBCount
				if seed > smartSnapshotSampleCap {
					seed = smartSnapshotSampleCap
				}
				for i := 0; i < seed; i++ {
					stats.ttfb.Push(saved.TTFBMedian)
				}
			}
			target.members[tag] = stats
		}
		table.put(target)
	}
	// Second pass: wire aggregate links.
	for key, entry := range s.Targets {
		if entry.AggregateKey == "" {
			continue
		}
		exact := table.get(key)
		agg := table.get(entry.AggregateKey)
		if exact != nil && agg != nil {
			exact.mu.Lock()
			exact.aggregate = agg
			exact.mu.Unlock()
		}
	}
}

func smartSaveSnapshot(path string, snapshot *smartSnapshot) error {
	data, err := json.Marshal(snapshot)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	// 0600: the cache is effectively a history of visited hostnames.
	tmpPath := path + ".tmp"
	file, err := os.OpenFile(tmpPath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	if _, err = file.Write(data); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}
	if err = file.Sync(); err != nil {
		file.Close()
		os.Remove(tmpPath)
		return err
	}
	if err = file.Close(); err != nil {
		os.Remove(tmpPath)
		return err
	}
	return os.Rename(tmpPath, path)
}

func smartLoadSnapshot(path string) (*smartSnapshot, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var snapshot smartSnapshot
	if err := json.Unmarshal(data, &snapshot); err != nil {
		return nil, err
	}
	if snapshot.Version != smartSnapshotVersion {
		return nil, os.ErrInvalid
	}
	return &snapshot, nil
}
