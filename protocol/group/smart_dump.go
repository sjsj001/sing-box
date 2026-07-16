package group

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"text/tabwriter"
	"time"
)

// FormatSmartCache renders a smart-group cache snapshot for humans: one block
// per learned target (current member, why, ages, switches, affinity) with
// per-member measurement lines. filter, when non-empty, keeps only targets
// whose key contains it. jsonOut re-emits the snapshot as indented JSON.
func FormatSmartCache(path string, filter string, jsonOut bool) (string, error) {
	snapshot, err := smartLoadSnapshot(path)
	if err != nil {
		return "", err
	}
	if jsonOut {
		data, err := json.MarshalIndent(snapshot, "", "  ")
		if err != nil {
			return "", err
		}
		return string(data), nil
	}

	now := time.Now()
	var output strings.Builder
	fmt.Fprintf(&output, "snapshot: %s (saved %s ago)\n", path, smartFormatAge(now.Sub(snapshot.SavedAt)))

	// Member baselines: the local→member floor and the protocol leg factor
	// used to derive remote RTT estimates.
	baselineMin := make(map[string]float64)
	var memberTags []string
	for tag := range snapshot.Baselines {
		memberTags = append(memberTags, tag)
	}
	sort.Strings(memberTags)
	if len(memberTags) > 0 {
		output.WriteString("\nmembers:\n")
		writer := tabwriter.NewWriter(&output, 2, 8, 2, ' ', 0)
		fmt.Fprintf(writer, "  TAG\tBASELINE\tLEG\tSAMPLES\n")
		for _, tag := range memberTags {
			samples := snapshot.Baselines[tag]
			minMs := 0.0
			for i, sample := range samples {
				if i == 0 || sample < minMs {
					minMs = sample
				}
			}
			baselineMin[tag] = minMs
			leg := snapshot.LegFactors[tag]
			if leg <= 0 {
				leg = 1
			}
			fmt.Fprintf(writer, "  %s\t%.0fms\t%.0f\t%d\n", tag, minMs, leg, len(samples))
		}
		writer.Flush()
	}

	type targetEntry struct {
		key   string
		entry *smartSnapshotTarget
	}
	targets := make([]targetEntry, 0, len(snapshot.Targets))
	for key, entry := range snapshot.Targets {
		if filter != "" && !strings.Contains(key, filter) {
			continue
		}
		targets = append(targets, targetEntry{key, entry})
	}
	sort.Slice(targets, func(i, j int) bool {
		return targets[i].entry.LastUsed.After(targets[j].entry.LastUsed)
	})

	fmt.Fprintf(&output, "\ntargets: %d", len(targets))
	if filter != "" {
		fmt.Fprintf(&output, " (filter %q, %d total)", filter, len(snapshot.Targets))
	}
	output.WriteString("\n")

	for _, target := range targets {
		entry := target.entry
		fmt.Fprintf(&output, "\n%s -> %s (%s)", target.key, valueOrDash(entry.Current), valueOrDash(entry.CurrentWhy))
		if !entry.CurrentAt.IsZero() {
			fmt.Fprintf(&output, " held %s", smartFormatAge(now.Sub(entry.CurrentAt)))
		}
		if !entry.LastUsed.IsZero() {
			fmt.Fprintf(&output, ", used %s ago", smartFormatAge(now.Sub(entry.LastUsed)))
		}
		if entry.Switches > 0 {
			fmt.Fprintf(&output, ", %d switches", entry.Switches)
		}
		if entry.Affinity != "" {
			fmt.Fprintf(&output, ", affinity=%s", entry.Affinity)
		}
		if entry.AggregateKey != "" && entry.AggregateKey != target.key {
			fmt.Fprintf(&output, ", agg=%s", entry.AggregateKey)
		}
		output.WriteString("\n")

		memberRows := make([]string, 0, len(entry.Members))
		for tag := range entry.Members {
			memberRows = append(memberRows, tag)
		}
		// Current member first, then by median total.
		medianOf := func(tag string) float64 {
			window := newSmartWindow(smartSnapshotSampleCap)
			for _, sample := range entry.Members[tag].Samples {
				window.Push(sample)
			}
			median, ok := window.Median()
			if !ok {
				return -1
			}
			return median
		}
		sort.Slice(memberRows, func(i, j int) bool {
			if (memberRows[i] == entry.Current) != (memberRows[j] == entry.Current) {
				return memberRows[i] == entry.Current
			}
			mi, mj := medianOf(memberRows[i]), medianOf(memberRows[j])
			if (mi < 0) != (mj < 0) {
				return mj < 0
			}
			if mi != mj {
				return mi < mj
			}
			return memberRows[i] < memberRows[j]
		})
		writer := tabwriter.NewWriter(&output, 2, 8, 2, ' ', 0)
		fmt.Fprintf(writer, "  MEMBER\tTOTAL(med)\tREMOTE(est)\tSAMPLES\tTTFB(med/n)\tLAST SAMPLE\n")
		for _, tag := range memberRows {
			stats := entry.Members[tag]
			marker := " "
			if tag == entry.Current {
				marker = "*"
			}
			median := medianOf(tag)
			totalCell, remoteCell := "-", "-"
			if median >= 0 {
				totalCell = fmt.Sprintf("%.0fms", median)
				leg := snapshot.LegFactors[tag]
				if leg <= 0 {
					leg = 1
				}
				remote := (median - baselineMin[tag]) / leg
				if remote < 0 {
					remote = 0
				}
				remoteCell = fmt.Sprintf("%.0fms", remote)
			}
			ttfbCell := "-"
			if stats.TTFBCount > 0 {
				ttfbCell = fmt.Sprintf("%.0fms/%d", stats.TTFBMedian, stats.TTFBCount)
			}
			lastCell := "-"
			if !stats.LastSample.IsZero() {
				lastCell = smartFormatAge(now.Sub(stats.LastSample)) + " ago"
			}
			fmt.Fprintf(writer, " %s%s\t%s\t%s\t%d\t%s\t%s\n",
				marker, tag, totalCell, remoteCell, len(stats.Samples), ttfbCell, lastCell)
		}
		writer.Flush()
	}
	return output.String(), nil
}

func valueOrDash(value string) string {
	if value == "" {
		return "-"
	}
	return value
}

func smartFormatAge(age time.Duration) string {
	if age < 0 {
		age = 0
	}
	switch {
	case age < time.Minute:
		return fmt.Sprintf("%ds", int(age.Seconds()))
	case age < time.Hour:
		return fmt.Sprintf("%dm%ds", int(age.Minutes()), int(age.Seconds())%60)
	case age < 24*time.Hour:
		return fmt.Sprintf("%dh%dm", int(age.Hours()), int(age.Minutes())%60)
	default:
		return fmt.Sprintf("%dd%dh", int(age.Hours())/24, int(age.Hours())%24)
	}
}
