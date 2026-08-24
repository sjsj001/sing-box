package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/sagernet/sing-box/log"

	"github.com/spf13/cobra"
)

var (
	flagSmartNodesList bool
	flagSmartNodesTop  int
)

var commandSmart = &cobra.Command{
	Use:   "smart",
	Short: "Read the smart outbound's audit trail",
}

var commandSmartNodes = &cobra.Command{
	Use:   "nodes <audit trail path>",
	Short: "Show what each node carried and which destinations it holds",
	Args:  cobra.ExactArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		if err := smartNodes(args[0]); err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	commandSmartNodes.Flags().BoolVarP(&flagSmartNodesList, "list", "l", false, "list each node's destinations and what they carried")
	commandSmartNodes.Flags().IntVar(&flagSmartNodesTop, "top", 0, "with --list, show only this many destinations per node, busiest first")
	commandSmart.AddCommand(commandSmartNodes)
	mainCommand.AddCommand(commandSmart)
}

// auditLine is the part of a trail record this command reads. The trail is
// JSON per line and self-describing, so anything it does not recognise is
// skipped rather than treated as an error — a newer sing-box may write records
// this build has never heard of.
type auditLine struct {
	Time        time.Time `json:"time"`
	Type        string    `json:"type"`
	Destination string    `json:"destination"`
	Key         string    `json:"key"`
	To          string    `json:"to"`
	Groups      []struct {
		Tag     string           `json:"tag"`
		Member  string           `json:"member"`
		Local   string           `json:"local"`
		TCP     int64            `json:"tcp"`
		UDP     int64            `json:"udp"`
		Reasons map[string]int64 `json:"reasons"`
	} `json:"groups"`
	Destinations []struct {
		Key         string `json:"key"`
		Destination string `json:"destination"`
		TCP         int64  `json:"tcp"`
		UDP         int64  `json:"udp"`
	} `json:"destinations"`
}

// destination is one target held by a node, and whether the trail was able to
// say what it is called. An unnamed one is reported under its digest rather than
// dropped: it still counts, and the digest still joins it to the other records
// about it.
type destination struct {
	name     string
	named    bool
	requests int64
}

type nodeUsage struct {
	node         string
	local        string
	tcp          int64
	udp          int64
	reasons      map[string]int64
	destinations []destination
}

func (n *nodeUsage) requests() int64 { return n.tcp + n.udp }

// smartNodes answers two questions from one file: how much each node carried,
// and which destinations are currently placed on it.
//
// The two come from different records and mean different things. Requests are
// summed over every usage window in the trail, which is why the outbound writes
// windows rather than running totals — a total cannot be added across a restart.
// Destinations come from the most recent full dump, so they are a snapshot of
// where things stand rather than of everything ever seen.
//
// Names are collected from the whole file rather than taken from the dump alone.
// The outbound can only name what it has dialed since it started, so a dump
// taken after a restart reports most of a restored cache under its digest — but
// the round that put each of those in the cache named it, and that record is
// still here.
func smartNodes(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()

	usage := make(map[string]*nodeUsage)
	node := func(group string, member string) *nodeUsage {
		name := group
		switch {
		case group == "":
			// A destination no group is holding: every group failed it, or it
			// has not been decided. It belongs in the table rather than in a
			// blank row.
			name = "(none)"
		case member != "":
			name = group + "/" + member
		}
		if existing, found := usage[name]; found {
			return existing
		}
		fresh := &nodeUsage{node: name, reasons: make(map[string]int64)}
		usage[name] = fresh
		return fresh
	}

	var (
		latestState time.Time
		states      []auditLine
		windows     int
		first, last time.Time
	)
	names := make(map[string]string)
	counts := make(map[string]int64)
	// Whether this trail counts per destination at all. Written by newer builds
	// only, and a column of zeroes reads as "nothing went there" rather than as
	// "this file cannot say".
	counted := false

	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 0, 64*1024), 8<<20)
	for scanner.Scan() {
		var record auditLine
		if json.Unmarshal(scanner.Bytes(), &record) != nil {
			continue
		}
		if record.Key != "" && record.Destination != "" {
			names[record.Key] = record.Destination
		}
		switch record.Type {
		case "usage":
			windows++
			if first.IsZero() {
				first = record.Time
			}
			last = record.Time
			for _, held := range record.Destinations {
				counted = true
				if held.Destination != "" {
					names[held.Key] = held.Destination
				}
				counts[held.Key] += held.TCP + held.UDP
			}
			for _, leg := range record.Groups {
				entry := node(leg.Tag, leg.Member)
				entry.tcp += leg.TCP
				entry.udp += leg.UDP
				if leg.Local != "" {
					entry.local = leg.Local
				}
				for reason, count := range leg.Reasons {
					entry.reasons[reason] += count
				}
			}
		case "state":
			// Only the newest dump describes where things stand now; earlier
			// ones describe where they stood.
			if record.Time.After(latestState) {
				latestState, states = record.Time, nil
			}
			if record.Time.Equal(latestState) {
				states = append(states, record)
			}
		}
	}
	if err = scanner.Err(); err != nil {
		return err
	}

	for _, record := range states {
		member := ""
		for _, leg := range record.Groups {
			if leg.Tag == record.To {
				member = leg.Member
				break
			}
		}
		held := destination{
			name:     record.Destination,
			named:    record.Destination != "",
			requests: counts[record.Key],
		}
		if !held.named {
			held.name, held.named = names[record.Key], names[record.Key] != ""
		}
		if !held.named {
			// Whatever named this key has been rotated away with the round that
			// decided it, so the digest is all there is left.
			held.name = record.Key
		}
		entry := node(record.To, member)
		entry.destinations = append(entry.destinations, held)
	}

	if len(usage) == 0 {
		return fmt.Errorf("%s: no usage or state records — is audit_path set, and has the outbound run long enough to write one?", path)
	}

	nodes := make([]*nodeUsage, 0, len(usage))
	var totalRequests int64
	var totalDestinations int
	for _, entry := range usage {
		nodes = append(nodes, entry)
		totalRequests += entry.requests()
		totalDestinations += len(entry.destinations)
	}
	sort.Slice(nodes, func(i, j int) bool {
		if nodes[i].requests() != nodes[j].requests() {
			return nodes[i].requests() > nodes[j].requests()
		}
		return len(nodes[i].destinations) > len(nodes[j].destinations)
	})

	fmt.Println(path)
	if windows > 0 {
		fmt.Printf("requests over %d windows, %s to %s\n",
			windows, first.Format(time.DateTime), last.Format(time.DateTime))
	} else {
		fmt.Println("no usage records yet; destination counts only")
	}
	if !latestState.IsZero() {
		fmt.Printf("destinations as of %s\n", latestState.Format(time.DateTime))
	}
	fmt.Println()

	width := len("node")
	for _, entry := range nodes {
		width = max(width, len(entry.node))
	}
	fmt.Printf("%-*s  %9s  %8s  %8s  %6s  %12s  %s\n",
		width, "node", "requests", "tcp", "udp", "share", "destinations", "local")
	for _, entry := range nodes {
		share := 0.0
		if totalRequests > 0 {
			share = 100 * float64(entry.requests()) / float64(totalRequests)
		}
		fmt.Printf("%-*s  %9d  %8d  %8d  %5.1f%%  %12d  %s\n",
			width, entry.node, entry.requests(), entry.tcp, entry.udp,
			share, len(entry.destinations), entry.local)
	}
	fmt.Printf("%-*s  %9d  %8s  %8s  %6s  %12d\n",
		width, "total", totalRequests, "", "", "", totalDestinations)

	explained := false
	for _, entry := range nodes {
		if len(entry.reasons) == 0 {
			continue
		}
		reasons := make([]string, 0, len(entry.reasons))
		for reason, count := range entry.reasons {
			reasons = append(reasons, fmt.Sprintf("%s=%d", reason, count))
		}
		sort.Strings(reasons)
		fmt.Printf("\n%s chosen by: %s", entry.node, strings.Join(reasons, " "))
		explained = true
	}
	if explained {
		fmt.Println()
	}

	if !flagSmartNodesList {
		return nil
	}
	for _, entry := range nodes {
		if len(entry.destinations) == 0 {
			continue
		}
		// Busiest first, then the ones that can be read at all, so --top spends
		// its budget on the lines worth having.
		sort.Slice(entry.destinations, func(i, j int) bool {
			left, right := entry.destinations[i], entry.destinations[j]
			if left.requests != right.requests {
				return left.requests > right.requests
			}
			if left.named != right.named {
				return left.named
			}
			return left.name < right.name
		})
		var listed int64
		for _, held := range entry.destinations {
			listed += held.requests
		}
		shown := entry.destinations
		if flagSmartNodesTop > 0 && len(shown) > flagSmartNodesTop {
			shown = shown[:flagSmartNodesTop]
		}
		fmt.Printf("\n%s (%d)\n", entry.node, len(entry.destinations))
		for _, held := range shown {
			if !counted {
				fmt.Println("  " + held.name)
				continue
			}
			fmt.Printf("  %9d  %s\n", held.requests, held.name)
		}
		if len(shown) < len(entry.destinations) {
			fmt.Printf("  ... %d more\n", len(entry.destinations)-len(shown))
		}
		if rest := entry.requests() - listed; counted && rest > 0 {
			// The node carried these, but the destination that took them is not
			// in the table any more — dropped since the dump, or never given an
			// entry to count against. Saying so beats a list that quietly does
			// not add up to the row above it.
			fmt.Printf("  %9d  (on destinations no longer held)\n", rest)
		}
	}
	return nil
}
