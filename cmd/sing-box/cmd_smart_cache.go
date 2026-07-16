package main

import (
	"os"

	C "github.com/sagernet/sing-box/constant"
	"github.com/sagernet/sing-box/log"
	"github.com/sagernet/sing-box/option"
	"github.com/sagernet/sing-box/protocol/group"
	E "github.com/sagernet/sing/common/exceptions"

	"github.com/spf13/cobra"
)

var (
	commandSmartCacheFlagFilter string
	commandSmartCacheFlagJSON   bool
)

var commandSmartCache = &cobra.Command{
	Use:   "smart-cache [cache-file]",
	Short: "Show learned smart-group decisions from a cache snapshot",
	Long: "Show, per learned destination, the smart group's current outbound choice\n" +
		"and its measurement data, read from the cache_path snapshot (written every\n" +
		"5 minutes and on shutdown; atomically replaced, so it is safe to read while\n" +
		"sing-box is running).\n\n" +
		"Without an argument the cache path is taken from the first smart outbound\n" +
		"with cache_path in the configuration (-c/-C).",
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		err := smartCache(args)
		if err != nil {
			log.Fatal(err)
		}
	},
}

func init() {
	commandSmartCache.Flags().StringVarP(&commandSmartCacheFlagFilter, "filter", "f", "", "only show targets whose key contains this substring")
	commandSmartCache.Flags().BoolVar(&commandSmartCacheFlagJSON, "json", false, "output the raw snapshot as indented JSON")
	mainCommand.AddCommand(commandSmartCache)
}

func smartCache(args []string) error {
	var path string
	if len(args) == 1 {
		path = args[0]
	} else {
		options, err := readConfigAndMerge()
		if err != nil {
			return err
		}
		for _, outbound := range options.Outbounds {
			if outbound.Type != C.TypeSmart {
				continue
			}
			smartOptions, isSmart := outbound.Options.(*option.SmartOutboundOptions)
			if !isSmart || smartOptions.CachePath == "" {
				continue
			}
			path = smartOptions.CachePath
			break
		}
		if path == "" {
			return E.New("no smart outbound with cache_path found in configuration")
		}
	}
	output, err := group.FormatSmartCache(path, commandSmartCacheFlagFilter, commandSmartCacheFlagJSON)
	if err != nil {
		return err
	}
	_, err = os.Stdout.WriteString(output)
	return err
}
