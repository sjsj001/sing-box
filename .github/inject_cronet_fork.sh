#!/usr/bin/env bash
# Repoint every github.com/sagernet/cronet-go* module at our sjsj001 fork.
#
# The fork carries API this tree depends on (protocol/naive/outbound.go uses
# NaiveConn.Headers, which upstream does not have), so ANY build that enables
# with_naive_outbound fails to compile against upstream. Every job that builds
# with that tag must run this after checkout and before the first go build:
# build, build_darwin, build_windows, build_windows_client, build_android,
# publish_android, build_apple.
#
# Runs under bash on all three runner families (ubuntu / macos / windows via
# Git bash). Portability notes:
#   - grep -oE, never -oP: macOS runners ship BSD grep, which has no -P.
#   - go.mod separates the module path from its version with a space, so
#     [^[:space:]]* captures the path and stops before the version.
set -euo pipefail

FORK=sjsj001

VERSION=$(go list -m -f '{{.Version}}' "github.com/$FORK/cronet-go@go")
echo "Using cronet-go fork version: $VERSION"

# Match the main module, /all, and every lib/<platform> submodule, then map
# each to the same path under the fork at the same pseudo-version.
grep -oE 'github\.com/sagernet/cronet-go[^[:space:]]*' go.mod | sort -u | while read -r MOD; do
    echo "replace $MOD => github.com/$FORK/${MOD#github.com/sagernet/} $VERSION" >>go.mod
done

go mod tidy

echo "--- injected replaces ---"
grep -c "github.com/$FORK/cronet-go" go.mod
