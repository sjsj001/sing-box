#!/usr/bin/env bash
# Smart-group integration harness. Intended to run ON 10.10.10.4 after the
# repo is rsynced there. Builds sing-box + the test forwarder, starts both,
# then runs the verification scenarios. All firewall/tc mutations are undone
# by the trap on exit.
set -uo pipefail

REPO="${REPO:-/root/ref1nd-sing-box-1}"
WORK="${WORK:-/root/smart-test}"
PROXY="127.0.0.1:2080"
LOG="$WORK/box.log"
GO="${GO:-/usr/local/go/bin/go}"

mkdir -p "$WORK"
rm -f "$WORK/smart-cache.json" "$LOG"

BOX_PID=""
FWD_PID=""
declare -a IPTABLES_RULES=()
NETEM_DEV=""

cleanup() {
  echo "--- cleanup ---"
  [ -n "$BOX_PID" ] && kill "$BOX_PID" 2>/dev/null
  [ -n "$FWD_PID" ] && kill "$FWD_PID" 2>/dev/null
  for rule in "${IPTABLES_RULES[@]}"; do
    iptables -D $rule 2>/dev/null
  done
  [ -n "$NETEM_DEV" ] && tc qdisc del dev "$NETEM_DEV" root 2>/dev/null
}
trap cleanup EXIT INT TERM

block_ip() {
  local ip="$1"
  local rule="OUTPUT -d $ip -j DROP"
  iptables -I $rule
  IPTABLES_RULES+=("$rule")
}

unblock_all() {
  for rule in "${IPTABLES_RULES[@]}"; do
    iptables -D $rule 2>/dev/null
  done
  IPTABLES_RULES=()
}

if [ ! -f "$REPO/test/smart/config.json" ]; then
  echo "missing test/smart/config.json — copy config.json.example and fill in node hosts/credentials (config.json is git-ignored)"
  exit 1
fi

echo "--- build ---"
cd "$REPO" || exit 1
# with_purego: the prebuilt static libcronet.a is incompatible with this host's
# linkers (gold/lld link it but the bundled allocator crashes in malloc at start);
# dynamic loading needs libcronet.so from the cronet-go lib module next to the binary.
CRONET_SO=$(find /root/go/pkg/mod/github.com/sagernet/cronet-go/lib -maxdepth 2 -path "*linux_amd64@*" -name libcronet.so | sort | tail -1)
[ -n "$CRONET_SO" ] && install -m 644 "$CRONET_SO" "$WORK/libcronet.so"
CGO_ENABLED=0 "$GO" build -tags with_naive_outbound,with_purego -o "$WORK/sing-box" ./cmd/sing-box || exit 1
# test/ is a separate Go module; build the forwarder from inside it.
(cd "$REPO/test" && "$GO" build -o "$WORK/forwarder" ./smart/forwarder) || exit 1

# Node hosts live only in the git-ignored config.json; derive them so the
# committed harness stays free of private infrastructure names.
outbound_field() {
  python3 - "$REPO/test/smart/config.json" "$1" "$2" <<'PY'
import json, sys
config = json.load(open(sys.argv[1]))
outbound = {o["tag"]: o for o in config["outbounds"]}[sys.argv[2]]
print(outbound[sys.argv[3]])
PY
}
HK_MAIN_HOST=$(outbound_field hk-main server)
HK_MAIN_PORT=$(outbound_field hk-main server_port)
JP_HOST=$(outbound_field jp-main server)

echo "--- start forwarder (hk primary) ---"
"$WORK/forwarder" -listen 127.0.0.1:8443 -target "$HK_MAIN_HOST:$HK_MAIN_PORT" &
FWD_PID=$!
sleep 1

echo "--- start sing-box ---"
"$WORK/sing-box" run -c "$REPO/test/smart/config.json" &
BOX_PID=$!
sleep 3

if ! kill -0 "$BOX_PID" 2>/dev/null; then
  echo "FAIL: sing-box did not start"
  tail -30 "$LOG"
  exit 1
fi

pget() { curl -sS --max-time 20 -o /dev/null -w '%{http_code}' -x "http://$PROXY" "$1"; }
decision_for() { grep "key=$1" "$LOG" | grep -oE 'via=[A-Za-z0-9_-]+' | tail -1; }

PASS=0
FAIL=0
check() {
  local name="$1" cond="$2"
  if [ "$cond" = "1" ]; then echo "PASS: $name"; PASS=$((PASS+1)); else echo "FAIL: $name"; FAIL=$((FAIL+1)); fi
}

echo "=== S1 cold-start race ==="
pget https://www.cloudflare.com >/dev/null
pget https://www.apple.com >/dev/null
sleep 2
grep -q "smart race" "$LOG"; check "S1 race logged" $((1-$?))

echo "=== S2/S3 regional + anycast convergence ==="
for i in $(seq 1 8); do
  pget https://www.google.com >/dev/null
  pget https://www.cloudflare.com >/dev/null
  pget https://www.apple.com >/dev/null
  sleep 1
done
# Preferred-challenge needs 2 confirm probes (>=15s apart, prober rate limit)
# plus the 20s dwell configured in config.json before it may switch.
for i in $(seq 1 12); do
  pget https://www.google.com >/dev/null
  sleep 3
done
sleep 2
GVIA=$(decision_for www.google.com)
echo "google -> $GVIA"
[ "$GVIA" = "via=JP" ]; check "S3 anycast google -> preferred JP" $((1-$?))

echo "=== S6 member failure (block JP node IP) ==="
JP_IP=$(getent hosts "$JP_HOST" | awk '{print $1}' | head -1)
if [ -n "$JP_IP" ]; then
  block_ip "$JP_IP"
  # Silent drop: requests through JP hang until the 3s zero-byte window
  # fires; two early failures cool the member. Overlapping requests measure
  # the wall-clock until the first success through the next-best member.
  START=$(date +%s%3N)
  S6_DIR=$(mktemp -d)
  S6_PIDS=()
  for i in $(seq 1 8); do
    (
      c=$(curl -sS --max-time 8 -o /dev/null -w '%{http_code}' -x "http://$PROXY" https://www.google.com 2>/dev/null)
      [ "$c" = "200" ] && date +%s%3N > "$S6_DIR/ok.$i"
    ) &
    S6_PIDS+=($!)
    sleep 1
  done
  # Wait only for the probe curls — a bare `wait` would also wait on the
  # sing-box and forwarder background jobs and hang forever.
  wait "${S6_PIDS[@]}"
  END_OK=$(cat "$S6_DIR"/ok.* 2>/dev/null | sort -n | head -1)
  rm -rf "$S6_DIR"
  if [ -n "$END_OK" ]; then
    ELAPSED=$((END_OK-START))
    echo "first success after block: ${ELAPSED}ms"
    [ "$ELAPSED" -lt 8000 ]; check "S6 failover to next-best (<8s)" $((1-$?))
  else
    echo "no request succeeded after block"
    check "S6 failover to next-best (<8s)" 0
  fi
  unblock_all
fi

echo "=== S7 link degradation (netem on eth0) ==="
NETEM_DEV=$(ip route | awk '/default/ {print $5; exit}')
if [ -n "$NETEM_DEV" ]; then
  tc qdisc add dev "$NETEM_DEV" root netem delay 200ms loss 10% || echo "netem add failed"
  sleep 3
  CODE=$(pget https://www.google.com)
  tc qdisc del dev "$NETEM_DEV" root 2>/dev/null
  NETEM_DEV=""
  echo "under netem: code=$CODE"
  [ "$CODE" = "200" ]; check "S7 survived degradation" $((1-$?))
fi

echo "=== S5 intra-region backup (kill hk forwarder) ==="
for i in $(seq 1 3); do pget https://ipinfo.io >/dev/null; done
kill "$FWD_PID" 2>/dev/null; FWD_PID=""
sleep 1
# Sacrificial request: the failed stream surfaces the dead forwarder to the
# HK urltest (5s interval), which promotes hk-main. Spec S5 wants the
# urltest to absorb the failover with smart staying on HK (无感、数据延续).
pget https://ipinfo.io >/dev/null 2>&1 || true
sleep 8
CODE=$(pget https://ipinfo.io)
IVIA=$(decision_for ipinfo.io)
echo "after kill: code=$CODE decision=$IVIA"
[ "$CODE" = "200" ] && [ "$IVIA" = "via=HK" ]; check "S5 hk backup took over, smart unaware" $((1-$?))

echo "=== S8 stability soak (short) ==="
SW_BEFORE=$(grep -c "switch" "$LOG")
for i in $(seq 1 20); do
  pget https://www.google.com >/dev/null
  pget https://www.apple.com >/dev/null
done
SW_AFTER=$(grep -c "switch" "$LOG")
echo "switch lines: before=$SW_BEFORE after=$SW_AFTER"
[ $((SW_AFTER-SW_BEFORE)) -le 2 ]; check "S8 no churn under steady state" $((1-$?))

echo "=== S9 persistence ==="
kill "$BOX_PID" 2>/dev/null; wait "$BOX_PID" 2>/dev/null; BOX_PID=""
[ -s "$WORK/smart-cache.json" ]; check "S9 cache written" $((1-$?))

echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
exit $((FAIL > 0 ? 1 : 0))
