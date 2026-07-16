#!/usr/bin/env bash
# Smart-group R2 scenarios (.scratch/smart-group-r2/spec.md §4), run ON the
# test host:
#   S13  hedge in-flight rescue: the sticky member dials fine but the
#        connection silently hangs (non-SYN packets dropped) → the standby
#        rescues the SAME connection, zero client-visible failures; two
#        hedge losses cool the primary per-target and switch the current
#   S14  exploration freshness: after samples outlive sample_ttl, sticky
#        decisions issue refresh-alt probes for the alternative, keeping the
#        improvement channel supplied — a degraded current is then replaced
#        without ever failing
# Uses two direct members with distinct routing_marks, like run4.sh.
set -uo pipefail

REPO="${REPO:-/root/ref1nd-sing-box-1}"
WORK="${WORK:-/root/smart-test}"
GO="${GO:-/usr/local/go/bin/go}"
PROXY="127.0.0.1:2084"
LOG="$WORK/box5.log"

MARK_A=21
MARK_B=22

BOX_PID=""
declare -a IPT_RULES=()
NETEM_DEV=""

cleanup() {
  echo "--- cleanup ---"
  [ -n "$BOX_PID" ] && kill "$BOX_PID" 2>/dev/null
  for rule in "${IPT_RULES[@]}"; do
    iptables -D $rule 2>/dev/null
  done
  IPT_RULES=()
  [ -n "$NETEM_DEV" ] && tc qdisc del dev "$NETEM_DEV" root 2>/dev/null
}
trap cleanup EXIT INT TERM

PASS=0
FAIL=0
check() {
  local name="$1" cond="$2"
  if [ "$cond" = "1" ]; then echo "PASS: $name"; PASS=$((PASS+1)); else echo "FAIL: $name"; FAIL=$((FAIL+1)); fi
}
pget() { curl -sS --max-time 10 -o /dev/null -w '%{http_code}' -x "http://$PROXY" "$1" 2>/dev/null; }
mark_of() { [ "$1" = "A" ] && echo "$MARK_A" || echo "$MARK_B"; }
other_of() { [ "$1" = "A" ] && echo "B" || echo "A"; }
current_for() { grep "smart decide key=$1" "$LOG" | grep -oE 'via=[A-Za-z0-9_-]+' | tail -1 | cut -d= -f2; }

# Drop everything except the SYN for one member+net: the TCP handshake still
# completes (dial succeeds) but the first payload byte never arrives — the
# silent post-dial hang the hedge exists for.
block_established() {
  local mark="$1" net="$2"
  local rule="OUTPUT -m mark --mark $mark -d $net -p tcp ! --syn -j DROP"
  iptables -I $rule && IPT_RULES+=("$rule")
}
unblock_all() {
  for rule in "${IPT_RULES[@]}"; do iptables -D $rule 2>/dev/null; done
  IPT_RULES=()
}
site_nets() {
  python3 -c "
import socket
nets = set()
for info in socket.getaddrinfo('$1', 443, socket.AF_INET):
    nets.add('.'.join(info[4][0].split('.')[:2]) + '.0.0/16')
print(' '.join(sorted(nets)))
"
}

mkdir -p "$WORK"
rm -f "$LOG" "$WORK/smart-cache-r2.json"
echo "--- build ---"
cd "$REPO" || exit 1
CRONET_SO=$(find /root/go/pkg/mod/github.com/sagernet/cronet-go/lib -maxdepth 2 -path "*linux_amd64@*" -name libcronet.so | sort | tail -1)
[ -n "$CRONET_SO" ] && install -m 644 "$CRONET_SO" "$WORK/libcronet.so"
CGO_ENABLED=0 "$GO" build -tags with_naive_outbound,with_purego -o "$WORK/sing-box" ./cmd/sing-box || exit 1

"$WORK/sing-box" run -c "$REPO/test/smart/config-r2.json" &
BOX_PID=$!
sleep 3
kill -0 "$BOX_PID" 2>/dev/null || { echo "FAIL: instance did not start"; tail -20 "$LOG"; exit 1; }

VICTIM=www.cloudflare.com
CONTROL=www.wikipedia.org
S14_SITE=www.bing.com

echo "=== S13 hedge in-flight rescue (silent post-dial hang) ==="
for i in 1 2 3; do pget "https://$VICTIM" >/dev/null; pget "https://$CONTROL" >/dev/null; sleep 1; done
CUR=$(current_for "$VICTIM")
[ -n "$CUR" ] || CUR=A
OTHER=$(other_of "$CUR")
echo "  current for $VICTIM: $CUR (blocking mark $(mark_of "$CUR"))"
for net in $(site_nets "$VICTIM"); do
  block_established "$(mark_of "$CUR")" "$net"
  echo "  blocked established $net for member $CUR"
done
S13_FAILS=0
FIRST_TOOK=0
LAST_TOOK=0
for i in $(seq 1 6); do
  start=$(date +%s%3N)
  code=$(pget "https://$VICTIM")
  took=$(( $(date +%s%3N) - start ))
  [ "$i" = "1" ] && FIRST_TOOK=$took
  LAST_TOOK=$took
  echo "  req$i code=$code took=${took}ms"
  [ "$code" = "200" ] || S13_FAILS=$((S13_FAILS+1))
  sleep 1
done
CTRL_CODE=$(pget "https://$CONTROL")
[ "$S13_FAILS" = "0" ]; check "S13 zero failed requests (hedge rescues in-flight)" $((1-$?))
grep -q "smart hedge key=$VICTIM rescued by $OTHER from $CUR" "$LOG"; check "S13 rescue logged ($OTHER rescued $CUR)" $((1-$?))
# Old behavior: the hung connection died after the fixed 3s window and the
# client saw the failure. The rescue must land well inside that.
[ "$FIRST_TOOK" -lt 3000 ]; check "S13 first request rescued inside the old 3s window (${FIRST_TOOK}ms)" $((1-$?))
grep -q "target $VICTIM: member $CUR cooling down" "$LOG"; check "S13 hedge losses cooled the primary" $((1-$?))
grep -q "target $VICTIM: switch $CUR -> $OTHER (failover)" "$LOG"; check "S13 current switched to the standby" $((1-$?))
[ "$LAST_TOOK" -lt 1500 ]; check "S13 later requests direct via $OTHER (${LAST_TOOK}ms)" $((1-$?))
grep -q "target $CONTROL: member" "$LOG" && CTRL_TOUCHED=1 || CTRL_TOUCHED=0
[ "$CTRL_TOUCHED" = "0" ] && [ "$CTRL_CODE" = "200" ]; check "S13 control site unaffected" $((1-$?))
unblock_all

echo "=== S14 exploration freshness (refresh-alt + improve after staleness) ==="
for i in 1 2 3; do pget "https://$S14_SITE" >/dev/null; sleep 1; done
CUR2=$(current_for "$S14_SITE")
[ -n "$CUR2" ] || CUR2=A
OTHER2=$(other_of "$CUR2")
echo "  current for $S14_SITE: $CUR2"
echo "  waiting out sample_ttl (25s)..."
sleep 25
pget "https://$S14_SITE" >/dev/null
sleep 4
grep "key=$S14_SITE" "$LOG" | grep -q "reason=refresh-alt"; check "S14 stale alternative probed (refresh-alt)" $((1-$?))
# Degrade the current member by ~70% of its observed total: enough to clear
# the improvement margin, not enough to read as a 2x spike.
TOTAL=$(grep -oE "smart probe key=$S14_SITE via=$CUR2 reason=[a-z-]+ total=[0-9]+ms" "$LOG" | grep -oE '[0-9]+ms' | tail -1 | tr -d 'ms')
[ -n "$TOTAL" ] || TOTAL=120
DELAY=$(( TOTAL * 7 / 10 ))
[ "$DELAY" -lt 40 ] && DELAY=40
[ "$DELAY" -gt 400 ] && DELAY=400
echo "  observed total=${TOTAL}ms via $CUR2 -> netem +${DELAY}ms on mark $(mark_of "$CUR2")"
NETEM_DEV=$(ip route | awk '/default/ {print $5; exit}')
tc qdisc add dev "$NETEM_DEV" root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1
tc filter add dev "$NETEM_DEV" parent 1: protocol ip handle "$(mark_of "$CUR2")" fw flowid 1:4
tc qdisc add dev "$NETEM_DEV" parent 1:4 handle 40: netem delay "${DELAY}ms"
SWITCHED=0
for i in $(seq 1 50); do
  pget "https://$S14_SITE" >/dev/null
  if grep -q "target $S14_SITE: switch $CUR2 -> $OTHER2" "$LOG"; then SWITCHED=1; break; fi
  sleep 2
done
check "S14 degraded current replaced by the fresh alternative" "$SWITCHED"
SWITCH_LINE=$(grep "target $S14_SITE: switch $CUR2 -> $OTHER2" "$LOG" | tail -1)
echo "  switch: ${SWITCH_LINE:-none}"
grep "key=$S14_SITE" "$LOG" | grep -q "reason=confirm"; check "S14 switch was probe-confirmed" $((1-$?))
tc qdisc del dev "$NETEM_DEV" root 2>/dev/null
NETEM_DEV=""

kill "$BOX_PID" 2>/dev/null; wait "$BOX_PID" 2>/dev/null; BOX_PID=""
echo "=== learned state (smart-cache dump) ==="
"$WORK/sing-box" smart-cache "$WORK/smart-cache-r2.json" 2>/dev/null | head -30

echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
exit $((FAIL > 0 ? 1 : 0))
