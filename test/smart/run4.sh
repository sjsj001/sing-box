#!/usr/bin/env bash
# Failover-group scenarios, run ON the test host:
#   S15  primary suddenly cannot reach ONE site: hedge absorbs (zero failed
#        requests), per-target cooldown isolates, control site unaffected
#   S16  primary member dies entirely: per-target hedging keeps requests
#        green; health checks demote the member (~3min) and recover it
#   S18  strategy=auto elects the lower-baseline member
# Uses two direct members with distinct routing_marks for fault injection.
set -uo pipefail

REPO="${REPO:-/root/ref1nd-sing-box-1}"
WORK="${WORK:-/root/smart-test}"
GO="${GO:-/usr/local/go/bin/go}"
PROXY="127.0.0.1:2082"
LOG="$WORK/box3.log"

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
pget() { curl -sS --max-time 15 -o /dev/null -w '%{http_code}' -x "http://$PROXY" "$1" 2>/dev/null; }
block() {
  local rule="OUTPUT -m mark --mark $1 -d $2 -j DROP"
  iptables -I $rule && IPT_RULES+=("$rule")
}
unblock_all() {
  for rule in "${IPT_RULES[@]}"; do iptables -D $rule 2>/dev/null; done
  IPT_RULES=()
}

mkdir -p "$WORK"
rm -f "$LOG"
if [ ! -x "$WORK/sing-box" ]; then
  echo "--- build ---"
  cd "$REPO" || exit 1
  CRONET_SO=$(find /root/go/pkg/mod/github.com/sagernet/cronet-go/lib -maxdepth 2 -path "*linux_amd64@*" -name libcronet.so | sort | tail -1)
  [ -n "$CRONET_SO" ] && install -m 644 "$CRONET_SO" "$WORK/libcronet.so"
  CGO_ENABLED=0 "$GO" build -tags with_naive_outbound,with_purego -o "$WORK/sing-box" ./cmd/sing-box || exit 1
fi

"$WORK/sing-box" run -c "$REPO/test/smart/config-failover.json" &
BOX_PID=$!
sleep 3
kill -0 "$BOX_PID" 2>/dev/null || { echo "FAIL: instance did not start"; tail -20 "$LOG"; exit 1; }

VICTIM=www.cloudflare.com
CONTROL=www.wikipedia.org

echo "=== S15 primary cannot reach one site (hedge + per-target isolation) ==="
for i in 1 2 3; do pget "https://$VICTIM" >/dev/null; pget "https://$CONTROL" >/dev/null; done
for net in $(python3 -c "
import socket
nets = set()
for info in socket.getaddrinfo('$VICTIM', 443, socket.AF_INET):
    nets.add('.'.join(info[4][0].split('.')[:2]) + '.0.0/16')
print(' '.join(sorted(nets)))
"); do
  block "$MARK_A" "$net"
  echo "  blocked $net for mark $MARK_A (member A)"
done
S15_FAILS=0
FIRST_TOOK=0
LAST_TOOK=0
for i in $(seq 1 6); do
  start=$(date +%s%3N)
  code=$(pget "https://$VICTIM")
  took=$(( $(date +%s%3N) - start ))
  [ "$i" = "1" ] && FIRST_TOOK=$took
  LAST_TOOK=$took
  echo "  req$i code=$code took=${took}ms"
  [ "$code" = "200" ] || S15_FAILS=$((S15_FAILS+1))
  sleep 1
done
CTRL_CODE=$(pget "https://$CONTROL")
[ "$S15_FAILS" = "0" ]; check "S15 zero failed requests (hedge absorbs)" $((1-$?))
# The primary's dial budget (~1s) fails first and wakes the hedge early, so
# the first request lands between the budget and the hedge ceiling.
[ "$FIRST_TOOK" -ge 800 ] && [ "$FIRST_TOOK" -lt 5000 ]; check "S15 first request hedged, bounded (${FIRST_TOOK}ms)" $((1-$?))
[ "$LAST_TOOK" -lt 1200 ]; check "S15 later requests direct via B (${LAST_TOOK}ms)" $((1-$?))
grep -q "failover target $VICTIM: member A cooling down" "$LOG"; check "S15 victim cooled A" $((1-$?))
grep -q "failover target $CONTROL: member A cooling down" "$LOG" && CTRL_COOLED=1 || CTRL_COOLED=0
[ "$CTRL_COOLED" = "0" ] && [ "$CTRL_CODE" = "200" ]; check "S15 control site unaffected" $((1-$?))
unblock_all

echo "=== S16 primary member dies entirely ==="
block "$MARK_A" "0.0.0.0/0"
S16_FAILS=0
for i in 1 2 3; do
  code=$(pget "https://www.bing.com")
  echo "  req$i code=$code"
  [ "$code" = "200" ] || S16_FAILS=$((S16_FAILS+1))
  sleep 1
done
[ "$S16_FAILS" = "0" ]; check "S16 requests survive member death" $((1-$?))
DOWN_OK=0
for i in $(seq 1 30); do
  if grep -q "failover member A is down" "$LOG"; then DOWN_OK=1; break; fi
  sleep 10
done
check "S16 member-level down detected (health checks)" "$DOWN_OK"
unblock_all
UP_OK=0
for i in $(seq 1 24); do
  if grep -q "failover member A is back up" "$LOG"; then UP_OK=1; break; fi
  sleep 10
done
check "S16 member recovered after unblock" "$UP_OK"
CODE=$(pget "https://$VICTIM")
[ "$CODE" = "200" ]; check "S16 traffic green after recovery" $((1-$?))

kill "$BOX_PID" 2>/dev/null; wait "$BOX_PID" 2>/dev/null; BOX_PID=""

echo "=== S18 strategy=auto elects lower-baseline member ==="
# Inflate member A's path by 80ms; auto election must pick B.
NETEM_DEV=$(ip route | awk '/default/ {print $5; exit}')
tc qdisc add dev "$NETEM_DEV" root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1
tc filter add dev "$NETEM_DEV" parent 1: protocol ip handle "$MARK_A" fw flowid 1:4
tc qdisc add dev "$NETEM_DEV" parent 1:4 handle 40: netem delay 80ms
AUTO_CFG=$WORK/config-failover-auto.json
python3 - "$REPO/test/smart/config-failover.json" "$AUTO_CFG" <<'PY'
import json, sys
config = json.load(open(sys.argv[1]))
config["log"]["output"] = "/root/smart-test/box4.log"
config["inbounds"][0]["listen_port"] = 2083
for outbound in config["outbounds"]:
    if outbound["type"] == "failover":
        outbound["strategy"] = "auto"
        outbound.pop("hedge_delay", None)
json.dump(config, open(sys.argv[2], "w"))
PY
rm -f "$WORK/box4.log"
"$WORK/sing-box" run -c "$AUTO_CFG" &
BOX_PID=$!
sleep 3
ELECT_OK=0
for i in $(seq 1 16); do
  if grep -q "failover elected primary: B" "$WORK/box4.log"; then ELECT_OK=1; break; fi
  sleep 10
done
check "S18 auto elected the faster member (B)" "$ELECT_OK"
tc qdisc del dev "$NETEM_DEV" root 2>/dev/null
NETEM_DEV=""

echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
exit $((FAIL > 0 ? 1 : 0))
