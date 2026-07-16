#!/usr/bin/env bash
# Smart-group extended scenarios, run ON the test host:
#   S12  external-link breadth: a batch of real sites through the node config
#   S11  one member suddenly cannot reach ONE site (per-target isolation)
#   S10  one member's network suddenly degrades (spike demotion)
# S11/S10 use two local direct members with distinct routing_marks so
# iptables/tc can inject faults per member without touching real nodes.
set -uo pipefail

REPO="${REPO:-/root/ref1nd-sing-box-1}"
WORK="${WORK:-/root/smart-test}"
GO="${GO:-/usr/local/go/bin/go}"
NODE_PROXY="127.0.0.1:2080"
LOCAL_PROXY="127.0.0.1:2081"
NODE_LOG="$WORK/box.log"
LOCAL_LOG="$WORK/box2.log"

MARK_L1=17
MARK_L2=18

BOX_PID=""
FWD_PID=""
declare -a IPT_RULES=()
NETEM_DEV=""

cleanup() {
  echo "--- cleanup ---"
  [ -n "$BOX_PID" ] && kill "$BOX_PID" 2>/dev/null
  [ -n "$FWD_PID" ] && kill "$FWD_PID" 2>/dev/null
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

pget() { # proxy url -> http code
  curl -sS --max-time 15 -o /dev/null -w '%{http_code}' -x "http://$1" "$2" 2>/dev/null
}
decision_for() { # log key -> member tag (without via=)
  grep "key=$2" "$1" | grep -oE 'via=[A-Za-z0-9_-]+' | tail -1 | cut -d= -f2
}

mkdir -p "$WORK"
if [ ! -x "$WORK/sing-box" ]; then
  echo "--- build ---"
  cd "$REPO" || exit 1
  CRONET_SO=$(find /root/go/pkg/mod/github.com/sagernet/cronet-go/lib -maxdepth 2 -path "*linux_amd64@*" -name libcronet.so | sort | tail -1)
  [ -n "$CRONET_SO" ] && install -m 644 "$CRONET_SO" "$WORK/libcronet.so"
  CGO_ENABLED=0 "$GO" build -tags with_naive_outbound,with_purego -o "$WORK/sing-box" ./cmd/sing-box || exit 1
fi

outbound_field() {
  python3 - "$REPO/test/smart/config.json" "$1" "$2" <<'PY'
import json, sys
config = json.load(open(sys.argv[1]))
outbound = {o["tag"]: o for o in config["outbounds"]}[sys.argv[2]]
print(outbound[sys.argv[3]])
PY
}

# ---------------------------------------------------------------- S12
echo "=== S12 external-link breadth (real nodes) ==="
if [ -f "$REPO/test/smart/config.json" ]; then
  rm -f "$NODE_LOG"
  # config.json's hk-fwd member needs the local forwarder.
  if [ ! -x "$WORK/forwarder" ]; then
    (cd "$REPO/test" && "$GO" build -o "$WORK/forwarder" ./smart/forwarder) || exit 1
  fi
  "$WORK/forwarder" -listen 127.0.0.1:8443 -target "$(outbound_field hk-main server):$(outbound_field hk-main server_port)" &
  FWD_PID=$!
  "$WORK/sing-box" run -c "$REPO/test/smart/config.json" &
  BOX_PID=$!
  sleep 3
  SITES="https://www.google.com https://www.youtube.com https://github.com https://www.cloudflare.com https://www.apple.com https://www.microsoft.com https://www.amazon.com https://www.wikipedia.org https://stackoverflow.com https://developer.mozilla.org https://www.bing.com https://ipinfo.io"
  S12_OK=1
  for round in 1 2 3; do
    for url in $SITES; do
      host=${url#https://}
      code=$(pget "$NODE_PROXY" "$url")
      via=$(decision_for "$NODE_LOG" "$host")
      [ "$round" = "3" ] && printf '  %-28s code=%s via=%s\n' "$host" "$code" "${via:-?}"
      # Rounds 1-2 are warm-up/recovery headroom. Any HTTP response (even a
      # bot-rejection 4xx/5xx) proves the path; only 000 means tunnel failure.
      if [ "$round" = "3" ] && [ "$code" = "000" ]; then
        echo "  UNREACHABLE: $host"
        S12_OK=0
      fi
    done
  done
  check "S12 all external sites reachable through smart" "$S12_OK"
  kill "$BOX_PID" 2>/dev/null; wait "$BOX_PID" 2>/dev/null; BOX_PID=""
  kill "$FWD_PID" 2>/dev/null; FWD_PID=""
else
  echo "skipping S12: no config.json (nodes) present"
fi

# ------------------------------------------------------- local members
echo "=== start local-member instance (direct + routing_mark) ==="
rm -f "$WORK/smart-cache2.json" "$LOCAL_LOG"
"$WORK/sing-box" run -c "$REPO/test/smart/config-local.json" &
BOX_PID=$!
sleep 3
if ! kill -0 "$BOX_PID" 2>/dev/null; then
  echo "FAIL: local instance did not start"
  tail -20 "$LOCAL_LOG"
  exit 1
fi

VICTIM=www.cloudflare.com
CONTROL=www.wikipedia.org
echo "--- warmup ---"
WARM_OK=1
for i in 1 2 3 4; do
  v=$(pget "$LOCAL_PROXY" "https://$VICTIM")
  c=$(pget "$LOCAL_PROXY" "https://$CONTROL")
  [ "$v" = "200" ] && [ "$c" = "200" ] || WARM_OK=0
  sleep 1
done
if [ "$WARM_OK" != "1" ]; then
  echo "FAIL: warmup through local members failed — aborting S11/S10"
  tail -10 "$LOCAL_LOG"
  check "S11/S10 local member warmup" 0
  echo "PASS=$PASS FAIL=$FAIL"; exit 1
fi
V_MEMBER=$(decision_for "$LOCAL_LOG" "$VICTIM")
C_MEMBER=$(decision_for "$LOCAL_LOG" "$CONTROL")
if [ "$V_MEMBER" = "L1" ]; then V_MARK=$MARK_L1; OTHER=L2; else V_MARK=$MARK_L2; OTHER=L1; fi
echo "victim $VICTIM on $V_MEMBER (mark $V_MARK); control $CONTROL on $C_MEMBER"

# ---------------------------------------------------------------- S11
echo "=== S11 member $V_MEMBER suddenly cannot reach $VICTIM ==="
# Block the victim site's nets for that member's fwmark only (silent drop).
for net in $(python3 -c "
import socket
nets = set()
for info in socket.getaddrinfo('$VICTIM', 443, socket.AF_INET):
    ip = info[4][0]
    nets.add('.'.join(ip.split('.')[:2]) + '.0.0/16')
print(' '.join(sorted(nets)))
"); do
  rule="OUTPUT -m mark --mark $V_MARK -d $net -j DROP"
  iptables -I $rule && IPT_RULES+=("$rule")
  echo "  blocked $net for mark $V_MARK"
done
S11_FAILED_REQS=0
S11_SWITCHED=0
for i in $(seq 1 8); do
  code=$(pget "$LOCAL_PROXY" "https://$VICTIM")
  now=$(decision_for "$LOCAL_LOG" "$VICTIM")
  echo "  req$i code=$code via=$now"
  [ "$code" = "200" ] || S11_FAILED_REQS=$((S11_FAILED_REQS+1))
  if [ "$now" = "$OTHER" ]; then S11_SWITCHED=1; [ "$i" -ge 3 ] && break; fi
  sleep 1
done
CTRL_CODE=$(pget "$LOCAL_PROXY" "https://$CONTROL")
CTRL_VIA=$(decision_for "$LOCAL_LOG" "$CONTROL")
echo "  control: code=$CTRL_CODE via=$CTRL_VIA (was $C_MEMBER)"
[ "$S11_SWITCHED" = "1" ]; check "S11 victim site switched to $OTHER" $((1-$?))
[ "$S11_FAILED_REQS" -le 1 ]; check "S11 <=1 user-visible failure (same-call retry)" $((1-$?))
[ "$CTRL_CODE" = "200" ] && [ "$CTRL_VIA" = "$C_MEMBER" ]; check "S11 control site unaffected (per-target isolation)" $((1-$?))
for rule in "${IPT_RULES[@]}"; do iptables -D $rule 2>/dev/null; done
IPT_RULES=()

# ---------------------------------------------------------------- S10
echo "=== S10 member network suddenly degrades ==="
# Fresh keys so this scenario is not entangled with S11 state.
SLOW_SITE=www.bing.com
for i in 1 2 3 4; do pget "$LOCAL_PROXY" "https://$SLOW_SITE" >/dev/null; sleep 1; done
# Let the sticky current age past the post-switch spike grace window so the
# injected degradation is attributed to the path, not to switch transients.
sleep 12
M=$(decision_for "$LOCAL_LOG" "$SLOW_SITE")
if [ "$M" = "L1" ]; then M_MARK=$MARK_L1; M_OTHER=L2; else M_MARK=$MARK_L2; M_OTHER=L1; fi
echo "$SLOW_SITE on $M (mark $M_MARK)"
NETEM_DEV=$(ip route | awk '/default/ {print $5; exit}')
tc qdisc add dev "$NETEM_DEV" root handle 1: prio bands 4 priomap 1 2 2 2 1 2 0 0 1 1 1 1 1 1 1 1 || echo "prio qdisc add failed"
tc qdisc add dev "$NETEM_DEV" parent 1:4 handle 40: netem delay 400ms loss 15% || echo "netem add failed"
tc filter add dev "$NETEM_DEV" parent 1: protocol ip handle "$M_MARK" fw flowid 1:4 || echo "fw filter add failed"
S10_SWITCHED=0
S10_FAILED=0
for i in $(seq 1 10); do
  start=$(date +%s%3N)
  code=$(pget "$LOCAL_PROXY" "https://$SLOW_SITE")
  took=$(( $(date +%s%3N) - start ))
  now=$(decision_for "$LOCAL_LOG" "$SLOW_SITE")
  echo "  req$i code=$code took=${took}ms via=$now"
  [ "$code" = "200" ] || S10_FAILED=$((S10_FAILED+1))
  if [ "$now" = "$M_OTHER" ] && [ "$code" = "200" ] && [ "$took" -lt 1000 ]; then S10_SWITCHED=1; break; fi
  sleep 1
done
tc qdisc del dev "$NETEM_DEV" root 2>/dev/null
NETEM_DEV=""
[ "$S10_SWITCHED" = "1" ]; check "S10 degraded member demoted, service fast again" $((1-$?))
[ "$S10_FAILED" -le 2 ]; check "S10 <=2 failures during degradation" $((1-$?))
grep -q "spiked\|cooling down" "$LOCAL_LOG"; check "S10 degradation visible in log (spike/cooldown)" $((1-$?))

echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
exit $((FAIL > 0 ? 1 : 0))
