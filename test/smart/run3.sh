#!/usr/bin/env bash
# S13 regional convergence (spec S2): region-pinned origins (AWS EC2 regional
# API endpoints) must converge to the geographically matching member, while
# anycast/CDN targets converge to the preferred member. Ends with a
# `smart-cache` dump so the learned table can be inspected.
set -uo pipefail

REPO="${REPO:-/root/ref1nd-sing-box-1}"
WORK="${WORK:-/root/smart-test}"
GO="${GO:-/usr/local/go/bin/go}"
PROXY="127.0.0.1:2080"
LOG="$WORK/box.log"
ROUNDS="${ROUNDS:-20}"

BOX_PID=""
FWD_PID=""
cleanup() {
  echo "--- cleanup ---"
  [ -n "$BOX_PID" ] && kill "$BOX_PID" 2>/dev/null
  [ -n "$FWD_PID" ] && kill "$FWD_PID" 2>/dev/null
}
trap cleanup EXIT INT TERM

outbound_field() {
  python3 - "$REPO/test/smart/config.json" "$1" "$2" <<'PY'
import json, sys
config = json.load(open(sys.argv[1]))
outbound = {o["tag"]: o for o in config["outbounds"]}[sys.argv[2]]
print(outbound[sys.argv[3]])
PY
}
decision_for() {
  grep "key=$1" "$LOG" | grep -oE 'via=[A-Za-z0-9_-]+' | tail -1 | cut -d= -f2
}

mkdir -p "$WORK"
rm -f "$LOG" "$WORK/smart-cache.json"
echo "--- build ---"
cd "$REPO" || exit 1
CRONET_SO=$(find /root/go/pkg/mod/github.com/sagernet/cronet-go/lib -maxdepth 2 -path "*linux_amd64@*" -name libcronet.so | sort | tail -1)
[ -n "$CRONET_SO" ] && install -m 644 "$CRONET_SO" "$WORK/libcronet.so"
CGO_ENABLED=0 "$GO" build -tags with_naive_outbound,with_purego -o "$WORK/sing-box" ./cmd/sing-box || exit 1
if [ ! -x "$WORK/forwarder" ]; then
  (cd "$REPO/test" && "$GO" build -o "$WORK/forwarder" ./smart/forwarder) || exit 1
fi

"$WORK/forwarder" -listen 127.0.0.1:8443 -target "$(outbound_field hk-main server):$(outbound_field hk-main server_port)" &
FWD_PID=$!
"$WORK/sing-box" run -c "$REPO/test/smart/config.json" &
BOX_PID=$!
sleep 3
kill -0 "$BOX_PID" 2>/dev/null || { echo "FAIL: sing-box did not start"; tail -20 "$LOG"; exit 1; }

# Region-pinned origins and the member expected to win them.
REGIONAL_HOSTS=(
  "ec2.ap-northeast-1.amazonaws.com JP"
  "ec2.ap-east-1.amazonaws.com HK"
  "ec2.ap-southeast-1.amazonaws.com SG"
  "ec2.eu-central-1.amazonaws.com DE"
  "ec2.us-east-1.amazonaws.com US"
  "ec2.us-west-1.amazonaws.com US"
)
# Anycast/CDN set: preferred (JP) must hold these even when not fastest.
ANYCAST_HOSTS=(www.gstatic.com www.cloudflare.com www.apple.com)

echo "--- convergence traffic: $ROUNDS rounds over $(( ${#REGIONAL_HOSTS[@]} + ${#ANYCAST_HOSTS[@]} )) targets ---"
for round in $(seq 1 "$ROUNDS"); do
  PIDS=()
  for entry in "${REGIONAL_HOSTS[@]}"; do
    host=${entry%% *}
    curl -sS --max-time 15 -o /dev/null -x "http://$PROXY" "https://$host/" 2>/dev/null &
    PIDS+=($!)
  done
  for host in "${ANYCAST_HOSTS[@]}"; do
    curl -sS --max-time 15 -o /dev/null -x "http://$PROXY" "https://$host/" 2>/dev/null &
    PIDS+=($!)
  done
  wait "${PIDS[@]}" 2>/dev/null
  sleep 5
done
sleep 2

PASS=0
FAIL=0
echo "--- verdict ---"
printf '%-38s %-8s %-8s %s\n' TARGET EXPECT ACTUAL RESULT
for entry in "${REGIONAL_HOSTS[@]}"; do
  host=${entry%% *}
  expect=${entry##* }
  actual=$(decision_for "$host")
  if [ "$actual" = "$expect" ]; then result=PASS; PASS=$((PASS+1)); else result=FAIL; FAIL=$((FAIL+1)); fi
  printf '%-38s %-8s %-8s %s\n' "$host" "$expect" "${actual:-?}" "$result"
done
for host in "${ANYCAST_HOSTS[@]}"; do
  actual=$(decision_for "$host")
  if [ "$actual" = "JP" ]; then result=PASS; PASS=$((PASS+1)); else result=FAIL; FAIL=$((FAIL+1)); fi
  printf '%-38s %-8s %-8s %s\n' "$host" "JP(pref)" "${actual:-?}" "$result"
done

# Graceful shutdown writes a fresh snapshot; dump it for inspection.
kill "$BOX_PID" 2>/dev/null; wait "$BOX_PID" 2>/dev/null; BOX_PID=""
echo
echo "--- smart-cache dump (amazonaws) ---"
"$WORK/sing-box" smart-cache -f amazonaws "$WORK/smart-cache.json"
echo
echo "--- smart-cache dump (anycast set) ---"
"$WORK/sing-box" smart-cache -f www. "$WORK/smart-cache.json" | sed -n '/^targets/,$p'

echo "======================================"
echo "PASS=$PASS FAIL=$FAIL"
exit $((FAIL > 0 ? 1 : 0))
