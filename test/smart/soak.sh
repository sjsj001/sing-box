#!/usr/bin/env bash
# Smart-group long-stability soak (spec S8 full version): mixed real sites
# through the node config for SOAK_MINUTES, then assert a low failure rate
# and no per-target churn during the fault-free window.
set -uo pipefail

REPO="${REPO:-/root/ref1nd-sing-box-1}"
WORK="${WORK:-/root/smart-test}"
GO="${GO:-/usr/local/go/bin/go}"
PROXY="127.0.0.1:2080"
LOG="$WORK/box.log"
SOAK_MINUTES="${SOAK_MINUTES:-30}"

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

mkdir -p "$WORK"
rm -f "$LOG" "$WORK/smart-cache.json"
if [ ! -x "$WORK/sing-box" ]; then
  cd "$REPO" || exit 1
  CRONET_SO=$(find /root/go/pkg/mod/github.com/sagernet/cronet-go/lib -maxdepth 2 -path "*linux_amd64@*" -name libcronet.so | sort | tail -1)
  [ -n "$CRONET_SO" ] && install -m 644 "$CRONET_SO" "$WORK/libcronet.so"
  CGO_ENABLED=0 "$GO" build -tags with_naive_outbound,with_purego -o "$WORK/sing-box" ./cmd/sing-box || exit 1
fi
if [ ! -x "$WORK/forwarder" ]; then
  (cd "$REPO/test" && "$GO" build -o "$WORK/forwarder" ./smart/forwarder) || exit 1
fi

"$WORK/forwarder" -listen 127.0.0.1:8443 -target "$(outbound_field hk-main server):$(outbound_field hk-main server_port)" &
FWD_PID=$!
"$WORK/sing-box" run -c "$REPO/test/smart/config.json" &
BOX_PID=$!
sleep 3
kill -0 "$BOX_PID" 2>/dev/null || { echo "FAIL: sing-box did not start"; tail -20 "$LOG"; exit 1; }

SITES=(https://www.google.com https://www.youtube.com https://github.com
       https://www.cloudflare.com https://www.apple.com https://www.microsoft.com
       https://www.wikipedia.org https://developer.mozilla.org https://www.bing.com
       https://ipinfo.io)

TOTAL=0
FAILED=0
END=$(( $(date +%s) + SOAK_MINUTES * 60 ))
WARMUP_END=$(( $(date +%s) + 120 ))
SW_BASE=""
echo "--- soak ${SOAK_MINUTES}m over ${#SITES[@]} sites ---"
while [ "$(date +%s)" -lt "$END" ]; do
  for url in "${SITES[@]}"; do
    code=$(curl -sS --max-time 15 -o /dev/null -w '%{http_code}' -x "http://$PROXY" "$url" 2>/dev/null)
    TOTAL=$((TOTAL+1))
    [ "$code" = "000" ] && FAILED=$((FAILED+1))
  done
  # Steady-state switch counting starts after a 2-minute convergence warmup.
  if [ -z "$SW_BASE" ] && [ "$(date +%s)" -ge "$WARMUP_END" ]; then
    SW_BASE=$(grep -c "switch" "$LOG")
    echo "warmup done: total=$TOTAL failed=$FAILED switches_so_far=$SW_BASE"
  fi
  sleep 5
done

SW_END=$(grep -c "switch" "$LOG")
SW_STEADY=$((SW_END - ${SW_BASE:-0}))
echo "requests=$TOTAL failed=$FAILED steady-state-switches=$SW_STEADY"
echo "--- all switch lines ---"
grep "switch" "$LOG"
echo "--- verdict ---"
PASS=1
FAIL_PCT=$(( FAILED * 100 / (TOTAL == 0 ? 1 : TOTAL) ))
[ "$FAIL_PCT" -le 2 ] || { echo "FAIL: failure rate ${FAIL_PCT}% > 2%"; PASS=0; }
# ~10 targets; spec: fault-free per-target switches <=1 (goal 0).
[ "$SW_STEADY" -le 10 ] || { echo "FAIL: $SW_STEADY steady-state switches (> 1 per target)"; PASS=0; }
[ "$PASS" = "1" ] && echo "SOAK PASS (failures=${FAILED}/${TOTAL}, steady switches=$SW_STEADY)" || echo "SOAK FAIL"
exit $((PASS == 1 ? 0 : 1))
