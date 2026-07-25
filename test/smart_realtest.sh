#!/bin/bash
# Representative real-machine test suite for the smart outbound group.
#
# Runs on the client box, driving traffic through the mixed/socks inbound and
# asserting behaviour against four geographically distributed naive nodes
# (HK / DE / SG / JP). JP (dd) has clean_priority=1 to exercise the CDN clean-IP
# preference. Deploy topology and exit-IP map are set below.
#
# Usage: ./smart_realtest.sh          (cases 1-8, self-contained)
#        Failover (case 9) is driven from the build host (needs SSH to a node).
set -u
PROXY="socks5h://127.0.0.1:1080"
HK="172.81.103.14"; DE="92.119.167.12"; SG="207.2.122.66"; JP="158.51.111.92"
px(){ curl -s -o /dev/null --max-time 25 -x "$PROXY" "$@"; }
pxi(){ curl -s --max-time 25 -x "$PROXY" "$@"; }
pass=0; fail=0
ck(){ if [ "$1" = "$2" ]; then echo "  PASS  $3"; pass=$((pass+1)); else echo "  FAIL  $3 (got '$1' want '$2')"; fail=$((fail+1)); fi }
name(){ case "$1" in "$HK") echo HK;; "$DE") echo DE;; "$SG") echo SG;; "$JP") echo JP;; *) echo "$1";; esac; }
sel(){ journalctl -u sb-smart-cli --no-pager -n 400 | grep 'smart status' | tail -1 | tr '|' '\n' | grep -oE "$1→node-[a-z]+" | grep -oE 'node-[a-z]+$'; }

echo "== warmup: prime CDN + first leaseweb host, let baselines settle =="
pxi https://ifconfig.me >/dev/null 2>&1
sleep 8

echo "[1] Connectivity"
ip=$(pxi https://ifconfig.me); [ -n "$ip" ] && ck ok ok "traffic flows (exit $(name "$ip"))" || ck fail ok "traffic flows"

echo "[2] First-request-optimal per leaseweb location (hit each ONCE, check pick)"
declare -A want=( [fra1.de]=node-de [hkg12.hk]=node-hk [sin1.sg]=node-sg [lax12.us]=node-jp )
for loc in fra1.de hkg12.hk sin1.sg lax12.us; do
  px "http://speedtest.${loc}.leaseweb.net/" 2>/dev/null   # single first request
done
sleep 16   # let one status line print
for loc in fra1.de hkg12.hk sin1.sg lax12.us; do
  got=$(sel "speedtest.${loc}.leaseweb.net")
  ck "$got" "${want[$loc]}" "leaseweb ${loc} first pick"
done

echo "[3] CDN → clean node JP (dd)"
ck "$(name "$(pxi https://ifconfig.me)")" JP "ifconfig.me (CDN 443) exits via JP"
px "http://1.1.1.1/"; sleep 16
ck "$(sel '1.1.1.1')" node-jp "1.1.1.1:80 (CDN) selects JP"

echo "[4] Local baseline ordering (HK < SG < JP < DE)"
st=$(journalctl -u sb-smart-cli --no-pager -n 400 | grep 'smart status' | tail -1)
g(){ echo "$st" | grep -oE "node-$1\[health=[0-9] local=[0-9]+" | grep -oE '[0-9]+$'; }
hk=$(g hk); sg=$(g sg); jp=$(g jp); de=$(g de)
echo "     HK=${hk} SG=${sg} JP=${jp} DE=${de} ms"
if [ -n "$hk" ] && [ "$hk" -lt "$sg" ] && [ "$sg" -lt "$jp" ] && [ "$jp" -lt "$de" ]; then ck ok ok "HK<SG<JP<DE"; else ck bad ok "baseline ordering"; fi

echo "[5] Stability / no flapping (20x one CDN site -> one node)"
d=$(for i in $(seq 1 20); do pxi https://ifconfig.me 2>/dev/null; echo; done | sort -u | grep -c .)
ck "$d" 1 "20x ifconfig.me single exit node"

echo "[6] UDP over TCP (socks5 UDP DNS resolves)"
an=$(python3 - <<'PY' 2>/dev/null
import socket,struct
def q(n): return struct.pack(">HHHHHH",0x1234,0x0100,1,0,0,0)+b"".join(bytes([len(p)])+p.encode() for p in n.split("."))+b"\x00"+struct.pack(">HH",1,1)
s=socket.create_connection(("127.0.0.1",1080),timeout=10);s.sendall(b"\x05\x01\x00");s.recv(2)
s.sendall(b"\x05\x03\x00\x01\x00\x00\x00\x00\x00\x00");r=s.recv(10)
ip=socket.inet_ntoa(r[4:8]);p=struct.unpack(">H",r[8:10])[0]
if ip=="0.0.0.0":ip="127.0.0.1"
u=socket.socket(socket.AF_INET,socket.SOCK_DGRAM);u.settimeout(12)
u.sendto(b"\x00\x00\x00\x01"+socket.inet_aton("1.1.1.1")+struct.pack(">H",53)+q("example.com"),(ip,p))
d,_=u.recvfrom(4096);print(struct.unpack(">H",d[16:18])[0])
PY
)
[ "${an:-0}" -gt 0 ] 2>/dev/null && ck ok ok "UDP DNS via proxy (ANCOUNT=$an)" || ck bad ok "UDP DNS via proxy"

echo "[7] Throughput (100MB download)"
r=$(pxi -o /dev/null -w "%{speed_download}" http://speedtest.fra1.de.leaseweb.net/100mb.bin 2>/dev/null)
mbps=$(awk "BEGIN{printf \"%.0f\", ${r:-0}*8/1000000}")
[ "${mbps:-0}" -gt 50 ] 2>/dev/null && ck ok ok "download ~${mbps} Mbps" || ck bad ok "throughput ${mbps} Mbps"

echo
echo "==== RESULT: $pass passed, $fail failed ===="
exit $fail
