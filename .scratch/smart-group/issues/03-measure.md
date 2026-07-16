# 03 measurement: conn wrapper + baseline
Status: resolved

smart_conn.go：首写→首读 M、TLS record 状态机 TTFB（1.2/1.3 启发式、0-RTT 放弃）、fast-path；UoT PacketConn 计时。单测用 net.Pipe 合成字节流。

## Comments

- 2026-07-15: Implemented and unit-tested (23+ tests, -race clean). Post-review hardening applied same day: see `.scratch/smart-group/review-fixes.md`.
