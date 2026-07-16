# 04 probe: anchor baseline + target probe + storm control
Status: resolved

smart_probe.go：锚点滚动最小基线、legFactor、tls.Client 探测原语、HandshakeContext duck-typing、singleflight+信号量+抖动+SWR。

## Comments

- 2026-07-15: Implemented and unit-tested (23+ tests, -race clean). Post-review hardening applied same day: see `.scratch/smart-group/review-fixes.md`.
