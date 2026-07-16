# 05 race: raceConn cold start
Status: resolved

smart_race.go：ClientHello 写扇出、首字节定胜、败者关闭、胜者样本入表、全败报错。仅 443/TLS 冷启动。

## Comments

- 2026-07-15: Implemented and unit-tested (23+ tests, -race clean). Post-review hardening applied same day: see `.scratch/smart-group/review-fixes.md`.
