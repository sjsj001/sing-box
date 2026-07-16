# 06 persist: JSON snapshot
Status: resolved

smart_persist.go：cache_path 可选、版本号、原子替换、5min 脏写+Close 落盘、14d TTL、损坏/未知 tag 干净启动。

## Comments

- 2026-07-15: Implemented and unit-tested (23+ tests, -race clean). Post-review hardening applied same day: see `.scratch/smart-group/review-fixes.md`.
