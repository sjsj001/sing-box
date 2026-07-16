# 02 engine: pure decision core
Status: resolved

smart_engine.go + smart_table.go：双写两级表、阈值遍历、tolerance 回退、双通道切换、冷却退避、全员失败、源站亲和、探索计数。fake clock 单测（TDD）。

## Comments

- 2026-07-15: Implemented and unit-tested (23+ tests, -race clean). Post-review hardening applied same day: see `.scratch/smart-group/review-fixes.md`.
