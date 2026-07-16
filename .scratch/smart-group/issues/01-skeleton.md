# 01 skeleton: type registration + options + pass-through dial
Status: resolved

option/smart.go, constant/proxy.go (TypeSmart), include/registry.go (RegisterSmart), protocol/group/smart.go 骨架（直通 preferred[0]/首成员）。本地 go build 通过。

## Comments

- 2026-07-15: Implemented and unit-tested (23+ tests, -race clean). Post-review hardening applied same day: see `.scratch/smart-group/review-fixes.md`.
