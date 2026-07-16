# 03 exploration freshness (refresh-alt + stale-TTFB rotation)
Status: resolved

smart_engine.go：decideLocked sticky 分支追加 refreshStaleAlternativeLocked——非 current
可用成员中 max(lastSample, lastProbe) 超 sample_ttl 者取最陈旧发 "refresh-alt" 探测，
每次 decide ≤1 个；无 stats 成员视为无限陈旧。

smart_table.go：smartTargetStats 增加 lastTTFB；onTTFBSample 写入（精确 + 聚合）。
exploreCandidate：先补 ttfb.Count() < 64 者（原规则），全员攒满后选 lastTTFB 最陈旧且
超 sample_ttl 者，全新鲜返回空。

## Comments

- 2026-07-15: Implemented and unit-tested (6 new tests; full package `go test -race -tags with_naive_outbound` green; whole-repo build green).
