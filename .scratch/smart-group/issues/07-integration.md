# 07 integration: test/smart on 10.10.10.4
Status: resolved

config 模板（5 naive 区域组+HK forwarder 主备）、run.sh、verify 断言（S1-S12）、iptables/tc 注入脚本（trap 清理）。

## Comments

- 2026-07-15: Suite green 7/7 on 10.10.10.4 (purego naive build). S6 failover 4.2s; S3 converges via the new preferred-challenge path; S5 urltest absorbs the forwarder kill with smart staying on HK. Committed as 2aabb9c97.
