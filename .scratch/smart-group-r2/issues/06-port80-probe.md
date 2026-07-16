# 06 port-80 probes with timing (R3-2)
Status: resolved

smart.go requestProbeSync：80 端口用 probeHTTP（首写→首读计时，喂 onProbeResult），
443 维持 probeTLS，其余端口维持 probeAlive（server-first 协议计时无意义）。
failover.go requestRecoveryProbe 同步 80 端口分支。

## Comments

- 2026-07-16: Implemented; TestSmartProbeHTTPTiming.
