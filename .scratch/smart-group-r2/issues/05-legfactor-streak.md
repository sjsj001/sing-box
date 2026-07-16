# 05 legFactor streak smoothing (R3-1)
Status: resolved

smart.go noteAnchorSuccess：分类与当前 factor 不一致时累计 legStreak，连续 2 轮才翻转；
一致即清零。验证发现的直接成员被误判 leg=2 的噪声来源由此免疫。

## Comments

- 2026-07-16: Implemented; TestSmartLegFactorStreak.
- 2026-07-16 (deeper fix): 复验发现误判是系统性的（1.1.1.1:80 服务端处理时间占 total
  近半，dial/total ≈ 0.5 恒落入旧 0.6 阈值），streak 挡不住。改为：拨到的 conn 暴露
  HandshakeContext（naive 等早数据协议）→ 协议真值直接定 leg=2；计时仅在极端比值下
  投票（<0.2 → 2，≥0.6 → 1），中间地带含糊、不参与 streak。TestSmartLegFactor 重写。
- 2026-07-16 (final): 复验又暴露第二层系统性来源——80 端口经运营商透明代理时 SYN 被
  本地应答（dial 几 ms、total 走真实链路），伪造出早数据计时特征。最终形态：计时投票
  仅在 TLS 探测（timingTrusted）下参与；HTTP 探测只认 HandshakeContext。远端复验：
  direct 成员稳定 leg=1，5 个真实 naive 节点全部 leg=2，run5 10/10 + run.sh 7/7。
