# pxd-kx-compare 基准说明

本目录用于对齐 `pxd` 与 `kdb+` 的微基准输出（write/reread/randomread/cpu/cpucache），配合 `scripts/bench_pxd_kx.sh` 生成可比的 CSV 结果。

## splayed 存储说明

`pxd-bench` 的微基准只覆盖内存路径（MemStore），不涉及落盘的 splayed 存储目录。
如需验证 splayed 布局或 WAL checkpoint，请使用 `pxd` 二进制运行带 `--data`/`--partition`
参数的服务端测试路径。

## 随机读 (randomread)

为了避免在 `kdb_bench.q` 中使用 scan 形态的 LCG 生成导致的卡住/内存异常，随机读索引使用纯向量化方式生成：

- 种子序列：`seed0 + lcgmult * til randreads`
- 索引：`(s >> lcgshift) mod rows`

这确保随机读阶段不会因为 scan 而挂起，同时保持与 `pxd` 侧 LCG 逻辑对齐。
