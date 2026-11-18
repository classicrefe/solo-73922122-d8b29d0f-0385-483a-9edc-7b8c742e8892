# 观测数据子集交付服务

面向观测数据工程师的子集交付 API：上传至多 **256 MiB** 的 CSV 原件
（`station,utc_time,temperature`），按**站号集合**与**温度闭区间**提取子集。
同原件的在途任务**共享一次源扫描**，支持途中加入补齐与即时取消；结果保留
**原表头、原行序与重复行**，且仅在整份原件校验通过后开放下载。

## 技术栈与资源

- FastAPI + Uvicorn + SQLite（WAL），纯标准库做 CSV 流式解析；
- 全程流式处理：上传分块落盘、扫描逐行读取、结果逐行写暂存，单进程实测
  处理 72 MiB / 200 万行原件时峰值 RSS 约 **55 MiB**，容器内存上限 **128 MiB**；
- 数据目录 `/data` 使用命名卷，容器重启后成功结果与元数据保留。

## 快速启动

```bash
docker compose up --build
# 接口文档（Swagger UI）
open http://localhost:8000/docs
```

本地无 Docker 时（Python 3.11+）：

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements.txt
uvicorn app.main:app --host 0.0.0.0 --port 8000
```

## 可控慢扫描示例

慢扫描用于演示共享扫描、途中加入与取消。两种打开方式：

1. 上传时带表单字段 `row_delay_ms`（每行额外等待毫秒数，0–60000）：

```bash
curl -X POST http://localhost:8000/sources \
  -F "file=@obs.csv;type=text/csv" -F "row_delay_ms=20"
```

2. 容器环境变量 `SCAN_ROW_DELAY_MS` / `SCAN_DELAY_EVERY_ROWS`（全局默认）。

端到端演示脚本（生成数据 → 上传 → 任务 A → 途中加入 B → 取消 C →
下载核对，仅用标准库）：

```bash
python scripts/demo_slow_scan.py http://localhost:8000 20
```

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/sources` | multipart 上传 CSV，可带 `row_delay_ms`；超 256 MiB 返回 413 |
| GET  | `/sources` / `/sources/{id}` | 原件列表/详情 |
| POST | `/tasks` | 受理筛选任务，**立即返回 201，不等待扫描** |
| GET  | `/tasks` / `/tasks/{id}` | 任务列表/详情（含 `scanned_rows` 扫描进度） |
| POST | `/tasks/{id}/cancel` | 取消任务 |
| GET  | `/tasks/{id}/result` | 下载结果 CSV；未完成返回 409，不供部分文件 |
| GET  | `/health` | 健康检查 |

`POST /tasks` 请求体：

```json
{ "source_id": "...", "stations": ["STA01", "STA02"], "temp_min": 10, "temp_max": 30 }
```

`stations` 为 `null` 或空数组表示全部站号；温度区间为**闭区间**。

任务状态机：`pending → running → completed | failed | cancelled | interrupted`
（取消受理瞬间可见 `cancelling`）。

## 关键语义与实现

- **共享一次源扫描**：同一 source 同一代扫描（`scans/<source>/gen<N>/`）只由
  一个扫描线程顺序读取原件一次，所有任务作为订阅者即时筛行；已用测试验证
  5 个任务（含途中加入）原件仅被打开 1 次。
- **途中加入补齐**：新订阅者先从本代 spool 日志与扫描线程内存缓冲（同一把
  条件锁做一致性快照，不重不漏）补齐已过部分，再接收在线投递，**绝不另读
  原件**；补读期间扫描线程在检查点等待。
- **取消**：单个任务取消不影响其他任务；最后一个活跃任务取消时，扫描线程
  在下一检查点真正停止、清空本代暂存后，接口才返回 `cancelled`，此后不发布
  结果（下载返回 409）。
- **收尾期受理**：旧扫描终态清理与新扫描使用不同代目录，互不干扰；收尾窗口
  内并发受理的任务全部能完整交付（有并发测试覆盖）。
- **完整校验才开放**：任一行格式错误（列数、空站号、时刻缺时区/不可解析、
  温度非整数）或扫描异常，本代所有未完任务 `failed`，暂存删除，不提供部分
  文件；只有全文件合法才原子改名发布结果。
- **重启恢复**：成功任务的结果与 SQLite 元数据保留可下载；所有非终态任务在
  启动时标记 `interrupted`，遗留暂存目录清空。
- **查询不阻塞**：所有 GET 与任务受理均为快速返回，扫描进度经
  `GET /tasks/{id}` 的 `scanned_rows` 观测。

## 目录结构

```
app/            FastAPI 应用、SQLite 层、共享扫描调度、CSV 校验
scripts/        慢扫描端到端演示（仅标准库）
tests/          pytest 自测（20 个用例）
data/           运行期数据（sources/results/scans + app.db），已 gitignore
Dockerfile      python:3.11-slim，非 root 运行，自带 HEALTHCHECK
docker-compose.yml  128m 内存限制、命名卷持久化
```

## 自测

```bash
pip install -r requirements.txt pytest httpx
python -m pytest tests/
```

覆盖：闭区间边界与原行序、重复行保留、空匹配仅表头、共享扫描只打开一次、
反复迟到加入不丢行、单任务/末任务取消与暂存清理、收尾窗口并发受理、
格式错误全量失败不供部分结果、256 MiB 上限拒收、重启保留成功结果并中断
未完任务、查询/受理不等待扫描。
