# 冷链温度日志采集系统（Cold-Chain Collector）

网关持续追加 UTF-8 NDJSON 温度日志，本服务监视本地目录，将完整合法的记录
恰好一次入库到 SQLite，供质控系统按运单追溯。支持重命名轮转、半行续写、
坏行隔离、进程重启续读，并提供可演示的 Web 控制台与 HTTP API。

**缺测核查**：按运单找出相邻采样时刻间隔严格大于 5 分钟的缺口，供数据员
决定补取哪些网关日志；可将缺口标记为已核查，迟到日志拆分/消除缺口时旧标记
自动失效。历史缺口在后台线程首次补建，期间采集不中断，完成切换前网页不
返回残缺结果；之后每次入库只增量调整受影响的相邻缺口。

## 目录结构

```
cold-chain-collector/
├── app/                    # Flask 采集服务
│   ├── config.py           # 环境变量配置
│   ├── db.py               # SQLite 连接与表结构（WAL）
│   ├── collector.py        # 核心：轮询读取、续读、轮转、坏行/半行、事务
│   ├── gaps.py             # 缺口检测、增量维护、标记与后台补建
│   ├── web.py              # HTTP 接口与页面
│   ├── wsgi.py             # 入口
│   └── templates/
│       ├── index.html      # 中文 Web 控制台（采集进度/坏行/运单追溯）
│       └── gaps.html       # 缺测核查页（缺口列表、标记、补建进度）
├── gateway/simulator.py    # 本地网关模拟器（半行 / 轮转 / 坏行 / 停摆注入）
├── tests/                  # unittest 用例（采集 + 缺口，共 40+ 个）
├── Dockerfile
├── docker-compose.yml
├── docker-entrypoint.sh
└── run-demo.sh             # 无 Docker 时的本地演示脚本
```

## 一条记录的格式（NDJSON，UTF-8，每行一条，以 `\n` 结束）

```json
{"waybill": "WB-1001", "ts_utc": "2026-11-12T04:42:24.063Z", "temperature": 3.9}
```

* `waybill`：非空字符串，运单号
* `ts_utc`：UTC 时间（必须带 UTC 时区，`Z` 或 `+00:00`），非 UTC/无时区一律拒收
* `temperature`：数值（布尔值、NaN、Infinity 拒收）

## 快速开始（Docker Compose）

```bash
docker compose up --build
# 浏览器打开
#   http://localhost:8000
```

两个服务共享一个命名卷 `gateway-data`：

| 服务 | 作用 |
| --- | --- |
| `web` | 采集 + Web/API，端口 8000，启动即自动采集并后台补建历史缺口 |
| `gateway` | 网关模拟器，每秒写 1 条；每 20 行重命名轮转；每 7 条制造一次半行；每 11 条写一条完整坏行；每 45 条好记录停摆 420 秒制造真实缺口 |

打开后访问：

* `http://localhost:8000/`：采集控制台
* `http://localhost:8000/gaps`：缺测核查页（补建完成前显示进度、不返回缺口）

数据持久化在命名卷 `gateway-data`（日志）与 `collector-data`（SQLite）中，
`docker compose down` 后数据保留；`docker compose down -v` 清空。

常用操作：

```bash
docker compose logs -f gateway          # 观察 half-line / rotated 日志
docker compose restart web              # 模拟采集进程重启，验证自动续读
docker compose exec web ls -l /data/gateway
```

## 无 Docker 本地运行

```bash
pip install -r requirements.txt
./run-demo.sh                  # 自动启动模拟器 + Web，数据在 ./_demo_data
# 或分别启动：
python3 gateway/simulator.py --watch-dir ./data/gateway
WATCH_DIR=./data/gateway DB_PATH=./data/collector.db python3 -m app.wsgi
```

## 运行测试

```bash
pip install -r requirements.txt
python3 -m unittest discover -s tests -v
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/` | Web 控制台（进度、坏行、运单查询，2 秒自动刷新） |
| GET | `/gaps` | 缺测核查页（补建进度、按运单缺口、标记/取消核查） |
| GET | `/healthz` | 健康检查 |
| GET | `/api/status` | 采集线程状态、各文件读取进度、汇总计数、缺口任务状态 |
| POST | `/api/start` / `/api/stop` | 启动 / 停止采集 |
| GET | `/api/errors?limit=100` | 坏行（含文件、行号、字节偏移、原因、原文） |
| GET | `/api/waybills` | 全部运单汇总 |
| GET | `/api/waybills/<运单号>?limit=500` | 按运单取数，附 min/max/avg 统计 |
| GET | `/api/gaps/status` | 缺口补建任务状态与进度（任何时候都可访问） |
| POST | `/api/gaps/backfill` | 手动触发/重试历史缺口补建（就绪后为空操作） |
| GET | `/api/gaps/waybills` | 有缺口的运单汇总（未就绪返回 503） |
| GET | `/api/gaps/waybills/<运单号>?marked=all\|marked\|unmarked` | 缺口明细 + 已失效标记 |
| POST | `/api/gaps/waybills/<运单号>/marks` | 标记缺口为已核查（缺口已失效则 404） |
| DELETE | `/api/gaps/waybills/<运单号>/marks` | 取消核查标记 |

示例：

```bash
curl http://localhost:8000/api/waybills/WB-1001
curl http://localhost:8000/api/gaps/waybills/WB-1001
curl -X POST http://localhost:8000/api/gaps/waybills/WB-1001/marks \
  -H 'Content-Type: application/json' \
  -d '{"start_ts":"2026-11-12T10:02:00Z","end_ts":"2026-11-12T10:20:00Z","note":"已补取日志"}'
```

## 关键设计与正确性保证

### 1. 文件身份 = `(st_dev, st_ino)`，不依赖文件名

轮转方式为“旧文件改名保留、新文件沿用原名”。采集器以 inode 身份为每个
文件维护独立进度：改名后的旧文件继续按原进度读完，新文件从 0 开始；
旧文件不会被删除，未采完的旧文件状态为 `rotated_pending`，补齐后变
`completed`。

### 2. 只处理以换行结束的记录，半行留待续写

进度只推进到最后一个 `\n` 之后。无换行结尾的内容留在文件里，下一轮（或
重启后）从已提交偏移重新读取拼接，既不丢失也不提前入库。页面上
`pending_bytes` 即当前半行字节数。

### 3. 完整坏行记录位置与原因后跳过

JSON 解析失败、字段缺失、时间非 UTC、温度非数值、UTF-8 非法、超长行等，
写入 `ingest_errors`（含行号与字节偏移），不影响后续行；坏行同样占用源
文件行号。

### 4. 恰好一次（exactly-once）

* 入库行、坏行、读取进度在**同一个 SQLite 事务**提交（WAL 模式）；
* 崩溃只会回滚到上一个提交点，重启后按 `(dev, ino)` + `committed_off`
  重读未提交部分，因此每条完整合法行恰好入库一次；
* 相同内容的不同原始行是独立行，分别保留（无去重逻辑）。

### 5. 内存安全

分块（64 KiB）流式读取；无换行且超过 `MAX_LINE_BYTES` 的行不会常驻内存，
跳过状态持久化在进度表中，跨 tick / 跨进程依然有效，换行到达时记一条坏行。

### 6. 采样点与缺测缺口

* **采样点**：同一运单、同一 UTC 时刻的任意多条原始行只算一个采样点
  （`samples` 视图按 `(waybill, ts_utc)` 去重）；原始 `readings` 行始终
  分别保留，不做合并或删除。
* **缺口**：同一运单按时间排序后，相邻两个采样点的间隔**严格大于**阈值
  （默认 300 秒）即为一个缺口，记录起点、终点与时长。
* 缺口身份是完整的 `(运单, 起点, 终点)`。迟到日志落在缺口内部会把它拆成
  若干新区间（新区间是新身份），或在采样变密后直接消除；区间未变的缺口
  身份不变。

### 7. 标记的保留与失效

核查标记单独存放在 `gap_marks`，主键同样是 `(运单, 起点, 终点)`：

* 缺口未变 → 标记继续有效（随缺口一起保留）；
* 缺口被拆分或消除 → 旧标记行保留作审计，但不再与任何现存缺口连接，
  页面归入“已失效标记”；
* 标记写入前先校验该缺口仍存在，旧页面持有的过期端点无法误标拆分后的
  新区间（返回 404 `gap_not_found`）。

### 8. 首次后台补建与之后的增量维护

* 首次启动时任务行为 `pending → running → ready`，由后台线程按运单逐个
  补建历史缺口，采集线程全程继续入库；`/api/gaps/*` 在 `ready` 之前一律
  返回 503，核查页只显示构建进度（已完成/总运单数、当前运单），不返回
  残缺结果。
* 每个运单补建后立即提交并更新进度，故重启后续建是幂等的；中途停止或
  失败会回到 `pending`，可通过 `POST /api/gaps/backfill` 重试。
* 切换到 `ready` 前用 `BEGIN IMMEDIATE` 取写锁并按 reading-id 水位再做
  一轮追赶重建，保证补建期间新入库的数据在切换瞬间全部纳入。
* `ready` 之后，每次入库在**同一个事务**里只删除/重建与本次插入时间窗
  相交的相邻缺口（窗口外缺口不动），因此采集记录、缺口与标记变化共同
  生效；缺口、标记、任务状态都在 SQLite 中，重启后保留。

## 配置项（环境变量）

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `WATCH_DIR` | `/data/gateway` | 监看目录 |
| `ACTIVE_FILE` | `temperature.log` | 活动文件名 |
| `DB_PATH` | `/data/collector/collector.db` | SQLite 路径 |
| `POLL_INTERVAL` | `1` | 轮询间隔秒 |
| `BATCH_SIZE` | `200` | 每事务最大行数 |
| `MAX_LINE_BYTES` | `1048576` | 单行最大字节，超过记坏行 |
| `AUTOSTART` | `true` | 进程启动即采集 |
| `GAP_THRESHOLD_SECONDS` | `300` | 相邻采样点间隔严格大于该秒数判为缺口 |
| `GAP_BACKFILL_AUTOSTART` | `true` | 启动即后台补建历史缺口（false 时手动触发） |
| 模拟器 `ROTATE_EVERY` | `20` | 每 N 行重命名轮转，0 关闭 |
| 模拟器 `HALF_LINE_EVERY` | `7` | 每 N 行拆成两次写（无换行半行），0 关闭 |
| 模拟器 `BAD_LINE_EVERY` | `11` | 每 N 行写一条完整坏行，0 关闭 |
| 模拟器 `OUTAGE_EVERY` | `45` | 每 N 条好记录模拟一次网关停摆，0 关闭 |
| 模拟器 `OUTAGE_SECONDS` | `420` | 每次停摆持续秒数（制造真实 >5 分钟缺口） |

## 数据表

* `readings`：入库温度记录，带来源文件、inode、源行号与原文
* `ingest_errors`：坏行（行号、字节偏移、原因、原文截断）
* `ingest_progress`：每个文件身份的已提交偏移、行数、半行字节、跳过状态、
  状态（`active` / `rotated` / `rotated_pending` / `completed`）
* `samples`：视图，`readings` 按 `(waybill, ts_utc)` 去重后的采样点
* `gaps`：超阈值的相邻采样点缺口（运单、起点、终点、时长）
* `gap_marks`：缺口核查标记与备注（缺口失效后行保留、不再连接）
* `gap_jobs`：单行历史补建任务状态机与进度（重启保留）
