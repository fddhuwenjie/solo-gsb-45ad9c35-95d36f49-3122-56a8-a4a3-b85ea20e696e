# 航标闪光巡检判定服务（navlight-inspector）

基于 FastAPI + SQLite 的航标巡检判定服务。接口接收闪光观测时间戳、航标特征库
（编号、周期、周期内闪光时刻）以及时间容差、最大线性钟漂、漏记/误记上限，
联合估计观测时钟的**相位与线性钟漂**，对跨多个周期的观测序列做**循环对齐**
（周期边界不会被误判为缺口），并按固定顺序裁决最佳航标，返回逐事件对应关系
与未匹配记录；无合格候选时给出超限原因。

`/patrols/analyze` 把一次巡检的多个观测窗口与计划航线**整体核对**：先为每个
窗口计算全部合格航标候选，再用动态规划把窗口一对一全局分配到严格递增的航点
（可跳过窗口或航点，禁止逐窗局部最佳），返回分配及候选证据、未分配窗口与
漏巡航点。

服务端特征库以**不可变版本**发布：草稿提交完整特征库并基于 `base_version`
计算增删改差异；激活在单个 SQLite 事务内确认基线仍是当前版本后原子切换，
冲突返回 409；历史版本不可改，可作为回退时新草稿的来源。

现场设备通过 `POST /field-devices/{device_id}/batches` 以**链式批次**上传观测：
每批含从 1 递增的 `sequence`、设备内唯一的 `batch_id`、引用上一批摘要的
`previous_digest` 和 1～100 条按 `(captured_at, local_id)` 递增的观测；服务端
只保存观测时间戳与 `/judge` 四项阈值（不判定），并对排除摘要字段的规范化载荷
计算 SHA-256 作为批次摘要。校验、追加记录、更新设备链头在**单个 SQLite 事务**
内完成：相同载荷重放在幂等事务内原子累加 `replay_count` 并更新游标最近事件
（不新增批次、不推进链头、不改写首次受理响应），返回原结果（`status=replayed`），
载荷变化或链头过期返回 409 及期望值，并发提交与并发重放都不会形成分叉或丢计数。

同一台设备的批次会被**多个下游处理方**消费（如归档、分析、备份）。各处理方
先登记为设备的**必需消费者**（登记后不可删除），再以
`consumer_id` + `sequence` + `batch_digest` 确认自己已处理完的批次——允许乱序、
并发与重复提交，仅接受已存在且摘要相符的批次，相同确认幂等，摘要冲突返回 409。
每个消费者在**同一 SQLite 事务**内保存稀疏确认并把连续水位推进到首个缺口；
设备的 `safe_sequence` 取全部必需消费者水位的最小值，即所有处理方都已确认、
**可安全归档的连续前缀**（尚无消费者或任一消费者未确认任何批次时为 0）。
状态接口列出各消费者水位、首个缺口与阻塞消费者；确认只追加记录，**不删除原批次**。

## 快速开始

单条命令启动（唯一前置要求：Docker 与 Docker Compose）：

```bash
docker compose up --build
```

访问地址：

| 地址 | 说明 |
| --- | --- |
| http://localhost:8000 | 服务信息 |
| http://localhost:8000/docs | Swagger 交互式 API 文档 |
| http://localhost:8000/health | 健康检查 |

停止并清理：`docker compose down`（加 `-v` 同时删除数据卷）。

## 配置项

全部通过环境变量配置（见 `docker-compose.yml` 中的 `environment`）：

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `NAVLIGHT_DB_PATH` | `/app/data/navlight.db`（容器内） | SQLite 数据库文件路径 |
| `NAVLIGHT_GRID_MAX_STEPS` | `201` | 钟漂网格搜索最大步数 |
| `NAVLIGHT_SEED_TOP_K` | `5` | 进入细化阶段的相位/钟漂种子数 |

端口映射在 `docker-compose.yml` 的 `ports` 中修改（默认 `8000:8000`）。
SQLite 数据通过命名卷 `navlight-data` 持久化。

## API 一览

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `POST` | `/library/versions` | 创建版本草稿：提交完整特征库 + `base_version`，返回增删改差异 |
| `GET` | `/library/versions` | 列出全部版本（`draft` / `active` / `historical`） |
| `GET` | `/library/versions/{id}` | 版本详情（元数据 + 完整特征库快照） |
| `GET` | `/library/versions/{id}/diff` | 查询版本相对 `base_version` 的增删改 |
| `POST` | `/library/versions/{id}/activate` | 激活草稿：单事务校验基线并原子切换，冲突返回 409 |
| `POST` | `/library/import` | 导入 JSON 特征库（兼容旧接口；基于当前版本原子创建并激活新版本） |
| `GET` | `/library` | 查看当前激活特征库（含 `library_version`） |
| `POST` | `/judge` | 判定观测序列，结果落库（响应与记录保存 `library_version`） |
| `POST` | `/patrols/analyze` | 巡检整体核对：观测窗口全局分配到计划航线航点（固定激活版本，不落库） |
| `GET` | `/judgments` | 查询判定记录（`limit`/`offset` 分页，按时间倒序） |
| `GET` | `/judgments/{id}` | 查询单条判定记录详情（含完整请求与响应） |
| `POST` | `/field-devices/{device_id}/batches` | 上传设备观测批次：链式追加、只保存不判定；重放返回原结果，冲突 409 |
| `GET` | `/field-devices/{device_id}/cursor` | 查询设备批次链游标（链头摘要、下一批期望值与最近事件状态/序号/时刻） |
| `GET` | `/field-devices/{device_id}/batches` | 按 `after_sequence` 游标分页列出设备批次（首次受理状态 + 重放计数） |
| `GET` | `/field-devices/{device_id}/batches/{sequence}` | 查询单条批次详情（观测时间戳、`/judge` 四项阈值与重放计数） |
| `POST` | `/field-devices/{device_id}/consumers` | 登记设备的必需下游消费者（不可删除；重复登记幂等） |
| `POST` | `/field-devices/{device_id}/acknowledgments` | 消费者确认已处理批次（`consumer_id`/`sequence`/`batch_digest`；乱序/并发/重复安全，冲突 409） |
| `GET` | `/field-devices/{device_id}/archive-status` | 查询安全归档水位：`safe_sequence`、首个缺口、各消费者水位与阻塞消费者 |

### 1. 导入特征库（原子发布新版本）

```bash
curl -s -X POST http://localhost:8000/library/import \
  -H 'Content-Type: application/json' \
  -d @samples/library.json
```

特征库条目格式：`{"aid_id": "CN-001", "period": 10.0, "flashes": [0.0, 1.5]}`，
其中 `flashes` 为周期内闪光时刻（秒，取值 `[0, period)`）。

`mode=merge` 按编号并入当前版本，`mode=replace` 清空重导。每次导入都基于
当前激活版本在**单个事务**内创建并直接激活一个新版本（`source="import"`），
旧版本转为 `historical`；响应中的 `version` 字段即新版本元数据（含差异）。

### 2. 特征库版本发布（草稿 → 激活）

```bash
# ① 创建草稿：提交完整特征库 + base_version（当前激活版本或历史版本）
curl -s -X POST http://localhost:8000/library/versions \
  -H 'Content-Type: application/json' \
  -d '{"base_version": 1, "aids": [
        {"aid_id": "CN-001", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "CN-009", "period": 6.0,  "flashes": [0.0]}
      ]}'

# ② 查询差异（创建响应中已带 diff，此处可随时复查）
curl -s http://localhost:8000/library/versions/2/diff

# ③ 激活草稿
curl -s -X POST http://localhost:8000/library/versions/2/activate
```

**版本模型**：

- 每个版本保存**完整特征库快照**，创建后不可变；状态机为
  `draft → active → historical`，任一时刻至多一个 `active`；
- `base_version` 可以是当前激活版本，也可以是历史版本（回退场景），
  但不能是草稿；基线不存在返回 404；
- 差异按航标编号计算：`added`（新增）、`removed`（删除）、
  `modified`（`period` 或 `flashes` 变化），创建时即算出并随版本保存，
  可通过 `GET /library/versions/{id}/diff` 复查。

**激活与冲突处理（409）**：

激活在**单个 SQLite 事务**内完成：先确认草稿的 `base_version` 仍是当前激活
版本，再把旧激活版本转为 `historical`、新草稿转为 `active` 并切换激活指针，
三步原子生效。若创建草稿后有其他版本被激活（基线过期），激活返回
`409 Conflict`，草稿保持 `draft` 状态不变。处理方式：以**当前激活版本**为
`base_version` 重新提交内容相同的草稿（服务端会基于新基线重新计算差异），
再激活即可。

**回退流程**：

历史版本不可变，可直接作为新草稿的内容来源：

```bash
# 查看历史版本完整快照
curl -s http://localhost:8000/library/versions/1

# 用历史快照在当前激活版本之上重建草稿并激活，即完成回退
curl -s -X POST http://localhost:8000/library/versions \
  -H 'Content-Type: application/json' \
  -d '{"base_version": <当前激活版本号>, "aids": [<历史版本的完整 aids>]}'
curl -s -X POST http://localhost:8000/library/versions/<新草稿号>/activate
```

注意：以历史版本为 `base_version` 创建的草稿在激活时同样会因基线过期返回
409——回退的草稿必须基于**当前激活版本**创建（内容可以来自任意历史版本）。

**首次迁移**：服务启动时若发现旧版平铺 `aids` 表，会在单个事务内将其整体
转为初始激活版本（`version_id=1`，`source="migration"`）并补充判定记录的
`library_version` 列；同时为旧版设备批次表补充 `replay_count` /
`last_replayed_at` 列（既有批次重放次数为零、`last_replayed_at` 为空）、
为设备链游标表补充 `last_event_status` / `last_event_sequence` /
`last_event_at` 列（回填为链头批次的受理事件）。全新部署则无初始版本，首次
`/library/import` 即创建版本 1。

### 3. 判定观测序列

生成一份合成观测（含钟漂、噪声、2 次漏记、2 次误记）并提交判定：

```bash
python3 samples/generate_request.py > /tmp/judge.json
curl -s -X POST http://localhost:8000/judge \
  -H 'Content-Type: application/json' -d @/tmp/judge.json | python3 -m json.tool
```

请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `observations` | `float[]` | 闪光观测时间戳（Unix 秒） |
| `tolerance` | `float>0` | 时间容差（秒），匹配残差上限 |
| `max_drift` | `float∈[0,1)` | 最大线性钟漂（无量纲，s/s） |
| `max_missed` | `int≥0` | 漏记上限 |
| `max_false` | `int≥0` | 误记上限 |
| `library` | 可选 | 内联特征库；缺省时在请求开始固定当前激活版本，整个判定只读该快照 |

响应要点：

- `status`：`matched` / `no_candidate`；无合格候选时 `reason` 写明超限原因；
- `library_source` / `library_version`：`stored` 时记录本次判定固定的激活版本号
  （请求开始即固定，计算期间发生版本切换也不受影响），`inline` 时为 `null`；
- `best`：最佳合格候选，含 `offset`（相位）、`drift`（钟漂）、`matched`/`missed`/`false`
  计数、`events`（逐事件对应关系：观测时刻、期望真时、预测观测时刻、周期序号、
  周期内闪光时刻、残差）、`missed_flashes`（漏记明细）、
  `unmatched_observations`（误记/未匹配观测）；
- `candidates`：全部候选按裁决顺序排列的摘要（含各自 `violations`）；
- `judgment_id`：判定记录 ID，可用于 `GET /judgments/{id}` 回溯；
  判定记录同样保存 `library_version`，可按版本号精确复现当时使用的特征库。

### 4. 巡检整体核对（多窗口 ↔ 计划航线）

生成一份示例巡检请求（3 个航点、3 个观测窗口：1 个噪声窗口、1 个漏巡航点）
并提交整体核对：

```bash
python3 samples/generate_patrol_request.py > /tmp/patrol.json
curl -s -X POST http://localhost:8000/patrols/analyze \
  -H 'Content-Type: application/json' -d @/tmp/patrol.json | python3 -m json.tool
```

请求体：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `waypoints` | `object[]`（≤50） | 计划航线航点，按航线顺序排列；`waypoint_id` 请求内唯一，`aid_id` 必须存在于请求开始时固定的激活特征库版本（否则 400） |
| `windows` | `object[]`（≤20） | 观测窗口，按采集顺序排列；每个窗口含 `observations`（闪光观测时间戳，Unix 秒） |
| `tolerance` | `float>0` | 时间容差（秒），同 `/judge` |
| `max_drift` | `float∈[0,1)` | 最大线性钟漂（无量纲，s/s），同 `/judge` |
| `max_missed` / `max_false` | `int≥0` | 漏记/误记上限，同 `/judge` |

处理流程：先为每个窗口用与 `/judge` 相同的联合估计计算航线上全部**合格航标
候选**，再用**动态规划**把窗口一对一分配到严格递增的航点（可跳过窗口或航点，
禁止逐窗采用局部最佳）。

响应要点：

- `status`：`assigned` / `no_assignment`；没有分配时 `reason` 写明原因
  （无航点、无观测窗口，或所有 (窗口, 航点) 对均无合格候选——附裁决键最小
  候选的违规原因）；
- `library_version`：请求开始即固定的激活特征库版本，计算期间发生版本切换
  也不受影响；
- `assignments`：分配结果及候选证据——窗口索引、航点索引/编号、航标编号，
  以及与 `/judge` 的 `best` 结构相同的候选详情（相位、钟漂、逐事件对应关系、
  漏记明细、未匹配观测）；
- `unassigned_windows`：未分配窗口（索引与观测数）；
- `missed_waypoints`：漏巡航点——首末已分配航点之间未被分配的航点
  （首个已分配航点之前、末个之后未覆盖的航点不计入）；
- `window_candidates`：每个窗口的全部合格航标候选摘要（按 `/judge` 裁决键
  排序，`waypoint_indices` 标注该候选可用于航线上哪些航点），即参与全局
  分配的候选证据。

**分配裁决顺序**（动态规划求全局最优，依次比较，前者优先）：

1. 已分配窗口最多；
2. 首末已分配航点之间的漏巡点最少；
3. 已分配候选的漏记与误记合计最少；
4. 匹配残差总和最小；
5. 钟漂绝对值总和最小；
6. 航点索引序列字典序最小（另以窗口索引序列作稳定性补充，保证结果确定可复现）。

### 5. 查询判定记录

```bash
curl -s http://localhost:8000/judgments?limit=10
curl -s http://localhost:8000/judgments/1
```

### 6. 现场设备批次链（断网续传与冲突恢复）

现场设备把观测按**批次链**上传：每台设备一条链，`sequence` 从 1 递增，
`previous_digest` 引用上一批摘要（首批为空串 `""`）。服务端只保存观测时间戳
与 `/judge` 四项阈值（`tolerance`/`max_drift`/`max_missed`/`max_false`），
**不做判定**；判定仍通过 `/judge` 或 `/patrols/analyze` 显式发起。

**请求体**：

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `batch_id` | `string`（1~128） | 客户端生成的批次编号，设备内唯一 |
| `sequence` | `int≥1` | 批次序号，从 1 递增，必须等于链头序号 + 1 |
| `previous_digest` | `string` | 上一批摘要（64 位十六进制）；首批为空串 `""` |
| `observations` | `object[]`（1~100） | 观测，按 `(captured_at, local_id)` 严格递增；`captured_at` 为采集时刻（Unix 秒），`local_id` 为设备本地编号 |
| `tolerance` | `float>0` | 时间容差（秒），同 `/judge` |
| `max_drift` | `float∈[0,1)` | 最大线性钟漂（无量纲，s/s），同 `/judge` |
| `max_missed` / `max_false` | `int≥0` | 漏记/误记上限，同 `/judge` |

**批次摘要（SHA-256）**：服务端对**排除摘要字段**（`previous_digest`）的规范化
载荷计算摘要——字段为 `batch_id`、`device_id`、`sequence`、`observations`
（`captured_at`/`local_id`）与四项阈值；JSON 序列化采用 **UTF-8 键序**
（键按 UTF-8 序排序）与**紧凑分隔符**（`,`/`:`，无空白），UTF-8 编码后取
SHA-256 十六进制小写。客户端可按同一规则独立复算：

```python
import hashlib, json
payload = {
    "batch_id": "b-001", "device_id": "dev-01", "sequence": 1,
    "observations": [{"captured_at": 1700000000.0, "local_id": "o001"}],
    "tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3,
}
blob = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                  separators=(",", ":")).encode("utf-8")
digest = hashlib.sha256(blob).hexdigest()
```

**上传与链式追加**：

```bash
# 首批：previous_digest 为空
curl -s -X POST http://localhost:8000/field-devices/dev-01/batches \
  -H 'Content-Type: application/json' -d '{
    "batch_id": "b-001", "sequence": 1, "previous_digest": "",
    "observations": [
      {"local_id": "o001", "captured_at": 1700000000.0},
      {"local_id": "o002", "captured_at": 1700000000.5}
    ],
    "tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3
  }'
# → 201 {"status": "accepted", "digest": "49740f…", "chain": {"next_sequence": 2,
#        "expected_previous_digest": "49740f…", …}, …}

# 后续批次：previous_digest 引用上一批摘要（即响应中的 digest）
curl -s -X POST http://localhost:8000/field-devices/dev-01/batches \
  -H 'Content-Type: application/json' -d '{
    "batch_id": "b-002", "sequence": 2,
    "previous_digest": "49740fb763c71461617735f601caeb89cc114aabc76e5dcd79ec05045767016a",
    "observations": [{"local_id": "o003", "captured_at": 1700000001.0}],
    "tolerance": 0.5, "max_drift": 0.0005, "max_missed": 3, "max_false": 3
  }'
```

校验、追加记录、更新设备链头在**单个 SQLite 事务**内完成；写串行化加
`(device_id, sequence)`、`(device_id, batch_id)` 唯一约束兜底，并发提交
不会形成分叉（同一序号只会有一个批次被接受，其余收到 409）。

**幂等重放**：以相同载荷重发同一 `batch_id`（或同一序号）时，服务端返回
**原结果**，仅 `status` 标记为 `replayed`（HTTP 200）；新接受的批次为
`status=accepted`（HTTP 201）。重放在**幂等事务**内完成两件持久化工作：
原子累加该批次的 `replay_count`、刷新其 `last_replayed_at`，并把设备游标的
最近事件改为本次重放；同时保证**三不**——不新增批次、不推进链头
（`last_sequence`/`head_digest`/`batch_count` 不变）、不改写首次受理响应。
并发重放由写锁 + `BEGIN IMMEDIATE` 串行化，计数用
`replay_count = replay_count + 1` 原子累加，不会丢计数。

批次的**列表与详情**始终保留 `status=accepted` 作为首次受理事实（不因重放而
变成 `replayed`），并额外返回含义相同的两个字段：

| 字段 | 说明 |
| --- | --- |
| `status` | 首次受理事实，恒为 `accepted`（重放只在当次上传响应中标记为 `replayed`） |
| `replay_count` | 相同载荷的累计重放次数；从未重放为 `0`（旧库迁移的既有批次也为 0） |
| `last_replayed_at` | 最近一次重放时刻（UTC ISO-8601）；从未重放为 `null` |

**游标**在链头信息之外另返回最近一次事件（受理或重放）的三个字段；新批次
受理后最近事件为该批次的 `accepted`，重放后改为被重放批次的 `replayed`：

| 字段 | 说明 |
| --- | --- |
| `last_event_status` | 最近事件类型：`accepted` / `replayed`；空链设备为 `null` |
| `last_event_sequence` | 最近事件所属批次序号（重放历史批次时为该批次序号，而非链头序号）；空链为 `0` |
| `last_event_at` | 最近事件时刻（UTC ISO-8601）；空链为 `null` |

注意 `updated_at` 只在链头被新批次推进时变化，重放不更新它；要判断「最近
一次设备交互」请使用 `last_event_*`。上传响应（`accepted`/`replayed`）与
三个查询接口的字段分工明确、含义一致，重试安全。

**断网续传**：设备离线期间在本地缓存批次；恢复联网后先查游标，从
`next_sequence` 与 `expected_previous_digest` 续传，已送达的批次按幂等
重放处理，不会产生重复记录：

```bash
# 查询服务端链头：下一批该用哪个 sequence 与 previous_digest
curl -s http://localhost:8000/field-devices/dev-01/cursor
# → {"last_sequence": 2, "head_digest": "5e7aa3…", "next_sequence": 3,
#    "expected_previous_digest": "5e7aa3…", "batch_count": 2,
#    "last_event_status": "accepted", "last_event_sequence": 2,
#    "last_event_at": "…", …}

# 断网期间未确认的批次原样重发：若服务端已接受，返回原结果且 status=replayed
curl -s -X POST http://localhost:8000/field-devices/dev-01/batches \
  -H 'Content-Type: application/json' -d '<与上次完全相同的请求体>'
# → 200 {"status": "replayed", "digest": "5e7aa3…", …}
```

**冲突恢复（409）**：响应 `detail` 携带期望值，设备据此对齐链头后重发：

```json
{
  "detail": {
    "error": "batch_conflict",
    "reason": "stale_chain_head",
    "message": "batch does not extend the chain head: …",
    "expected_sequence": 3,
    "expected_previous_digest": "5e7aa3…"
  }
}
```

- `reason=stale_chain_head`（链头过期）：`sequence` 或 `previous_digest` 不是
  当前链头的后继。用 `expected_sequence` 与 `expected_previous_digest` 重建
  批次重发；若本地链与服务端不一致，以 `GET /field-devices/{device_id}/batches`
  分页核对服务端已接受的批次；
- `reason=payload_changed`（载荷变化）：同一 `batch_id` 或序号已以**不同**载荷
  记录。`detail.existing_batch` 给出既有批次的 `sequence`/`batch_id`/`digest`，
  可用 `GET /field-devices/{device_id}/batches/{sequence}` 核对内容；确认已
  送达后跳到下一批，切勿以新载荷覆盖原编号。

**游标与批次查询**：

```bash
# 分页列出批次（after_sequence 为上一页响应的 next_cursor）
# 每条批次带 status（首次受理事实，恒为 accepted）、replay_count、last_replayed_at
curl -s "http://localhost:8000/field-devices/dev-01/batches?limit=2"
curl -s "http://localhost:8000/field-devices/dev-01/batches?after_sequence=2&limit=2"

# 单条批次详情：观测时间戳、/judge 四项阈值与重放计数
curl -s http://localhost:8000/field-devices/dev-01/batches/2
```

### 7. 多处理方确认与安全归档水位（safe_sequence）

同一台设备的批次会被多个下游处理方消费。服务端为每台设备维护必需消费者
登记、稀疏确认与连续水位，给出**全部处理方都确认完毕、可安全归档的连续
前缀**：

- **登记必需消费者**（只增不删）：
  `POST /field-devices/{device_id}/consumers`，请求体 `{"consumer_id": "..."}`。
  登记后该消费者即参与 `safe_sequence` 最小值计算；重复登记同一
  `consumer_id` 幂等（HTTP 200，`status=already_registered`）。消费者可在任何
  批次上传之前登记；
- **确认批次**：`POST /field-devices/{device_id}/acknowledgments`，请求体为
  `consumer_id`、`sequence`、`batch_digest`（即上传响应中的 `digest`）。
  允许**乱序、并发、重复**提交：
  - 仅接受**已存在且摘要相符**的批次：批次不存在返回 404，消费者未登记或
    摘要冲突返回 409（`reason=digest_mismatch`，`detail.expected_digest`
    给出服务端保存的摘要）；
  - 相同确认幂等：新确认 HTTP 201（`status=confirmed`），重复确认 HTTP 200
    （`status=duplicate`，保留首次 `confirmed_at`）；
- **水位推进**：每个消费者的确认是稀疏集合；保存确认与把连续水位
  （`watermark`：从 1 起连续确认到的最大序号）推进至**首个缺口**在**同一个
  SQLite 事务**内完成，写锁 + `BEGIN IMMEDIATE` 串行化并发确认，推进不会
  丢失、不会回退；
- **安全归档水位**：`safe_sequence = min(所有必需消费者的 watermark)`。
  尚无消费者，或任一消费者尚未确认任何批次时为 `0`；只有前缀
  `1..safe_sequence` 可安全归档。确认只追加记录，**不删除任何原批次**。

**多处理方确认示例**（两个处理方 `archiver` 与 `ml-pipeline` 各跑各的，
乱序确认互不影响）：

```bash
DEV=http://localhost:8000/field-devices/dev-01

# ① 登记两个必需消费者（登记后不可删除；重复登记幂等）
curl -s -X POST $DEV/consumers -H 'Content-Type: application/json' \
  -d '{"consumer_id": "archiver"}'
# → 201 {"status": "registered", "consumer_id": "archiver", "watermark": 0, ...}
curl -s -X POST $DEV/consumers -H 'Content-Type: application/json' \
  -d '{"consumer_id": "ml-pipeline"}'

# ② 设备照常上传链式批次（见上一节），取每批响应中的 digest
#    下面的 $D1 / $D2 分别为 seq1 / seq2 上传响应中的 digest
D1=…; D2=…

# ③ 处理方各自确认（携带 consumer_id / sequence / batch_digest）
#    乱序也安全：archiver 先确认 seq2 不会推进水位（seq1 是缺口）
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"archiver\", \"sequence\": 2, \"batch_digest\": \"$D2\"}"
# → 201 {"status": "confirmed", "watermark": 0, "safe_sequence": 0, ...}
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"archiver\", \"sequence\": 1, \"batch_digest\": \"$D1\"}"
# → 201 {"status": "confirmed", "watermark": 2, "safe_sequence": 0, ...}
#    archiver 已到 2，但 ml-pipeline 尚未确认 → 设备 safe_sequence 仍是 0

# 相同确认重复提交幂等（重试安全）：HTTP 200，status=duplicate
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"archiver\", \"sequence\": 1, \"batch_digest\": \"$D1\"}"

# 摘要不符 → 409，detail 给出服务端保存的 expected_digest
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"archiver\", \"sequence\": 1, \"batch_digest\": \"00…00\"}"
# → 409 {"detail": {"error": "consumer_conflict", "reason": "digest_mismatch",
#        "sequence": 1, "provided_digest": "00…00", "expected_digest": "<D1>"}}

# ④ ml-pipeline 也确认 seq1 后，连续前缀推进到 1
curl -s -X POST $DEV/acknowledgments -H 'Content-Type: application/json' \
  -d "{\"consumer_id\": \"ml-pipeline\", \"sequence\": 1, \"batch_digest\": \"$D1\"}"
# → 201 {"status": "confirmed", "watermark": 1, "safe_sequence": 1, ...}
```

**状态查询**：`GET /field-devices/{device_id}/archive-status` 返回链头、
`safe_sequence`、首个缺口、阻塞消费者与每个消费者的水位/缺口：

```bash
curl -s $DEV/archive-status
```

```json
{
  "device_id": "dev-01",
  "head_sequence": 2,
  "consumer_count": 2,
  "safe_sequence": 1,
  "first_gap": 2,
  "blocking_consumers": ["ml-pipeline"],
  "consumers": [
    {"consumer_id": "archiver", "watermark": 2, "first_gap": null,
     "registered_at": "…"},
    {"consumer_id": "ml-pipeline", "watermark": 1, "first_gap": 2,
     "registered_at": "…"}
  ]
}
```

| 字段 | 说明 |
| --- | --- |
| `safe_sequence` | 所有必需消费者水位的最小值；可安全归档的连续前缀 `1..safe_sequence`。无消费者或任一消费者水位为 0 时为 `0` |
| `first_gap` | 安全前缀之后的首个待归档序号（恒为 `safe_sequence + 1`） |
| `blocking_consumers` | 水位等于该最小值且尚未追平链头的消费者（按编号排序）；全部追平或链上无批次时为空列表 |
| `consumers[].watermark` | 该消费者从 1 起连续确认到的最大序号（乱序确认只填空缺，不越过首个缺口） |
| `consumers[].first_gap` | 该消费者水位之后的首个缺口；已追平链头时为 `null` |

归档作业只需轮询该接口并归档 `sequence <= safe_sequence` 的批次；`consumers`
与 `blocking_consumers` 可用于定位卡在哪个处理方。确认记录与原批次始终保留，
不做物理删除。

## 判定模型与裁决规则

**观测模型**：观测时钟相对真时存在线性钟漂与固定相位差，

```
t_obs ≈ (1 + drift) · T_true + c
```

航标闪光真时时刻为 `T = ψ + n·period + f_j`（ψ 相位，n 周期序号，f_j 周期内闪光时刻）。

**估计流程**：

1. 在 `[-max_drift, +max_drift]` 内自适应网格搜索钟漂（步长保证整个观测跨度上
   的残差变化不超过容差 1/4）；
2. 每个钟漂下用 `(t/s − f_j) mod period` 循环投票估计相位（滑动窗口 + 圆周均值，
   周期边界 0 与 period 相接处的聚集不会被切断）；
3. 取投票数最高的种子，交替执行「循环匹配 → 约束最小二乘」细化至收敛。

**循环对齐与计数约定**：

- 期望闪光序列按周期连续逐周期生成，跨周期边界不产生伪缺口；
- 观测与期望一对一匹配（残差 ≤ `tolerance`）。重叠容差窗下**不按残差贪心**
  （贪心会抢占共享期望闪光、压低配对数，虚增漏记/误记），而是求最优匹配，
  依次按以下三级规则裁决：
  1. **配对数最大**；
  2. 配对数相同时，**配对残差绝对值总和最小**；
  3. 总残差仍相同时，按配对的**（观测索引, 期望索引）序列字典序**最小裁决，
     保证结果确定可复现。

  相位/钟漂迭代细化与最终候选构建使用同一匹配规则；逐事件对应关系、
  漏记、未匹配记录均由该匹配结果导出；
- **漏记**：仅统计首个与末个匹配闪光之间的期望闪光缺失——观测起止处的
  不完整周期不计入；
- **误记**：未匹配到任何期望闪光的观测。

**候选裁决顺序**（依次比较，前者小者胜）：

1. 漏记 + 误记总数；
2. 匹配残差总和；
3. 钟漂绝对值；
4. 航标编号（字典序）。

候选合格条件：`missed ≤ max_missed` 且 `false ≤ max_false` 且 `|drift| ≤ max_drift`；
最佳候选为合格候选中裁决键最小者。

## 本地开发

```bash
pip install -r requirements-dev.txt
pytest                       # 运行测试（算法 + 巡检分配 + API + 批次链 + 多消费者确认共 74 例）
uvicorn app.main:app --reload   # 本地起服务，默认使用 ./data/navlight.db
```

## 项目结构

```
app/
  main.py    FastAPI 路由与应用工厂
  align.py   相位-钟漂联合估计与循环对齐核心算法
  patrol.py  巡检整体核对：窗口合格候选 + 全局最优分配动态规划
  batches.py 现场设备批次：规范化载荷与 SHA-256 链式摘要
  models.py  请求数据模型（Pydantic）
  db.py      SQLite 持久层（不可变特征库版本 + 判定记录 + 设备批次链 +
             多消费者稀疏确认与安全归档水位，含首次迁移）
  config.py  环境变量配置
samples/
  library.json                示例特征库
  generate_request.py         合成示例判定请求
  generate_patrol_request.py  合成示例巡检核对请求
tests/                  pytest 测试
Dockerfile / docker-compose.yml
```
