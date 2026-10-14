# Session 空闲超时自动提交调度器 VibeCoding 实施文档

- 状态：**采用临时 shard 租约、真实 Redis I/O 生命周期跟踪和有界关闭观察；可靠性与检验边界见第 10、13、14 节。**
- 来源：[内核超时自动触发技术方案](https://bytedance.my.larkoffice.com/docx/KUbYdV4SJoxxsyxhgz1mW1wMypb)，最近读取 revision 3059；与文档不同之处以本轮用户明确修正为准。
- 代码基线：OpenViking `7d77e3d68fea82070a0b700a9acd64c40e7f4a9a`；本轮对比原基线 `6e1030d9951c4c19048e4758ef130be5da4abfe0` 后的 7 个提交。
- 更新时间：2026-10-11。
- 附录：[逐文件代码骨架](session-idle-auto-commit-scheduler-code-design.md)。主稿说明行为，附录列出字段、函数签名及 Lua 契约；省略体只省去实现细节，不作为行为验收证据。

## 1. 当前结论与本轮变更

采用业务统一接入、Redis 资源桶、调度桶、注册表及游标的结构，首期支持 session 的 IDLE_ONESHOT。调度和回调实例均通过 Redis 临时领取 shard，持锁处理全部积压后释放，轮内按 shard_id 递增。

本轮明确移除资源伴随状态 V、资源更新 revision、Redis 失败记录及元数据格式版本字段。报告按 R → M 执行；调度按读 R → 写 B → 原子写 G/C 执行。仅 report 写入/更新 R 的 score，调度只按已发布 C 删除源成员，不把调度读取与报告更新混为同一条调用链。

**有两个保证不能由这些步骤直接推出：** R 写完、M 尚未写完时退出会留下缺失元数据；续租失败后取消本地任务不能撤销已发出的跨槽 Redis 请求。前者由完整 item_id 回源兜底；后者仍是当前共享调度桶在自动接管时的残余风险，见第 10.3 节。本稿不把这两点写成已证明安全，也不擅自加入新的桶隔离标识。

### 1.1 最新内核适配与本轮放宽保证

Redis 是可选能力，默认 `idle_backend=legacy` 与既有提交能力保持不变。本轮覆盖旧的锁内 idle 检查及 idle 归档恢复方案：`SessionService.commit_on_timeout` 加载最新 session/meta，在服务层完成纯检查；通过后直接调用原 `Session.commit_async`。Session 的通用提交内部不增加 Redis 或 idle 判断，也不为 timer 拆分提交实现。

检查与提交不在同一把业务锁内原子执行。检查通过后、原提交读取消息前追加的新消息允许一起提交；policy 或 timeout 在此窗口变化也不触发一次新的最终 idle 检查。`last_idle_commit_accepted_at` 仅为外层 best-effort 接受记录，写失败不改变 accepted，不提供归档查证或修复缓存。这一方案不承诺严格空闲边界、零重复提交或每次 accepted 都能持久记录。

| 最新代码契约 | 本方案保留方式 |
|---|---|
| Session.commit/commit_async、SessionService.commit/commit_async 的 enable_working_memory | 原签名与三态透传保持不变，False 不能当作未传；不新增 idle 专用 commit API |
| MemoryPolicy.enable_working_memory 默认 False | idle 调用不覆盖 WM，继承当前 session/user policy；保留显式启用策略 |
| accepted 和原有 skipped 结果包含 effective_enable_working_memory | 直接返回原 commit 结果，不重建丢字段的精简结果 |
| QueueFS 的 memory_policy_version 及完整 policy 快照 | 沿用原序列化、兼容读取和恢复，不把它当成 Redis M.schema_version |
| .done 的 enable_working_memory 与旧字段兼容；关闭 WM 可无 summary | 原 ArchiveStore 能力保持不变；timer 只以原 commit API 返回 accepted 为成功，不等待后台任务或摘要 |

原通用提交已有的 ready、QueueFS、恢复及 Working Memory 契约均继续保留，不成为 idle 接受时间的存储或恢复机制。

## 2. 已确认约束

1. 业务只使用 report、unregister、register_handler 三个方法，通过依赖注入取得协议；调度、Redis、消费、生命周期均封装在内部。
2. 各业务自行注册处理器，通用模块不硬编码 session 类型、policy 或并发映射。
3. 首期 IDLE_ONESHOT；FIXED 仅预留，调用时明确不支持。
4. 消息成功追加后直接 await report，无内存去抖；按先 R 后 M 的顺序写 Redis。
5. 不使用 V、源关闭水位或更新顺序号；接受同 item_id 近间隔并发写入时旧请求覆盖新请求的误差。
6. 只处理实际到期数据：U 为当前时间去掉秒和毫秒，扫描 `(C,U]`；U=C 或没有数据就跳过。
7. 不使用 group_id。分钟调度桶全部写完后，G/C 同槽 Lua 注册并推进游标；已发布桶不再生产。
8. 元数据 TTL 为 `max(2×timeout,24小时)`；完整 item_id 承载 type/account/user/local_id，M 缺失时仍可回源。
9. 已投递任务由服务层检查加载时的最新状态；未到最后消息到期时间则 deferred。检查通过后不再做持锁最终 idle 校验，期间追加的新消息允许一起提交；新活动的未来计时独立保留。
10. idle 冷却统一使用 `last_idle_commit_accepted_at`，它是服务层 best-effort 接受记录，不使用通用自动提交统计时间作判断，也不建立 idle 归档指针或修复缓存。
11. timeout 修改不批量重写旧 R；回调冷却校验不通过时 report 新计时，due 为本次检查时间加当前 timeout；report 成功后完成旧事件。
12. 同一调度事件最多三次调用机会，包含首次；耗尽只写日志并条件清除 B，不在 Redis 保存失败记录。
13. 每个实例每个角色一次领取一个 shard，处理全部积压后释放，再尝试更大的 shard；一轮结束从头开始。
14. 锁 TTL 120 秒，每 20 秒续租；续租失败立即停止本次处理，不续跑旧任务，后续重新领取必须使用新 token。
15. 不处理同名资源删除重建；不增加 incarnation、持久待注册集合、outbox 或零漏触发承诺。旧扫描器仅参考业务处理步骤。

## 3. 业务契约与内部边界

```python
from typing import Protocol
from openviking.scheduling.expiry import (
    HandlerOptions, ItemIdentity, ExpiryHandler, ExpiryRegistration, ExpiryRegistrationResult,
)


class ExpiryScheduler(Protocol):
    async def report(self, task: ExpiryRegistration) -> ExpiryRegistrationResult:
        """先写入资源计时，再创建或刷新元数据；等待本次结果。"""
        ...

    async def unregister(self, identity: ItemIdentity) -> None:
        """幂等注销当前计时及元数据；已投递事件按业务当前状态结束。"""
        ...

    def register_handler(self, item_type: str, handler: ExpiryHandler, *,
                         options: HandlerOptions | None = None) -> None:
        """业务启动时注册自己的处理器及处理能力。"""
        ...
```

| 类型 | 字段及约定 |
|---|---|
| ItemIdentity | item_type、account_id、user_id、local_id |
| ExpiryRegistration | identity、due_at_ms、timeout_seconds、mode=IDLE_ONESHOT；无 revision |
| ExpiryRegistrationResult | accepted_due_at_ms；不再返回需要版本比较才能判断的 DUPLICATE/STALE |
| HandlerOptions | concurrency 默认 4、max_attempts 默认 3（含首次），同类型各副本保持一致 |
| CallbackRequest | identity；框架不透出 claim、缓存命中、游标或 Redis 状态 |
| CallbackResult | COMPLETED / RETRY 与原因码 |
| ExpiryHandler | async handle(request) -> CallbackResult |
| ExpiryError | 业务错误码、错误信息、retryable |

`api.py` 定义公开契约，包入口只重导出上述协议/类型。业务不构造 store、keys、runtime，不调用启动、关闭、冻结或 ACK。SessionService.configure_expiry 委托 session_expiry.install_session_expiry，业务模块自行安装上报适配器并注册 handler；core 只组装、注入和启停。

`api.py` 中的 `SUPPORTED_ITEM_TYPES` 是类型信息的唯一来源。内部 HandlerRegistry 只允许注册清单内类型，并在启动消费前冻结；report 在写 Redis 前按同一清单校验。独立调度器直接使用代码清单定位未发布桶，不访问 Redis 类型目录；处理器和选项只在本地。只有 G 注册后的桶可消费。

同一 namespace 的回调副本加载相同的业务处理器集合；缺 handler 的类型不领取，限频报警，不能假设其他实例会自动接管该类型。session 只有选择 Redis idle backend 时才安装本业务接入，其他业务启用 timer 不应顺带启动 session 的 Redis 计时。

完整 item_id 使用 `item_type:encoded_account:encoded_user:encoded_local_id`，身份组件 UTF-8 后用无填充 URL-safe Base64，严格解码并重编码验证，禁止花括号或冒号改变 key 结构。编码规则和 xxhash64 seed 固定，不增加逐资源格式版本字段。

## 4. Redis 数据结构与 report

`<...>` 是占位符，只有明确列出的 `{...}` 才是 Redis hash tag；service_key 为编码后的共享 service_name。

| 名称 | Key 示意 | 内容 |
|---|---|---|
| R 发布源 | `expiry:publisher:source:<service_key>:<shard>` | ZSET，member=完整 item_id，score=due_at_ms |
| M 元数据 | `expiry:metadata:<service_key>:<item_id>` | HASH，item_type、local_id、account_id、user_id；带 TTL |
| B 消费桶 | `expiry:consumer:bucket:<service_key>:<type>:<shard>:<minute>` | ZSET，初始 score=0；minute 为窗口结束边界 |
| G 注册表 | `expiry:consumer:registry:{<service_key>:<shard>}` | ZSET，member=type:shard:minute，score=minute |
| C 发布进度 | `expiry:publisher:progress:{<service_key>:<shard>}` | 最后已发布的包含式分钟上界，UTC 毫秒 |
| owner | `expiry:publisher:owner:{<service_key>:<shard>}` 或 `expiry:consumer:owner:<type>:{<service_key>:<shard>}` | 当前领取 token，TTL=120秒；publisher/consumer 分开 |
| namespace | `expiry:namespace:<service_key>` | 初始化状态、固定 N/hash/编码配置及内部类型名称目录 |

仅 G/C/publisher owner 需要同槽发布；R、M、B 各自的单 key 操作不必同槽。没有 V 或 Redis 失败存储 key。

### 4.1 元数据与格式字段

M 只保存身份字段。原设计的 `schema_version` 用于区分序列化格式；当前没有多版本解析或迁移流程，字段本身不能完成兼容处理，**本期删除**。配置仍比较实际的 shard_count、hash_name、hash_seed、identity_encoding，防止不同实例使用不同寻址规则；这属于命名空间配置校验，不是给每条资源增加版本。

TTL 由框架内部统一计算，业务不另传 TTL：

```text
metadata_ttl_ms = max(timeout_seconds × 2000, 86_400_000)
```

创建、续期使用同一公式，从 Redis 执行时开始计时。timeout≤12小时取24小时，超过12小时取两倍 timeout。ensure_metadata.lua 在单 key 内检查身份、必要时 HSET、最后 PEXPIRE，避免 EXISTS/EXPIRE 的临界过期竞态。

### 4.2 report 的真实顺序和错误语义

1. 校验身份、模式、正 timeout 和整数范围，确认任务类型已发布。
2. 调用只访问 R 的 upsert_resource.lua，用源节点 TIME 校正为未来 due，然后 ZADD 覆盖 score。
3. R 明确成功后，调用 ensure_metadata.lua 写 M 或续 TTL。
4. 两步均成功才返回 accepted_due_at_ms。R 失败不继续写 M；R 成功、M 失败则接口报错，但不回滚已写 R，避免覆盖或删除并发的新报告。

先 R 后 M **不能保证 R 中每个成员都有 M**：两步间退出、M 写失败或后来过期都会造成缺失。完整 item_id 回源是必要正常路径。M 的 Redis 网络错误与明确缺失仍区分，前者按可重试错误处理。

本期接受上报乱序覆盖，不做 CAS、不保留更新 revision、不识别重复请求。报告重试可能覆盖较新 due，或在原任务已处理后建立新计时；不声称资源级无限幂等。删除 R/M 可重复执行，但并发删除与新上报的生命周期顺序由业务负责，同名重建不在范围内。

### 4.3 report 与调度并行时的时间边界

report 不读取 C；调度读取 C 时也不通过该调用更新 R。不同请求仍可以同时执行 report 和调度，不能因为代码属于两个阶段就认为全局互斥。

保留一个无需伴随状态的单 R Lua 约束：

```text
accepted_due = max(requested_due, source_redis_now_ms + 1)
ZADD R accepted_due item_id
```

TIME 与 ZADD 在同一脚本中执行；调度的 U 也取该 R 所在 Redis 节点的 TIME。节点时间正常前进时，调度取得 U 后到达的 report 会写到 U 之后；之前写入的未来项随时间到期后可扫描。此设计不比较业务请求先后，仅确保写入时 due 在未来。

调度取得 U 的脚本只读时间、不修改 R。若改用另一个节点时间或在客户端算完时间后长时间滞留再 ZADD，这个保证不成立。未保留历史关闭水位时，也不承诺任意时钟回退下无遗漏；U<C 时暂停该 shard 并报警，主节点时间同步及切换后的时钟偏差属于上线检查。

## 5. Session 上报与注销

在 Session._append_messages_authoritatively 接入，覆盖 HTTP、MCP、内部直接 Session 调用。复用已有路径锁追加消息并保存 meta，构造身份、last_message_at 和有效 policy 的不可变快照；释放锁后立即 await 适配器 report，一次追加批次报告一次。没有 timer_revision，也不修改已有业务版本机制。

适配器使用 `due=last_message_at+timeout`。无消息创建不计时；禁用或 timeout=0 不新增 idle 计时。追加前的 meta 重读失败时，只要同一路径锁内的新 meta 已成功持久化，就以该刚持久化状态构造快照并补 report；真正无法构造快照时才记录失败。消息已持久化而 report 最终失败时保留消息成功结果并记录计时失败，不把已写消息表示为回滚；无 outbox，后续不再活跃时仍可能漏触发。

已发布到 B 的旧事件不能被新 append 撤回。callback 由 SessionService 加载最新 session/meta，在外层检查 `last_message_at + 当前 timeout`；尚未到期则返回 `deferred/idle_activity`，由 handler 重新 report 成功后才完成旧事件。检查通过即调用原通用提交，不在 Session 提交锁内再次检查 idle；期间追加的新消息允许随这次提交一起归档。Session 提交内部不感知 deferred 或 Redis。

业务删除成功后 await unregister，依次删除 R 成员和 M，不扫描历史 B。部分失败可重试；已入 B 的任务由业务 NotFound 完成。Redis 客户端固定在专属 I/O loop，调用方立即提交并等待 future，不做去抖；session handler 和业务锁仍运行在 API loop。

## 6. 调度：扫描 R，写完 B，发布 G/C

### 6.1 时间与跳过规则

```text
U = floor(source_redis_now_ms / 60000) × 60000
scan_range = (C,U]
window(M) = (M-60000,M]
```

例如 `12:34:04.234 → U=12:34:00.000`。due 等于 U 可处理；due 为 `12:34:00.001` 等到下一分钟。资源 due 不截断，桶 minute 为 ceil(due/60000)×60000；发布后 C=M。

- U=C：跳过扫描；可先补做已确认 C 以下的清理。
- U<C：时间异常，停止本次处理并记录，不倒退 C。
- `(C,U]` 无数据：跳过，不创建空 B、不写 G、不空推进 C。
- 初始 C=0，READY 前为各 shard 幂等 SET NX 0。扫描可直接跳过空时间跨度，不从 Unix epoch 逐分钟循环。READY 后 C 缺失视为状态损坏。

### 6.2 持锁处理全部积压

1. 取得 shard 租约，启动续租；读取 C，补清理已发布源成员，读取 U。
2. U>C 时，按 score/member 分页查找下一条到期成员，确定下一个有数据的分钟 M。没有成员即结束本轮范围。
3. 使用 `SUPPORTED_ITEM_TYPES`，确定 C 未覆盖 M 后清理该分钟未注册的临时 B，按当前 R 重建；不恢复旧内存分组。
4. 分页读该分钟所有成员，按 type 批量 ZADD 0 到 B。每页有限，但不因页数/分钟数达到某个配额而交出 shard。
5. M 全部读完且 B 写入结果明确后，单条 Lua 检查 owner token 和预期 C，注册实际有数据的 B 并设 C=M；随后按 C 清理 R。
6. 继续后续有数据分钟。扫描追平本轮 U 后重新检查当前上界和新增到期积压；确认无可处理数据，结束并释放锁，继续更大的 shard。

扫描期间成员可能续期或被删除。若选定分钟在真正分页时已经变空，只跳过该分钟，不发布；本地查询下界向前移动以避免空转，持久 C 保持不变。若已读成员随后活跃，则本次事件仍可投递，新 due 留在 R。

G/C Lua 允许跨过已经扫描确认为空的分钟，不再要求 M=C+60000，只要求 M>C 且已完成中间扫描。分钟结束前不推进 C。读取 U、读 R、写 B 都不写入或更新 R 的 score；源删除是发布后单独的清理步骤。

### 6.3 分页与恢复

源页使用 `(last_score,last_item_id)`，不使用递增 OFFSET。单 key Lua 根据游标当前 rank 定位；末项续期或被删时可按 score/member 二分定位，约 O(log²N+page_size)，不全量扫描同 score 集合。

重领后丢弃旧内存状态，重新读 C。C 已覆盖的分钟不清桶、不重投，G 已被消费摘除也不能重新发布。确定未注册的分钟按当前 R 重做。空范围不推进 C，曾写入但未注册、成员后来全部续期的 B 可能成为临时遗留 key；不注册它，不给已注册 B 设置短 TTL，必要时离线核对清理。

发布响应未知时，租约仍有效可读 C 查证：C 已推进则确认；C 未推进不证明旧请求永远不会执行，保留已写完 B 并重试同一个发布，不能回到 reset/stage。租约失败立即取消本次处理，不在重新领取后接着执行旧的挂起函数。已发出跨槽命令的残余影响见第 10.3 节。

源清理使用 ZREMRANGEBYSCORE R -inf C，保留正常未来 due。复杂度 O(logN+K)，大积压和集中到期需测单命令耗时；若过大再改为单 key 有界批删，不把分页投递等同于删除已无大 key 风险。

## 7. Lua 与故障处理

| 脚本 | 原子域 | 职责 |
|---|---|---|
| upsert_resource | R | TIME + 未来 due + ZADD，不读 C、不写伴随状态 |
| source_time | R 所在 slot 的只读脚本 | 返回源节点 TIME，不修改 R |
| ensure_metadata | M | 校验身份、创建/刷新和 TTL |
| read_source_page | R | score/member 有界分页 |
| publish_minute | G/C/scheduler owner | 比较 token、预期 C，注册非空分钟并推进 |
| read_registry_page | G | 已发布桶有界发现 |
| claim | B | 原子领取并计数，最后一次超时只返回待终结项，不再领取 |
| ack | B | 按 claim_score 条件删除，成功和耗尽终结共用 |
| acquire/renew/release_owner | 对应 owner | NX 领取、比较 token 续期、比较 token 释放 |

脚本写前检查参数、key 类型；Lua 运行异常不提供跨命令回滚。删除 R 使用单 key ZREM，删除 M 使用 DEL，不再需要注销伴随状态脚本。

| 故障 | 处理 |
|---|---|
| R 成功、M 失败 | report 报错，保留 R；完整身份仍可回源 |
| M 缺失 | 解码完整身份，正常调用业务 |
| 写部分 B 后取消/退出 | 未发布不消费；重新领取后读 C 和当前 R |
| 发布后响应丢失 | 查 C 或在同一有效租约下重试原发布，不重写已发布 B |
| 清理失败 | 以后只补清理，不重投已发布分钟 |
| 续租失败 | 本次租约立即失效，取消处理；旧 token 不复活 |
| callback 未耗尽且失败 | 保留 B，超时后重领 |
| 第三次失败/超时 | 写耗尽日志，然后条件 ACK；无 Redis 失败记录 |
| report 重新计时失败 | 旧 B 不 ACK，按剩余机会重试 |

## 8. 回调、三次机会与仅日志终结

回调实例持有 shard 后，分页发现 G，按 type 的进程级并发处理全部可执行积压。发现循环需继续遍历，不能被最老页或单个慢类型阻塞。重试等待期间仍续租；已注册桶清空、在途任务收尾后释放 shard。未知 handler 的桶保留并报警，不无限持锁等待不存在的处理器。

```text
score = attempt × 2**40 + started_at_seconds
initial_score = 0
session attempt = 0,1,2 → 最多三次机会
```

claim 在 B 内原子占用一次机会，每个 claim 至多调用一次 handler。进程可能领取后、调用前退出，因此实际调用可能少于三次；不退还结果不明的机会。新事件包括冷却重投产生的事件有独立预算。各类型上限由业务 HandlerOptions 注册，session 固定采用3。

按 attempt 层有界查询超时项；新项优先但为重试保留预算。第三次超时的成员不生成第四次 claim，而是返回 exhausted 列表，由 consumer 写日志后条件 ACK。脚本不直接删除这些项，避免正常路径中先删后无法输出日志。

明确 RETRY/异常且最后机会耗尽时，consumer 同样先写结构化日志，再条件 ACK。日志包含资源身份、桶分钟、claim_score、机会数、失败原因及是否结果未知，不记录消息正文。Redis 不保存失败 HASH、索引、死信或重放记录。ACK 失败时留 B 以后重复终结，日志可能重复；日志输出与 Redis 删除不构成事务，不能承诺故障下日志必达或恰好一次。

COMPLETED 后 ACK 出错，只重试同一条件 ACK，不再调用 handler、不算一次新的业务失败。最后一次 accepted 后 ACK 是否生效无法查清时，最多记录结果未知并清理，不额外调用业务来突破三次上限。条件 ACK 不匹配则不删除新 claim。B 空后单独摘 G，不删新活动 R/M。

回调处理超时与 shard 租约是独立参数；前者决定任务何时可重领，后者每20秒续租，不以任务耗时长为由主动释放。

## 9. Session idle 时间与冷却重投

### 9.1 服务层 best-effort 接受记录

SessionMeta 仅增加 `last_idle_commit_accepted_at`，缺字段按空串处理；现有普通统计字段保持兼容但不参与 idle 决策。此字段不是原通用提交 ready 的原子记录，不存储 idle archive 指针，不提供 idle readiness 查证或恢复。

原 `Session.commit_async` 返回 `status=accepted` 即为本次 timer 业务成功，不等待后台任务。之后服务层 await `_record_idle_commit_time(session_id, ctx, accepted_at)`：复用既有 session root exact 路径锁，锁内直接重读最新 raw meta 字典，仅 patch `last_idle_commit_accepted_at` 和 `updated_at`，写入传 `lease_ref=lease`，不以检查时的旧 Session 对象全量保存。接受时间只以有效新旧值的 `max` 合并：已有有效更大/相等时间时不写，坏旧值记录日志后可覆盖，避免较早并发调用迟到回写旧成功时间。

记录更新失败只写日志，仍原样返回 accepted；若 session 已删除，跳过记录，不创建目录或重建 meta。该 helper 的读取/锁/写异常属于 best-effort 记录失败；入口加载、纯检查和原 commit 的异常则向外传播，不能伪装成无消息或 accepted。无消息等 skipped 和 deferred 不更新时间。

### 9.2 timeout 变更、外层检查与重新报告

timeout 更新不批量改已有 score。`SessionService.commit_on_timeout` 加载最新 session/meta 后，以本次 `now` 调用纯检查 `_check_timeout_commit(session: Session, now: datetime) -> Optional[dict]`，顺序如下：

1. 明确 session 已删除：skipped/not_found，handler 转 COMPLETED；其他加载/读取异常向外传播。
2. policy 禁用或 timeout≤0：skipped/idle_disabled，不重新计时。
3. 没有可提交消息：skipped/no_messages，不更新时间，避免无内容 session 反复重投。
4. 有消息且 `now-last_idle_commit_accepted_at < current_timeout`：返回 deferred/idle_cooldown，due 为本次检查 `now + 当前 timeout`。
5. 冷却通过但 `now < last_message_at + current_timeout`：返回 deferred/idle_activity，due 为 `last_message_at + 当前 timeout`。
6. deferred 仅由 SessionService 和 SessionTimeoutHandler 掌握，不修改 meta 或调用 commit。handler 统一 report；R→M 两步成功后才返回 COMPLETED、ACK 旧 B，失败传播以保留旧 B。旧 B 的 ACK 不删除未来 R/M。
7. 检查返回 None：直接 `await session.commit_async(keep_recent_count=0, persist_keep_recent_count=False, record_auto_commit_success=True)`，不新增 idle 专用提交入口，不在提交锁内进行最终 idle 判断。原结果为 accepted 才执行 best-effort 接受时间记录，然后返回原结果；commit 异常传播。

例如最近 idle accepted 为12:00，旧事件12:10到期，而 timeout 已调大为30分钟。12:10回调因冷却未满足，report 新 due=12:40；不是回填12:30，也不复用旧 timeout。这是“当前时间+当前 timeout”语义。最后消息尚未到期的活动延后则使用最后消息时间加当前 timeout。

服务层复用 `parse_iso_datetime` 和 `get_current_timestamp` 处理时间，纯检查只读加载状态，不获取提交锁、不写 Redis 或 meta。重投不是成功提交，不更新接受时间；重复重投可能顺延 R，仍接受无版本顺序保护的覆盖行为。首次无接受时间不构造虚假冷却窗口。

检查完成后的追加、policy/timeout 变化不触发最终复检，允许新消息一起提交；检查与 commit 之间的并发以及接受时间记录失败都可能弱化冷却、导致重复调用。这是本轮明确放宽的保证，不要求从原 ready 反查或补写该字段。

### 9.3 与 Working Memory 策略的关系

idle timeout 的外层触发检查与 Working Memory 策略独立。直接调用原 `commit_async` 时不传一次性 WM 覆盖，沿用默认 `enable_working_memory=None`，继承 session/user 设置，未配置时采用默认 False。原始归档、QueueFS 及符合 policy 的长期记忆提取继续存在。

原手工提交的 `enable_working_memory=True/False` 只覆盖本次有效 policy，不写回用户或 SessionMeta 默认 policy；原队列快照、`memory_policy_version` 及恢复兼容契约不变。本方案移除的仅是 Redis 元数据中的 schema_version，不删除原通用提交能力。

WM 关闭时没有 `.overview.md` 是正常结果，不据此重试或推迟 timer ACK。后台任务继续使用既有失败/恢复流程，不回流为 timer 的三次回调重试；accepted 后 timer 只处理自身 ACK。

## 10. 实例抢占、续租与退出

### 10.1 一次一个 shard，轮内递增

每次抢占生成独立随机 owner token，不依赖进程身份或 Kubernetes。每个角色独立遍历 `0..N-1`，一次最多持有一个 shard。抢不到继续尝试更大的编号；抢到则处理全部积压，完成才释放；遍历和轮间节奏由worker自己控制：scheduler本轮后等下一分钟边界，callback本轮后等callback_poll_seconds，再从0开始。示例编号1..256与代码0..255仅是编号约定差异。

不使用成员名单、Rendezvous Hash、固定 shard 分配或每批强制移交。分页和 pipeline 有界只是控制内存与单命令工作量，不是释放锁的依据。热点 shard 若持续产生积压，可能长期由同一实例处理；其余实例继续争抢其他 shard，不承诺严格轮转公平或按资源量均衡。

### 10.2 token 操作、单 shard 执行与 worker 节奏

职责只拆到必要边界，不引入通用 worker 框架：

| 对象/文件 | 职责 |
|---|---|
| ShardLeaseCoordinator / leases.py | 仅 try_acquire、renew、release，调用既有 Redis token 操作；不遍历 shard、不创建业务/heartbeat任务、不决定轮询节奏 |
| ShardLease / models.py | owner、保守 monotonic 截止、lost 与 revoked 两种单向状态；ensure_active 检查两者 |
| ShardExecution / shard_runner.py | owner、shard_id、ensure_active、spawn 工厂、wait；强引用跟踪 processor 子任务和辅助 waiters，汇总异常/unknown |
| ShardRunner / shard_runner.py | run_if_acquired 一次执行一个 shard；后台 heartbeat、stop grace、失锁撤销取消、有界 join、同 role pending 保护 |
| publisher.py / consumer.py | 自己遍历所有 shard，分别控制 publisher 下一分钟边界、callback poll 节奏；只把单 shard process 交给 runner |
| runtime.py | 构造同一个 runner 并注入两个 worker，保留角色任务，协调有界停止和依赖关闭 |

1. `run_if_acquired(role, shard_id, process, stop)` 在 stop 时不领取、busy 返回 False；成功取得新随机 token 后构造全新 execution/process。每 role 只允许一个 active，**同 role 任一 shard 尚有 pending 都禁止再抢锁**，抛 `shard_not_stopped`；另一角色独立。
2. Coordinator 的 try_acquire 使用 SET NX PX 120000；renew 比较 token，runner 后台每20秒调用。请求异常、超时、取消、token 不同/缺失或响应晚于之前本地截止，均保守置 lost；迟到成功也不能复活 lease。截止为请求发起 monotonic+TTL，不按收到响应重新延长。
3. lost 表示 ownership 已不可信；revoked 表示本地执行已撤销，正常清理也会设置，**不把正常撤销冒充失锁**。`ShardExecution.ensure_active` 首先检查 `asyncio.current_task().cancelling()`，若存在取消请求，**同步置 outcome_unknown 并抛 CancelledError**，再检查 lease 的 lost、revoked 和截止。即使 handler 吞掉取消并返回 COMPLETED，后续检查点也不能放行 ACK，不依赖较晚执行的 child done callback 才标记未知。spawn 先检查，再创建执行工厂的 task，业务 processor 不取得续租/释放接口。
4. runner 监视 processor、heartbeat、lost 和有效截止。正常 stop 开始 shutdown grace，heartbeat 继续；**grace 期间仍监视失锁及有效截止**，失锁立即 revoke/cancel，不只等待慢 processor。grace 超时、外层取消或 processor 返回却仍有 children，均按 forced 收尾。
5. 收尾先 revoke，再 cancel process、heartbeat、children/waiters；若 renew 正在请求中，取消前同步置 lost，不能被抑制取消后的成功响应复活。join 用 `asyncio.wait(timeout=request_budget_seconds)` 有界观察，不能用取消后无界 await/gather，或把 wait_for 超时当作任务已经终止。
6. 未终止 task 在等待前放入 runner pending 强引用（含 acquire/release/heartbeat），done callback 读取异常且只摘除自己的集合；二次取消 cleanup 也不能孤儿化 task。预算结束仍未退出则报 `shard_not_stopped`，worker 必须向 runtime 传播而非继续下一 shard。旧 tasks 真实退出后才允许新 token/新 execution；不恢复旧函数挂起点。
7. process/children 普通业务异常但任务全部终止、无 forced/unknown 且 ownership 仍可信，允许停止 heartbeat 后比较 token 释放，再传播异常；**不是所有业务异常都等于失锁**。forced、lost、未知结果或本地工作未退出均保守停止续租、留 TTL，不主动释放。release 本身也有界观察，超时/未知不能假装已交接。

consumer 每个 callback 用 execution.spawn 工厂创建并被 runner 跟踪；poll 用 execution.wait，不自建旧式 lost task 等待。正常 stop 可等已启动 callback，期限/失锁由 runner 在外层持续监视。仅旧 tasks 全部退出后才清共享队列、reserved 和 task state，避免旧 finally 释放槽污染新轮；retry 层缓存在每次单 shard 执行边界清理，不跨租约累积。

所有有界等待都以事件循环能继续调度为前提。async task 不能消除进程暂停、阻塞事件循环，也**不能强杀抑制取消的协程**；pending 强引用和禁止同 role 重领是本地生命周期防护，不证明 Redis 请求已撤销。

### 10.3 必须保留的可靠性限制

本轮按用户指定的“续租失败停止、下次重新抢锁”实现，不加入按 token 隔离的 B。该措施可以停止本地后续动作，**不能撤回 Redis 已接收但尚未完成的 ZADD/UNLINK，也不能让跨槽的检查与写入原子化**。

因此仍有窗口：旧写入已发出或进程暂停在检查之后，新实例因 TTL 到期接管并发布同一个 B，旧请求随后执行。G/C/owner Lua 会拒绝旧发布，但无法拒绝另一个 slot 上的旧 B 写入。加长 TTL、取消 coroutine、换新 token 均不构成完整防护。

上线前需要故障注入验证这个边界，并决定是否接受残余风险。如果要求在暂停、分区和请求延迟下仍严格防止旧写入影响新 owner，需要桶隔离或 B 所在槽的写入版本校验等进一步变更；本轮不替用户自动选择。回调侧的 claim/条件 ACK 只能保护消费记录，不能抵御生产者后来无条件 ZADD 0。

## 11. 配置、启动与文件清单

顶层 `expiry` 配置保留 `service_name` 并增加 `publisher_separate_process=true`，删除 `reporting_enabled`、`scheduler_enabled`、`callback_enabled` 三个可组合角色开关。默认 `session_auto_commit.idle_backend=legacy`；只有 `enabled=true && idle_backend=redis` 时才整体启用 report、publisher、consumer。`service_name` 是所有实例共享且稳定的文档服务名。`shard_count` 默认 128、范围为 1 到 512，身份编码/hash seed 统一；共享同一 namespace 的实例必须使用相同 N。Session consumer 并发默认 8，可通过 `session_consumer_concurrency` 配置为任意正整数。Redis 扩物理节点由 slot 迁移处理。

新增公共角色参数 `owner_ttl_seconds=120`、`owner_renew_interval_seconds=20`，删除静态 scheduler_shards、callback_shards、领取数量上限及处理若干分钟就移交的参数。保留 scan_batch_size、registry_page_size、类型并发、processing_timeout_seconds、callback_poll_seconds、shutdown_timeout_seconds。

API 顺序：配置检查 → Redis backend 未启用则跳过 Runtime → 创建 API Runtime → 注册 session handler/reporter → 统一启动 storage/namespace → 冻结处理器并启动 consumer；`publisher_separate_process=false` 时同一 Runtime 也启动 publisher。默认值为 `true`，此时 `openviking-server` 在启动 API 前拉起并监督唯一 publisher 子进程。关闭先停止 expiry 角色及在途业务调用，再关闭QueueFS/AGFS/Redis。runtime用asyncio.wait有界等待role tasks和runner pending，仅真实done才移除引用；未退出抛shard_not_stopped并保持相关依赖打开，不能在finally强关。core._stop_expiry保持runtime强引用，runtime.close成功后才清runtime并detach adapter；pending失败时runtime和adapter均保留，禁止新calls由runtime.accepting_calls=False负责；shard_not_stopped向外传播，中止后续QueueFS/AGFS关闭，允许后续再观察/重试关闭。其余core启动与optional backend不变。只启动storage不代表report已开放。API Runtime 内部固定允许report，以便 consumer 完成冷却重投。

runtime组装为 `store → ShardLeaseCoordinator(store, config, metrics) → ShardRunner(leases, config, metrics)`，同一个runner注入 `ExpiryPublisher(store, runner, config, metrics)` 与 `ExpiryConsumer(store, runner, bindings, config, metrics)`。

RedisLoopExecutor 以 `_RedisRequest` 强引用登记实际 Redis-loop task；`finished` 仅在该 task 及其原始 operation 真正终止时完成。`run` 使用 shield 等待 finished，取消时只向远端发一次取消，再通过 `_join` 保留调用方 wrapper 至实际退出；多次取消也不能切断桥接。因此 runner 能观察未终止 I/O，而不是把跨线程 Future 的 cancelled 状态当作远端退出。`quiesce(timeout_seconds)` 拒绝新外部请求，以有界等待观察所有登记请求（含尚未开始的请求）的真实 finished；未退出抛 `shard_not_stopped`，保留请求和相关依赖。

`ExpiryRuntime.close` 依次执行 `stop_roles → io.quiesce → io.run_cleanup(store.close) → io.close`。`run_cleanup(operation, timeout_seconds)` 在 Redis loop 提交受跟踪的 raw parent，不先占用 gate；store.close 内部可嵌套 io.run 关闭 client，既允许 quiesce 后清理，也避免 max_inflight=1 时递归等待 gate。cleanup 超时/取消不等于退出，仍保留 request。io.close 再次 quiesce 后仅发一次 loop.stop，以本次 close 的剩余预算 join 线程；线程仍 alive 则保留 loop/thread/gate 引用并报未退出，后续 close 可重试、不重复 stop，start 不得创建第二线程。runtime 增加 `_storage_closed=False`，start_storage 成功重置；仅在其为 False 时 run_cleanup，成功后置 True。若随后线程 join 超时，仍保留 `_storage_started/storage_ready`，重试 close 跳过已成功的 store cleanup，只重新观察/等待 join，不向已 stop_requested 的 loop 再提交 cleanup。只有线程真实退出才清 I/O 引用及 `_storage_started/storage_ready`。

各关闭阶段分别使用配置预算，不能宣称整个 runtime.close 只有一个总预算；run 取消后的 join、原始 operation 收尾及线程内清理也不提供强杀保证。外层有界观察失败时保留实际工作引用与依赖，不把取消、超时或 join 返回误写成已关闭。

内部 `openviking_cli.expiry_bootstrap` 模块读取完整 `ov.conf` 后只运行 publisher，不初始化 AGFS、模型或 SessionService。它通过 readiness 文件与 server 父进程握手，自动遍历全部 N 竞争，不要求显式分片列表；启动失败阻止 API 启动，运行期异常退出会带动父进程退出。

| 文件 | 计划职责 |
|---|---|
| scheduling/expiry/api.py、__init__.py | 三个业务方法、DTO、处理器选项及受限导出 |
| scheduling/expiry/models.py | 内部桶/页/claim/租约、TTL与时间计算，无资源版本 |
| scheduling/expiry/config.py | 轻量 expiry 配置 |
| scheduling/expiry/keys.py | 完整身份、hash、R/M/B/G/C/owner寻址 |
| scheduling/expiry/metrics.py | 低基数指标和耗尽日志入口 |
| scheduling/expiry/registry.py、service.py | 本地处理器注册、私有facade、R→M调用链 |
| scheduling/expiry/redis_store.py、lua/ | 单key操作、G/C发布、抢锁续租、结果未知分类 |
| scheduling/expiry/leases.py | 仅try_acquire/renew/release，Redis token操作，不持有worker循环 |
| scheduling/expiry/shard_runner.py | ShardExecution检查点/spawn/wait；单shard heartbeat、grace/失锁撤销、有界join、pending强引用 |
| scheduling/expiry/publisher.py | 自遍历全部shard/分钟节奏，到期积压、空范围、发布和清理 |
| scheduling/expiry/consumer.py | 自遍历全部shard/poll节奏，runner跟踪子tasks、旧tasks退出后清state、三次机会及日志终结 |
| scheduling/expiry/runtime.py | 共享runner组装、I/O loop、有界启停，未退出工作保留引用/依赖 |
| service/session_expiry.py | 消息上报、业务handler及冷却重投 |
| session/session.py | 追加reporter、SessionMeta接受时间字段；原通用提交不加idle/Redis判断 |
| service/session_service.py | 最新状态加载、纯超时检查、直接原commit调用、best-effort时间patch |
| service/core.py | 注入、正确启停与backend选择 |
| openviking_cli/utils/config/*.py | timer配置集成及idle_backend兼容 |
| openviking_cli/expiry_bootstrap.py、server/bootstrap.py、pyproject.toml | 内部 publisher 子进程、父进程监督、Redis可选依赖及Lua包资源 |

上述内核路径均以 openviking/ 为前缀。QueueFS/SessionCommitProcessor、原通用 ready/恢复、阈值提交和手工提交语义继续保留。

## 12. 实施顺序

| 阶段 | 交付与验证 |
|---|---|
| P1 | 公开契约、namespace实际配置、身份编码、store和单key Lua |
| P2 | R→M上报、TTL、注销、元数据缺失回源 |
| P3 | Coordinator三操作/ShardRunner单shard执行、heartbeat/grace失锁监视、lost/revoked不可复活、有界join/pending及可信异常释放 |
| P4 | 当前分钟扫描、非空分钟发布、分页、未知结果恢复 |
| P5 | runner跟踪回调子tasks/旧tasks退出后清state与每轮retry缓存；并发、三次机会、仅日志终结、旧ACK保护 |
| P6 | 服务层纯检查/直接原commit、锁内最新meta局部patch、记录失败仍accepted、冷却/活动重投 |
| P7 | CLI/多worker/legacy回归、Cluster故障注入和容量压测 |

跨槽旧请求风险应在早期验证，不能到最后才用普通功能测试代替接管安全性检查。

## 13. 验收矩阵

| 场景 | 预期 |
|---|---|
| 同local_id跨account/user | 身份与回源隔离，绝不提升为root |
| R失败 / R成功M失败 / M过期 | 顺序明确；缺M可回源；无错误回滚R |
| timeout为1小时/12小时/13小时 | TTL分别24/24/26小时 |
| 旧A覆盖新B | 接受覆盖，不出现隐藏CAS或revision状态 |
| report与调度并行、延迟report到达 | 单R TIME+ZADD写未来due；正常时钟下不落入本轮旧范围 |
| 12:34:04.234、边界due±1ms | U=12:34:00；包含等值，晚1ms下轮处理 |
| U=C / 无到期成员 / 空分钟跨度很大 | 跳过，无空桶和空推进，无逐空分钟循环 |
| 分页末项续期/删除、大量同score | 不因OFFSET位移漏扫 |
| 处理大量积压 | 保持租约直到追平，不按页/分钟配额交出shard |
| 两个角色、多实例、轮内递增 | 每角色一次一个shard，锁分开，下一轮补扫跳过项 |
| 20秒续租失败 / 迟到成功 / 长暂停 | lost永久生效，不复活旧lease；revoke/cancel旧process/children，保留未决命令限制 |
| 正常stop grace期间失锁 | heartbeat继续且失锁/有效截止仍被监视，立即撤销，不等grace耗尽 |
| 抑制取消的process/child/renew/acquire/release | asyncio.wait有界join，pending强引用不丢；同role所有shard停止再抢，另一role独立；不宣称强杀 |
| 普通业务异常但任务已终止 | 无forced/unknown、ownership可信可按token释放，错误仍传播；revoked不等于lost |
| forced / 失锁 / 未知结果 | 不主动release，停止续租留TTL；本地取消不证明Redis写已撤回 |
| callback旧task的finally迟到 | 未退出前不清state/reserved、不重领同role；退出后再清，retry缓存每次单shard运行清理 |
| handler吞取消后返回COMPLETED | ensure_active先检查current_task.cancelling，同步unknown并抛CancelledError，不继续ACK，也不主动release |
| run取消及多次取消、远端operation抑制取消 | wrapper保留至真实finished；requests强引用不丢，quiesce超时报shard_not_stopped，依赖仍保留 |
| quiesce后新外部请求 / nested cleanup | 外部run拒绝；run_cleanup raw parent在Redis loop运行，nested io.run可关client，max_inflight=1无gate递归 |
| runtime/core关闭仍有工作 | stop_roles/quiesce未退出保留runtime/roles/pending/requests；shard_not_stopped传播，不提前关依赖 |
| store已关但I/O线程join超时 | _storage_closed=True而_storage_started/storage_ready保留；重试跳过cleanup、不重复stop，只观察join；真实退出才清引用，start不能建第二线程 |
| store cleanup成功后thread重试回归 | test_runtime_retries_thread_join_without_resubmitting_closed_storage_cleanup验证不向stop_requested loop重复提交cleanup |
| worker角色节奏 | 各自遍历0..N-1；scheduler等下一分钟边界，callback按poll，Coordinator无轮次逻辑 |
| publish响应未知 | 不盲目重做可能已发布的B |
| claim attempt=0/1/2 | 不生成第四次调用；最后失败日志后条件清理 |
| 日志输出/ACK故障 | 可重复日志，无Redis失败归档；不伪称日志与删除原子 |
| last_idle_commit_accepted_at记录 | 原commit accepted后外层best-effort patch；无idle归档指针/查证/修复要求 |
| accepted后并发append/config更新 | root exact锁内重读最新raw meta，仅patch接受时间/updated_at，写入传lease_ref=lease |
| 并发成功记录时间乱序或坏旧值 | 有效更大/相等时间不写，较旧调用不回写；坏旧值日志后可覆盖 |
| accepted后记录读取/锁/写失败或session删除 | 失败日志后仍accepted；missing不重建，不从ready恢复记录 |
| 检查通过后新消息到达 | 原commit允许一起提交，无锁内最终idle检查；不承诺严格空闲或零重复 |
| 入口读取/commit异常 | 向外传播供重试，不伪装skipped或accepted |
| 最新last_message_at尚未到期 | 外层deferred/idle_activity，report成功才ACK旧B且保留未来R/M |
| timeout调大、冷却未满足 | report due=检查now+当前timeout，成功后才ACK旧B |
| 重投report失败 | 旧B保留，未耗尽可重试；R成功M失败的部分结果可解释 |
| 无消息/禁用policy | skipped后完成，不更新时间，不产生无限空重投 |
| 新版 commit 参数 None/True/False | HTTP/Service/Session 透传，False 不丢失；非 bool 拒绝；一次性覆盖不持久化 |
| 默认 WM 关闭 / 业务 policy 显式开启 | 沿用原commit，accepted后ACK、不等后台任务；按有效policy决定摘要，保留effective_enable_working_memory |
| 新旧 QueueFS policy 快照、重启前后配置不同 | 新任务保留显式 WM 值；旧任务按 from_dict 兼容规则恢复，不重新套用当前用户 policy |
| 新旧 .done 字段、无 summary 的成功归档 | 原ArchiveStore兼容契约不变；无摘要不影响timer accepted判定，不修改原通用恢复 |
| 多loop/独立CLI/legacy | Redis连接不跨loop误用，CLI不加载业务依赖，原提交路径回归 |

实施时复用最新版已有回归：`tests/session/test_working_memory_default_off.py`、`tests/unit/session/test_memory_policy.py`、`tests/unit/session/test_session_commit_resume.py`、`tests/session/test_session_commit.py`、`tests/server/test_api_sessions.py`，并在新增 idle 路径中覆盖上述交互。测试执行结论以最终 suite 输出为准，文档矩阵只定义预期。

## 14. 性能、容量与证据

报告至少一次R操作和一次M操作，通常跨槽两次往返；无V，无每资源更新版本成本。schema字段删除不代表取消实际namespace配置校验。延长M到至少24小时后，按最近一天内上报的去重资源量估算容量，不能按几分钟活跃量估算。

调度扫描量与到期数据相关，但每轮逐shard抢锁、读C/时间、空扫和释放存在固定成本。20秒续租约为每个持锁角色每分钟3次；需要测大量逻辑内核/实例同时轮询时的总负载。轮内递增减少回头竞争，不保证严格公平。空范围不推进C后，C年龄不能单独视为积压，应同时看最老到期R成员、G年龄和实际处理延迟。

B/G随积压增长；无Redis失败归档容量，但日志吞吐、保留时间和查询能力需要现有日志设施承担。每类型并发限制回调/原commit调用，不限制已接受后的后台任务总积压。批量范围删除、单type大分钟桶、热点shard和持续积压都需要真实Redis Cluster压测。

默认关闭 WM 可省去未显式启用时的摘要和 checkpoint 相关工作；长期记忆提取仍可能执行。不能把“没有摘要”等同于“没有后台成本”，也不能以此提高 timer 并发而跳过 QueueFS 积压与模型调用容量评估。本轮未提供新调度器的性能数据。

证据分为源码/骨架静态核对、自动化回归、真实 Redis/Lua 集成、Cluster 故障注入和吞吐压测，不能相互替代。静态核对重点为取消检查点、真实任务 finished 桥接、quiesce/cleanup/join 顺序及重试状态。2026-10-11 完整自动化回归为 **235 passed、8条既有 deprecation warnings、40.68s**，包含 store cleanup 成功后的线程 join 重试边界；日志及检验范围见附录第19节。

真实 Redis/Lua、Cluster 接管故障和吞吐测试未执行；自动化回归须以最终 suite 输出为准。不宣称已经解决第10.3节的残余风险，也不以文档骨架或模拟任务测试代表真实服务端行为已通过验收。

## 15. 工程约束与上线检查

以下约束与可靠性限制需在上线检查中保持一致：

- 上报通过单R Lua校正过去due到源TIME之后，不读取C，不引入关闭水位；任意时钟回退仍不在无状态保证内。
- C初始为0、非空发布允许跳过空分钟；完整身份及namespace寻址配置需全实例一致。
- 三次预算按领取占用，耗尽仅日志；log与ACK非事务，允许重复或故障下缺失日志。
- 冷却重投只有R/M均成功后才完成旧事件；失败不等于已经写入的R被撤销。
- shard租约按120/20执行；取消本地工作不能撤回已发跨槽请求，自动接管安全性仍需明确接受范围或进一步修改协议。
- 切换前已静默session是否做一次性导入、具体性能预算和灰度范围仍在上线计划中确认，不恢复周期全量扫描。

## 16. 逐文件代码附录

[代码骨架](session-idle-auto-commit-scheduler-code-design.md)与本文同步：删除V/更新revision/Redis失败记录/格式版本字段，同步leases.py三操作、shard_runner.py单shard执行、worker自有节奏、consumer子tasks与state、runtime/core未退出不关依赖；保留R→M、空范围跳过和Session外层冷却重投。新增/修改方法列出签名、注释及关键控制流；已有文件未涉及部分保留，省略体只省去实现细节。
