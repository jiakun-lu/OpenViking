# Session Redis 计时调度：逐文件代码设计

本文配套 [VibeCoding 实施文档](session-idle-auto-commit-scheduler-vibecoding.md)，把实现方案展开为可审阅的代码骨架。业务仍只使用三个接口；骨架展示签名和关键控制流，不代替可运行实现或行为验收。

本轮以用户修正为准：报告 R→M，无伴随状态或资源更新版本；耗尽仅日志；逐 shard 临时抢占，TTL 120 秒、20 秒续租，失败停止旧任务。冷却未满足经统一 report 重新计时。元数据仍可缺失，跨槽未决请求仍有残余风险，不能用取消任务宣称完全隔离旧写入。

代码基线为 `7d77e3d68fea82070a0b700a9acd64c40e7f4a9a`，本轮更新时间为2026-10-11。Redis optional、默认legacy不变，Redis协议与业务三个接口沿用已确认设计。最新服务层方案覆盖旧idle专用提交/恢复设计：外层加载和纯检查通过即调用原 `Session.commit_async`，不做持锁最终idle检查，期间新消息允许一起提交。accepted后服务层锁内重读最新meta，仅best-effort patch接受时间与updated_at；失败仍accepted、删除不重建。没有idle归档指针或可修复缓存，不承诺严格空闲或零重复。原通用提交的ready、QueueFS、恢复与WM契约继续保留。

## 1. 阅读方式、依赖和文件目录

- 新文件列出计划中的全部类、数据字段、公开接口及主要私有函数，包括 Lua 入口。
- 已有文件只列本次新增或需要修改的函数；未涉及的已有函数继续保留，不在此复制整个内核。
- `...` 表示实现体待编码，但参数、返回值、异常、锁/线程边界由注释约定。关键交接流程给出具体控制流。
- 片段中的既有类型沿用项目 import；新增类型来自本文指定模块。本文做语法和接口一致性检查，不宣称这些骨架已经可以运行。
- 字段和函数名以本附录为具体建议；如与主文档的简略伪代码不同，以这里展开的签名为准，业务约束仍以主文档为准。

```text
openviking/scheduling/expiry/
    __init__.py          # 只导出 api.py 的业务契约，不导出实现类
    api.py               # 三个业务接口、业务 DTO、处理器协议及注册选项
    models.py            # 调度/消费内部数据结构、校验和纯计算函数
    config.py            # 轻量配置定义，独立进程不经过聚合配置包
    keys.py              # 身份编码、hash、key 和注册项寻址
    metrics.py           # timer 指标的轻量包装
    service.py           # 私有 facade 实现，只由 runtime 构造
    registry.py          # 内部处理器注册表及不可变注册快照
    redis_store.py       # Redis 数据操作；所有网络 I/O 经专属 loop
    publisher.py         # 分页扫描、分钟构建和发布
    consumer.py          # 已注册桶发现、claim、业务回调、条件 ACK
    leases.py            # 仅 try_acquire/renew/release：Redis token 操作
    shard_runner.py      # 单 shard 执行、后台 heartbeat、撤销/有界 join/pending 强引用
    runtime.py           # 构造共享 runner、Redis loop 与有界启停
    lua/
        _common.lua
        ensure_metadata.lua
        upsert_resource.lua
        source_time.lua
        read_source_page.lua
        publish_minute.lua
        read_registry_page.lua
        claim.lua
        ack.lua
        acquire_owner.lua
        renew_owner.lua
        release_owner.lua
openviking/service/session_expiry.py      # session 业务适配
openviking/session/session.py             # append reporter/接受时间字段，原通用commit不拆分
openviking/service/session_service.py     # 加载、纯检查、原commit调用及best-effort局部patch
openviking/service/core.py                # 启停顺序与旧 backend 选择
openviking_cli/utils/config/expiry_config.py
openviking_cli/utils/config/open_viking_config.py
openviking_cli/utils/config/memory_config.py
openviking_cli/expiry_bootstrap.py
pyproject.toml
```

模块分层从底向上为 `api → models/keys → store/registry/service → leases → shard_runner → publisher/consumer → runtime`，上层引用下层，api 不反向 import 实现模块。Coordinator 只封装 token 操作；runner 只负责一段单 shard 工作的租约执行生命周期；遍历 shard 和调度节奏由具体 worker 自己持有，不引入通用 worker 框架。业务模块通过包入口只依赖 ExpiryScheduler 协议和业务 DTO，启动组装层才依赖 runtime。session 适配另外依赖自身业务的 SessionService。Session 本身只持有自己定义的 reporter 协议，不 import session_expiry。runtime 不 import SessionService，内部 publisher 子进程不加载业务 handler。

两个 loop 的责任明确分开：Redis 客户端、连接池和原始 Redis 命令只在 Redis I/O loop；业务 handler、session commit、scheduler/consumer 的控制流在它们的运行 loop（API 中为 API loop）。report 可以从不同调用 loop 发起，store 负责桥接网络 I/O，不能将整个 session commit 丢进 Redis loop。

## 2. api.py 与 models.py：业务契约和内部模型分开

### 2.1 api.py：唯一稳定的业务契约

业务只接收 ExpiryScheduler 实例，不构造实现类。下面的三个方法是全部业务操作；没有启动、freeze、Redis client、队列领取或 ACK 接口。

```python
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import Protocol


SESSION_ITEM_TYPE = "session"
SUPPORTED_ITEM_TYPES: frozenset[str] = frozenset({SESSION_ITEM_TYPE})


class ExpiryMode(str, Enum):
    IDLE_ONESHOT = "idle_oneshot"
    FIXED = "fixed"  # 本期预留；尚不支持时明确报错。


class CallbackStatus(str, Enum):
    COMPLETED = "completed"
    RETRY = "retry"


class ExpiryError(RuntimeError):
    """公开错误只含业务可理解的错误码和是否可重试，不提供内部执行凭据。"""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        """参数/注册错误不可重试；暂时不可用由调用者执行自己的失败策略。"""
        super().__init__(message)
        self.code = code
        self.retryable = retryable


@dataclass(frozen=True)
class ItemIdentity:
    item_type: str
    account_id: str
    user_id: str
    local_id: str


@dataclass(frozen=True)
class ExpiryRegistration:
    identity: ItemIdentity
    due_at_ms: int
    timeout_seconds: int
    mode: ExpiryMode = ExpiryMode.IDLE_ONESHOT


@dataclass(frozen=True)
class ExpiryRegistrationResult:
    accepted_due_at_ms: int  # 服务接受的到期时间；不是实际入队时间，不返回内部执行凭据。


@dataclass(frozen=True)
class CallbackRequest:
    identity: ItemIdentity  # 业务用此身份回源；不携带元数据缓存是否命中的标志。


@dataclass(frozen=True)
class CallbackResult:
    status: CallbackStatus
    reason: str


@dataclass(frozen=True)
class HandlerOptions:
    concurrency: int = 4  # 通用默认值；各业务在启动注册时可指定，运行中不可动态更改。
    max_attempts: int = 3  # 包含首次；session 采用 3。相同类型各回调副本必须一致。


class ExpiryHandler(Protocol):
    async def handle(self, request: CallbackRequest) -> CallbackResult:
        """只执行业务操作；返回完成或重试。队列重领、超时和清理由框架负责。"""
        ...


class ExpiryScheduler(Protocol):
    async def report(self, task: ExpiryRegistration) -> ExpiryRegistrationResult:
        """写入或更新计时；业务提供身份、到期时间和 timeout，等待本次操作结果。"""
        ...

    async def unregister(self, identity: ItemIdentity) -> None:
        """注销当前计时；重复调用幂等，已交付处理的任务按业务当前状态完成。"""
        ...

    def register_handler(self, item_type: str, handler: ExpiryHandler, *,
                         options: HandlerOptions | None = None) -> None:
        """业务模块启动时注册类型及处理器；重复注册或运行中修改明确报错。"""
        ...
```

HandlerOptions 表达业务处理能力，不暴露队列实现。首期只支持 session，`SUPPORTED_ITEM_TYPES` 是上报、Handler 注册和调度恢复共用的代码清单。report 不做请求顺序比较，接口不再返回重复或陈旧状态。

### 2.2 models.py：不向业务导出的内部类型

这些类型仅供框架内部使用，包入口不会导出 Claim、BucketRef、OwnerToken、游标或 Redis 状态。内部模块明确从 api.py 引用公开 DTO，不再把公开/私有模型混放。

```python
from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass, field
from enum import Enum
from typing import Awaitable, Callable, Literal, Protocol, TypeVar
from .api import ItemIdentity, ExpiryRegistration, ExpiryError, ExpiryMode

T = TypeVar("T")
MINUTE_MS = 60_000
MIN_METADATA_TTL_MS = 24 * 60 * 60 * 1000
SCORE_TIME_BASE = 1 << 40
MAX_ATTEMPT = 255
MAX_EXACT_INTEGER = (1 << 53) - 1


class StoreOperationError(ExpiryError):
    """内部存储错误；执行结果是否未知只供调度/重试恢复使用。"""

    def __init__(self, code: str, message: str, *, retryable: bool = False,
                 outcome_unknown: bool = False) -> None:
        """保留内部执行状态；facade 会映射为公开错误，不直接透传此类型。"""
        super().__init__(code, message, retryable=retryable)
        self.outcome_unknown = outcome_unknown


class OwnerRole(str, Enum):
    SCHEDULER = "scheduler"
    CALLBACK = "callback"


@dataclass(frozen=True)
class ItemMetadata:
    identity: ItemIdentity


@dataclass(frozen=True)
class NamespaceSpec:
    shard_count: int
    hash_name: str
    hash_seed: int
    identity_encoding: str  # 固定实际规则名称：urlsafe_base64，无独立版本号。


@dataclass(frozen=True)
class OwnerToken:
    role: OwnerRole
    shard_id: int
    token: str               # 每次成功领取的随机 token；不复用上次进程 token。
    expires_at_ms: int  # 两个角色均有 TTL；仅用于观测，本地截止使用 monotonic。


@dataclass
class ShardLease:
    owner: OwnerToken
    lost: asyncio.Event  # ownership 已不可信；一旦置位，后续迟到成功也不可复活。
    valid_until_monotonic: float
    revoked: asyncio.Event = field(default_factory=asyncio.Event)  # 本地执行已撤销，不等于失锁。

    def ensure_active(self) -> None:
        """动作前检查 ownership 截止和本地撤销；不能撤回已发外部请求。"""
        if self.lost.is_set() or time.monotonic() >= self.valid_until_monotonic:
            self.lost.set()
            raise StoreOperationError("owner_lost", "Shard lease is no longer valid")
        if self.revoked.is_set():
            raise StoreOperationError("owner_lost", "Shard lease execution is revoked")


@dataclass(frozen=True)
class ShardWindow:
    progress_ms: int  # C：最后发布的分钟上界。
    upper_ms: int  # U：当前源 TIME 去掉秒和毫秒；只是本轮局部快照。
    redis_now_ms: int


@dataclass(frozen=True)
class SourceCursor:
    due_at_ms: int
    item_id: str  # score 相同按 Redis member 的二进制字节顺序续读。


@dataclass(frozen=True)
class SourceItem:
    item_id: str
    due_at_ms: int


@dataclass(frozen=True)
class SourcePage:
    items: tuple[SourceItem, ...]
    next_cursor: SourceCursor | None  # None 表示本次分钟范围已读完。


@dataclass(frozen=True)
class BucketRef:
    item_type: str
    shard_id: int
    minute_ms: int  # 窗口 (minute_ms - 60000, minute_ms] 的结束边界。


@dataclass(frozen=True)
class RegistryCursor:
    minute_ms: int
    member: str


@dataclass(frozen=True)
class RegistryPage:
    buckets: tuple[BucketRef, ...]
    next_cursor: RegistryCursor | None


@dataclass(frozen=True)
class PublishResult:
    status: Literal["published", "already_published"]
    progress_ms: int


@dataclass(frozen=True)
class Claim:
    bucket: BucketRef
    item_id: str
    claim_score: int


@dataclass(frozen=True)
class ClaimBatch:
    claims: tuple[Claim, ...]
    next_retry_layer: int  # 消费者下次从该层继续，避免每次只检查低 attempt。
    layers_scanned: int
    exhausted: tuple[Claim, ...]  # 最终机会已超时，尚未删除；供 consumer 写日志后条件 ACK。


# 内部模型不承载 session policy、消息内容、event_id 或 group_id。


class AsyncExecutor(Protocol):
    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        """在固定 I/O loop 创建并执行 coroutine，调用方立即等待。

        operation 必须是工厂，不能把在别的 loop 创建的 Redis Future 传进来。
        取消/超时不能被解释为服务端已回滚；写入结果未知由 store 分类。
        """
        ...


def validate_item_type(item_type: str) -> None:
    """只校验 [a-z][a-z0-9_]{0,63} 格式；不认识 session/watch 等具体业务类型。"""
    ...


def validate_identity(identity: ItemIdentity) -> None:
    """校验类型名及非空字符串身份，具体组件由 keys.py 可逆编码。"""
    ...


def validate_report(task: ExpiryRegistration) -> None:
    """校验类型名称格式、非空身份、正 timeout、毫秒整数范围。

    类型是否已注册由 facade/内部目录判断，这里没有业务类型白名单。
    FIXED -> unsupported_mode；坏身份 -> invalid_identity；参数错误不可重试。
    bool 不当作 int 接受；所有值必须处于 Redis Lua 的精确整数范围。
    使用 metadata_ttl_ms 校验两倍 timeout 的毫秒计算；TTL 不由业务单独传入。
    """
    ...


def minute_start_ms(timestamp_ms: int) -> int:
    """Unix 毫秒归一到分钟起点。"""
    return timestamp_ms // MINUTE_MS * MINUTE_MS


def scan_cutoff_ms(now_ms: int) -> int:
    """扫描包含式上界：源 TIME 向下取整到当前分钟，不减 1ms、不预取下一分钟。"""
    return minute_start_ms(now_ms)


def bucket_minute_ms(due_at_ms: int) -> int:
    """due 保留原精度；返回包含它的结束分钟，整分钟 due 属于该分钟窗口。"""
    if type(due_at_ms) is not int or not 0 <= due_at_ms <= MAX_EXACT_INTEGER:
        raise ExpiryError("invalid_report", "Due time is outside the supported range")
    minute = ((due_at_ms + MINUTE_MS - 1) // MINUTE_MS) * MINUTE_MS
    if minute > MAX_EXACT_INTEGER:
        raise ExpiryError("invalid_report", "Due time is outside the supported range")
    return minute


def metadata_ttl_ms(timeout_seconds: int) -> int:
    """M 创建与续期统一使用 max(两倍 timeout,24小时)，返回 PEXPIRE 的毫秒值。"""
    if type(timeout_seconds) is not int or not 0 < timeout_seconds <= MAX_EXACT_INTEGER // 2000:
        raise ExpiryError("invalid_report", "Timeout is outside the supported range")
    return max(timeout_seconds * 2000, MIN_METADATA_TTL_MS)


def encode_claim_score(attempt: int, started_at_seconds: int) -> int:
    """与 Lua 算术完全一致；非法范围报错，不静默截断。"""
    if type(attempt) is not int or not 0 <= attempt <= MAX_ATTEMPT:
        raise ValueError("attempt out of range")
    if type(started_at_seconds) is not int or not 0 < started_at_seconds < SCORE_TIME_BASE:
        raise ValueError("claim timestamp out of range")
    return attempt * SCORE_TIME_BASE + started_at_seconds


def decode_claim_score(score: int) -> tuple[int, int]:
    """仅解码已领取记录；0 是 ready 哨兵，由调用者先处理。"""
    if type(score) is not int or not 0 < score < (MAX_ATTEMPT + 1) * SCORE_TIME_BASE:
        raise ValueError("invalid claim score")
    attempt, started_at = divmod(score, SCORE_TIME_BASE)
    if started_at == 0:
        raise ValueError("invalid claim timestamp")
    return attempt, started_at
```

公开错误码包括 invalid_identity、invalid_report、unsupported_mode、unregistered_type、duplicate_handler、invalid_handler、registration_closed、reporting_disabled、not_started、timer_unavailable。namespace、owner、progress、clock 及结果未知状态属于内部恢复错误，由 facade 映射后返回业务；业务 NotFound 由业务 handler 转成 COMPLETED。


## 3. keys.py：编码和寻址

```python
from dataclasses import dataclass
from .api import ItemIdentity
from .models import BucketRef, OwnerRole, validate_identity

HASH_SEED = 0
IDENTITY_ENCODING = "urlsafe_base64"


def encode_component(value: str) -> str:
    """UTF-8 -> URL-safe Base64，无 '=' 填充；身份字段不能为空。"""
    ...


def decode_component(value: str) -> str:
    """严格解码并重编码校验规范形式，拒绝别名、坏 UTF-8、非法字符。"""
    ...


def encode_item_id(identity: ItemIdentity) -> str:
    """类型名 + 三个编码组件；不能将用户原始花括号放进 Redis key。"""
    validate_identity(identity)
    return ":".join((identity.item_type, encode_component(identity.account_id),
                     encode_component(identity.user_id), encode_component(identity.local_id)))


def decode_item_id(item_id: str) -> ItemIdentity:
    """严格四段，校验类型名格式；不查业务白名单，用于框架内部精确路由。"""
    ...


def shard_for(item_id: str, shard_count: int) -> int:
    """xxhash.xxh64(item_id.encode('utf-8'), seed=HASH_SEED).intdigest() % N。"""
    ...


@dataclass(frozen=True)
class ExpiryKeys:
    service_key: str  # 构造时先 encode_component(service_name)，禁止直接传原始字符串。

    def namespace(self) -> str:
        """协议 manifest HASH；只保存固定的分片、哈希和身份编码配置。"""
        return f"expiry:namespace:{self.service_key}"

    def metadata(self, item_id: str) -> str:
        """M：单 item 身份缓存。"""
        return f"expiry:metadata:{self.service_key}:{item_id}"

    def source(self, shard_id: int) -> str:
        """R：不加真实 hash tag。"""
        return f"expiry:publisher:source:{self.service_key}:{shard_id}"



    def control_tag(self, shard_id: int) -> str:
        """返回带花括号的真实 tag，避免不同逻辑内核控制 key 扎堆。"""
        return f"{{{self.service_key}:{shard_id}}}"

    def progress(self, shard_id: int) -> str:
        """C：与 G/调度 owner 同槽。"""
        return f"expiry:publisher:progress:{self.control_tag(shard_id)}"

    def registry(self, shard_id: int) -> str:
        """G：只登记已经写完并发布的 B。"""
        return f"expiry:consumer:registry:{self.control_tag(shard_id)}"

    def owner(self, role: OwnerRole, shard_id: int, *, item_type: str | None = None) -> str:
        """不同角色用不同 key，scheduler 与 callback 不互抢。"""
        ...

    def bucket(self, ref: BucketRef) -> str:
        """B：无需与 G 同槽。"""
        return (f"expiry:consumer:bucket:{self.service_key}:{ref.item_type}:"
                f"{ref.shard_id}:{ref.minute_ms}")



    def registry_member(self, ref: BucketRef) -> str:
        """序列化注册项，不带 namespace，因为 G 已按 service_name 隔离。"""
        return f"{ref.item_type}:{ref.shard_id}:{ref.minute_ms}"

    def parse_registry_member(self, value: str, *, expected_shard: int) -> BucketRef:
        """校验 type、shard 和分钟对齐，拒绝跨 shard/损坏注册项。"""
        ...
```

## 4. metrics.py：可观测性接口

```python
from collections.abc import Callable, Mapping
from .models import Claim


class ExpiryMetrics:
    """包装内核指标 sink；内部 publisher 进程可使用同一 sink 或结构化日志。

    标签限于固定 operation/item_type/outcome；不以 item_id/account_id 作标签。
    sink 失败不能影响业务结果，批量任务不逐项写成功日志。
    """

    def __init__(self, sink: Callable[[str, float, Mapping[str, str]], None] | None = None):
        """绑定可选指标 sink；不连接 Redis 或初始化业务服务。"""
        self._sink = sink

    def count(self, name: str, value: int = 1, *, item_type: str = "",
              outcome: str = "") -> None:
        """计数：report/claim/ACK/回源/失败等；内部隔离 sink 异常。"""
        ...

    def observe(self, name: str, value: float, *, item_type: str = "",
                outcome: str = "") -> None:
        """耗时、页大小、retry 层数等分布。单位在指标名中固定。"""
        ...

    def log_exhausted(self, claim: "Claim", *, max_attempts: int,
                      reason: str, outcome_unknown: bool) -> None:
        """输出结构化耗尽日志：身份/窗口/claim/机会数/原因，不含消息正文。

        日志进入现有日志设施，不写 Redis。失败向调用者抛错，不能声称已记录。
        日志与随后 ACK 不是事务，允许重复或崩溃时丢失；不伪造恰好一次。
        """
        ...

    def gauge(self, name: str, value: float, *, item_type: str = "") -> None:
        """当前 backlog、inflight、owner 数等；不可逐资源生成系列。"""
        ...
```

## 5. redis_store.py：完整存储接口

这个类集中管理 Redis 数据操作，scheduler/consumer 不直接取得 Redis 客户端。以下所有公开 async 方法都通过 `_io.run(...)` 执行原始客户端操作；标记 `_on_loop` 的方法只能从该 I/O loop 调用。逻辑流程可以跨多次 I/O，不伪装成跨 slot 事务。

```python
from __future__ import annotations

from typing import Any, Literal, Sequence
from .keys import ExpiryKeys
from .metrics import ExpiryMetrics
from .api import ExpiryRegistration, ExpiryRegistrationResult
from .models import (
    AsyncExecutor, BucketRef, Claim, ClaimBatch, NamespaceSpec, OwnerRole,
    ItemMetadata, OwnerToken, PublishResult, RegistryCursor, RegistryPage,
    ShardWindow, SourceCursor, SourcePage, StoreOperationError, metadata_ttl_ms,
)
from .config import ExpiryRedisConfig

RetryPolicy = Literal["read", "idempotent", "resolve_by_caller"]


class RedisExpiryStore:
    def __init__(self, config: ExpiryRedisConfig, keys: ExpiryKeys,
                 io: AsyncExecutor, metrics: ExpiryMetrics) -> None:
        """仅保存配置；此处不创建绑定调用方 loop 的 Redis 连接。"""
        self._config, self._keys, self._io, self._metrics = config, keys, io, metrics
        self._client: Any = None  # 仅 _on_loop 方法访问。
        self._scripts: dict[str, str] = {}
        self._ready = False

    async def open(self) -> None:
        """在 I/O loop 创建 standalone/cluster 客户端，PING 并检查所需命令。"""
        await self._io.run(self._open_on_loop)

    async def close(self) -> None:
        """先拒绝新请求，再在 I/O loop 关闭连接；loop 此时仍须存活。"""
        await self._io.run(self._close_on_loop)

    async def ensure_namespace(self, spec: NamespaceSpec) -> None:
        """新 namespace: INITIALIZING -> 幂等初始化 N 个 C=0 -> READY。

        所有 reporter 等待 READY。中断初始化只补缺项、不覆盖已有 C。
        READY namespace 的缺 C 是损坏，不按当前时间自动重置。
        实际N/hash/编码配置不匹配 -> namespace_mismatch；没有独立格式版本字段。
        """
        ...

    async def ensure_metadata(self, item_id: str, task: ExpiryRegistration) -> None:
        """内部计算 TTL 后调用 ensure_metadata.lua；创建和续期使用同一个毫秒值。

        ttl_ms=max(2*timeout_seconds*1000,86_400_000)，无独立客户端 EXISTS。
        """
        identity = task.identity
        ttl_ms = metadata_ttl_ms(task.timeout_seconds)
        await self._eval(
            "ensure_metadata", [self._keys.metadata(item_id)],
            [identity.item_type, identity.local_id, identity.account_id, identity.user_id, ttl_ms],
            retry="idempotent",
        )

    async def read_metadata(self, item_id: str) -> ItemMetadata | None:
        """None 只表示 Redis key 不存在；网络失败抛错，身份冲突也抛错。"""
        ...

    async def upsert_resource(self, shard_id: int, item_id: str,
                              task: ExpiryRegistration) -> ExpiryRegistrationResult:
        """仅 report 调用：单 R Lua 用 TIME 计算 max(requested_due,now+1) 后 ZADD。

        不读 C，不写伴随状态，不做业务顺序比较；同 item_id 允许后到请求覆盖。
        网络未知结果不自动无限重放，业务是否再次 report 按失败策略决定。
        """
        ...

    async def unregister_resource(self, shard_id: int, item_id: str) -> None:
        """单 key ZREM R 后 DEL M；部分失败报错，不扫描 B，重复删除幂等。"""
        ...

    async def read_progress(self, shard_id: int) -> int:
        """读 C；READY namespace 中不存在则抛 namespace_corrupt。"""
        ...

    async def read_scan_window(self, shard_id: int) -> ShardWindow:
        """读 C，再通过 source_time.lua(R) 取得源 TIME，U=去掉秒和毫秒。

        只读，不更新 R 或 C。U=C 直接跳过；U<C 记录时间异常并停止本次处理。
        """
        progress = await self.read_progress(shard_id)
        source_now_ms = int(await self._eval(
            "source_time", [self._keys.source(shard_id)], [], retry="read",
        ))
        upper = source_now_ms // 60_000 * 60_000
        if upper < progress:
            raise StoreOperationError("clock_regression", "Source time is behind published progress")
        return ShardWindow(progress, upper, source_now_ms)

    async def read_source_page(self, shard_id: int, *, lower_exclusive_ms: int,
                               upper_inclusive_ms: int, after: SourceCursor | None,
                               limit: int) -> SourcePage:
        """调用原子分页脚本；after 为稳定 score/member 游标，不是 OFFSET。"""
        ...

    async def reset_unpublished_minute(self, owner: OwnerToken, *, minute_ms: int,
                                       expected_progress_ms: int,
                                       item_types: Sequence[str]) -> None:
        """先校验 owner/C，本地 lease 有效且 C 尚未覆盖，再 UNLINK 已知 type 的 B。

        仅允许 minute_ms > C；已发布分钟拒绝清空。
        校验 C 与跨槽 UNLINK 不是事务；租约过期后的旧请求仍是主稿 10.3 的限制。
        """
        ...

    async def stage_items(self, owner: OwnerToken, bucket: BucketRef,
                          item_ids: Sequence[str]) -> None:
        """未注册 B 上的有界 ZADD 0；只由该 shard 独占 scheduler 调用。

        返回意味着本批明确成功；超时返回 result_unknown，不在后台盲目重放。
        整个分钟所有 stage 调用都结束后，才能 publish。
        """
        ...

    async def publish_minute(self, owner: OwnerToken, *, expected_progress_ms: int,
                             minute_ms: int, buckets: Sequence[BucketRef]) -> PublishResult:
        """同槽 Lua 注册所有 B 并推进 C；owner/C 错误在任何写入前返回。

        minute_ms>expected_progress_ms，允许跨过扫描已确认的空分钟；发布后 C=minute_ms。
        buckets 必须非空，空分钟跳过；源扫描完成由 scheduler 保证。
        """
        ...

    async def cleanup_source(self, shard_id: int, through_ms: int) -> int:
        """ZREMRANGEBYSCORE R -inf through_ms；返回删除数，单次耗时要监控。

        只接受已确认 C；不执行任何 ZADD。是否换成有界批删需压测后另行决定。
        """
        ...

    async def read_registry_page(self, shard_id: int, *, after: RegistryCursor | None,
                                 limit: int) -> RegistryPage:
        """G 只包含已发布分钟，按 score/member 稳定分页；不全量 ZRANGE。

        页末成员可能被摘除，使用与资源分页相同的原子定位策略。
        """
        ...

    async def claim(self, bucket: BucketRef, *, limit: int, max_attempts: int,
                    processing_timeout_seconds: int,
                    retry_layer: int, retry_layers_per_poll: int) -> ClaimBatch:
        """单 B Lua：有界领取，最后机会超时项只返回 exhausted，不删除或重领。

        每 claim 占一次机会；session 查询0/1/2层，超时检查不可被新项饿死。
        exhausted 由 consumer 日志后条件 ACK，无 Redis 失败存储。
        响应未知不自动重放 claim，等超时按计数恢复。
        """
        ...

    async def ack(self, claim: Claim) -> bool:
        """相同 score 才删除；False 表示已删除或被新领取，不再调用业务补偿。

        使用 retry=idempotent 在请求预算内重试同一条件 ACK，不能重跑 handler。
        """
        ...





    async def remove_registration_if_empty(self, bucket: BucketRef) -> bool:
        """读 B 为空后摘 G；依赖发布后不再生产，允许遗留空注册项。"""
        ...

    async def try_acquire_owner(self, role: OwnerRole, shard_id: int, token: str,
                                ttl_seconds: int) -> OwnerToken | None:
        """SET NX PX，两个角色均使用120秒TTL；忙返回None。

        同token仅用于确认本次领取响应，不延长已失败lease；下一次领取必须新token。
        """
        ...

    async def renew_owner(self, owner: OwnerToken, ttl_seconds: int) -> OwnerToken | None:
        """比较 token 后 PEXPIRE；缺失/不匹配返回None，异常即本次lease失败。

        不能重新创建锁，不能在丢锁后用同token恢复处理；两种角色均适用。
        """
        ...

    async def assert_owner(self, owner: OwnerToken) -> None:
        """读取 token 验证；失败抛 owner_lost。非跨槽 B 写 fencing。"""
        ...

    async def release_owner(self, owner: OwnerToken) -> bool:
        """Lua 比较 token 后删除，不清除后继 owner。"""
        ...

    async def _open_on_loop(self) -> None:
        """延迟 import redis.asyncio；缺 extra 时给可操作的依赖错误。"""
        ...

    async def _close_on_loop(self) -> None:
        """关闭两个 Redis 模式下的客户端；清空缓存引用。"""
        ...

    async def _initialize_shard(self, shard_id: int) -> None:
        """仅 namespace INITIALIZING 时 SET C 0 NX，已有值不覆盖。

        全部初始化完成后置READY；report须等待READY，不按当前时间跳过历史源数据。
        """
        ...

    async def _command(self, name: str, *args: object,
                       retry: RetryPolicy = "read") -> Any:
        """将客户端命令封装为 coroutine 工厂，经 _io.run 发送并分类错误。"""
        ...

    async def _eval(self, script: str, keys: Sequence[str], args: Sequence[object],
                    *, retry: RetryPolicy) -> Any:
        """非 I/O loop 入口；只调度 _eval_on_loop，业务锁不带入这里。"""
        return await self._io.run(lambda: self._eval_on_loop(script, keys, args, retry=retry))

    async def _eval_on_loop(self, script: str, keys: Sequence[str], args: Sequence[object],
                            *, retry: RetryPolicy) -> Any:
        """EVALSHA，NOSCRIPT 则对实际目标 slot EVAL；MOVED/ASK 遵从客户端。

        网络超时不对 resolve_by_caller 自动重试；返回 result_unknown。
        Lua 输入 key 显式列在 KEYS；跨 slot 参数在发送前拒绝。
        """
        ...

    def _read_script(self, name: str) -> str:
        """读取 lua/_common.lua + lua/name.lua 并拼接，进程内缓存源码/SHA。"""
        ...

    def _translate_redis_error(self, exc: Exception, *, retry: RetryPolicy) -> StoreOperationError:
        """区分协议错误、可重试读错误、服务端执行结果未知；保留原异常链。"""
        ...
```

不提供 `get_client()`，也不提供通用“跨 slot pipeline 事务”。`stage_items/reset_unpublished_minute` 是结果未知时需要上层协调的操作；条件 ACK 与读取有各自可重试语义；report 无版本去重，不自动声称重复请求无影响。

## 6. registry.py、service.py 与 __init__.py：三个业务方法，内部自行管理

### 6.1 registry.py：内部处理器注册表

```python
from dataclasses import dataclass
from types import MappingProxyType
from typing import Mapping
from .api import HandlerOptions, ExpiryHandler, ExpiryError
from .models import validate_item_type


@dataclass(frozen=True)
class HandlerBinding:
    handler: ExpiryHandler
    options: HandlerOptions


class HandlerRegistry:
    """只在框架内部使用；处理器和并发设置不写入 Redis。"""

    def __init__(self) -> None:
        """创建空注册表，不硬编码任何业务类型。"""
        self._bindings: dict[str, HandlerBinding] = {}
        self._frozen = False

    def register(self, item_type: str, handler: ExpiryHandler, options: HandlerOptions) -> None:
        """校验类型名、异步 handle、正整数并发及 max_attempts∈[1,256]（bool 非法）。

        坏 handler/options -> invalid_handler；重复类型 -> duplicate_handler；
        freeze 后修改 -> registration_closed；均使用公开 ExpiryError。
        """
        ...

    def freeze(self) -> Mapping[str, HandlerBinding]:
        """runtime 在开始消费前冻结，返回只读注册快照；不能从业务接口调用。"""
        ...
```

### 6.2 service.py：不导出的 facade 实现

```python
from dataclasses import dataclass, field
from .api import (
    HandlerOptions, ItemIdentity, ExpiryHandler, ExpiryRegistration, ExpiryRegistrationResult, ExpiryError,
)
from .keys import encode_item_id, shard_for
from .metrics import ExpiryMetrics
from .models import StoreOperationError, validate_report
from .redis_store import RedisExpiryStore
from .registry import HandlerRegistry


@dataclass
class ExpiryRuntimeState:
    """框架私有生命周期状态，runtime 持有；业务对象不提供状态修改入口。"""
    storage_ready: bool = False
    accepting_calls: bool = False


class _ExpirySchedulerFacade:
    """实现 ExpiryScheduler；只有 runtime 构造，业务通过依赖注入得到协议引用。"""

    def __init__(self, store: RedisExpiryStore, registry: HandlerRegistry,
                 state: ExpiryRuntimeState, *, shard_count: int,
                 reporting_enabled: bool, metrics: ExpiryMetrics) -> None:
        """保存内部依赖；业务不负责构造 store、keys、registry 或 state。"""
        self._store, self._registry, self._state = store, registry, state
        self._metrics, self._shard_count = metrics, shard_count
        self._reporting_enabled = reporting_enabled

    async def report(self, task: ExpiryRegistration) -> ExpiryRegistrationResult:
        """执行一次计时写入，将内部存储错误映射为业务错误。"""
        self._require_ready()
        if not self._reporting_enabled:
            raise ExpiryError("reporting_disabled", "Timer reporting is disabled")
        validate_report(task)
        self._ensure_supported_type(task.identity.item_type)
        try:
            item_id = encode_item_id(task.identity)
            shard_id = shard_for(item_id, self._shard_count)
            result = await self._store.upsert_resource(shard_id, item_id, task)
            await self._store.ensure_metadata(item_id, task)
        except StoreOperationError as exc:
            raise self._business_error(exc) from exc
        self._metrics.count("timer_report", item_type=task.identity.item_type,
                            outcome="written")
        return result

    async def unregister(self, identity: ItemIdentity) -> None:
        """删除受支持类型的当前计时；不要求本进程仍有该类型 handler。"""
        self._require_ready()
        self._ensure_supported_type(identity.item_type)
        item_id = encode_item_id(identity)
        try:
            await self._store.unregister_resource(shard_for(item_id, self._shard_count), item_id)
        except StoreOperationError as exc:
            raise self._business_error(exc) from exc

    def register_handler(self, item_type: str, handler: ExpiryHandler, *,
                         options: HandlerOptions | None = None) -> None:
        """同步本地注册，允许在 storage 启动前调用；运行后由内部 registry 拒绝修改。"""
        self._registry.register(item_type, handler, options or HandlerOptions())

    @staticmethod
    def _ensure_supported_type(item_type: str) -> None:
        """按 SUPPORTED_ITEM_TYPES 校验；未知类型在任何 Redis 写入前失败。"""
        ...

    def _require_ready(self) -> None:
        """未初始化或关闭中抛 not_started；就绪状态只能由内部生命周期管理。"""
        if not (self._state.storage_ready and self._state.accepting_calls):
            raise ExpiryError("not_started", "Expiry scheduling is not ready")

    def _business_error(self, exc: StoreOperationError) -> ExpiryError:
        """参数/身份错误保留公开错误码；内部不可用映射 timer_unavailable 并记内部指标。

        不向业务返回 owner、claim score、Lua 错误格式或 outcome_unknown 字段。
        """
        ...
```

### 6.3 __init__.py：限定业务可依赖的导出

```python
from .api import (
    CallbackRequest, CallbackResult, CallbackStatus, HandlerOptions,
    ItemIdentity, ExpiryHandler, ExpiryRegistration, ExpiryScheduler,
    ExpiryRegistrationResult, SESSION_ITEM_TYPE, ExpiryError, ExpiryMode,
)

__all__ = [
    "ExpiryScheduler", "SESSION_ITEM_TYPE", "ItemIdentity", "ExpiryRegistration", "ExpiryRegistrationResult",
    "ExpiryMode", "ExpiryError", "HandlerOptions", "ExpiryHandler",
    "CallbackRequest", "CallbackResult", "CallbackStatus",
]
```

包入口不导出 facade、store、registry、runtime、Redis 配置、Claim 或 BucketRef。Python 模块路径不构成安全隔离，但业务模块只依赖此公开导出，内部类型不作为兼容接口。

### 6.4 独立调度器如何得到类型信息

`api.py` 中的 `SUPPORTED_ITEM_TYPES` 是类型信息的唯一来源。业务 `register_handler()` 只允许注册清单内类型；report 在写 Redis 前按同一清单校验。独立调度器直接引用清单，不需要加载业务 Handler，也不访问 Redis 类型目录；处理器对象和 HandlerOptions 只留在本地进程。

调度器先取得本轮源时间上界，再使用 `SUPPORTED_ITEM_TYPES` 定位并清理未发布分钟的临时 B。注册表 G 仍是唯一的“桶可以消费”的标志。类型下线前必须先排空 source、bucket 和 processing 数据，再从代码清单删除。

回调实例应加载其负责业务的注册模块；遇到自己没有 handler 的历史/新类型，保留任务并报警，不静默 ACK。首期不做运行中替换处理器或调整并发；新增业务在进程启动阶段完成注册。

当前 callback owner 按 shard 分配，同一 namespace 的回调副本应加载同一组业务处理器。缺少 handler 的 owner 不会自动把该类型让给其他副本；新增类型应先部署对应回调注册，再开放业务上报。按类型部署不同 worker 属于另一种路由模型，不在首期隐式支持。


## 7. publisher.py：持锁清空到期积压，空范围跳过

```python
from __future__ import annotations

import asyncio
import time
from collections import defaultdict
from typing import Sequence
from .keys import decode_item_id
from .metrics import ExpiryMetrics
from .models import (
    MINUTE_MS, BucketRef, OwnerRole, PublishResult,
    StoreOperationError, bucket_minute_ms,
)
from .redis_store import RedisExpiryStore
from .config import ExpiryConfig
from .shard_runner import ShardExecution, ShardRunner


async def _wait_until_next_round(stop: asyncio.Event, delay: float) -> None:
    """无租约时的轮间等待；stop可唤醒，不负责续租或失锁监视。"""
    try:
        await asyncio.wait_for(stop.wait(), delay)
    except TimeoutError:
        pass


class ExpiryPublisher:
    def __init__(self, store: RedisExpiryStore, runner: ShardRunner,
                 config: ExpiryConfig, metrics: ExpiryMetrics) -> None:
        """worker拥有shard遍历与分钟节奏，runner只执行一次单shard工作。"""
        self._store, self._runner, self._config, self._metrics = store, runner, config, metrics

    async def run(self, stop: asyncio.Event) -> None:
        """每轮递增遍历全部shard；shard_not_stopped必须上抛，不能继续抢锁。"""
        while not stop.is_set():
            for shard_id in range(self._config.shard_count):
                if stop.is_set():
                    break
                try:
                    await self._runner.run_if_acquired(
                        OwnerRole.SCHEDULER, shard_id, self.process_shard, stop,
                    )
                except StoreOperationError as exc:
                    if exc.code == "shard_not_stopped":
                        raise
                    self._metrics.count("timer_shard_round", outcome="failed")
                except Exception:
                    self._metrics.count("timer_shard_round", outcome="failed")
            if not stop.is_set():
                delay = max(0.05, 60.0 - time.time() % 60.0)
                await _wait_until_next_round(stop, delay)

    async def process_shard(self, lease: ShardExecution, stop: asyncio.Event) -> None:
        """持续处理全部到期积压，不因页数/分钟数预算交出锁。

        每步检查lease；取消后退出旧函数，新领取从C重建。
        _process_minute返回None表示重读后该分钟已空，本地扫描下界前进但不写C。
        """
        while not stop.is_set():
            lease.ensure_active()
            window = await self._store.read_scan_window(lease.owner.shard_id)
            progress = window.progress_ms
            lease.ensure_active()
            await self._store.cleanup_source(lease.owner.shard_id, progress)
            if window.upper_ms <= progress:
                return
            after_ms = progress
            published_any = False
            while after_ms < window.upper_ms and not stop.is_set():
                lease.ensure_active()
                page = await self._store.read_source_page(
                    lease.owner.shard_id, lower_exclusive_ms=after_ms,
                    upper_inclusive_ms=window.upper_ms, after=None, limit=1,
                )
                if not page.items:
                    break
                minute = bucket_minute_ms(page.items[0].due_at_ms)
                result = await self._process_minute(lease, progress_ms=progress, minute_ms=minute)
                if result is not None:
                    progress = result
                    published_any = True
                after_ms = minute
            if not published_any:
                return
            # 确认追平当前上界后再读一次新上界，不持锁忙等尚未到期资源。

    async def _process_minute(self, lease: ShardExecution, *, progress_ms: int,
                              minute_ms: int) -> int | None:
        """分页读完整个非空分钟 -> 写完B -> 原子发布G/C -> 清理R。"""
        lease.ensure_active()
        await self._store.reset_unpublished_minute(
            lease.owner, minute_ms=minute_ms, expected_progress_ms=progress_ms,
            item_types=tuple(sorted(SUPPORTED_ITEM_TYPES)),
        )
        cursor = None
        types_with_data: set[str] = set()
        while True:
            lease.ensure_active()
            page = await self._store.read_source_page(
                lease.owner.shard_id, lower_exclusive_ms=max(progress_ms, minute_ms - MINUTE_MS),
                upper_inclusive_ms=minute_ms, after=cursor, limit=self._config.scan_batch_size,
            )
            by_type: dict[str, list[str]] = defaultdict(list)
            for item in page.items:
                identity = decode_item_id(item.item_id)
                if identity.item_type not in SUPPORTED_ITEM_TYPES:
                    raise StoreOperationError("unregistered_type", "Expiry item type is not supported")
                by_type[identity.item_type].append(item.item_id)
            for item_type, item_ids in by_type.items():
                lease.ensure_active()
                ref = BucketRef(item_type, lease.owner.shard_id, minute_ms)
                await self._store.stage_items(lease.owner, ref, item_ids)
                types_with_data.add(item_type)
            cursor = page.next_cursor
            if cursor is None:
                break
        if not types_with_data:
            return None
        lease.ensure_active()
        refs = tuple(BucketRef(t, lease.owner.shard_id, minute_ms) for t in sorted(types_with_data))
        result = await self._publish_completed_minute(
            lease, expected_progress_ms=progress_ms, minute_ms=minute_ms, buckets=refs,
        )
        lease.ensure_active()
        await self._store.cleanup_source(lease.owner.shard_id, result.progress_ms)
        return result.progress_ms

    async def _publish_completed_minute(self, lease: ShardExecution, *, expected_progress_ms: int,
                                        minute_ms: int, buckets: Sequence[BucketRef]) -> PublishResult:
        """同一有效lease中保留不可变发布参数，超时查C或重试同一publish。

        C未前进不代表旧请求不会执行，不能重置B。每次I/O前检查lease。
        lease失败立即退出，不在后续新lease中恢复本函数；未决请求风险见主稿10.3。
        """
        ...
```

分页是内存/命令预算，持锁期间可持续处理任意多个分钟。source_time只读，R的score只有report更新；调度的R变更仅为按已确认C清理。

## 8. consumer.py：清空积压、三次机会、仅日志终结

```python
from __future__ import annotations

import asyncio
from collections import deque
from dataclasses import dataclass, field
from typing import Mapping
from .api import CallbackRequest, CallbackStatus, ExpiryError
from .keys import decode_item_id
from .metrics import ExpiryMetrics
from .models import BucketRef, Claim, OwnerRole, StoreOperationError, decode_claim_score
from .registry import HandlerBinding
from .redis_store import RedisExpiryStore
from .config import ExpiryConfig
from .shard_runner import ShardExecution, ShardRunner


@dataclass
class _TypeState:
    limit: int
    reserved: int = 0
    buckets: deque[BucketRef] = field(default_factory=deque)
    queued: set[BucketRef] = field(default_factory=set)
    tasks: set[asyncio.Task[None]] = field(default_factory=set)


class ExpiryConsumer:
    def __init__(self, store: RedisExpiryStore, runner: ShardRunner,
                 bindings: Mapping[str, HandlerBinding], config: ExpiryConfig,
                 metrics: ExpiryMetrics) -> None:
        """类型并发不跨shard叠加；旧tasks未退出时不得重置共享state。"""
        self._store, self._runner, self._bindings = store, runner, bindings
        self._config, self._metrics = config, metrics
        self._types = {t: _TypeState(b.options.concurrency) for t, b in bindings.items()}
        self._retry_layers: dict[BucketRef, int] = {}

    async def run(self, stop: asyncio.Event) -> None:
        """worker自己逐个竞争全部shard；轮后按callback_poll_seconds等待。"""
        while not stop.is_set():
            for shard_id in range(self._config.shard_count):
                if stop.is_set():
                    break
                try:
                    await self._runner.run_if_acquired(
                        OwnerRole.CALLBACK, shard_id, self.process_shard, stop,
                    )
                except StoreOperationError as exc:
                    if exc.code == "shard_not_stopped":
                        raise
                    self._metrics.count("timer_shard_round", outcome="failed")
                except Exception:
                    self._metrics.count("timer_shard_round", outcome="failed")
            if not stop.is_set():
                try:
                    await asyncio.wait_for(stop.wait(), self._config.callback_poll_seconds)
                except TimeoutError:
                    pass

    async def process_shard(self, lease: ShardExecution, stop: asyncio.Event) -> None:
        """消费业务流程不自建lost等待task；runner监视失锁/stop/grace并撤销子任务。"""
        self._reset_shard_state()
        try:
            while not stop.is_set():
                lease.ensure_active()
                found = await self._discover(lease)
                for item_type in self._types:
                    await self._fill_slots(lease, item_type)
                tasks = {task for state in self._types.values() for task in state.tasks}
                has_queued = any(state.buckets for state in self._types.values())
                if not found and not has_queued and not tasks:
                    return
                await lease.wait(self._config.callback_poll_seconds, stop)
            # 正常stop等待已启动callbacks；外层runner仍续租并监视lost/有效截止，
            # 到grace截止会取消process/children，故此处不是独立无界关闭路径。
            tasks = {task for state in self._types.values() for task in state.tasks}
            if tasks:
                await asyncio.wait(tasks)
                lease.ensure_active()
        finally:
            if not any(not task.done() for state in self._types.values() for task in state.tasks):
                self._reset_shard_state()

    def _reset_shard_state(self) -> None:
        """仅在旧tasks全部终止后清队列/计数；每次单shard执行清retry层缓存。"""
        for state in self._types.values():
            if any(not task.done() for task in state.tasks):
                raise StoreOperationError("shard_not_stopped", "Previous callbacks are still running")
            state.tasks.clear()
            state.buckets.clear()
            state.queued.clear()
            state.reserved = 0
        self._retry_layers.clear()

    async def _discover(self, lease: ShardExecution) -> bool:
        """lease检查后从G稳定分页，返回是否仍有可处理桶；每次发现有缓存预算。

        一页满不等于处理完；持续推进游标、绕回确认空。没有handler的类型不领取。
        """
        ...

    async def _fill_slots(self, lease: ShardExecution, item_type: str) -> None:
        """计算空闲槽并claim；claim前后检查execution，retry层仅本次shard运行使用。"""
        state = self._types[item_type]
        available = state.limit - state.reserved
        if available <= 0 or not state.buckets:
            return
        bucket = state.buckets.popleft()
        state.queued.discard(bucket)
        lease.ensure_active()
        binding = self._bindings[item_type]
        batch = await self._store.claim(
            bucket, limit=available, max_attempts=binding.options.max_attempts,
            processing_timeout_seconds=self._config.processing_timeout_seconds,
            retry_layer=self._retry_layers.get(bucket, 0),
            retry_layers_per_poll=self._config.retry_layers_per_poll,
        )
        lease.ensure_active()
        self._retry_layers[bucket] = batch.next_retry_layer
        for claim in batch.exhausted:
            await self._log_exhausted_and_ack(lease, claim, "processing_timeout")
        for claim in batch.claims:
            # 传工厂，不提前创建coroutine；runner强引用跟踪每个_execute子task。
            task = lease.spawn(lambda claim=claim: self._execute(lease, claim))
            state.reserved += 1
            state.tasks.add(task)
            task.add_done_callback(state.tasks.discard)
        lease.ensure_active()
        removed = await self._store.remove_registration_if_empty(bucket)
        if not removed and bucket not in state.queued:
            state.queued.add(bucket)
            state.buckets.append(bucket)
        elif removed:
            self._retry_layers.pop(bucket, None)

    async def _execute(self, lease: ShardExecution, claim: Claim) -> None:
        """最多调用一次handler；COMPLETED后只重试同一ACK，不变成业务失败。"""
        item_type = claim.bucket.item_type
        completed = False
        try:
            lease.ensure_active()
            request = await self._resolve_request(claim)
            lease.ensure_active()
            binding = self._bindings[item_type]
            result = await asyncio.wait_for(binding.handler.handle(request),
                                            timeout=self._config.processing_timeout_seconds)
            lease.ensure_active()
            if result.status is CallbackStatus.COMPLETED:
                completed = True
                await self._store.ack(claim)
                lease.ensure_active()
                await self._store.remove_registration_if_empty(claim.bucket)
            else:
                await self._finish_failed_attempt(lease, claim, "callback_retry")
        except asyncio.CancelledError:
            raise
        except StoreOperationError as exc:
            if exc.code == "owner_lost" or exc.outcome_unknown:
                raise  # 让runner保留失锁/未知结果语义，不吞掉后主动释放。
            self._record_callback_error(claim, exc)
            if not completed:
                lease.ensure_active()
                await self._finish_failed_attempt(lease, claim, "callback_exception")
        except Exception as exc:
            self._record_callback_error(claim, exc)
            if not completed:
                lease.ensure_active()
                await self._finish_failed_attempt(lease, claim, "callback_exception")
        finally:
            self._release_slot(item_type)

    async def _finish_failed_attempt(self, lease: ShardExecution, claim: Claim, reason: str) -> None:
        """未耗尽留B等待超时；已耗尽先日志再条件ACK。lease失效不再执行清理。"""
        attempt, _ = decode_claim_score(claim.claim_score)
        if attempt + 1 >= self._bindings[claim.bucket.item_type].options.max_attempts:
            await self._log_exhausted_and_ack(lease, claim, reason)

    async def _log_exhausted_and_ack(self, lease: ShardExecution, claim: Claim, reason: str) -> None:
        """先输出耗尽日志，再ACK相同claim；错误保留B，之后可重复日志和清理。

        日志无Redis副本，与ACK不是事务。lease_expired/异常均保守标记结果未知。
        """
        try:
            lease.ensure_active()
            self._metrics.log_exhausted(
                claim, max_attempts=self._bindings[claim.bucket.item_type].options.max_attempts,
                reason=reason, outcome_unknown=True,
            )
            lease.ensure_active()
            await self._store.ack(claim)
            lease.ensure_active()
            await self._store.remove_registration_if_empty(claim.bucket)
        except StoreOperationError as exc:
            if exc.code == "owner_lost" or exc.outcome_unknown:
                raise
            self._record_callback_error(claim, exc)
        except Exception as exc:
            self._record_callback_error(claim, exc)

    async def _resolve_request(self, claim: Claim) -> CallbackRequest:
        """M缺失可完整身份回源；M网络错误抛异常，身份冲突不可跨用户猜测。"""
        identity = decode_item_id(claim.item_id)
        if identity.item_type != claim.bucket.item_type:
            raise ExpiryError("invalid_identity", "Bucket type differs from item ID")
        metadata = await self._store.read_metadata(claim.item_id)
        if metadata is not None and metadata.identity != identity:
            raise ExpiryError("invalid_identity", "Metadata differs from item ID")
        self._metrics.count("timer_metadata_lookup", item_type=identity.item_type,
                            outcome="miss" if metadata is None else "hit")
        return CallbackRequest(identity)

    async def drain(self, timeout_seconds: float) -> None:
        """兼容辅助入口，不替代runner关闭：有界观察，超时cancel后报未退出，不无界join。"""
        tasks = {task for state in self._types.values() for task in state.tasks}
        if not tasks:
            return
        _, pending = await asyncio.wait(tasks, timeout=max(0.0, timeout_seconds))
        for task in pending:
            task.cancel()
        if pending:
            raise StoreOperationError("shard_not_stopped", "Callbacks did not drain within budget")

    def _release_slot(self, item_type: str) -> None:
        """同一运行loop释放一次预留，任务完成/取消均执行。"""
        ...

    def _record_callback_error(self, claim: Claim, exc: Exception) -> None:
        """低基数错误指标和限频日志；不保存失败到Redis，不直接清成员。"""
        ...

    # 持租约的poll等待使用ShardExecution.wait；consumer不维护单独lost task。
```

任务claim超时与shard租约超时是两种机制；首期不续租单任务，条件ACK保护超时重领。第三次超时只日志清理，不再发起业务调用。

## 9. leases.py、shard_runner.py 与 runtime.py：token、执行和生命周期

### 9.1 leases.py：Coordinator 仅三种 token 操作

```python
from __future__ import annotations

import asyncio
import time
import uuid
from .config import ExpiryConfig
from .metrics import ExpiryMetrics
from .models import OwnerRole, ShardLease, StoreOperationError
from .redis_store import RedisExpiryStore


class ShardLeaseCoordinator:
    def __init__(self, store: RedisExpiryStore, config: ExpiryConfig,
                 metrics: ExpiryMetrics) -> None:
        """只封装Redis token操作；不遍历shard，不创建process/heartbeat或等待轮间节奏。"""
        self._store, self._config, self._metrics = store, config, metrics

    async def try_acquire(self, role: OwnerRole, shard_id: int) -> ShardLease | None:
        """新随机token；截止=request开始monotonic+TTL，返回前检查未过期。"""
        started = time.monotonic()
        token = uuid.uuid4().hex
        owner = await self._store.try_acquire_owner(
            role, shard_id, token, self._config.owner_ttl_seconds,
        )
        if owner is None:
            return None
        lease = ShardLease(owner, asyncio.Event(), started + self._config.owner_ttl_seconds)
        lease.ensure_active()
        return lease

    async def renew(self, lease: ShardLease) -> bool:
        """任一失锁/异常/未知/取消或迟到成功使ownership永久不可信，旧lease不可复活。"""
        try:
            lease.ensure_active()
        except StoreOperationError:
            return False
        started = time.monotonic()
        previous_deadline = lease.valid_until_monotonic
        try:
            renewed = await self._store.renew_owner(lease.owner, self._config.owner_ttl_seconds)
        except asyncio.CancelledError:
            lease.lost.set()
            raise
        except Exception:
            lease.lost.set()
            return False
        task = asyncio.current_task()
        if (renewed is None or lease.lost.is_set()
                or (task is not None and task.cancelling())
                or time.monotonic() >= previous_deadline):
            lease.lost.set()
            return False
        if lease.revoked.is_set():
            return False
        lease.owner = renewed
        lease.valid_until_monotonic = started + self._config.owner_ttl_seconds
        return True

    async def release(self, lease: ShardLease) -> None:
        """runner确认本地工作/续租均终止且ownership可信后才调用；revoked本身不禁止释放。"""
        if time.monotonic() >= lease.valid_until_monotonic:
            lease.lost.set()
        if lease.lost.is_set():
            return
        try:
            await self._store.release_owner(lease.owner)
        except asyncio.CancelledError:
            lease.lost.set()
            raise
        except Exception as exc:
            if isinstance(exc, StoreOperationError) and exc.outcome_unknown:
                lease.lost.set()
            self._metrics.count("timer_owner_release", outcome="failed")
```

### 9.2 shard_runner.py：一次单 shard 执行，不替 worker 排轮次

```python
from __future__ import annotations

import asyncio
import time
from typing import Awaitable, Callable
from .config import ExpiryConfig
from .leases import ShardLeaseCoordinator
from .metrics import ExpiryMetrics
from .models import OwnerRole, OwnerToken, ShardLease, StoreOperationError

ShardProcessor = Callable[["ShardExecution", asyncio.Event], Awaitable[None]]


class ShardExecution:
    """内部可撤销执行句柄；业务processor只见检查点、owner和受跟踪的spawn/wait。"""

    def __init__(self, lease: ShardLease) -> None:
        self._lease = lease
        self._children: set[asyncio.Task[None]] = set()
        self._waiters: set[asyncio.Task[object]] = set()
        self._error: BaseException | None = None
        self._outcome_unknown = False

    @property
    def owner(self) -> OwnerToken:
        return self._lease.owner

    @property
    def shard_id(self) -> int:
        return self.owner.shard_id

    def ensure_active(self) -> None:
        task = asyncio.current_task()
        if task is not None and task.cancelling():
            # handler吞掉取消后也不能执行ACK；先同步标记unknown，不能等done callback。
            self._outcome_unknown = True
            raise asyncio.CancelledError
        self._lease.ensure_active()

    def spawn(self, operation: Callable[[], Awaitable[None]]) -> asyncio.Task[None]:
        """先检查再创建task；传coroutine工厂，避免撤销后遗留未await的coroutine。"""
        self.ensure_active()

        async def guarded() -> None:
            self.ensure_active()
            await operation()

        task = asyncio.create_task(guarded())
        self._children.add(task)
        task.add_done_callback(self._child_done)
        return task

    async def wait(self, delay_seconds: float, stop: asyncio.Event) -> None:
        """等待poll/stop/lost/revoked；两侧检查，timeout不超过本地lease截止。

        stop/lost/revoked的辅助waiters保持强引用，finally cancel；runner有界收集。
        """
        ...

    def _waiter_done(self, task: asyncio.Task[object]) -> None:
        """摘除准确task并读取异常，不能泄漏辅助等待。"""
        ...

    def _child_done(self, task: asyncio.Task[None]) -> None:
        """摘除task并记录错误；cancelled/cancelling或outcome_unknown均保守标记unknown。"""
        ...

    def _revoke(self) -> None:
        self._lease.revoked.set()  # 只撤销本地执行，不伪造ownership失锁。


def _consume_exception(task: asyncio.Task[object]) -> None:
    if not task.cancelled():
        task.exception()


class ShardRunner:
    def __init__(self, leases: ShardLeaseCoordinator, config: ExpiryConfig,
                 metrics: ExpiryMetrics) -> None:
        self._leases, self._config, self._metrics = leases, config, metrics
        self._active: set[OwnerRole] = set()
        self._pending: dict[OwnerRole, set[asyncio.Task[object]]] = {}

    @property
    def has_pending(self) -> bool:
        return any(not task.done() for tasks in self._pending.values() for task in tasks)

    async def wait_pending(self, timeout: float) -> bool:
        """仅有界观察强引用保留的工作，不取消；返回是否全部终止。"""
        tasks = {task for tasks in self._pending.values() for task in tasks if not task.done()}
        if tasks:
            await asyncio.wait(tasks, timeout=max(0.0, timeout))
        return not self.has_pending

    async def run_if_acquired(self, role: OwnerRole, shard_id: int,
                              process: ShardProcessor, stop: asyncio.Event) -> bool:
        """同role已有active或pending时抛shard_not_stopped；stop/busy返回False。

        acquire放入独立task并shield：调用者cancel后cancel请求task并有界_settle，
        不让抑制取消的Redis实现拖住join；未知领取只留TTL，不补release。
        成功后创建全新ShardExecution并_run；返回True表示取得过租约，不代表业务成功。
        """
        ...

    async def _run(self, role: OwnerRole, execution: ShardExecution,
                   process: ShardProcessor, stop: asyncio.Event) -> None:
        """创建processor/heartbeat/stop/lost tasks并独立监视有效截止。

        正常stop启动shutdown_timeout_seconds grace，heartbeat继续；grace期间仍监视
        lost/heartbeat/lease截止，不只等processor。失锁、grace超时、外层取消均forced。
        finally先revoke，再cancel processor/heartbeat/children/waiters，并有界_settle。
        若renew请求正进行，同步置lost后再cancel，抑制取消后的成功不能复活lease。
        processor已返回却仍有children属于生命周期违例，forced且不得主动release。
        所有task终止后读取processor/children异常，再决定release，最后向worker传播错误。
        普通业务异常但已终止、无forced/unknown且ownership可信时可比较token释放；
        forced/失锁/未知结果保守TTL。release本身也用asyncio.wait有界观察/保留pending。
        """
        ...

    async def _heartbeat(self, lease: ShardLease, renewing: asyncio.Event) -> None:
        """后台每20秒调用Coordinator.renew；renewing标记请求在途，失败结束。"""
        while True:
            await asyncio.sleep(self._config.owner_renew_interval_seconds)
            renewing.set()
            try:
                if not await self._leases.renew(lease):
                    return
            finally:
                renewing.clear()

    async def _settle(self, role: OwnerRole, tasks: set[asyncio.Task]) -> None:
        """cancel后的join用asyncio.wait(timeout=request_budget_seconds)，不等取消确认。

        await之前把未终止task放_pending强引用（包括acquire/release/heartbeat），
        done callback读取异常并只移除自己持有的准确集合，不清后轮同role的state。
        预算后仍pending抛shard_not_stopped；再次取消cleanup也不能丢失强引用。
        """
        ...

    def _pending_done(self, role: OwnerRole, retained: set[asyncio.Task[object]],
                      task: asyncio.Task[object]) -> None:
        """旧task真实退出才移除；保留集合identity检查避免误删后轮记录。"""
        ...

    @staticmethod
    def _not_stopped(role: OwnerRole) -> StoreOperationError:
        return StoreOperationError("shard_not_stopped", f"The {role.value} shard still has active local work")
```

runner 的 pending 限制是**同角色的全部 shard**，不是只禁止重领原 shard；另一角色的生命周期独立。`asyncio.wait` 仅保证事件循环能运行时等待有界，不强杀协程；未合作取消的工作仍被强引用保留，worker必须停止同role轮次，依赖不能先关闭。只有正常终止、ownership仍可信且没有未知结果才可主动释放；普通业务异常本身不是“结果未知”，错误仍须传播。

这不改变Redis/Lua/protocol，不撤回已发Redis写入，不构成跨槽fencing；当前B不按token隔离的残余风险继续按主稿10.3保留。Session外层timeout检查和原commit设计不因runner重构而改变。

### 9.3 runtime.py：共享 runner 的组装与有界关闭

Redis I/O 的终止凭据是 `_RedisRequest.finished`，不是调用方 wrapper 被取消，也不是 `run_coroutine_threadsafe` 返回的 Future 状态。请求提交前登记强引用，Redis-loop task 及其原始 operation 真正退出后才完成 finished 并摘除登记；请求取消或超时后仍保留生命周期桥接。runner 的有界观察和 executor 的 quiesce/join 分别保护业务工作与远端 I/O，不能用 role task 的退出代替 Redis-loop 的真实退出。

关闭顺序为 `stop_roles → io.quiesce → io.run_cleanup(store.close) → io.close`。quiesce 拒绝新外部请求，等待已登记请求真实 done，预算耗尽抛 `shard_not_stopped`，不提前关 store。cleanup 在 Redis loop 运行 raw parent，不先占用 `_gate`；store.close 内部嵌套 `_io.run` 可在 quiesce 后执行并正常关闭 client，避免 max_inflight=1 时递归等待自身持有的 gate。io.close 再次 quiesce 后仅请求一次 loop.stop，以剩余预算 join 线程；线程仍活着则保留 loop/thread/gate 引用，后续 close 可重试，但 start 不能创建第二线程。store cleanup 成功即置 `_storage_closed=True`；若后续线程 join 超时，仍保留 `_storage_started/storage_ready`，重试 close 跳过已成功的 cleanup，只重新观察/等待线程退出，不向已 stop_requested 的 loop 再提交 store.close。只有线程真实退出后清 I/O 引用和 runtime 的 `_storage_started/storage_ready`；start_storage 成功重置 `_storage_closed=False`。core 保留 runtime/adapter 并传播未退出异常，阻止 QueueFS/AGFS 提前关闭。

这里的预算是调用方的有界观察，不是强杀保证：`run` 取消后的 join、`_execute` 的取消收尾和线程内部清理可能等待非合作协程；其生命周期必须仍可由 pending/requests/thread 引用观察。此机制不撤回服务端已收到的 Redis 写入，也不提供跨槽 fencing。Session 外层 timeout 与其余 core startup/optional backend 不变。

```python
from __future__ import annotations

import asyncio
import concurrent.futures
import threading
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable, TypeVar
from .api import ExpiryScheduler
from .config import ExpiryConfig
from .consumer import ExpiryConsumer
from .keys import ExpiryKeys, encode_component
from .leases import ShardLeaseCoordinator
from .metrics import ExpiryMetrics
from .models import StoreOperationError
from .redis_store import RedisExpiryStore
from .publisher import ExpiryPublisher
from .registry import HandlerRegistry
from .service import _ExpirySchedulerFacade, ExpiryRuntimeState
from .shard_runner import ShardRunner

T = TypeVar("T")


@dataclass(eq=False)
class _RedisRequest:
    finished: concurrent.futures.Future = field(default_factory=concurrent.futures.Future)
    cancelled: threading.Event = field(default_factory=threading.Event)
    task: asyncio.Task | None = None  # 仅Redis loop读写；finished只在实际task退出后完成。


class RedisLoopExecutor:
    """专属后台线程 + asyncio loop；跟踪真实Redis任务，不运行session commit。"""

    def __init__(self, *, max_inflight: int, request_budget_seconds: float) -> None:
        self._max_inflight = max_inflight
        self._request_budget_seconds = request_budget_seconds
        self._loop: asyncio.AbstractEventLoop | None = None
        self._thread: threading.Thread | None = None
        self._gate: asyncio.Semaphore | None = None
        self._accepting = False
        self._ready = threading.Event()
        self._requests: set[_RedisRequest] = set()
        self._requests_lock = threading.Lock()
        self._stop_requested = False

    async def start(self) -> None:
        """已accepting时幂等；旧thread引用仍在则shard_not_stopped，不创建第二线程。

        清ready/stop_requested，启动线程并等待ready；gate仅在Redis loop创建。
        """
        ...

    async def run(self, operation: Callable[[], Awaitable[T]]) -> T:
        """调用方取消后仍等待真实finished；多次取消不能切断wrapper的生命周期桥接。"""
        loop = self._loop
        if loop is not None and asyncio.get_running_loop() is loop:
            # cleanup parent已登记，嵌套client关闭允许在quiesce之后执行。
            return await self._execute(operation)
        if not self._accepting or loop is None:
            raise StoreOperationError("not_started", "Redis expiry I/O loop is not running")
        request = self._submit(operation)
        finished = asyncio.wrap_future(request.finished)
        try:
            return await asyncio.shield(finished)
        except asyncio.CancelledError:
            self._cancel(request)
            await self._join(finished)
            raise

    async def quiesce(self, timeout_seconds: float) -> None:
        """先拒绝新外部请求，以asyncio.wait有界等待登记请求的真实finished。

        包括尚未开始的请求；done读取异常，pending安装异常消费回调但不丢强引用。
        未退出抛shard_not_stopped，不关闭client/loop/thread。
        """
        ...

    async def run_cleanup(self, operation: Callable[[], Awaitable[T]],
                          timeout_seconds: float) -> T:
        """quiesce后提交tracked raw parent；不经_execute先占gate，避免nested run递归。

        cleanup自身使用显式timeout而非普通请求预算；超时/取消发一次cancel，
        保留request并消费后续异常，超时报shard_not_stopped，不把取消当退出。
        """
        ...

    async def close(self, timeout_seconds: float) -> None:
        deadline = time.monotonic() + max(0.0, timeout_seconds)
        await self.quiesce(timeout_seconds)
        loop, thread = self._loop, self._thread
        if loop is not None and not self._stop_requested:
            self._stop_requested = True
            loop.call_soon_threadsafe(loop.stop)
        if thread is not None:
            await asyncio.to_thread(thread.join, max(0.0, deadline - time.monotonic()))
            if thread.is_alive():
                raise self._not_stopped()  # 保留引用，重试不重复stop；start不能新建线程。
        self._loop = None
        self._thread = None
        self._gate = None

    def _submit(self, operation: Callable[[], Awaitable[T]], *,
                cleanup: bool = False) -> _RedisRequest:
        """requests先登记后call_soon_threadsafe创建task，关闭须看到尚未开始的请求。

        普通请求task运行_execute；cleanup task直接await operation，不先占gate。
        task的done callback调用_request_done；预先取消的request启动时立即cancel。
        无loop或已stop_requested拒绝；调度失败撤销登记。
        """
        ...

    def _cancel(self, request: _RedisRequest) -> None:
        """threading.Event确保只向远端task发一次cancel；先取消后启动也不会漏掉。"""
        ...

    def _request_done(self, request: _RedisRequest, task: asyncio.Task) -> None:
        """真实task终止才将result/error/CancelledError写finished，然后摘除requests。"""
        ...

    @staticmethod
    def _consume_exception(future: asyncio.Future) -> None:
        if not future.cancelled():
            future.exception()

    @staticmethod
    async def _join(future: asyncio.Future) -> None:
        while not future.done():
            try:
                await asyncio.shield(future)
            except (asyncio.CancelledError, Exception):
                pass  # 即使调用方再次cancel也保留等待；不重复cancel原始operation。
        if not future.cancelled():
            future.exception()

    @staticmethod
    def _not_stopped() -> StoreOperationError:
        return StoreOperationError(
            "shard_not_stopped", "Redis expiry work or its I/O thread has not terminated",
        )

    def _thread_main(self) -> None:
        """创建loop/gate并发布ready，run_forever结束后cancel/gather残余任务并关loop。

        线程内清理不冒充有界强杀：close在外层有界join，非合作清理仍alive则保留引用。
        """
        ...

    async def _execute(self, operation: Callable[[], Awaitable[T]]) -> T:
        """gate内创建原始operation task，总请求预算含gate等待。

        超时cancel原始task后_join至真实done再报redis_timeout/outcome_unknown；
        finally也join尚未退出的operation，防止外层task先done而远端工作仍存活。
        预算触发取消并非硬终止，亦不证明服务端Redis写入撤销。
        """
        ...


@dataclass(frozen=True)
class ExpiryRuntimeRoles:
    report: bool = False
    publish: bool = False
    consume: bool = False


class ExpiryRuntime:
    def __init__(self, config: ExpiryConfig, *, roles: ExpiryRuntimeRoles,
                 metrics: ExpiryMetrics | None = None) -> None:
        """构造 executor/store/service；不初始化 AGFS 或模型。"""
        self._config = config
        self._metrics = metrics or ExpiryMetrics()
        self._keys = ExpiryKeys(encode_component(config.service_name))
        self._io = RedisLoopExecutor(
            max_inflight=config.redis.max_inflight_requests,
            request_budget_seconds=config.redis.request_budget_seconds,
        )
        self._store = RedisExpiryStore(config.redis, self._keys, self._io, self._metrics)
        self._registry = HandlerRegistry()
        self._state = ExpiryRuntimeState()
        self._facade = _ExpirySchedulerFacade(
            self._store, self._registry, self._state, shard_count=config.shard_count,
            reporting_enabled=roles.report, metrics=self._metrics,
        )
        self._leases = ShardLeaseCoordinator(self._store, config, self._metrics)
        self._shard_runner = ShardRunner(self._leases, config, self._metrics)
        self._publisher: ExpiryPublisher | None = None
        self._consumer: ExpiryConsumer | None = None
        self._tasks: dict[str, asyncio.Task[None]] = {}
        self._stop = asyncio.Event()
        self._storage_started = False
        self._storage_closed = False  # store cleanup成功与I/O线程退出分别记录。

    @property
    def scheduler(self) -> ExpiryScheduler:
        """仅供启动组装层取得协议引用，再注入业务；不把 runtime 交给业务。"""
        return self._facade

    async def start_storage(self) -> None:
        """依次 executor.start -> store.open -> ensure_namespace，置内部 storage_ready。

        成功时置_storage_started/storage_ready，并重置_storage_closed=False。
        此时只开放本地 handler 注册，report 等 start_roles 完成才可用。
        任一步失败按逆序释放已建资源，不启动 consumer。
        """
        ...

    async def start_roles(self) -> None:
        """冻结本地 registry 后启动组装层选择的 publisher/consumer。

        同一_shard_runner注入scheduler/consumer，不把Coordinator交给worker。
        API 须先完成业务注册和 QueueFS 启动，再调用这里。
        独立 publisher 没有本地 handler，直接使用 SUPPORTED_ITEM_TYPES。
        """
        bindings = self._registry.freeze()
        self._stop.clear()
        if self._roles.publish:
            self._publisher = ExpiryPublisher(
                self._store, self._shard_runner, self._config, self._metrics,
            )
            self._spawn_role("publisher", self._publisher.run(self._stop))
        if self._roles.consume:
            self._consumer = ExpiryConsumer(
                self._store, self._shard_runner, bindings, self._config, self._metrics,
            )
            self._spawn_role("callback", self._consumer.run(self._stop))
        self._state.accepting_calls = True

    async def wait(self) -> None:
        """独立进程等待角色任务结束；任一角色非预期退出向 CLI 传播异常。"""
        ...

    async def stop_roles(self) -> None:
        """先拒绝新调用/set stop；grace后cancel，所有join均用asyncio.wait有界观察。

        保留未终止role task和runner pending；仅真实done才读异常并移除引用。
        未退出则抛shard_not_stopped，保持store/I/O/业务依赖打开，允许后续再次观察。
        """
        self._state.accepting_calls = False
        self._stop.set()
        tasks = set(self._tasks.values())
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=self._config.shutdown_timeout_seconds)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.wait(pending, timeout=self._config.redis.request_budget_seconds)
            for name, task in list(self._tasks.items()):
                if task.done():
                    if not task.cancelled():
                        task.exception()
                    self._tasks.pop(name, None)
        settled = await self._shard_runner.wait_pending(self._config.redis.request_budget_seconds)
        if self._tasks or not settled:
            raise StoreOperationError("shard_not_stopped", "Expiry work is still running; dependencies remain open")

    async def close(self) -> None:
        """依次停止role、等待真实I/O、同loop清理store、停止并join线程。

        stop_roles/quiesce未退出不进入依赖清理；cleanup之后finally尝试io.close，
        io.close若报未退出则保留storage标志，不能提前清状态或继续关业务依赖。
        各阶段分别使用配置预算，非整个close共享一个总截止。
        """
        await self.stop_roles()
        if not self._storage_started:
            return
        await self._io.quiesce(self._config.shutdown_timeout_seconds)
        try:
            if not self._storage_closed:
                await self._io.run_cleanup(self._store.close, self._config.shutdown_timeout_seconds)
                self._storage_closed = True
        finally:
            await self._io.close(self._config.shutdown_timeout_seconds)
            self._storage_started = False
            self._state.storage_ready = False

    def _spawn_role(self, name: str, operation: Awaitable[None]) -> None:
        """保存 Task 强引用与退出结果；角色异常要可见，不能静默失去调度循环。"""
        ...
```

## 10. lua/：精简后的全部脚本契约

仅保留12个Lua文件（含共享前缀）。不再有源状态初始化、源关闭水位、注销伴随状态或失败归档脚本。所有脚本在写前校验参数与key类型；脚本执行异常不保证回滚。

### 10.1 _common.lua

```lua
local BASE = 1099511627776
local MAX_INT = 9007199254740991

local function decimal(n)
    -- 时间和claim score使用精确十进制字符串。
    return string.format('%.0f', n)
end

local function now_ms()
    -- 返回当前执行节点TIME，不能用另一slot节点或客户端时间替代源准入时间。
    local t = redis.call('TIME')
    return tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
end

local function integer_arg(value, min_value, max_value)
    -- 严格校验规范十进制、整数和范围，不接受NaN/inf；错误发生在任何写入前。
end

local function expect_type(key, allowed)
    -- TYPE必须为none或指定类型，否则写前报错。
end

local function require_owner(key, token)
    -- GET必须等于token且PTTL>0，否则owner_lost；不重新创建或续活失效锁。
end

local function page_after(key, lower, upper, last_score, last_member, limit)
    -- 稳定score/member分页：原游标仍在则rank快路径，否则rank二分定位。
    -- 有界读取limit+1，返回items及next，不使用客户端OFFSET，不扫描全部同score。
end
```

### 10.2 ensure_metadata.lua

```lua
-- KEYS = M
-- ARGV = item_type, local_id, account_id, user_id, ttl_ms
-- RETURN = 'created' | 'refreshed'
-- ttl_ms=max(2*timeout_seconds*1000,86400000)，由store统一计算。
-- 先校验HASH/none、全部身份和ttl范围；无schema字段。
-- 不存在则HSET身份；存在逐字段校验，冲突在任何写入前报错。
-- 最后PEXPIRE ttl_ms；只有前一步R明确成功后才调用本脚本。
```

### 10.3 upsert_resource.lua

```lua
-- KEYS = R；ARGV = item_id, requested_due_ms
-- RETURN = decimal(accepted_due_ms)
-- 仅report调用；不存在V，不读C，没有revision/CAS/请求顺序比较。
expect_type(KEYS[1], 'zset')
local requested = integer_arg(ARGV[2], 0, MAX_INT)
local current_time = now_ms()
local accepted = math.max(requested, current_time + 1)
-- 写前检查item_id非空；向上对齐后的桶分钟必须仍在精确整数范围内。
local max_supported_due = math.floor(MAX_INT / 60000) * 60000
if accepted > max_supported_due then return redis.error_reply('invalid_report') end
redis.call('ZADD', KEYS[1], decimal(accepted), ARGV[1])
return decimal(accepted)
```

这里校正的是写入时的未来到期时间，不能识别业务先后顺序。重试报告可能覆盖并发更新；接口不返回DUPLICATE/STALE。R已经成功但M失败时不补偿删除R。

### 10.4 source_time.lua

```lua
-- KEYS = R （用于路由到R所在Redis slot）；无ARGV。
-- RETURN = decimal(source_now_ms)
-- 本脚本只读，不修改R，也不创建任何伴随key。
return decimal(now_ms())
```

store读C并使用该时间构造U=floor(T/60000)*60000。U=C或无数据则跳过；不执行“关闭窗口”写入。

### 10.5 read_source_page.lua

```lua
-- KEYS = R
-- ARGV = lower_exclusive, upper_inclusive, after_score_or_empty, after_member, limit
-- RETURN = cjson {items=[[item,score_string],...], next=[score_string,item] | false}
-- 校验后调用page_after；整个范围为空返回空items，不修改C。
-- scheduler先limit=1定位下一个非空分钟，再分页读完整分钟，避免逐空分钟循环。
```

### 10.6 publish_minute.lua

```lua
-- KEYS = G, C, scheduler_owner （同槽）
-- ARGV = owner_token, expected_C, minute_ms, member_count, registry_member...
-- RETURN = {'published'|'already_published', decimal(C)}
-- 先验证key类型、C存在、分钟对齐、全部注册member与shard/minute一致。
-- 空分钟不调用，本脚本要求member_count>=1。
require_owner(KEYS[3], ARGV[1])
local expected = integer_arg(ARGV[2], 0, MAX_INT)
local minute = integer_arg(ARGV[3], 0, MAX_INT)
local count = integer_arg(ARGV[4], 1, 1024)
local current = tonumber(redis.call('GET', KEYS[2]))
if current >= minute then
    return {'already_published', decimal(current)}
end
if current ~= expected or minute <= current then
    return redis.error_reply('progress_conflict')
end
-- 中间空分钟已由scheduler扫描证实；只要求单调，不要求minute=current+60000。
for i = 1, count do
    redis.call('ZADD', KEYS[1], decimal(minute), ARGV[4+i])
end
redis.call('SET', KEYS[2], decimal(minute))
return {'published', decimal(minute)}
```

owner比较与发布原子，但不能约束其他slot上已经发出的B写入；保留主文档10.3的限制。

### 10.7 read_registry_page.lua

```lua
-- KEYS = G；ARGV = after_minute_or_empty, after_member, limit
-- RETURN = cjson {items=[[registry_member,minute_string],...], next=[minute,member] | false}
-- 使用page_after；G只包含发布完成的桶，无需额外用客户端minute<now过滤。
```

### 10.8 claim.lua

```lua
-- KEYS = B
-- ARGV = limit, processing_timeout_seconds, retry_layer, retry_layers_per_poll, max_attempts
-- RETURN = cjson {claims=[[item,new_score],...], exhausted=[[item,old_score],...],
--                 next_retry_layer, layers_scanned}
-- 先校验key/参数和全部候选新score；max_attempts∈[1,256]，session=3。
-- 按轮转的attempt层有界检查超时，总超时候选<=limit，不能被新项永久饿死。
-- attempt+1<max_attempts：可重领，更新score后加入claims。
-- attempt+1>=max_attempts：仅加入exhausted，保持原score，不删除、不新增Redis记录。
-- score=0新项优先，但至少为已有可重试项保留一个槽；claims总数<=limit。
-- consumer对exhausted先日志再ack.lua，不调用handler；总检查/写入<=2*limit。

local function next_score(old_score, now_seconds, max_attempts)
    -- now_seconds在外部通过范围检查。
    if old_score == 0 then return now_seconds end
    local attempt = math.floor(old_score / BASE)
    if attempt + 1 >= max_attempts then error('attempts_exhausted') end
    local next_time = math.max(now_seconds, old_score % BASE + 1)
    if next_time >= BASE then error('invalid_claim_time') end
    return (attempt + 1) * BASE + next_time
end
```

### 10.9 ack.lua

```lua
-- KEYS = B；ARGV = item_id, exact_claim_score；RETURN = 0 | 1。
-- 成功回调ACK和耗尽日志后的清理共用；不删除R/M。
local expected = integer_arg(ARGV[2], 1, 256 * BASE - 1)
local current = redis.call('ZSCORE', KEYS[1], ARGV[1])
if current and tonumber(current) == expected then
    return redis.call('ZREM', KEYS[1], ARGV[1])
end
return 0
```

### 10.10 acquire_owner.lua

```lua
-- KEYS = owner；ARGV = token, ttl_ms（本期120000）
-- RETURN = {'owned', expires_at_ms} | {'busy', 0}
-- SET NX PX；同token只确认本次请求已取得锁，不因此延长lease。
-- 新token未取得则busy；返回TIME/PTTL供观测，本地使用请求起点+TTL保守截止。
```

### 10.11 renew_owner.lua

```lua
-- KEYS = owner；ARGV = token, ttl_ms（120000）；RETURN = expires_at_ms | 0。
-- GET==token且PTTL>0才PEXPIRE；不创建缺失锁，不延长其他token的锁。
-- 任意不匹配/异常/超时在调用方立即使当前lease失效，不等到下一个20秒周期。
```

### 10.12 release_owner.lua

```lua
-- KEYS = owner；ARGV = token；RETURN = 0 | 1。
-- 本地续租task已经停止并await后调用，防止释放与自己的续租交错。
if redis.call('GET', KEYS[1]) == ARGV[1] then
    return redis.call('DEL', KEYS[1])
end
return 0
```

## 11. session_expiry.py：业务适配，不把 policy 塞进调度层

```python
from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING
from openviking.server.identity import RequestContext, Role
from openviking.session.auto_commit_policy import AutoCommitPolicy
from openviking.session.session import SessionExpirySnapshot
from openviking.scheduling.expiry import (
    CallbackRequest, CallbackResult, CallbackStatus, HandlerOptions, ItemIdentity,
    ExpiryRegistration, ExpiryScheduler, ExpiryMode,
)
from openviking_cli.session.user_id import UserIdentifier

if TYPE_CHECKING:
    from openviking.service.session_service import SessionService


def install_session_expiry(
    scheduler: ExpiryScheduler, sessions: SessionService, *, concurrency: int = 8
) -> None:
    """业务模块自己的启动注册函数；core 只调用业务初始化，不认识处理器细节。"""
    scheduler.register_handler("session", SessionTimeoutHandler(sessions, scheduler),
                               options=HandlerOptions(concurrency=concurrency, max_attempts=3))
    sessions.set_session_expiry_adapter(SessionExpiryAdapter(scheduler))


def effective_idle_timeout(policy: dict | None) -> int | None:
    """None 表示禁用；不能将 None 传给 from_dict 后意外启用默认 policy。"""
    if policy is None:
        return None
    timeout = AutoCommitPolicy.from_dict(policy).idle_timeout_seconds
    return timeout if timeout > 0 else None


def timestamp_ms(value: str) -> int:
    """沿用内核时间格式解析规则，统一 UTC 毫秒；错误显式上报。"""
    ...


class SessionExpiryAdapter:
    """实现本业务的 SessionExpiryReporter；承接已成功持久化的消息快照。"""

    def __init__(self, scheduler: ExpiryScheduler) -> None:
        """只依赖到期调度协议；不依赖 store、runtime 或框架内部指标类型。"""
        self._scheduler = scheduler

    def build_report(self, snapshot: SessionExpirySnapshot) -> ExpiryRegistration | None:
        """在业务层计算 due；禁用时返回 None，不回改已有历史 score。"""
        timeout = effective_idle_timeout(snapshot.auto_commit_policy)
        if timeout is None:
            return None
        return ExpiryRegistration(
            identity=ItemIdentity("session", snapshot.account_id,
                                      snapshot.user_id, snapshot.session_id),
            due_at_ms=timestamp_ms(snapshot.last_message_at) + timeout * 1000,
            timeout_seconds=timeout,
            mode=ExpiryMode.IDLE_ONESHOT,
        )

    async def report_after_append(self, snapshot: SessionExpirySnapshot) -> None:
        """消息已落盘，报告最终失败记录指标，不伪装消息回滚。

        立即 await，没有内存去抖；CancelledError 不被 except Exception 吞掉。
        """
        try:
            report = self.build_report(snapshot)
            if report is not None:
                await self._scheduler.report(report)
        except Exception as exc:
            self.record_report_failure("report_failed", exc)

    async def unregister_after_delete(self, identity: ItemIdentity) -> None:
        """业务删除已成功后尽力注销；失败记录日志/指标，不反向恢复业务数据。"""
        ...

    def record_report_failure(self, reason: str, exc: Exception | None = None) -> None:
        """使用业务自身的日志/指标记录无法报告计时；不接触框架内部队列或指标对象。"""
        ...


class SessionTimeoutHandler:
    def __init__(self, sessions: SessionService, scheduler: ExpiryScheduler) -> None:
        """持有业务服务和公开scheduler协议；不持有Redis实现，冷却重投也走report。"""
        self._sessions, self._scheduler = sessions, scheduler

    async def handle(self, request: CallbackRequest) -> CallbackResult:
        """只按业务身份构造 ctx 并提交，不感知元数据缓存、调度桶或消费凭据。"""
        ctx = self._make_context(request.identity)
        # SessionService.commit_on_timeout 自行把明确业务 NotFound 转 skipped。
        # 存储/网络异常继续抛给 consumer，保留 Redis 成员。
        result = await self._sessions.commit_on_timeout(request.identity.local_id, ctx)
        if result.get("status") == "deferred":
            await self._report_deferred(request.identity, result)
            return CallbackResult(CallbackStatus.COMPLETED, "idle_rescheduled")
        return self._to_callback_result(result)

    async def _report_deferred(self, identity: ItemIdentity, result: dict) -> None:
        """使用服务层纯检查生成的新due报告，不持有Session提交锁，不取旧score或旧timeout。

        必须await R→M成功后才返回。异常传播为旧事件RETRY，不吞错假装已重投。
        只接受 idle_cooldown 和 idle_activity；disabled/无消息不生成deferred；
        重投不更新last_idle_commit_accepted_at。
        """
        if result.get("reason") not in {"idle_cooldown", "idle_activity"}:
            raise ValueError("Unexpected deferred reason")
        await self._scheduler.report(ExpiryRegistration(
            identity=identity, due_at_ms=result["due_at_ms"],
            timeout_seconds=result["timeout_seconds"], mode=ExpiryMode.IDLE_ONESHOT,
        ))

    def _make_context(self, identity: ItemIdentity) -> RequestContext:
        """使用资源自身 account/user，沿用业务用户权限，不提升到 root。"""
        return RequestContext(
            user=UserIdentifier(account_id=identity.account_id, user_id=identity.user_id),
            role=Role.USER,
        )

    def _to_callback_result(self, result: dict) -> CallbackResult:
        """accepted 或明确 skipped -> COMPLETED；未知返回结构 -> RETRY/抛协议错误。

        不把HTTP 200或任意truthy字典当作accepted；原commit API返回accepted即成功，不等后台任务。
        """
        status = result.get("status")
        if status == "accepted":
            return CallbackResult(CallbackStatus.COMPLETED, "accepted")
        if status == "skipped" and result.get("reason") in {
            "not_found", "idle_disabled", "no_messages",
        }:
            return CallbackResult(CallbackStatus.COMPLETED, result["reason"])
        return CallbackResult(CallbackStatus.RETRY, "unexpected_commit_result")
```

`effective_idle_timeout` 只用于上报适配器；超时判断由SessionService复用自身的AutoCommitPolicy与时间工具完成。Session提交内部不做idle判断、不掌握deferred或Redis，也不反向import此service文件。

## 12. session.py：新增时间字段与追加 reporter，保留原通用提交

### 12.1 新增字段和内部类型

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(frozen=True)
class SessionExpirySnapshot:
    account_id: str
    user_id: str
    session_id: str
    last_message_at: str
    auto_commit_policy: dict[str, Any] | None
    # policy 必须深拷贝；后续 Session 对象更新不得改变这次报告。


class SessionExpiryReporter(Protocol):
    async def report_after_append(self, snapshot: SessionExpirySnapshot) -> None:
        """持久化成功、释放 session lock 后立即等待；reporter 自己处理上报失败策略。"""
        ...

    def record_report_failure(self, reason: str, exc: Exception | None = None) -> None:
        """消息仍然成功，但无法构造可信计时快照时调用。"""
        ...
```

在现有 `SessionMeta` 追加字段，不新建第二份 metadata 文件：

```python
@dataclass
class SessionMeta:
    # 现有全部字段原样保留；这里只展示新增字段和要修改的方法。
    last_idle_commit_accepted_at: str = ""  # 服务层best-effort记录，不是ready原子记录或修复缓存。

    def to_dict(self) -> dict[str, Any]:
        """只加入接受时间字段，保留现有全部字段与输出兼容性，不新增idle归档指针。"""
        ...

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> SessionMeta:
        """缺字段使用空串；时间解析沿用内核工具，不从归档查证或恢复此字段。"""
        ...
```

### 12.2 Session 新增/修改的完整方法目录

以下 class 是现有类的**增量视图**，不是替换整个类。构造函数保留原参数，仅在末尾加可选 reporter：

```python
class Session:
    def __init__(
        self, viking_fs: "VikingFS", vikingdb_manager: "VikingDBManager | None" = None,
        session_compressor: "SessionCompressor | None" = None,
        user: "UserIdentifier | None" = None, ctx: "RequestContext | None" = None,
        session_id: str | None = None, session_uri: str | None = None,
        auto_commit_threshold: int = 8000,
        tool_output_externalization_config: "ToolOutputExternalizationConfig | None" = None,
        agent_evolution_enabled: bool = True, usage_reporter: "UsageReporter | None" = None,
        agent_evolution_enabled_provider: "Callable[[], bool | Awaitable[bool]] | None" = None,
        memory_policy_provider: "MemoryPolicyProvider | None" = None,
        vlm_resolver: "VLMResolver | None" = None,
        expiry_reporter: SessionExpiryReporter | None = None,
    ) -> None:
        """原初始化保留，仅增加 self._expiry_reporter = expiry_reporter。"""
        ...

    async def _append_messages_authoritatively(self, messages: "list[Message]") -> None:
        """修改现有方法：锁内追加并保存业务meta，锁外立即 await reporter。

        原写消息失败时不报告；无 VikingFS 的原内存分支不宣称持久化成功。
        元信息读取失败时维持既有消息处理规则，但不伪造可信的policy/时间快照。
        """
        ...

    def _build_expiry_snapshot_locked(self) -> SessionExpirySnapshot:
        """新增：只在最新消息/meta 写入成功后调用；深拷贝 policy 和身份。

        仅深拷贝当前时间、policy和身份，不分配资源更新版本。
        """
        ...

    # commit/commit_async及原通用提交内部实现保持不变，不为timer新增或拆分提交函数。
    # 服务层直接调用原commit_async；Session内部没有idle判断、deferred、Redis调用。
    # 原返回结构、参数校验、锁、ready、QueueFS及恢复能力继续存在。
    # 各既有meta更新路径须保留接受时间字段，但不生成或恢复该字段。
```

### 12.3 追加函数内部放置位置

下面是已有 `_append_messages_authoritatively()` 中的插入结构，省略的行保留原写消息逻辑，不是另新增一个消息入口：

```python
# 已持有 session path lock，且最新 meta 读取状态已知。
# 原有：reload -> _apply_appended_messages_to_state -> append/write messages.jsonl。
snapshot = None
snapshot_error = None
metadata_reload_error = None
await self._save_meta()
if self._expiry_reporter is not None:
    try:
        # save成功后，self._meta已是本次路径锁内刚持久化的权威状态。
        snapshot = self._build_expiry_snapshot_locked()
    except Exception as exc:
        snapshot_error = exc
# 原 finally 释放 session path lock。
# 以下放到 try/finally 外，只在持久化成功后执行：
if self._expiry_reporter is not None:
    if snapshot is not None:
        await self._expiry_reporter.report_after_append(snapshot)
    else:
        self._expiry_reporter.record_report_failure("snapshot_build_failed", snapshot_error)
```

实现时处理“消息文件已写成，但 meta 写失败”的既有业务结果，不额外声称它们是事务。追加前 meta 重读失败不再单独阻止 report：只要随后 `_save_meta()` 成功，就必须用刚持久化的状态构造 snapshot；只有 snapshot 本身无法构造时才不 report。`update_config()` 已在 session lock 内重读 meta，序列化时保留新增的接受时间字段；`update_event_search_tags()` 继续委托它，无需新建计时更新逻辑。

### 12.4 原通用 commit、QueueFS 与 Working Memory 契约

以下是保留清单，不新增一套提交实现：

| 位置/函数 | 必须保留的原行为 |
|---|---|
| Session.commit / commit_async | 原公开签名、参数校验、锁和返回结构不变；enable_working_memory的None/True/False完整透传 |
| 原通用ready与归档恢复 | 原能力和兼容协议不删除、不添加idle标记、接受时间或额外严格判断 |
| MemoryPolicy.from_dict / to_dict | enable_working_memory缺省False；序列化保留working_memory.enabled的真实布尔值 |
| SessionCommitMsg.to_dict / from_dict | 新消息memory_policy_version=1；旧队列沿用既有WM默认值兼容，显式False不覆盖 |
| Session.resume_queued_commit / _run_memory_extraction | 使用队列策略及原后台任务流程，不因timer重新解析当前用户策略 |
| Session._write_done_file | enable_working_memory参数和同名.done字段继续保留 |
| ArchiveStore.normalize_done / read_done / scan_states | 保留旧working_memory_enabled兼容；WM关闭且无摘要仍可正常完成 |

`memory_policy_version` 和原归档格式版本是既有持久业务协议，与Redis M.schema_version不同，不删除、不复制到R/M/B。无需因本方案修改memory_policy.py、archive_store.py或session_commit_msg.py。

idle服务层调用不覆盖WM，原始归档、QueueFS及符合policy的长期记忆提取继续存在。原commit的accepted及skipped字段（包括`effective_enable_working_memory`）原样返回；deferred只在SessionService/外层handler之间使用，不扩展Session公开commit返回语义。timer以原API返回accepted为成功，不等后台任务，不用ready恢复idle接受记录。

## 13. session_service.py：外层纯检查、原提交与 best-effort 接受记录

以下是服务层增量骨架；使用现有factory/get、路径锁、身份和时间工具，不将检查迁入Session。`get(..., auto_create=False)`必须加载最新session/meta，明确不存在才转skipped，其他读取错误不能退回旧内存状态。

```python
from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional
from openviking.core.namespace import canonical_session_uri
from openviking.server.identity import RequestContext
from openviking.session import Session
from openviking.session.archive_store import is_storage_not_found
from openviking.session.auto_commit_policy import AutoCommitPolicy
from openviking.utils.time_utils import get_current_timestamp, parse_iso_datetime
from openviking.scheduling.expiry import ExpiryScheduler

if TYPE_CHECKING:
    from .session_expiry import SessionExpiryAdapter

# 复用现有logger与meta锁超时配置，不新增另一套时间解析或锁策略。
logger = logging.getLogger(__name__)
_SESSION_META_LOCK_TIMEOUT_SECONDS = 30.0


class SessionService:
    # __init__ 原签名不变，初始化 self._session_expiry_adapter = None。

    def configure_expiry(self, scheduler: "ExpiryScheduler") -> None:
        """业务初始化入口：由 session 模块注册自己的 handler 和适配器。"""
        from .session_expiry import install_session_expiry
        install_session_expiry(scheduler, self)

    def set_session_expiry_adapter(self, adapter: "SessionExpiryAdapter | None") -> None:
        """新增：初始化阶段注入；关闭时摘除，不扫描已经创建的 Session 对象。"""
        self._session_expiry_adapter = adapter

    def session(self, ctx: RequestContext, session_id: str | None = None, *,
                session_uri: str | None = None) -> Session:
        """原 factory 参数/行为保留；构造 Session 时增加 expiry_reporter=adapter。

        原 memory_policy_provider、usage_reporter、vlm_resolver 等参数全部继续传递。
        """
        ...

    async def commit_on_timeout(self, session_id: str, ctx: RequestContext) -> dict[str, Any]:
        """加载最新状态、检查一次，直接复用原commit；不自动创建session。

        不在Session提交锁内最终复检idle，期间追加的新消息允许一起提交。
        原API返回accepted即成功，不等待后台任务；返回结构原样保留。
        入口加载/检查/commit异常传播，只有明确NotFound转skipped。
        """
        try:
            session = await self.get(session_id, ctx, auto_create=False)
        except Exception as exc:
            if not is_storage_not_found(exc):
                raise
            return {"status": "skipped", "reason": "not_found"}
        decision = self._check_timeout_commit(session, datetime.now(timezone.utc))
        if decision is not None:
            return decision  # skipped/deferred不记录接受时间，不调用commit。
        result = await session.commit_async(
            keep_recent_count=0,
            persist_keep_recent_count=False,
            record_auto_commit_success=True,
        )
        if result.get("status") == "accepted":
            await self._record_idle_commit_time(session_id, ctx, get_current_timestamp())
        # 原生命周期/归档指标保留；不把记录失败改成业务RETRY。
        return result

    @staticmethod
    def _check_timeout_commit(session: Session, now: datetime) -> Optional[dict]:
        """纯检查：policy/timeout -> no_messages -> cooldown -> last_message_due。

        只读最新加载状态，不取提交锁，不读归档，不写meta/Redis。
        None表示直接调用原commit；deferred只由服务层/外层handler掌握。
        时间解析使用parse_iso_datetime；缺失last_message_at或解析异常向外抛。
        """
        policy_data = session.meta.auto_commit_policy
        if policy_data is None:
            return {"status": "skipped", "reason": "idle_disabled"}
        timeout = AutoCommitPolicy.from_dict(policy_data).idle_timeout_seconds
        if timeout <= 0:
            return {"status": "skipped", "reason": "idle_disabled"}
        if not session.messages:
            return {"status": "skipped", "reason": "no_messages"}
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        now_ms = int(now.timestamp() * 1000)
        accepted_at = session.meta.last_idle_commit_accepted_at
        if accepted_at:
            accepted = parse_iso_datetime(accepted_at)
            if accepted.tzinfo is None:
                accepted = accepted.replace(tzinfo=timezone.utc)
            if (now - accepted).total_seconds() < timeout:
                return {
                    "status": "deferred", "reason": "idle_cooldown",
                    "timeout_seconds": timeout, "due_at_ms": now_ms + timeout * 1000,
                }
        if not session.meta.last_message_at:
            raise RuntimeError("Session last_message_at is missing")
        last_message = parse_iso_datetime(session.meta.last_message_at)
        if last_message.tzinfo is None:
            last_message = last_message.replace(tzinfo=timezone.utc)
        activity_due_ms = int(last_message.timestamp() * 1000) + timeout * 1000
        if now_ms < activity_due_ms:
            return {
                "status": "deferred", "reason": "idle_activity",
                "timeout_seconds": timeout, "due_at_ms": activity_due_ms,
            }
        return None

    async def _record_idle_commit_time(
        self, session_id: str, ctx: RequestContext, accepted_at: str,
    ) -> None:
        """accepted之后的best-effort记录，失败日志后仍返回accepted。

        服务层取session root exact路径锁，重读最新raw meta字典，只patch两字段；
        保留并发append/config更新及未知字段，不调用旧Session._save_meta。
        新旧有效接受时间只取max，旧时间不回写；不读idle归档或修复缓存。
        meta不存在时跳过，不创建目录/文件，不重建已删除session。
        helper内部读取/锁/写失败只记录日志；取消沿用既有异常语义。
        """
        try:
            self._ensure_initialized()
            fs = self._viking_fs
            session_uri = canonical_session_uri(ctx, session_id)
            path = fs._uri_to_path(session_uri, ctx=ctx)
            lease = await fs._async_agfs.pathlock_acquire_exact(
                path, timeout_secs=_SESSION_META_LOCK_TIMEOUT_SECONDS,
            )
            try:
                # 在与既有delete协作的session锁内直接读取；缺失不能以旧meta补建。
                metadata = json.loads(await fs.read_file(f"{session_uri}/.meta.json", ctx=ctx))
                if not isinstance(metadata, dict):
                    raise ValueError("Session metadata must be a JSON object")
                current = parse_iso_datetime(accepted_at)
                if current.tzinfo is None:
                    current = current.replace(tzinfo=timezone.utc)
                previous = metadata.get("last_idle_commit_accepted_at")
                if previous:
                    try:
                        previous_dt = parse_iso_datetime(previous)
                        if previous_dt.tzinfo is None:
                            previous_dt = previous_dt.replace(tzinfo=timezone.utc)
                    except (TypeError, ValueError):
                        logger.warning("Replacing invalid idle commit time for %s", session_id)
                    else:
                        if previous_dt >= current:
                            return  # max合并：已有时间更晚/相同，不写旧值。
                metadata["last_idle_commit_accepted_at"] = accepted_at
                metadata["updated_at"] = get_current_timestamp()
                await fs.write_file(
                    f"{session_uri}/.meta.json", json.dumps(metadata, ensure_ascii=False),
                    ctx=ctx, lease_ref=lease,
                )
            finally:
                await fs._async_agfs.pathlock_release(lease)
        except Exception:
            logger.warning("Failed to record idle commit time for %s", session_id, exc_info=True)

    async def delete(self, session_id: str, ctx: RequestContext) -> bool:
        """原业务删除成功后，await adapter.unregister_after_delete(identity)。

        identity 取 ctx 的 account/user 和传入 session_id；不依赖可能已过期的 M。
        原删除失败则不注销；不搜寻已投递 B；注销失败由适配器记录。
        """
        ...
```

`create/get/update_config/commit/commit_async/maybe_schedule_auto_commit/run_auto_commit` 的最新外部签名保留。SessionService.commit/commit_async已有的 `enable_working_memory: Optional[bool] = None` 不删除；继续以 `is not None` 筛选后传给Session，False必须保留。HTTP CommitRequest的StrictBool校验和router透传不变，新增idle handler不提供一次性WM覆盖。只有 factory 注入影响它们返回的 Session；Redis idle 回调不依赖旧自动提交的 has_running 扫描。

## 14. 配置文件：字段、校验和兼容入口

### 14.1 scheduling/expiry/config.py：轻量配置定义（新增拆分）

实际读取发现 `openviking_cli.utils.config.__init__` 会聚合导入总配置、模型和解析器配置。配置类定义放入这个轻量文件，CLI 配置目录只保留重导出入口。store/publisher/consumer/runtime 均从 `.config` 导入；内部 publisher 启动模块仅在校验完整 `ov.conf` 时加载聚合配置。

```python
from __future__ import annotations

import os
from typing import Literal
from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExpiryRedisConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    mode: Literal["standalone", "cluster"] = "standalone"
    endpoints: list[str] = Field(default_factory=list)
    username: str = ""
    password_env: str = ""
    password: str = Field(default="", repr=False)
    tls: bool = False
    connect_timeout_seconds: float = Field(default=1.0, gt=0)
    socket_timeout_seconds: float = Field(default=2.0, gt=0)
    request_budget_seconds: float = Field(default=5.0, gt=0)
    max_connections: int = Field(default=64, ge=1)
    max_inflight_requests: int = Field(default=128, ge=1)

    @model_validator(mode="after")
    def validate_connection(self) -> ExpiryRedisConfig:
        """校验 endpoint 格式、独立凭证字段和 standalone 地址数量。

        endpoints 为空允许 Redis idle backend 未启用；启用时由聚合配置拒绝。
        endpoint 不允许携带凭证、path、query 或 fragment。
        不尝试网络连接，不读密码值，不在配置对象构建时启动客户端。
        """
        ...

    def resolve_password(self) -> str | None:
        """优先按 password_env 读取；变量不存在时失败，不静默降级为无密码。"""
        ...


class ExpiryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")
    service_name: str = ""
    publisher_separate_process: bool = True
    shard_count: int = Field(default=128, ge=1, le=512)
    session_consumer_concurrency: int = Field(default=8, ge=1)
    precision_seconds: Literal[60] = 60
    owner_ttl_seconds: Literal[120] = 120
    owner_renew_interval_seconds: Literal[20] = 20
    scan_batch_size: int = Field(default=128, ge=1, le=4096)
    cleanup_batch_size: int = Field(default=128, ge=1, le=4096)
    registry_page_size: int = Field(default=128, ge=1, le=4096)
    processing_timeout_seconds: int = Field(default=120, ge=1)
    callback_poll_seconds: float = Field(default=0.5, gt=0)
    shutdown_timeout_seconds: float = Field(default=30.0, gt=0)
    redis: ExpiryRedisConfig = Field(default_factory=ExpiryRedisConfig)

    def validate_for_runtime(self) -> None:
        """Redis idle backend 启用时要求 service_name/endpoints 并校验预算。

        publisher 与 consumer 自动遍历0..N-1，每次持有一个shard；无静态候选清单。
        TTL固定120秒、续租20秒；任务处理超时独立配置。
        """
        ...
```

TTL 120秒和20秒续租来自用户约定，其余数值为压测起点。持续积压会长期持有当前shard，不承诺按资源量或实例严格均衡。

### 14.2 openviking_cli/utils/config/expiry_config.py

```python
# 兼容内核配置目录的入口；类型定义只保留一份，避免默认值/校验分叉。
from openviking.scheduling.expiry.config import ExpiryConfig, ExpiryRedisConfig

__all__ = ["ExpiryConfig", "ExpiryRedisConfig"]
```

### 14.3 memory_config.py

```python
class SessionAutoCommitConfig(BaseModel):
    # 原 enabled/check_interval_seconds/scan_rate_limit_files_per_second 保留。
    idle_backend: Literal["legacy", "redis"] = "legacy"
    # enabled 的 description 同步修改：默认 policy 开关和所选 idle backend 的启用条件。
    # 不再写成一定启动 AGFS 扫描器。没有新增方法。
```

### 14.4 open_viking_config.py

```python
class OpenVikingConfig(BaseModel):
    # 原全部字段保留。
    expiry: ExpiryConfig = Field(default_factory=ExpiryConfig)

    @model_validator(mode="after")
    def _validate_expiry_backend(self) -> OpenVikingConfig:
        """Redis idle backend 启用时校验完整 expiry 运行配置。"""
        idle = self.memory.session_auto_commit
        if idle.enabled and idle.idle_backend == "redis":
            self.expiry.validate_for_runtime()
        return self

    @classmethod
    def from_dict(cls, config: dict[str, Any]) -> OpenVikingConfig:
        """原加载逻辑保留，新增字段走 Pydantic 嵌套解析。

        现有实现先构造 instance，再单独设置 memory：因此 memory 应用完成后，
        必须再次显式调用 instance._check_expiry_backend()，不能只依赖首次构造的 validator。
        """
        ...

    def to_dict(self) -> dict[str, Any]:
        """原 model_dump 已自动包含 expiry；保留签名，增加往返检查。"""
        ...
```

既有配置仍默认 legacy。选择 Redis backend 后，report、publisher、consumer 作为一个完整能力启用，不再提供可独立组合的用户角色开关。

## 15. core.py：初始化位置必须放对

```python
class OpenVikingService:
    # __init__ 原参数保留；初始化 _expiry_runtime = None。

    async def initialize(self) -> None:
        """在原初始化流程中插入：

        1. SessionService 依赖/policy 已设置后，判断是否启用 Redis expiry。
        2. 根据 idle_backend 选择是否启动原 SessionAutoCommitScheduler。
        3. 原 QueueFS handler 注册、TaskTracker 恢复、queue_manager.start()。
        4. Redis backend 启用时创建 API runtime，注册业务后统一 start_storage/start_roles。
        5. 再完成其余初始化并对外提供服务。

        publisher_separate_process=false 时 API runtime 额外运行 publisher。
        中途失败须清理已建 expiry storage；不能留下空转 callback。
        """
        ...

    async def _initialize_expiry(self) -> None:
        """启动组装层先调用所有已启用业务的注册入口，再统一启动 runtime。

        仅在 session 选择启用 Redis idle backend 时，调用
        self._session_service.configure_expiry(runtime.scheduler)。
        API runtime 固定使用 ExpiryRuntimeRoles(report=True, consume=True)，
        publisher 是否同进程由 publisher_separate_process 决定。
        timer 为其他业务启用时不能顺带给 legacy session 安装 Redis 上报适配器。
        新业务由其自己的初始化入口完成注册，core 不 import 各业务 handler 类。
        Redis idle backend 未启用时不 import redis，不创建线程。
        """
        ...

    async def _start_expiry_roles(self) -> None:
        """所有业务注册完成后依次 start_storage/start_roles；无 runtime 则直接返回。"""
        ...

    async def _stop_expiry(self) -> None:
        """保持runtime强引用，await runtime.close；成功后才清runtime并摘除adapter。

        runtime.close抛shard_not_stopped时原样向外传播，保留runtime和adapter，
        禁止新calls由runtime.accepting_calls=False负责，不能提前detach造成状态不一致。
        不能继续关闭QueueFS/AGFS或其他未退出工作依赖；允许后续重试关闭/观察。
        """
        runtime = self._expiry_runtime
        if runtime is None:
            return
        await runtime.close()
        self._expiry_runtime = None
        self._session_service.set_session_expiry_adapter(None)

    async def close(self) -> None:
        """在QueueManager.stop之前await _stop_expiry；未退出错误传播并中止依赖关闭。

        其余startup/optional backend和既有无timer关闭路径保持不变。
        """
        ...
```

旧 scanner 创建条件具体改为：

```python
use_legacy_idle = (
    session_auto_commit_config.enabled
    and session_auto_commit_config.idle_backend == "legacy"
)
# 仅 use_legacy_idle 为 True 时走原 SessionAutoCommitScheduler 构造/start 分支。
# enabled 的 policy 默认值逻辑继续传给 SessionService，不因为 Redis backend 被关闭。
```

## 16. expiry_bootstrap.py：内部 publisher 子进程

```python
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from pathlib import Path
from typing import Any


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """解析父进程传入的配置路径与 readiness 状态文件。"""
    parser = argparse.ArgumentParser(description="Internal OpenViking expiry publisher")
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--startup-status", required=True, type=Path)
    return parser.parse_args(argv)


def load_expiry_config(path: Path) -> "ExpiryConfig":
    """读取完整 ov.conf，确认 Redis backend 和独立 publisher 均已启用。"""
    from openviking_cli.utils.config.open_viking_config import OpenVikingConfig
    raw = json.loads(os.path.expandvars(path.expanduser().read_text(encoding="utf-8-sig")))
    config = OpenVikingConfig.from_dict(raw)
    ...
    return config.expiry


def install_signal_handlers(stop: asyncio.Event) -> None:
    """在主线程 loop 上把 SIGINT/SIGTERM 转为 stop.set，信号处理器不做 I/O。"""
    ...


async def run_publisher(config: "ExpiryConfig", stop: asyncio.Event,
                        startup_status: Path) -> None:
    """创建 runtime，启动 storage/roles，同时等待 stop 或角色故障。

    任一角色异常立即结束命令并返回失败；finally await runtime.close。
    storage/role 启动成功后写 ready；不能仅 await stop.wait 而让 publisher
    task 异常后进程继续假存活。
    """
    ...


def main(argv: list[str] | None = None) -> int:
    """解析/配置失败返回非零；正常信号退出走有界关闭；不打印密码值。"""
    ...
```

该模块不注册公开 CLI。`openviking-server` 通过 `python -m` 拉起它，readiness 失败则拒绝启动 API；运行期异常退出会触发父进程退出。publisher 自动遍历全部 shard 逐个抢锁，只有成功取得 lease 才处理；不依赖 Pod ID 或静态分片列表。

## 17. pyproject.toml 与包资源

这里只列需要合并进现有 TOML table 的字段，不重复创建已有 table：

```toml
[project.optional-dependencies]
# 新增到原 table；redis 版本是待验证的候选范围。
expiry = ["redis>=5,<6"]

[tool.setuptools.package-data]
# 保留原全部 package-data；增加独立 package 的 Lua 数据。
"openviking.scheduling.expiry" = ["lua/*.lua"]
```

新增 Python 文件沿用仓库许可证头。`lua/` 仅保存数据文件，无需 Python `__init__.py`；`scheduling/expiry/__init__.py` 要存在。验证构建后的 wheel 包含全部 Lua；publisher 只作为 server 内部模块运行。

## 18. 启动与四条运行链路

```text
启动
core汇总已启用Expiry业务 -> 无业务则跳过
  -> 创建runtime -> SessionService.configure_expiry(runtime.scheduler)
  -> session及其他业务逐个注册handler/adapter
  -> start_storage -> namespace实际配置/C初始0
  -> start_roles -> 冻结注册/开放report

报告
Session._append_messages_authoritatively
  -> 锁内消息/meta保存，构造无revision快照 -> 释放锁
  -> SessionExpiryAdapter.report_after_append -> ExpiryScheduler.report
  -> upsert_resource.lua(R)：TIME + 未来due + ZADD
  -> ensure_metadata.lua(M)：身份 + max(2*timeout,24小时) TTL
  -> 两步成功返回；R成功M失败报错但保留R

逐shard调度
ExpiryPublisher.run：0..N-1递增遍历，本轮后等待下一分钟边界
  -> ShardRunner.run_if_acquired(SCHEDULER,shard_id,process,stop)
  -> Coordinator.try_acquire取得120秒lease，runner启动每20秒heartbeat
  -> ExpiryPublisher.process_shard：读C/源TIME，U截断至分钟
  -> U=C/无数据跳过；有数据定位下一非空分钟M
  -> read_source_page(R) -> stage_items(B)，读完整分钟
  -> publish_minute.lua(G,C,owner)，C=M -> cleanup_source(R)
  -> 持锁追平全部到期积压 -> revoke/停heartbeat/有界join -> 可信终止时token释放 -> 更大shard
  -> 续租失败：lost/revoke/cancel process与children，有界join；pending阻止同role再抢
  -> 旧tasks真实退出后，下次新token/新execution从C重新开始

回调
ExpiryConsumer.run：全部shard递增遍历，轮后callback_poll_seconds
  -> ShardRunner.run_if_acquired(CALLBACK,shard_id,process,stop) -> process_shard
  -> reset state/retry缓存（旧tasks必须已退出），ShardExecution.wait执行poll等待
  -> 分页读G -> claim.lua(B,max_attempts=3)
  -> claims：execution.spawn工厂创建runner跟踪子task，解析完整身份、读M（可缺失） -> SessionTimeoutHandler
  -> SessionService.commit_on_timeout -> get(auto_create=False)加载最新session/meta
  -> _check_timeout_commit：外层policy/timeout/no_messages/cooldown/last_message_due纯检查
  -> 检查通过：直接原Session.commit_async(keep_recent_count=0,
       persist_keep_recent_count=False, record_auto_commit_success=True)
  -> 不做持锁最终idle检查，期间新消息允许一起提交
  -> 原API返回accepted -> _record_idle_commit_time：服务层锁内重读最新meta
       -> 仅patch接受时间(max前后)/updated_at；失败日志仍accepted，删除不重建
  -> handler返回后ensure_active先检查current_task.cancelling；吞取消也同步unknown/CancelledError，不ACK
  -> 正常返回原accepted且检查通过 -> ack.lua(B) -> 空B摘G；不删除未来R/M
  -> 未耗尽失败：留B；最终失败/claim.exhausted：日志 -> 条件ACK
  -> 当前shard积压/在途清空后释放；仅旧tasks全部退出后清共享state，重试等待期间继续续租

关闭（四条业务链路之外的生命周期）
core保持runtime强引用 -> runtime.stop_roles拒绝新调用/set stop
  -> runner grace期间heartbeat继续，仍监视lost/截止；失锁立即revoke/cancel
  -> grace超时cancel，有界join role tasks/runner pending
  -> 未退出：shard_not_stopped向外传播，保留runtime/tasks/pending，不关闭QueueFS/AGFS/store/I/O
  -> role/pending退出 -> io.quiesce拒绝外部请求，等真实_RedisRequest.finished
  -> quiesce超时：保留requests和依赖，shard_not_stopped；run多次取消仍桥接至实际退出
  -> 未关闭store：run_cleanup(store.close,timeout)提交Redis loop raw parent，nested io.run关client
  -> cleanup成功置_storage_closed=True -> io.close再次quiesce -> 一次loop.stop -> 剩余budget join
  -> 线程alive：保留loop/thread及_storage_started/storage_ready，重试跳过成功cleanup、不重复stop
  -> 线程真实退出：清I/O引用和storage就绪标志；core成功后才清runtime/adapter并关业务依赖

冷却/活动重投
SessionService外层纯检查：冷却未满足或最新last_message_due未到
  -> deferred：冷却due=检查now+timeout，活动due=last_message_at+timeout
  -> SessionTimeoutHandler._report_deferred，不进入Session提交内部
  -> 统一report，R成功再M成功 -> COMPLETED -> ACK旧B，不删除未来R/M
  -> report失败：RETRY，保留旧B；skipped/deferred不更新时间
```

## 19. 检验范围与证据边界

静态检查覆盖 Markdown 围栏、Python 骨架语法、runtime/shard_runner 相关签名与两文档控制流一致性；省略体和模拟任务不能作为真实 Redis 行为证据。

生命周期回归须区分以下边界：

- stop grace 期间仍监视失锁/截止；迟到 renew 不复活，普通可信业务异常可释放，forced/失锁/unknown 留 TTL。
- handler 吞取消返回 COMPLETED 时，ensure_active 同步检查 cancelling、置 unknown、抛 CancelledError，阻止 ACK，不等待 done callback。
- Redis operation 抑制取消及调用方多次取消时，wrapper/requests 保留到真实 finished；quiesce 拒绝外部新请求，超时保留依赖。
- run_cleanup 的 raw parent 不先占 gate，nested io.run 在 quiesce 后可关 client；timeout/cancel 不冒充 cleanup 已退出。
- io.close quiesce 后只发一次 stop，以剩余预算 join；线程 alive 保留引用、禁止 start 第二线程。
- `test_runtime_retries_thread_join_without_resubmitting_closed_storage_cleanup` 覆盖 cleanup 成功后 join 超时：_storage_closed=True，_storage_started/storage_ready 保留，重试不重复提交 store.close，只重新观察 join。
- runner pending 在 cleanup 再次取消时不丢；同 role 全 shard 禁重领，旧 callback finally 不污染新 state；core 保留 runtime/adapter 并传播未退出异常，阻止业务依赖提前关闭。

自动化完整回归结果：**235 passed，8 条既有 deprecation warnings，40.68s**（2026-10-11，日志 `/tmp/OpenViking_shard_refactor_final3.log`）。该结果包含线程 join 重试回归，不等同于真实 Redis/Lua、Cluster 故障注入或吞吐验证。

真实 Redis/Lua、Cluster 接管故障和吞吐测试未执行。Lua 共12个文件，R→M、源 TIME、空范围、未决跨槽请求、仅日志终结等协议限制保持不变；本地取消和真实 task done 仍不证明服务端写入已撤回，不构成跨槽 fencing。

Session 外层纯检查、原 commit 参数、best-effort 两字段 patch、deferred report 成功才 ACK 及 WM/QueueFS 兼容验收继续保留。检查与 commit 不原子，接受时间可能记录失败，不承诺严格 idle 边界、零重复或每次成功可恢复；原通用 ready/QueueFS 恢复不用于恢复 idle 记录。其他 Redis 可靠性限制以主文档为准。
