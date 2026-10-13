# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Configuration independent from the aggregate OpenViking config package."""

from __future__ import annotations

from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator


class ExpiryRedisConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    mode: Literal["standalone", "cluster"] = "standalone"
    endpoints: list[str] = Field(default_factory=list)
    username: str | None = None
    password: str | None = None
    password_env: str | None = None
    tls: bool = False
    connect_timeout_seconds: float = Field(default=1.0, gt=0)
    socket_timeout_seconds: float = Field(default=2.0, gt=0)
    request_budget_seconds: float = Field(default=5.0, gt=0)
    max_connections: int = Field(default=64, ge=1)
    max_inflight_requests: int = Field(default=128, ge=1)

    @model_validator(mode="after")
    def validate_connection(self) -> "ExpiryRedisConfig":
        if self.password is not None and self.password_env is not None:
            raise ValueError("Redis password and password_env are mutually exclusive")
        if self.mode == "standalone" and len(self.endpoints) > 1:
            raise ValueError("standalone Redis accepts at most one endpoint")
        schemes: set[str] = set()
        for endpoint in self.endpoints:
            normalized = (
                endpoint
                if "://" in endpoint
                else (("rediss" if self.tls else "redis") + f"://{endpoint}")
            )
            parsed = urlsplit(normalized)
            if parsed.scheme not in {"redis", "rediss"} or not parsed.hostname or not parsed.port:
                raise ValueError(f"invalid Redis endpoint: {endpoint}")
            if parsed.username or parsed.password or parsed.path not in {"", "/"}:
                raise ValueError("Redis endpoints must not contain credentials or paths")
            schemes.add(parsed.scheme)
        if len(schemes) > 1:
            raise ValueError("Redis cluster endpoints must use one transport scheme")
        return self


class ExpiryConfig(BaseModel):
    model_config = ConfigDict(extra="forbid")

    service_name: str = ""
    reporting_enabled: bool = False
    scheduler_enabled: bool = False
    callback_enabled: bool = False
    shard_count: Literal[512] = 512
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

    @property
    def enabled(self) -> bool:
        return self.reporting_enabled or self.scheduler_enabled or self.callback_enabled

    @model_validator(mode="after")
    def validate_roles(self) -> "ExpiryConfig":
        if self.enabled:
            if not self.service_name.strip():
                raise ValueError("expiry.service_name is required when enabled")
            if not self.redis.endpoints:
                raise ValueError("expiry.redis.endpoints is required when enabled")
            if self.redis.request_budget_seconds >= self.owner_renew_interval_seconds:
                raise ValueError("Redis request budget must be shorter than owner renew interval")
        if self.callback_enabled and not self.reporting_enabled:
            raise ValueError("callback_enabled requires reporting_enabled for deferred reports")
        return self
