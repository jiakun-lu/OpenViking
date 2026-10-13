# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Standalone distributed expiry publisher."""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import signal
from pathlib import Path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="OpenViking expiry publisher")
    parser.add_argument("--config", required=True, type=Path)
    return parser.parse_args(argv)


def load_expiry_config(path: Path):
    from openviking.scheduling.expiry.config import ExpiryConfig

    raw = json.loads(os.path.expandvars(path.expanduser().read_text(encoding="utf-8-sig")))
    section = dict(raw.get("expiry", {}))
    section["reporting_enabled"] = False
    section["callback_enabled"] = False
    config = ExpiryConfig.model_validate(section)
    if not config.scheduler_enabled:
        raise ValueError("expiry.scheduler_enabled must be true for expiry CLI")
    return config


def install_signal_handlers(stop: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()
    for signum in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(signum, stop.set)
        except NotImplementedError:
            signal.signal(signum, lambda *_args: loop.call_soon_threadsafe(stop.set))


async def run_publisher(config, stop: asyncio.Event) -> None:
    from openviking.scheduling.expiry.runtime import ExpiryRuntime

    runtime = ExpiryRuntime(config)
    try:
        await runtime.start_storage()
        await runtime.start_roles()
        wait_task = asyncio.create_task(runtime.wait())
        stop_task = asyncio.create_task(stop.wait())
        done, _ = await asyncio.wait({wait_task, stop_task}, return_when=asyncio.FIRST_COMPLETED)
        if wait_task in done:
            await wait_task
    finally:
        await runtime.close()


def main(argv: list[str] | None = None) -> int:
    try:
        args = parse_args(argv)
        config = load_expiry_config(args.config)

        async def runner() -> None:
            stop = asyncio.Event()
            install_signal_handlers(stop)
            await run_publisher(config, stop)

        asyncio.run(runner())
        return 0
    except KeyboardInterrupt:
        return 0
    except Exception:
        logging.exception("Expiry publisher failed")
        return 1
