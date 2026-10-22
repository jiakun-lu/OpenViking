# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Packaging checks must reject mixed service/config revisions."""

from zipfile import ZipFile

import pytest

from scripts.build_support.verify_wheel_sources import verify_wheel_sources

CORE = "openviking/service/core.py"
CONFIG = "openviking_cli/utils/config/open_viking_config.py"


@pytest.fixture
def wheel_sources(tmp_path):
    sources = {CORE: b"expiry_config = config.expiry\n", CONFIG: b"expiry = ExpiryConfig()\n"}
    for name, content in sources.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
    return tmp_path, sources


def _write_wheel(root, sources):
    wheel = root / "openviking-0.0.0-py3-none-any.whl"
    with ZipFile(wheel, "w") as archive:
        for name, content in sources.items():
            archive.writestr(name, content)
    return wheel


def test_accepts_consistent_sources_and_generated_version(wheel_sources):
    root, sources = wheel_sources
    sources = {name: content.replace(b"\n", b"\r\n") for name, content in sources.items()}
    sources["openviking/_version.py"] = b"version = '0.0.0'\n"

    verify_wheel_sources(_write_wheel(root, sources), root)


@pytest.mark.parametrize("changed", [CORE, CONFIG])
def test_rejects_stale_service_or_config(wheel_sources, changed):
    root, sources = wheel_sources
    sources[changed] = b"# An older revision left in the build directory\n"

    with pytest.raises(ValueError, match=f"different content: {changed}"):
        verify_wheel_sources(_write_wheel(root, sources), root)


def test_rejects_missing_config_module(wheel_sources):
    root, sources = wheel_sources
    del sources[CONFIG]

    with pytest.raises(ValueError, match=f"missing: {CONFIG}"):
        verify_wheel_sources(_write_wheel(root, sources), root)


def test_rejects_removed_module_left_in_wheel(wheel_sources):
    root, sources = wheel_sources
    sources["openviking/obsolete_config.py"] = b"# Deleted from the source checkout\n"

    with pytest.raises(ValueError, match="unexpected: openviking/obsolete_config.py"):
        verify_wheel_sources(_write_wheel(root, sources), root)
