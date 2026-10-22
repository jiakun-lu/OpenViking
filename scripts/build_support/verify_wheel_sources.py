# Copyright (c) 2026 Beijing Volcano Engine Technology Co., Ltd.
# SPDX-License-Identifier: AGPL-3.0
"""Reject server wheels containing stale or missing Python runtime sources."""

from __future__ import annotations

import argparse
from pathlib import Path
from zipfile import ZipFile

RUNTIME_PACKAGES = ("openviking", "openviking_cli")
# setuptools-scm generates this file during the build.
GENERATED_SOURCES = {"openviking/_version.py"}


def verify_wheel_sources(wheel: Path, source_root: Path) -> None:
    expected = {
        path.relative_to(source_root).as_posix(): path
        for package in RUNTIME_PACKAGES
        for path in (source_root / package).rglob("*.py")
        if path.relative_to(source_root).as_posix() not in GENERATED_SOURCES
    }
    if not expected:
        raise ValueError(f"No runtime sources found in {source_root}")

    with ZipFile(wheel) as archive:
        packaged = {
            name
            for name in archive.namelist()
            if name.split("/", 1)[0] in RUNTIME_PACKAGES
            and name.endswith(".py")
            and name not in GENERATED_SOURCES
        }
        problems = [f"missing: {name}" for name in sorted(expected.keys() - packaged)]
        problems.extend(f"unexpected: {name}" for name in sorted(packaged - expected.keys()))
        for name in sorted(expected.keys() & packaged):
            # Windows checkouts may use CRLF for the same source revision.
            source = expected[name].read_bytes().replace(b"\r\n", b"\n")
            content = archive.read(name).replace(b"\r\n", b"\n")
            if source != content:
                problems.append(f"different content: {name}")

    if problems:
        raise ValueError(
            f"{wheel.name} does not match the source checkout:\n" + "\n".join(problems)
        )
    print(f"Verified {len(expected)} runtime sources in {wheel.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheels", type=Path, nargs="+")
    parser.add_argument("--source-root", type=Path, default=Path(__file__).resolve().parents[2])
    args = parser.parse_args()
    for wheel in args.wheels:
        verify_wheel_sources(wheel, args.source_root.resolve())


if __name__ == "__main__":
    main()
