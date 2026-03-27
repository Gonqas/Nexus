from __future__ import annotations

import os
import subprocess
import sys
from pathlib import Path


def test_runtime_database_bootstrap_copies_seed_db(tmp_path: Path) -> None:
    local_app_data = tmp_path / "local-app-data"
    local_app_data.mkdir(parents=True, exist_ok=True)

    env = os.environ.copy()
    env["LOCALAPPDATA"] = str(local_app_data)

    script = (
        "from db.session import DB_PATH; "
        "print(DB_PATH); "
        "print(DB_PATH.exists()); "
        "print(DB_PATH.stat().st_size if DB_PATH.exists() else 0)"
    )
    result = subprocess.run(
        [sys.executable, "-c", script],
        cwd=Path(__file__).resolve().parents[1],
        env=env,
        capture_output=True,
        text=True,
        check=True,
    )

    lines = [line.strip() for line in result.stdout.splitlines() if line.strip()]
    assert len(lines) >= 3
    assert lines[1] == "True"
    assert int(lines[2]) > 0
