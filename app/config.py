"""运行期配置，全部可由环境变量覆盖。"""
from __future__ import annotations

import os
from pathlib import Path


def _int_env(name: str, default: int) -> int:
    try:
        return int(os.environ.get(name, str(default)))
    except ValueError:
        return default


# 数据根目录（容器内为 /data，本地默认项目下 data/）
DATA_DIR = Path(os.environ.get("DATA_DIR", str(Path(__file__).resolve().parent.parent / "data")))

# 单次上传上限，默认 256 MiB
MAX_UPLOAD_BYTES = _int_env("MAX_UPLOAD_BYTES", 256 * 1024 * 1024)

# 可控慢扫描：每读多少行 sleep 一次、每次 sleep 多少毫秒（演示/测试用，0 表示关闭）
SCAN_DELAY_EVERY_ROWS = max(1, _int_env("SCAN_DELAY_EVERY_ROWS", 1))
SCAN_ROW_DELAY_MS = _int_env("SCAN_ROW_DELAY_MS", 0)

SOURCES_DIR = DATA_DIR / "sources"
RESULTS_DIR = DATA_DIR / "results"
SCANS_DIR = DATA_DIR / "scans"
DB_PATH = DATA_DIR / "app.db"
