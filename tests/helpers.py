"""辅助函数：构造 CSV 与通用轮询。"""
from __future__ import annotations

import io
import time
from datetime import datetime, timedelta, timezone

HEADER = "station,utc_time,temperature"


def make_csv(rows: int, stations=("STA01", "STA02", "STA03"), temp_cycle=100,
             dup: bool = False, crlf: bool = True) -> bytes:
    buf = io.StringIO()
    nl = "\r\n" if crlf else "\n"
    buf.write(HEADER + nl)
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(rows):
        station = stations[i % len(stations)]
        ts = (base + timedelta(minutes=i)).strftime("%Y-%m-%dT%H:%M:%S+00:00")
        buf.write(f"{station},{ts},{i % temp_cycle}{nl}")
        if dup and i == 0:
            buf.write(f"{station},{ts},{i % temp_cycle}{nl}")
    return buf.getvalue().encode()


def upload(client, content: bytes, name: str = "obs.csv", delay_ms: int = 0):
    resp = client.post(
        "/sources",
        files={"file": (name, content, "text/csv")},
        data={"row_delay_ms": str(delay_ms)},
    )
    return resp


def wait_terminal(client, task_id: str, timeout: float = 30.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.get(f"/tasks/{task_id}").json()
        if info["status"] in ("completed", "failed", "cancelled", "interrupted"):
            return info
        time.sleep(0.05)
    raise AssertionError(f"task {task_id} did not finish: {info}")


def create_task(client, source_id: str, stations=None, tmin=0, tmax=10_000):
    return client.post(
        "/tasks",
        json={"source_id": source_id, "stations": stations,
              "temp_min": tmin, "temp_max": tmax},
    )
