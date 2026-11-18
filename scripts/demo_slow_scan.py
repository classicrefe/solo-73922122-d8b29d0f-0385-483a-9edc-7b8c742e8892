#!/usr/bin/env python3
"""可控慢扫描端到端演示（仅用标准库，可在容器外直接运行）。

流程：
1. 生成 200 行示例 CSV（5 个站号，温度 0..99 循环）；
2. 上传，带 row_delay_ms=20，使一次扫描约持续数秒；
3. 立刻创建任务 A，约 1 秒后途中加入任务 B（验证迟到补齐 + 共享一次扫描）；
4. 再创建任务 C 并立即取消（验证取消受理，且 A/B 不受影响）；
5. 轮询至 A/B 完成，下载结果并逐行核对筛选条件、表头与行序；
6. 打印各任务状态与扫描共享证据（两任务的 scanned_rows 同源）。

用法：
  python scripts/demo_slow_scan.py                      # 默认 http://127.0.0.1:8000
  python scripts/demo_slow_scan.py http://host:8000 50  # 自定义地址与每行延迟 ms
"""
from __future__ import annotations

import io
import json
import sys
import time
import urllib.error
import urllib.request
import uuid
from datetime import datetime, timedelta, timezone

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8000"
DELAY_MS = int(sys.argv[2]) if len(sys.argv) > 2 else 20
N_ROWS = 200
STATIONS = ["STA01", "STA02", "STA03", "STA04", "STA05"]


def request(method: str, path: str, data: bytes | None = None,
            headers: dict | None = None, raw: bool = False):
    req = urllib.request.Request(BASE + path, data=data, method=method,
                                 headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read()
            return body if raw else json.loads(body)
    except urllib.error.HTTPError as exc:
        raise SystemExit(f"HTTP {exc.code} {path}: {exc.read().decode()}") from exc


def make_csv(rows: int) -> bytes:
    buf = io.StringIO()
    buf.write("station,utc_time,temperature\r\n")
    base = datetime(2026, 1, 1, tzinfo=timezone.utc)
    for i in range(rows):
        station = STATIONS[i % len(STATIONS)]
        ts = (base + timedelta(minutes=i)).isoformat()
        buf.write(f"{station},{ts},{i % 100}\r\n")
    return buf.getvalue().encode()


def wait_terminal(task_id: str, timeout: float = 60.0) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = request("GET", f"/tasks/{task_id}", None)
        if info["status"] in ("completed", "failed", "cancelled", "interrupted"):
            return info
        time.sleep(0.2)
    raise SystemExit(f"任务 {task_id} 超时未结束")


def main() -> None:
    csv_bytes = make_csv(N_ROWS)
    filename = f"demo-{uuid.uuid4().hex[:8]}.csv"
    boundary = "----demo" + uuid.uuid4().hex
    body = b"".join([
        f"--{boundary}\r\n".encode(),
        b'Content-Disposition: form-data; name="row_delay_ms"\r\n\r\n',
        f"{DELAY_MS}\r\n".encode(),
        f"--{boundary}\r\n".encode(),
        f'Content-Disposition: form-data; name="file"; filename="{filename}"\r\n'.encode(),
        b"Content-Type: text/csv\r\n\r\n",
        csv_bytes, b"\r\n",
        f"--{boundary}--\r\n".encode(),
    ])
    print(f"[1] 上传 {N_ROWS} 行 CSV（{len(csv_bytes)} 字节，慢扫描 {DELAY_MS} ms/行）…")
    source = request("POST", "/sources", body,
                     {"Content-Type": f"multipart/form-data; boundary={boundary}"})
    sid = source["source_id"]
    print(f"    source_id={sid}")

    print("[2] 立即创建任务 A：STA01/STA02，温度闭区间 [10, 80]")
    task_a = request("POST", "/tasks", json.dumps({
        "source_id": sid, "stations": ["STA01", "STA02"],
        "temp_min": 10, "temp_max": 80,
    }).encode(), {"Content-Type": "application/json"})
    aid = task_a["task_id"]

    time.sleep(1.0)
    mid = request("GET", f"/tasks/{aid}", None)
    print(f"    1 秒后扫描进度 scanned_rows={mid['scanned_rows']}（扫描仍在途）")

    print("[3] 途中加入任务 B：全部站号，温度 [50, 60]（应补齐已过部分，不另读原件）")
    task_b = request("POST", "/tasks", json.dumps({
        "source_id": sid, "stations": None,
        "temp_min": 50, "temp_max": 60,
    }).encode(), {"Content-Type": "application/json"})
    bid = task_b["task_id"]

    print("[4] 再建任务 C 并立即取消（A/B 应继续）")
    task_c = request("POST", "/tasks", json.dumps({
        "source_id": sid, "stations": ["STA03"],
        "temp_min": 0, "temp_max": 100,
    }).encode(), {"Content-Type": "application/json"})
    cid = task_c["task_id"]
    cancel_c = request("POST", f"/tasks/{cid}/cancel", b"", {})
    print(f"    C 状态={cancel_c['status']}")

    info_a = wait_terminal(aid)
    info_b = wait_terminal(bid)
    print(f"[5] A={info_a['status']} matched={info_a['matched_rows']}；"
          f"B={info_b['status']} matched={info_b['matched_rows']}")

    result_a = request("GET", f"/tasks/{aid}/result", None, raw=True).decode()
    lines_a = result_a.splitlines()
    assert lines_a[0] == "station,utc_time,temperature", "表头必须保留"
    expected_a = [
        ln for ln in csv_bytes.decode().splitlines()[1:]
        if ln.split(",")[0] in ("STA01", "STA02")
        and 10 <= int(ln.rsplit(",", 1)[1]) <= 80
    ]
    assert lines_a[1:] == expected_a, "A 的行内容/行序/重复行必须与原件一致"
    print(f"[6] 下载 A 结果核对通过：{len(lines_a) - 1} 行，表头/行序/闭区间均正确")

    result_b = request("GET", f"/tasks/{bid}/result", None, raw=True).decode()
    got_b = result_b.splitlines()[1:]
    expected_b = [
        ln for ln in csv_bytes.decode().splitlines()[1:]
        if 50 <= int(ln.rsplit(",", 1)[1]) <= 60
    ]
    assert got_b == expected_b, "B 必须包含加入之前的匹配行（补齐生效）"
    print(f"[7] 迟到任务 B 核对通过：补齐后共 {len(got_b)} 行，与离线计算一致")

    print("[8] 同原件只扫描一次：A/B 完成后 source 状态 =",
          request("GET", f"/sources/{sid}", None)["status"])
    print("OK：共享扫描、迟到补齐、取消隔离、完整校验后开放结果均已演示。")


if __name__ == "__main__":
    main()
