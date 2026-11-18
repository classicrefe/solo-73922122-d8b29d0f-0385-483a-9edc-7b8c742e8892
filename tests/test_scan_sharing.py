"""共享扫描与内存预算的专项测试。"""
from __future__ import annotations

import os
import threading

from helpers import create_task, make_csv, upload, wait_terminal


def test_source_file_opened_once_for_many_tasks(client, monkeypatch):
    """同原件 5 个任务（含途中加入）只能触发一次对原件的打开读取。"""
    import builtins

    opened: list[tuple[str, str]] = []
    real_open = builtins.open

    def counting_open(file, mode="r", *args, **kwargs):
        handle = real_open(file, mode, *args, **kwargs)
        try:
            path = os.fspath(file)
            if path.endswith(".csv") and "/sources/" in path and "r" in mode:
                opened.append((path, threading.current_thread().name))
        except (TypeError, ValueError):
            pass
        return handle

    monkeypatch.setattr(builtins, "open", counting_open)

    content = make_csv(300)
    sid = upload(client, content, delay_ms=2).json()["source_id"]
    tids = [
        create_task(client, sid, stations=[s]).json()["task_id"]
        for s in ("STA01", "STA02", "STA03")
    ]
    import time
    time.sleep(0.1)  # 确保后两个任务在扫描途中加入
    tids.append(create_task(client, sid, stations=["STA01"]).json()["task_id"])
    tids.append(create_task(client, sid).json()["task_id"])

    for tid in tids:
        assert wait_terminal(client, tid)["status"] == "completed"

    assert len(opened) == 1, f"原件被打开 {len(opened)} 次，期望 1 次"
    assert opened[0][1].startswith("scan-")


def test_concurrent_acceptance_during_teardown(client):
    """收尾窗口并发受理：新一代扫描互不干扰，全部完整交付。"""
    sid = upload(client, make_csv(500), delay_ms=3).json()["source_id"]
    first = create_task(client, sid).json()["task_id"]
    import time
    time.sleep(0.1)

    task_ids: list[str] = []
    errors: list[Exception] = []

    def worker(i: int) -> None:
        try:
            resp = create_task(client, sid, stations=[f"STA{i:03d}"])
            task_ids.append(resp.json()["task_id"])
        except Exception as exc:  # pragma: no cover - 并发不应出错
            errors.append(exc)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
    client.post(f"/tasks/{first}/cancel")
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not errors
    assert len(task_ids) == 8
    for tid in task_ids:
        info = wait_terminal(client, tid)
        assert info["status"] == "completed"
        body = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
        assert body[0] == "station,utc_time,temperature"


def test_repeated_late_joiners_never_lose_rows(client):
    """扫描途中反复加入新任务，每个迟到者都必须完整补齐（覆盖 pending 窗口竞态）。"""
    import time

    content = make_csv(300)
    sid = upload(client, content, delay_ms=2).json()["source_id"]
    expected_all = content.decode().splitlines()[1:]

    task_ids: list[str] = []
    first = create_task(client, sid).json()["task_id"]
    task_ids.append(first)
    # 扫描约每 2 行就加入一个新任务，制造大量“部分 spool + 部分 pending”的补读时刻
    for _ in range(20):
        time.sleep(0.004)
        task_ids.append(create_task(client, sid).json()["task_id"])

    for tid in task_ids:
        info = wait_terminal(client, tid)
        assert info["status"] == "completed", info
        body = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
        # 无条件任务必须得到与原件完全一致的全部行（顺序、重复行保留）
        assert body == ["station,utc_time,temperature"] + expected_all


def test_query_endpoints_return_while_scan_in_flight(client):
    """查询接口不等待扫描：扫描在途时 source/task 列表立即可读。"""
    sid = upload(client, make_csv(400), delay_ms=5).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]

    assert client.get("/sources").status_code == 200
    assert client.get("/tasks").status_code == 200
    detail = client.get(f"/tasks/{tid}").json()
    assert detail["status"] in ("running", "cancelling", "completed")
    assert "scanned_rows" in detail
    wait_terminal(client, tid)
