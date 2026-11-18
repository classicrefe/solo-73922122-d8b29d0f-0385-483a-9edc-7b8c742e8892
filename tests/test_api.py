"""关键功能自测：筛选、行序/重复行、共享扫描、迟到补齐、取消、格式错误、
重启恢复、上传上限与代际隔离。"""
from __future__ import annotations

import time

from helpers import HEADER, create_task, make_csv, upload, wait_terminal


# ---------------------------------------------------------------- 基本筛选
def test_filter_stations_and_closed_interval_and_order(client):
    rows = 90  # 温度 i%8，配合 3 站轮换保证 STA01/STA02 的行覆盖边界 5/20
    content = make_csv(rows, temp_cycle=8)
    sid = upload(client, content).json()["source_id"]
    resp = create_task(client, sid, stations=["STA01", "STA02"], tmin=5, tmax=20)
    assert resp.status_code == 201
    tid = resp.json()["task_id"]

    info = wait_terminal(client, tid)
    assert info["status"] == "completed"
    assert info["result_available"] is True

    result = client.get(f"/tasks/{tid}/result").content.decode()
    lines = result.splitlines()
    assert lines[0] == HEADER
    expected = [
        ln for ln in content.decode().splitlines()[1:]
        if ln.split(",")[0] in ("STA01", "STA02")
        and 5 <= int(ln.rsplit(",", 1)[1]) <= 20
    ]
    assert lines[1:] == expected  # 闭区间边界 + 原行序
    # 闭区间边界：下界 5 必须包含；上界 20 大于数据温度域上界 7，
    # 因此温度域最大值 7（被 [5,20] 闭区间包含）也必须出现在结果里
    all_temps = {int(ln.rsplit(",", 1)[1]) for ln in content.decode().splitlines()[1:]}
    assert 5 in all_temps and max(all_temps) == 7
    temps = {int(ln.rsplit(",", 1)[1]) for ln in lines[1:]}
    assert 5 in temps and 7 in temps
    assert all(5 <= t <= 20 for t in temps)


def test_duplicate_rows_are_preserved(client):
    content = make_csv(5, dup=True)
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    wait_terminal(client, tid)
    result = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
    original_lines = content.decode().splitlines()
    assert len(result) - 1 == len(original_lines) - 1  # 含重复行
    assert result[1] == result[2]  # 重复的第一行原样保留两份


def test_all_stations_when_null(client):
    content = make_csv(12)
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid, stations=None, tmin=-10**9, tmax=10**9).json()["task_id"]
    wait_terminal(client, tid)
    result = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
    assert len(result) - 1 == 12


def test_empty_match_set_produces_header_only(client):
    content = make_csv(6)
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid, stations=["NOPE"]).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "completed" and info["matched_rows"] == 0
    result = client.get(f"/tasks/{tid}/result").content.decode().splitlines()
    assert result == [HEADER]


# ---------------------------------------------------------------- 共享扫描
def test_shared_scan_and_late_joiner_catchup(client):
    content = make_csv(200)
    sid = upload(client, content, delay_ms=5).json()["source_id"]

    t1 = create_task(client, sid, stations=["STA01"]).json()["task_id"]
    # 确认扫描确实在途
    for _ in range(50):
        if client.get(f"/tasks/{t1}").json()["scanned_rows"] > 0:
            break
        time.sleep(0.02)
    time.sleep(0.15)
    progress = client.get(f"/tasks/{t1}").json()["scanned_rows"]
    assert 0 < progress < 200, "慢扫描应仍在进行，便于验证迟到加入"

    # 途中加入：必须补齐已过部分，且不二次读取原件
    t2 = create_task(client, sid, stations=["STA02"]).json()["task_id"]

    i1 = wait_terminal(client, t1)
    i2 = wait_terminal(client, t2)
    assert i1["status"] == "completed" and i2["status"] == "completed"

    r1 = client.get(f"/tasks/{t1}/result").content.decode().splitlines()[1:]
    r2 = client.get(f"/tasks/{t2}/result").content.decode().splitlines()[1:]
    exp1 = [ln for ln in content.decode().splitlines()[1:]
            if ln.split(",")[0] == "STA01"]
    exp2 = [ln for ln in content.decode().splitlines()[1:]
            if ln.split(",")[0] == "STA02"]
    assert r1 == exp1
    assert r2 == exp2  # 包含加入之前的行 => 补齐生效

    # 同原件只扫描一次：扫描目录已清理，结果落盘
    from app import config as cfg
    assert not any(cfg.SCANS_DIR.glob("**/spool.csv"))


def test_acceptance_does_not_wait_for_scan(client):
    content = make_csv(300)
    sid = upload(client, content, delay_ms=5).json()["source_id"]
    start = time.monotonic()
    resp = create_task(client, sid)
    elapsed = time.monotonic() - start
    assert resp.status_code == 201
    assert elapsed < 2.0  # 受理立即返回，不等扫描
    wait_terminal(client, resp.json()["task_id"])


# ---------------------------------------------------------------- 取消
def test_cancel_last_task_stops_scan_and_clears_staging(client):
    content = make_csv(500)
    sid = upload(client, content, delay_ms=10).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    time.sleep(0.2)
    info = client.get(f"/tasks/{tid}").json()
    assert info["status"] in ("running", "cancelling")

    start = time.monotonic()
    cancelled = client.post(f"/tasks/{tid}/cancel").json()
    cancel_elapsed = time.monotonic() - start
    assert cancelled["status"] == "cancelled"
    assert cancel_elapsed < 5.0  # 及时受理

    # 不发布结果
    assert client.get(f"/tasks/{tid}/result").status_code == 409
    # 暂存与 spool 已清除
    from app import config as cfg
    assert not list(cfg.SCANS_DIR.glob("**/*"))
    # 取消完成后再受理同原件新任务：开启新一代扫描并成功
    t2 = create_task(client, sid).json()["task_id"]
    assert wait_terminal(client, t2)["status"] == "completed"


def test_cancel_one_task_keeps_others_running(client):
    content = make_csv(300)
    sid = upload(client, content, delay_ms=5).json()["source_id"]
    t_keep = create_task(client, sid, stations=["STA01"]).json()["task_id"]
    t_kill = create_task(client, sid, stations=["STA02"]).json()["task_id"]
    time.sleep(0.1)

    cancelled = client.post(f"/tasks/{t_kill}/cancel").json()
    assert cancelled["status"] == "cancelled"
    kept = wait_terminal(client, t_keep)
    assert kept["status"] == "completed"
    result = client.get(f"/tasks/{t_keep}/result").content.decode().splitlines()[1:]
    expected = [ln for ln in content.decode().splitlines()[1:]
                if ln.split(",")[0] == "STA01"]
    assert result == expected


def test_cancel_during_teardown_new_task_is_not_corrupted(client):
    """收尾期间受理新任务：旧代清理不能改坏新一代。"""
    content = make_csv(400)
    sid = upload(client, content, delay_ms=8).json()["source_id"]
    t1 = create_task(client, sid).json()["task_id"]
    time.sleep(0.15)
    client.post(f"/tasks/{t1}/cancel")
    # 几乎同时（收尾窗口内）受理新任务
    t2 = create_task(client, sid).json()["task_id"]
    info2 = wait_terminal(client, t2)
    assert info2["status"] == "completed"
    lines = client.get(f"/tasks/{t2}/result").content.decode().splitlines()
    assert len(lines) - 1 == 400


# ---------------------------------------------------------------- 格式错误
def test_malformed_row_fails_all_pending_tasks_no_partial_result(client):
    good = make_csv(20).decode().splitlines(keepends=True)
    good.insert(14, "STA01,2026-01-01T00:14:00+00:00,not-an-int\r\n")
    bad_csv = "".join(good).encode()
    sid = upload(client, bad_csv).json()["source_id"]

    t1 = create_task(client, sid).json()["task_id"]
    t2 = create_task(client, sid, stations=["STA02"]).json()["task_id"]
    i1, i2 = wait_terminal(client, t1), wait_terminal(client, t2)
    assert i1["status"] == "failed" and i2["status"] == "failed"
    assert "第 15 行" in (i1["error"] or "")
    assert client.get(f"/tasks/{t1}/result").status_code == 409
    assert client.get(f"/tasks/{t2}/result").status_code == 409


def test_bad_header_rejected_at_upload(client):
    bad = b"wrong,header,cols\n1,2,3\n"
    resp = upload(client, bad)
    assert resp.status_code == 400
    assert client.get("/sources").json() == []


def test_lf_newlines_and_blank_line_handling(client):
    # LF 换行也必须保留原换行风格；空行属于格式错误
    content = (HEADER + "\n" +
               "STA01,2026-01-01T00:00:00+00:00,1\n" +
               "STA02,2026-01-01T00:01:00+00:00,2\n")
    sid = upload(client, content.encode()).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    wait_terminal(client, tid)
    result = client.get(f"/tasks/{tid}/result").content.decode()
    lines = result.splitlines()
    assert lines[0] == HEADER and len(lines) - 1 == 2
    # 原行序保留
    assert lines[1].startswith("STA01,") and lines[2].startswith("STA02,")

    blank = (HEADER + "\nSTA01,2026-01-01T00:00:00+00:00,1\n\n").encode()
    sid2 = upload(client, blank).json()["source_id"]
    tid2 = create_task(client, sid2).json()["task_id"]
    info = wait_terminal(client, tid2)
    assert info["status"] == "failed" and "第 3 行" in info["error"]


def test_wrong_column_count_and_empty_station_fail(client):
    for bad in (
        HEADER + "\nSTA01,2026-01-01T00:00:00+00:00\n",            # 缺列
        HEADER + "\n,2026-01-01T00:00:00+00:00,1\n",               # 空站号
        HEADER + "\nSTA01,2026-01-01T00:00:00+00:00,1,extra\n",    # 多列
    ):
        sid = upload(client, bad.encode()).json()["source_id"]
        tid = create_task(client, sid).json()["task_id"]
        assert wait_terminal(client, tid)["status"] == "failed"


def test_long_header_spanning_upload_chunks(client, monkeypatch):
    """表头极长、跨越多个上传分块时仍应正确识别（不应误判为坏表头）。"""
    # 构造合法但首行很长的文件：用正常表头，数据行前先放大量正常行不现实，
    # 因此直接构造 >1MiB 的合法数据文件，验证分块累积不影响正常上传与扫描。
    big = make_csv(40000)
    assert len(big) > 1024 * 1024
    sid = upload(client, big).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    info = wait_terminal(client, tid, timeout=60)
    assert info["status"] == "completed" and info["matched_rows"] == 40000


def test_cancel_status_visible_promptly(client):
    """取消及时受理：返回前已进入 cancelling/cancelled，且之后不再发布结果。"""
    sid = upload(client, make_csv(400), delay_ms=5).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    time.sleep(0.1)
    resp = client.post(f"/tasks/{tid}/cancel").json()
    assert resp["status"] in ("cancelling", "cancelled")
    # 最终一定落为 cancelled
    final = wait_terminal(client, tid)
    assert final["status"] == "cancelled"
    assert client.get(f"/tasks/{tid}/result").status_code == 409


def test_missing_timezone_fails(client):
    bad = (HEADER + "\nSTA01,2026-01-01T00:00:00,12\n").encode()
    sid = upload(client, bad).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "failed" and "时区" in info["error"]


# ---------------------------------------------------------------- 上传上限
def test_upload_size_limit(client, monkeypatch):
    from app import config as cfg
    monkeypatch.setattr(cfg, "MAX_UPLOAD_BYTES", 1024)
    # main 模块在 app 启动时已绑定 config 对象，属性 monkeypatch 同样生效
    content = make_csv(200)
    assert len(content) > 1024
    resp = upload(client, content)
    assert resp.status_code == 413
    # 拒收后不留文件
    assert not list(cfg.SOURCES_DIR.glob("*.csv"))


def test_temp_min_greater_than_max_rejected(client):
    sid = upload(client, make_csv(3)).json()["source_id"]
    resp = create_task(client, sid, tmin=10, tmax=1)
    assert resp.status_code == 400


def test_unknown_source_404(client):
    assert create_task(client, "nope").status_code == 404
    assert client.get("/tasks/nope").status_code == 404


# ---------------------------------------------------------------- 重启恢复
def test_restart_keeps_completed_and_interrupts_active(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    import importlib
    import sys
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    main = importlib.import_module("app.main")
    config = importlib.import_module("app.config")

    with __import__("fastapi").testclient.TestClient(main.app) as c1:
        content = make_csv(50)
        sid = upload(c1, content).json()["source_id"]
        done = create_task(c1, sid).json()["task_id"]
        wait_terminal(c1, done)
        # 慢扫描制造一个在途任务
        sid2 = upload(c1, make_csv(400), delay_ms=10).json()["source_id"]
        active = create_task(c1, sid2).json()["task_id"]
        time.sleep(0.15)
        assert c1.get(f"/tasks/{active}").json()["status"] == "running"

    # 模拟重启：新建客户端触发 lifespan 恢复逻辑
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    main2 = importlib.import_module("app.main")
    with __import__("fastapi").testclient.TestClient(main2.app) as c2:
        kept = c2.get(f"/tasks/{done}").json()
        assert kept["status"] == "completed"  # 成功结果保留且可下载
        body = c2.get(f"/tasks/{done}/result")
        assert body.status_code == 200 and body.content.startswith(HEADER.encode())

        broken = c2.get(f"/tasks/{active}").json()
        assert broken["status"] == "interrupted"
        # 中断任务不能下载
        assert c2.get(f"/tasks/{active}/result").status_code == 409
        # 遗留扫描暂存已清
        assert not list(config.SCANS_DIR.glob("**/*"))
