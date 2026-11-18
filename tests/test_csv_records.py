"""引号内换行等合法 CSV 记录的专项测试：

* 含引号内换行站号的记录按完整记录校验/筛选/写出，不拆行；
* 结果保留原记录字节（含引号、内嵌换行、原终止符）、顺序与重复记录；
* 引号未闭合或字段非法时，未完任务失败且不提供部分结果；
* 共享扫描/迟到补齐在多条目记录下仍然成立。
"""
from __future__ import annotations

import time

from helpers import HEADER, create_task, make_csv, upload, wait_terminal

STATION_NL = "STA\n01"  # 含换行的站号（引号内换行）


def make_quoted_csv() -> bytes:
    """构造含引号内换行站号、转义引号、引号内 CRLF 的合法 CSV。"""
    lines = [
        HEADER,
        '"STA\n01",2026-01-01T00:00:00+00:00,5',      # 站号含 \n
        'STA02,2026-01-01T00:01:00+00:00,12',
        '"STA""X",2026-01-01T00:02:00+00:00,7',       # 转义引号 -> STA"X
        '"STA\r\n03",2026-01-01T00:03:00+00:00,-2',   # 站号含 \r\n
        '"STA,04",2026-01-01T00:04:00+00:00,20',      # 站号含逗号
        '"STA\n01",2026-01-01T00:05:00+00:00,5',      # 与首条重复
    ]
    return ("\r\n".join(lines) + "\r\n").encode()


# ---------------------------------------------------------------- 记录完整性
def test_quoted_newline_station_record_kept_intact(client):
    content = make_quoted_csv()
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid, tmin=-10**9, tmax=10**9).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "completed", info
    assert info["matched_rows"] == 6  # 按记录计，不按物理行

    body = client.get(f"/tasks/{tid}/result").content
    lines = body.decode().splitlines()
    assert lines[0] == HEADER
    # 数据区与原件数据区逐字节一致（顺序、重复记录、引号、内嵌换行全保留）
    original_data = content.decode().split("\r\n", 1)[1]
    assert body.decode().split("\r\n", 1)[1] == original_data


def test_filter_by_station_containing_newline(client):
    content = make_quoted_csv()
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid, stations=[STATION_NL]).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "completed"
    assert info["matched_rows"] == 2  # 两条 "STA\n01" 记录（含重复）

    result = client.get(f"/tasks/{tid}/result").content.decode()
    assert result == (
        HEADER + "\r\n"
        '"STA\n01",2026-01-01T00:00:00+00:00,5\r\n'
        '"STA\n01",2026-01-01T00:05:00+00:00,5\r\n'
    )


def test_filter_by_escaped_quote_and_comma_stations(client):
    content = make_quoted_csv()
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid, stations=['STA"X', "STA,04"]).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "completed"
    assert info["matched_rows"] == 2
    result = client.get(f"/tasks/{tid}/result").content.decode()
    assert result == (
        HEADER + "\r\n"
        '"STA""X",2026-01-01T00:02:00+00:00,7\r\n'
        '"STA,04",2026-01-01T00:04:00+00:00,20\r\n'
    )


def test_quoted_crlf_inside_field_preserved(client):
    content = make_quoted_csv()
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid, stations=["STA\r\n03"], tmin=-10**9, tmax=10**9).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "completed" and info["matched_rows"] == 1
    body = client.get(f"/tasks/{tid}/result").content
    assert body == (
        HEADER + "\r\n" + '"STA\r\n03",2026-01-01T00:03:00+00:00,-2\r\n'
    ).encode()


def test_scanned_rows_counts_records_not_physical_lines(client):
    content = make_quoted_csv()  # 6 条记录，9 个物理行
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid, tmin=-10**9, tmax=10**9).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "completed"
    assert info["matched_rows"] == 6


def test_temperature_filter_with_multiline_records(client):
    content = make_quoted_csv()
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid, tmin=5, tmax=12).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "completed"
    assert info["matched_rows"] == 4  # 5, 12, 7, 5（闭区间）
    result = client.get(f"/tasks/{tid}/result").content.decode()
    assert result == (
        HEADER + "\r\n"
        '"STA\n01",2026-01-01T00:00:00+00:00,5\r\n'
        "STA02,2026-01-01T00:01:00+00:00,12\r\n"
        '"STA""X",2026-01-01T00:02:00+00:00,7\r\n'
        '"STA\n01",2026-01-01T00:05:00+00:00,5\r\n'
    )


# ---------------------------------------------------------------- 引号未闭合
def test_unclosed_quote_fails_all_pending_tasks_no_partial_result(client):
    good = make_csv(20).decode().splitlines(keepends=True)
    # 第 15 条记录站号开引号后不再闭合，后续内容全被吞进引用段
    good.insert(14, '"STA01,2026-01-01T00:14:00+00:00,5\r\n')
    bad_csv = "".join(good).encode()
    sid = upload(client, bad_csv).json()["source_id"]

    t1 = create_task(client, sid).json()["task_id"]
    t2 = create_task(client, sid, stations=["STA02"]).json()["task_id"]
    i1, i2 = wait_terminal(client, t1), wait_terminal(client, t2)
    assert i1["status"] == "failed" and i2["status"] == "failed"
    assert "引号未闭合" in (i1["error"] or "")
    assert "第 15 行" in (i1["error"] or "")
    assert client.get(f"/tasks/{t1}/result").status_code == 409
    assert client.get(f"/tasks/{t2}/result").status_code == 409


def test_unclosed_quote_with_valid_looking_fields_still_fails(client):
    """温度列开引号直到 EOF：csv 解析恰好得到 3 个合法字段，也必须判失败。"""
    content = (
        HEADER + "\r\n"
        "STA01,2026-01-01T00:00:00+00:00,3\r\n"
        'STA02,2026-01-01T00:01:00+00:00,"5'  # 引号未闭合，文件结束
    ).encode()
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "failed"
    assert "引号未闭合" in (info["error"] or "")
    assert client.get(f"/tasks/{tid}/result").status_code == 409


def test_unclosed_quote_mid_file_line_number(client):
    content = (
        HEADER + "\n"
        "STA01,2026-01-01T00:00:00+00:00,1\n"
        '"STA02,2026-01-01T00:01:00+00:00,2\n'  # 第 3 行开引号未闭合
        "STA03,2026-01-01T00:02:00+00:00,3\n"
    ).encode()
    sid = upload(client, content).json()["source_id"]
    tid = create_task(client, sid).json()["task_id"]
    info = wait_terminal(client, tid)
    assert info["status"] == "failed"
    assert "第 3 行" in (info["error"] or "")


# ---------------------------------------------------------------- 共享扫描
def test_late_joiner_catchup_with_multiline_records(client):
    """慢扫描中含引号内换行的记录：迟到任务必须从 spool 补齐完整记录。"""
    rows = [
        HEADER,
        '"STA\n01",2026-01-01T00:00:00+00:00,5',
        'STA02,2026-01-01T00:01:00+00:00,12',
    ]
    # 大量普通行 + 周期性多行记录，保证迟到者补读窗口内有多行记录
    for i in range(2, 120):
        if i % 10 == 0:
            rows.append(f'"STA\n{i:03d}",2026-01-01T01:{i % 60:02d}:00+00:00,{i % 50}')
        else:
            rows.append(f"STA{i % 7:02d},2026-01-01T01:{i % 60:02d}:00+00:00,{i % 50}")
    content = ("\r\n".join(rows) + "\r\n").encode()
    sid = upload(client, content, delay_ms=3).json()["source_id"]

    t1 = create_task(client, sid).json()["task_id"]
    for _ in range(100):
        if client.get(f"/tasks/{t1}").json()["scanned_rows"] > 5:
            break
        time.sleep(0.02)
    t2 = create_task(client, sid).json()["task_id"]  # 途中加入

    i1 = wait_terminal(client, t1)
    i2 = wait_terminal(client, t2)
    assert i1["status"] == "completed" and i2["status"] == "completed"
    expected_data = content.decode().split("\r\n", 1)[1]
    for tid in (t1, t2):
        body = client.get(f"/tasks/{tid}/result").content.decode()
        assert body.split("\r\n", 1)[1] == expected_data  # 含补齐的多行记录
        assert '"STA\n01",2026-01-01T00:00:00+00:00,5\r\n' in body


def test_cancel_one_task_with_multiline_records_keeps_others(client):
    rows = [HEADER]
    for i in range(150):
        if i % 5 == 0:
            rows.append(f'"STA\n{i:03d}",2026-01-01T00:{i % 60:02d}:00+00:00,{i % 30}')
        else:
            rows.append(f"STA01,2026-01-01T00:{i % 60:02d}:00+00:00,{i % 30}")
    content = ("\r\n".join(rows) + "\r\n").encode()
    sid = upload(client, content, delay_ms=3).json()["source_id"]
    t_keep = create_task(client, sid).json()["task_id"]
    t_kill = create_task(client, sid, stations=["NOPE"]).json()["task_id"]
    time.sleep(0.1)

    cancelled = client.post(f"/tasks/{t_kill}/cancel").json()
    assert cancelled["status"] == "cancelled"
    kept = wait_terminal(client, t_keep)
    assert kept["status"] == "completed"
    body = client.get(f"/tasks/{t_keep}/result").content.decode()
    assert body.split("\r\n", 1)[1] == content.decode().split("\r\n", 1)[1]
