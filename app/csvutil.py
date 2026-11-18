"""CSV 原件的表头与行校验。

原件约定三列：station（站号，字符串）、utc_time（带时区的 UTC 时刻）、
temperature（整数温度）。校验只判断合法性，原始字节行原样保留，
从而保证交付文件的行序、重复行与原件逐字节一致。
"""
from __future__ import annotations

import csv
import io
from datetime import datetime, timezone

HEADER = ("station", "utc_time", "temperature")


class CsvFormatError(ValueError):
    """原件格式错误。"""


def validate_header(header_line: str) -> None:
    """校验首行表头，允许 UTF-8 BOM。"""
    reader = csv.reader(io.StringIO(header_line.lstrip("﻿")))
    try:
        fields = next(reader)
    except StopIteration:
        raise CsvFormatError("空文件，缺少表头")
    if tuple(f.strip() for f in fields) != HEADER:
        raise CsvFormatError(
            f"表头必须为 {','.join(HEADER)}，实际为 {','.join(fields)!r}"
        )


def parse_row(line: str, line_no: int) -> tuple[str, datetime, int]:
    """解析并校验一行数据，返回 (站号, UTC 时刻, 整数温度)。

    非法行抛 CsvFormatError，携带行号便于定位。
    """
    reader = csv.reader(io.StringIO(line))
    try:
        fields = next(reader)
    except StopIteration:
        raise CsvFormatError(f"第 {line_no} 行：空行")
    if len(fields) != 3:
        raise CsvFormatError(f"第 {line_no} 行：应为 3 列，实际 {len(fields)} 列")

    station, time_text, temp_text = (f.strip() for f in fields)
    if not station:
        raise CsvFormatError(f"第 {line_no} 行：站号为空")

    if not time_text:
        raise CsvFormatError(f"第 {line_no} 行：UTC 时刻为空")
    iso_text = time_text[:-1] + "+00:00" if time_text.endswith("Z") else time_text
    try:
        dt = datetime.fromisoformat(iso_text)
    except ValueError as exc:
        raise CsvFormatError(f"第 {line_no} 行：时刻 {time_text!r} 无法解析") from exc
    if dt.tzinfo is None:
        raise CsvFormatError(f"第 {line_no} 行：时刻 {time_text!r} 缺少时区信息")
    dt = dt.astimezone(timezone.utc)

    if not temp_text:
        raise CsvFormatError(f"第 {line_no} 行：温度为空")
    try:
        temperature = int(temp_text)
    except ValueError as exc:
        raise CsvFormatError(f"第 {line_no} 行：温度 {temp_text!r} 不是整数") from exc

    return station, dt, temperature


def row_matches(
    station: str,
    temperature: int,
    stations: set[str] | None,
    temp_min: int,
    temp_max: int,
) -> bool:
    """站号集合 + 温度闭区间筛选；stations 为 None/空集表示全部站号。"""
    if stations and station not in stations:
        return False
    return temp_min <= temperature <= temp_max
