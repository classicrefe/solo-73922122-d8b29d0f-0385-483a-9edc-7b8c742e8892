"""CSV 原件的表头与记录校验、记录切分。

原件约定三列：station（站号，字符串）、utc_time（带时区的 UTC 时刻）、
temperature（整数温度）。校验只判断合法性，原始记录字节原样保留，
从而保证交付文件的记录内容、顺序与重复记录和原件逐字节一致。

记录切分遵循 RFC 4180 / Python csv 默认方言的引号规则：
* 引号仅在字段起始位置（记录开头或逗号后）开启引用段；
* 引用段内 ``""`` 为转义引号，单个 ``"`` 闭合引用段；
* 引用段内的 \\r / \\n 属于字段内容，不构成记录边界；
* 记录终止符支持 \\n、\\r、\\r\\n 三种，原始字节保留在记录内。
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
    reader = csv.reader(io.StringIO(header_line.lstrip("﻿"), newline=""))
    try:
        fields = next(reader)
    except StopIteration:
        raise CsvFormatError("空文件，缺少表头")
    if tuple(f.strip() for f in fields) != HEADER:
        raise CsvFormatError(
            f"表头必须为 {','.join(HEADER)}，实际为 {','.join(fields)!r}"
        )


def parse_row(record: str, line_no: int) -> tuple[str, datetime, int]:
    """解析并校验一条完整数据记录，返回 (站号, UTC 时刻, 整数温度)。

    record 为一条完整记录的原始文本（可能含引号内换行，可带原终止符）。
    非法记录抛 CsvFormatError，携带记录起始行号便于定位。
    """
    reader = csv.reader(io.StringIO(record, newline=""))
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


def decode_record(record: bytes, line_no: int) -> str:
    """把一条记录的原始字节解码为文本；非法 UTF-8 视为格式错误。"""
    try:
        return record.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise CsvFormatError(f"第 {line_no} 行：记录不是合法 UTF-8") from exc


def count_line_breaks(record: bytes) -> int:
    """统计记录字节中的物理行终止符数量（\\n、\\r、\\r\\n 各计 1）。

    用于把“第几条记录”换算成原件中的物理行号，便于错误定位。
    """
    return record.count(b"\n") + record.count(b"\r") - record.count(b"\r\n")


class RecordSplitter:
    """增量 CSV 记录切分器：把字节流按完整记录切开。

    引号字段内的换行属于记录内容，不会误拆记录；记录原始字节（含原
    终止符）原样保留。逐块 feed()，完成的记录立即返回；输入结束后调用
    finalize() 取末尾无终止符的记录，引号未闭合则抛 CsvFormatError。

    切分只关心 '"'/','/'\\r'/'\\n' 四个 ASCII 字节，它们不会出现在
    UTF-8 多字节序列中，因此可以直接作用于未解码的字节流。
    """

    _FIELD_START = 0      # 字段起始（记录开头或逗号后）
    _IN_FIELD = 1         # 未引用字段内
    _IN_QUOTES = 2        # 引用段内
    _QUOTE_IN_QUOTES = 3  # 引用段内刚遇到一个 "（闭合或转义对的一半）
    _AFTER_CR = 4         # 记录外刚遇到 \r（等待是否紧跟 \n）

    def __init__(self) -> None:
        self._state = self._FIELD_START
        self._buf = bytearray()  # 当前记录尚未完成的字节

    @property
    def in_quotes(self) -> bool:
        return self._state in (self._IN_QUOTES, self._QUOTE_IN_QUOTES)

    def feed(self, data: bytes) -> list[bytes]:
        """喂入一块字节，返回本次完成的完整记录列表（含原始终止符）。"""
        records: list[bytes] = []
        i, n = 0, len(data)
        while i < n:
            b = data[i]
            st = self._state
            if st == self._IN_QUOTES:
                self._buf.append(b)
                i += 1
                if b == 0x22:  # '"'
                    self._state = self._QUOTE_IN_QUOTES
            elif st == self._QUOTE_IN_QUOTES:
                if b == 0x22:  # 转义的一对引号
                    self._buf.append(b)
                    i += 1
                    self._state = self._IN_QUOTES
                else:
                    # 引号已闭合；当前字节按字段内规则重新处理
                    self._state = self._IN_FIELD
            elif st == self._AFTER_CR:
                if b == 0x0A:  # \r\n 是同一个终止符
                    self._buf.append(b)
                    i += 1
                records.append(bytes(self._buf))
                self._buf.clear()
                self._state = self._FIELD_START
                # 当前字节（若非 \n）属于下一条记录，不前进、重新处理
            else:  # _FIELD_START 或 _IN_FIELD
                if b == 0x22 and st == self._FIELD_START:
                    self._buf.append(b)
                    i += 1
                    self._state = self._IN_QUOTES
                elif b == 0x2C:  # ','
                    self._buf.append(b)
                    i += 1
                    self._state = self._FIELD_START
                elif b == 0x0A:  # \n
                    self._buf.append(b)
                    i += 1
                    records.append(bytes(self._buf))
                    self._buf.clear()
                    self._state = self._FIELD_START
                elif b == 0x0D:  # \r
                    self._buf.append(b)
                    i += 1
                    self._state = self._AFTER_CR
                else:
                    self._buf.append(b)
                    i += 1
                    self._state = self._IN_FIELD
        return records

    def finalize(self) -> bytes | None:
        """输入结束：返回末尾无终止符的记录；引号未闭合抛 CsvFormatError。"""
        if self._state == self._IN_QUOTES:
            raise CsvFormatError("引号未闭合，记录直到文件末尾仍未结束")
        # _QUOTE_IN_QUOTES 结尾即记录以闭合引号收尾，合法
        if self._buf:
            rec = bytes(self._buf)
            self._buf.clear()
            self._state = self._FIELD_START
            return rec
        return None

    def flush_record_boundary(self) -> bytes | None:
        """调用方保证输入恰好停在记录边界时，取出可能滞留的末尾记录。

        只有记录以 \\r 结尾（_AFTER_CR，等待判断是否有 \\n）才会滞留；
        其余滞留说明上游字节没有按记录对齐，属于内部不一致，直接丢弃。
        """
        if self._state == self._AFTER_CR and self._buf:
            rec = bytes(self._buf)
            self._buf.clear()
            self._state = self._FIELD_START
            return rec
        self._buf.clear()
        self._state = self._FIELD_START
        return None
