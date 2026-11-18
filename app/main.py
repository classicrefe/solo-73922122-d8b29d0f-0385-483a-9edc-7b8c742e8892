"""FastAPI 应用：上传原件、受理筛选任务、查询/取消/下载。

查询与任务受理接口均为异步立即返回，不等待后台扫描；扫描进度通过
GET /tasks/{id} 的 scanned_rows / matched_rows 观察。
"""
from __future__ import annotations

import hashlib
import json
import uuid
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.responses import FileResponse, PlainTextResponse
from pydantic import BaseModel, Field

from . import config
from .csvutil import HEADER, CsvFormatError, validate_header
from .db import Database
from .scan import ScanManager, _now


def _row_to_dict(row) -> dict:
    return {
        "task_id": row["id"],
        "source_id": row["source_id"],
        "stations": json.loads(row["stations"]) if row["stations"] else None,
        "temp_min": row["temp_min"],
        "temp_max": row["temp_max"],
        "status": row["status"],
        "matched_rows": row["matched_rows"],
        "error": row["error"],
        "result_path": row["result_path"],
        "result_available": row["status"] == "completed",
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }


def _source_to_dict(row) -> dict:
    return {
        "source_id": row["id"],
        "filename": row["filename"],
        "size_bytes": row["size_bytes"],
        "sha256": row["sha256"],
        "uploaded_at": row["uploaded_at"],
        "status": row["status"],
        "header_ok": bool(row["header_ok"]),
        "row_delay_ms": row["row_delay_ms"],
    }


class TaskCreate(BaseModel):
    source_id: str
    # 空列表/null 表示全部站号
    stations: list[str] | None = Field(default=None)
    temp_min: int
    temp_max: int


@asynccontextmanager
async def lifespan(app: FastAPI):
    for d in (config.DATA_DIR, config.SOURCES_DIR, config.RESULTS_DIR, config.SCANS_DIR):
        d.mkdir(parents=True, exist_ok=True)
    app.state.db = Database(config.DB_PATH)
    app.state.manager = ScanManager(app.state.db)
    # 重启恢复：未完任务标记 interrupted，清理遗留暂存
    app.state.manager.recover_on_startup()
    yield
    # 先停在途扫描并等待线程退出，再关闭 DB，防止线程访问已关闭连接
    app.state.manager.shutdown()
    app.state.db.close()


app = FastAPI(
    title="观测数据子集交付服务",
    description=(
        "上传至多 256 MiB 的观测 CSV（station, utc_time, temperature），"
        "按站号集合与温度闭区间提取子集。同原件的在途任务共享一次源扫描，"
        "支持迟到补入与即时取消。"
    ),
    version="1.0.0",
    lifespan=lifespan,
)


def _manager(app: FastAPI) -> ScanManager:
    return app.state.manager


@app.get("/health")
def health():
    return {"status": "ok", "time": datetime.now(timezone.utc).isoformat()}


@app.post("/sources")
async def upload_source(
    file: UploadFile = File(..., description="CSV 原件，列：station,utc_time,temperature"),
    row_delay_ms: int = Form(
        default=0,
        description="可控慢扫描：扫描时每行额外等待的毫秒数，便于演示取消/迟到加入",
        ge=0,
        le=60_000,
    ),
):
    """流式落盘上传，超过 MAX_UPLOAD_BYTES（默认 256 MiB）立即拒收。"""
    source_id = uuid.uuid4().hex
    dest = config.SOURCES_DIR / f"{source_id}.csv"
    sha = hashlib.sha256()
    total = 0
    header_checked = False
    header_buf = ""  # 累积到首个换行，防止表头跨 1 MiB 分块边界
    try:
        with open(dest, "wb") as out:
            while True:
                chunk = await file.read(1024 * 1024)
                if not chunk:
                    break
                total += len(chunk)
                if total > config.MAX_UPLOAD_BYTES:
                    out.close()
                    dest.unlink(missing_ok=True)
                    raise HTTPException(
                        status_code=413,
                        detail=(
                            f"文件超过上限 {config.MAX_UPLOAD_BYTES} 字节"
                            f"（{config.MAX_UPLOAD_BYTES // (1024 * 1024)} MiB）"
                        ),
                    )
                sha.update(chunk)
                out.write(chunk)
                if not header_checked:
                    header_buf += chunk.decode("utf-8", errors="replace")
                    if "\n" in header_buf:
                        first_line = header_buf.split("\n", 1)[0]
                        try:
                            validate_header(first_line)
                            header_checked = True
                        except CsvFormatError as exc:
                            out.close()
                            dest.unlink(missing_ok=True)
                            raise HTTPException(status_code=400, detail=str(exc))
    except HTTPException:
        raise
    except UnicodeDecodeError as exc:
        dest.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail=f"文件不是合法 UTF-8：{exc}") from exc

    if total == 0 or not header_checked:
        dest.unlink(missing_ok=True)
        raise HTTPException(status_code=400, detail="空文件或缺少表头")

    app.state.db.insert_source(
        source_id,
        file.filename or "upload.csv",
        total,
        sha.hexdigest(),
        _now(),
        row_delay_ms,
    )
    return {
        "source_id": source_id,
        "filename": file.filename,
        "size_bytes": total,
        "sha256": sha.hexdigest(),
        "row_delay_ms": row_delay_ms,
    }


@app.get("/sources")
def list_sources():
    return [_source_to_dict(r) for r in app.state.db.list_sources()]


@app.get("/sources/{source_id}")
def get_source(source_id: str):
    row = app.state.db.get_source(source_id)
    if row is None:
        raise HTTPException(status_code=404, detail="source 不存在")
    return _source_to_dict(row)


@app.post("/tasks", status_code=201)
def create_task(body: TaskCreate):
    if body.temp_min > body.temp_max:
        raise HTTPException(status_code=400, detail="temp_min 不能大于 temp_max")
    try:
        state = _manager(app).create_task(
            body.source_id, body.stations, body.temp_min, body.temp_max
        )
    except KeyError:
        raise HTTPException(status_code=404, detail="source 不存在")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    return state.public()


@app.get("/tasks")
def list_tasks(source_id: str | None = None):
    return [_row_to_dict(r) for r in app.state.db.list_tasks(source_id)]


@app.get("/tasks/{task_id}")
def get_task(task_id: str):
    row = app.state.db.get_task(task_id)
    if row is None:
        raise HTTPException(status_code=404, detail="task 不存在")
    data = _row_to_dict(row)
    # 运行中的任务附带当前扫描进度
    data["scanned_rows"] = app.state.manager.get_scanned_rows(row["source_id"])
    return data


@app.post("/tasks/{task_id}/cancel")
def cancel_task(task_id: str):
    manager = _manager(app)
    if task_id not in manager.tasks:
        # 重启前遗留的任务已在启动时标 interrupted
        row = app.state.db.get_task(task_id)
        if row is None:
            raise HTTPException(status_code=404, detail="task 不存在")
        return _row_to_dict(row)
    state = manager.cancel_task(task_id)
    return state.public()


@app.get("/tasks/{task_id}/result")
def download_result(task_id: str):
    row = app.state.db.get_task(task_id)
    if row is None:
        raise HTTPException(status_code=404, detail="task 不存在")
    if row["status"] != "completed":
        raise HTTPException(
            status_code=409,
            detail=f"任务状态为 {row['status']}，结果尚未开放（完整原件校验通过后才可下载）",
        )
    result = Path(row["result_path"])
    if not result.exists():
        raise HTTPException(status_code=410, detail="结果文件已丢失")
    return FileResponse(
        result,
        media_type="text/csv",
        filename=f"{task_id}.csv",
    )


@app.get("/", response_class=PlainTextResponse)
def root():
    return (
        "观测数据子集交付服务\n"
        "- 接口文档：/docs （Swagger UI）/ /redoc\n"
        "- 健康检查：/health\n"
        f"- 上传上限：{config.MAX_UPLOAD_BYTES} 字节\n"
        f"- CSV 表头：{','.join(HEADER)}\n"
    )
