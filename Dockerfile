FROM python:3.11-slim

# 128 MiB 容器预算：只装最小依赖，编译期产物不保留
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    DATA_DIR=/data

WORKDIR /app

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt \
    && groupadd --system app && useradd --system --gid app --home-dir /app app

COPY app ./app

RUN mkdir -p /data && chown -R app:app /app /data
USER app

EXPOSE 8000

# slim 镜像没有 curl/wget，用 python 做健康检查
HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/health', timeout=2).status==200 else 1)"

# 单 worker：共享扫描与订阅者调度为进程内组件；流式处理内存占用远低于 128 MiB
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "1"]
