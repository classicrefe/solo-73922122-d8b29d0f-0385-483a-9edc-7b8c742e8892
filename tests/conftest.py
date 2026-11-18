"""pytest 全局夹具：每个测试使用独立临时 DATA_DIR 的 TestClient。"""
from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


@pytest.fixture()
def client(tmp_path, monkeypatch):
    data_dir = tmp_path / "data"
    monkeypatch.setenv("DATA_DIR", str(data_dir))
    # 确保导入到的是带本次环境变量的全新模块
    for name in list(sys.modules):
        if name == "app" or name.startswith("app."):
            del sys.modules[name]
    config = importlib.import_module("app.config")
    main = importlib.import_module("app.main")
    importlib.reload(config)
    importlib.reload(main)
    with TestClient(main.app) as c:
        yield c
