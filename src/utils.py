from __future__ import annotations

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

from src.config_loader import AppConfig

LOGGER_NAME = "site_keyword_analyzer"


def create_run_output_dir(output_root: Path) -> Path:
    """実行ごとの出力フォルダを作成する。"""
    output_root.mkdir(parents=True, exist_ok=True)

    base_name = datetime.now().strftime("%Y%m%d_%H%M%S")
    candidate = output_root / base_name
    counter = 1

    while candidate.exists():
        candidate = output_root / f"{base_name}_{counter:02d}"
        counter += 1

    candidate.mkdir(parents=True, exist_ok=False)
    return candidate


def setup_logging(log_file: Path) -> logging.Logger:
    """UTF-8 のログファイルを初期化して logger を返す。"""
    logger = logging.getLogger(LOGGER_NAME)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    logger.handlers.clear()

    handler = logging.FileHandler(log_file, encoding="utf-8")
    formatter = logging.Formatter(
        fmt="%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    return logger


def write_run_config(config: AppConfig, output_dir: Path) -> Path:
    """実行時設定を JSON で保存する。"""
    payload: dict[str, Any] = {
        "_meta": {
            "project_name": "site_keyword_analyzer",
            "created_at": datetime.now().isoformat(timespec="seconds"),
            "config_path": str(config.config_path),
            "config_dir": str(config.config_dir),
            "output_dir": str(output_dir),
            "resolved_paths": {key: str(path) for key, path in config.resolved_paths.items()},
        },
        "config": config.data,
    }

    destination = output_dir / "run_config.json"
    with destination.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    return destination


def write_dataframe_csv(dataframe: pd.DataFrame, destination: Path) -> Path:
    """DataFrame を UTF-8 with BOM で CSV 保存する。"""
    dataframe.to_csv(destination, index=False, encoding="utf-8-sig")
    return destination
