from __future__ import annotations

from dataclasses import dataclass
from os import PathLike
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlparse

import yaml

from src.font_utils import resolve_font_path


class ConfigError(Exception):
    """設定ファイルの読み込み・検証に関する例外。"""


@dataclass(slots=True)
class AppConfig:
    """アプリケーション設定。"""

    config_path: Path
    config_dir: Path
    data: dict[str, Any]
    resolved_paths: dict[str, Path]

    def get(self, key: str, default: Any | None = None) -> Any:
        return self.data.get(key, default)

    def get_path(self, key: str) -> Path | None:
        return self.resolved_paths.get(key)


def load_config(config_path: str | Path) -> AppConfig:
    """YAML 設定ファイルを読み込む。"""
    path = Path(config_path).expanduser()
    if not path.is_absolute():
        path = (Path.cwd() / path).resolve()
    else:
        path = path.resolve()

    if not path.exists():
        raise ConfigError(f"設定ファイルが見つかりません: {path}")
    if not path.is_file():
        raise ConfigError(f"設定ファイルではありません: {path}")

    try:
        with path.open("r", encoding="utf-8") as f:
            loaded = yaml.safe_load(f)
    except yaml.YAMLError as exc:
        raise ConfigError(f"YAMLの読み込みに失敗しました: {exc}") from exc
    except OSError as exc:
        raise ConfigError(f"設定ファイルを開けませんでした: {exc}") from exc

    if loaded is None:
        loaded = {}
    if not isinstance(loaded, dict):
        raise ConfigError("設定ファイルの最上位は辞書形式で記述してください")

    config_dir = path.parent
    data = _apply_defaults(loaded)
    resolved_paths = _resolve_paths(data, config_dir)

    return AppConfig(
        config_path=path,
        config_dir=config_dir,
        data=data,
        resolved_paths=resolved_paths,
    )


def build_config_from_settings(
    settings: Mapping[str, Any],
    *,
    base_dir: str | Path | None = None,
    config_path: str | Path | None = None,
) -> AppConfig:
    """UI などから受け取った設定辞書を AppConfig に変換する。"""
    if not isinstance(settings, Mapping):
        raise ConfigError("settings は辞書形式で指定してください")

    resolved_base_dir = _resolve_base_dir(base_dir)
    resolved_config_path = _resolve_runtime_config_path(config_path, resolved_base_dir)

    data = _apply_defaults(dict(settings))
    resolved_paths = _resolve_paths(data, resolved_base_dir)
    return AppConfig(
        config_path=resolved_config_path,
        config_dir=resolved_base_dir,
        data=data,
        resolved_paths=resolved_paths,
    )


def validate_config(config: AppConfig) -> list[str]:
    """設定ファイルの妥当性を検証し、エラーメッセージ一覧を返す。"""
    errors: list[str] = []

    input_mode = config.get("input_mode")
    if input_mode not in {"xlsx", "domain", "url_list"}:
        errors.append('input_mode は "xlsx" / "domain" / "url_list" のいずれかを指定してください')

    if input_mode == "xlsx":
        input_xlsx = config.get_path("input_xlsx")
        if input_xlsx is None or not input_xlsx.exists() or not input_xlsx.is_file():
            display_value = config.get("input_xlsx", "<未設定>")
            errors.append(f"{display_value} が見つかりません")

    if input_mode == "domain":
        domain_url = str(config.get("domain_url", "")).strip()
        if not _is_http_url(domain_url):
            errors.append("domain_url は http:// または https:// で始まるURLを指定してください")

    if input_mode == "url_list":
        url_list_text = str(config.get("url_list_text", "")).strip()
        input_urls = config.get("input_urls", [])
        has_input_urls = isinstance(input_urls, list) and any(str(item).strip() for item in input_urls)
        if not url_list_text and not has_input_urls:
            errors.append("url_list_text または input_urls に少なくとも1件のURLを指定してください")

    font_path = config.get_path("font_path")
    if font_path is None or not font_path.exists() or not font_path.is_file():
        display_value = config.get("font_path", "<未設定>")
        errors.append(f"日本語フォントを解決できませんでした: {display_value}。font_path を指定するか、packages.txt で fonts-noto-cjk を導入してください")

    use_default_stopwords = bool(config.get("use_default_stopwords", True))
    if use_default_stopwords:
        stopwords_default = config.get_path("stopwords_default_file")
        if stopwords_default is None or not stopwords_default.exists() or not stopwords_default.is_file():
            display_value = config.get("stopwords_default_file", "<未設定>")
            errors.append(f"ストップワードファイルが見つかりません: {display_value}")

    use_user_stopwords_file = bool(config.get("use_user_stopwords_file", True))
    if use_user_stopwords_file:
        stopwords_user = config.get_path("stopwords_user_file")
        if stopwords_user is None or not stopwords_user.exists() or not stopwords_user.is_file():
            display_value = config.get("stopwords_user_file", "<未設定>")
            errors.append(f"ユーザー追加用ストップワードファイルが見つかりません: {display_value}")

    return errors


def _apply_defaults(data: dict[str, Any]) -> dict[str, Any]:
    """将来の拡張を見据えて最低限のデフォルト値を補う。"""
    defaults: dict[str, Any] = {
        "input_mode": "domain",
        "input_xlsx": "input/urls.xlsx",
        "sheet_name": "Sheet1",
        "url_column": "URL",
        "domain_url": "https://example.com/",
        "url_list_text": "",
        "input_urls": [],
        "max_list_urls": 50,
        "include_subdomains": False,
        "same_domain_only": True,
        "exclude_pdf": True,
        "respect_robots_txt": True,
        "crawl_strategy": "sitemap_then_bfs",
        "prefer_wordpress_api": True,
        "max_pages": 200,
        "max_depth": 3,
        "exclude_url_patterns": ["/tag/", "/category/", "/search"],
        "request_timeout_sec": 15,
        "retry_count": 2,
        "sleep_min_sec": 2.0,
        "sleep_max_sec": 3.0,
        "user_agent": "SiteKeywordAnalyzer/1.0",
        "min_text_length": 100,
        "include_title": False,
        "detect_noindex": True,
        "exclude_noindex_pages": False,
        "font_path": "",
        "top_n_wordcloud": 200,
        "use_default_stopwords": True,
        "use_user_stopwords_file": True,
        "additional_stopwords_text": "",
        "additional_stopwords": [],
        "compound_mode": True,
        "forced_compounds_text": "",
        "forced_compounds": [],
        "stopwords_default_file": "config/stopwords_ja.txt",
        "stopwords_user_file": "config/user_stopwords.txt",
        "output_root": "output",
        "save_raw_frequency": True,
        "save_extracted_text": False,
        "save_robots_report": True,
        "save_noindex_report": True,
        "save_url_audit": True,
    }

    merged = defaults.copy()
    merged.update(data)

    if "same_domain_only" in data:
        same_domain_only = bool(data.get("same_domain_only", True))
        merged["same_domain_only"] = same_domain_only
        merged["include_subdomains"] = not same_domain_only
    else:
        include_subdomains = bool(merged.get("include_subdomains", False))
        merged["include_subdomains"] = include_subdomains
        merged["same_domain_only"] = not include_subdomains

    merged["exclude_pdf"] = bool(merged.get("exclude_pdf", True))
    merged["include_title"] = bool(merged.get("include_title", False))
    merged["use_default_stopwords"] = bool(merged.get("use_default_stopwords", True))
    merged["use_user_stopwords_file"] = bool(merged.get("use_user_stopwords_file", True))
    merged["compound_mode"] = bool(merged.get("compound_mode", True))
    merged["prefer_wordpress_api"] = bool(merged.get("prefer_wordpress_api", True))

    if isinstance(merged.get("additional_stopwords"), str):
        merged["additional_stopwords"] = [merged["additional_stopwords"]]
    if isinstance(merged.get("forced_compounds"), str):
        merged["forced_compounds"] = [merged["forced_compounds"]]
    return merged


def _resolve_paths(data: dict[str, Any], config_dir: Path) -> dict[str, Path]:
    path_keys = {
        "input_xlsx",
        "stopwords_default_file",
        "stopwords_user_file",
        "output_root",
    }

    resolved: dict[str, Path] = {}
    for key in path_keys:
        path_value = _coerce_path_value(data.get(key))
        if path_value is None:
            continue

        path = path_value.expanduser()
        if not path.is_absolute():
            path = (config_dir / path).resolve()
        else:
            path = path.resolve()
        resolved[key] = path

    font_path = resolve_font_path(preferred=data.get("font_path"), base_dir=config_dir)
    if font_path is not None:
        resolved["font_path"] = font_path

    return resolved


def _is_http_url(value: str) -> bool:
    if not value:
        return False
    parsed = urlparse(value)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def _coerce_path_value(value: Any) -> Path | None:
    if isinstance(value, Path):
        return value
    if isinstance(value, PathLike):
        return Path(value)
    if isinstance(value, str) and value.strip():
        return Path(value)
    return None


def _resolve_base_dir(base_dir: str | Path | None) -> Path:
    if base_dir is None:
        return Path.cwd().resolve()

    candidate = Path(base_dir).expanduser()
    if not candidate.is_absolute():
        candidate = (Path.cwd() / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate


def _resolve_runtime_config_path(config_path: str | Path | None, base_dir: Path) -> Path:
    if config_path is None:
        return (base_dir / "config.runtime.yaml").resolve()

    candidate = Path(config_path).expanduser()
    if not candidate.is_absolute():
        candidate = (base_dir / candidate).resolve()
    else:
        candidate = candidate.resolve()
    return candidate
