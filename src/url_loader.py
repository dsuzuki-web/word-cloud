from __future__ import annotations

from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import Any, Iterable
from urllib.parse import parse_qsl, urlencode, urlparse, urlunparse

import pandas as pd

from src.config_loader import AppConfig


NON_HTML_EXTENSIONS = {
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".gif",
    ".svg",
    ".zip",
    ".doc",
    ".docx",
    ".xls",
    ".xlsx",
    ".ppt",
    ".pptx",
    ".mp4",
}

EXPLICIT_INVALID_SCHEMES = {"mailto", "tel", "javascript"}
TRACKING_QUERY_PARAMS = {"fbclid", "gclid"}


class UrlLoadError(Exception):
    """Excel からの URL 読み込みに関する例外。"""


@dataclass(slots=True)
class UrlLoadResult:
    """URL 読み込み結果。"""

    target_records: list[dict[str, str]]
    skipped_records: list[dict[str, str]]

    @property
    def target_count(self) -> int:
        return len(self.target_records)

    @property
    def skipped_count(self) -> int:
        return len(self.skipped_records)

    @property
    def input_count(self) -> int:
        return self.target_count + self.skipped_count


def load_urls_from_excel(config: AppConfig) -> UrlLoadResult:
    """Excel から URL 一覧を読み込み、正規化・除外判定を行う。"""
    excel_path = config.get_path("input_xlsx")
    if excel_path is None:
        raise UrlLoadError("input_xlsx の設定が不正です。config.yaml を確認してください。")

    sheet_name = str(config.get("sheet_name", "Sheet1"))
    url_column = str(config.get("url_column", "URL"))
    exclude_patterns = _coerce_exclude_patterns(config.get("exclude_url_patterns", []))
    exclude_pdf = bool(config.get("exclude_pdf", True))

    dataframe = _read_excel_sheet(excel_path, sheet_name)
    _ensure_url_column_exists(dataframe, url_column)

    raw_values = [_stringify_cell_value(raw_value) for raw_value in dataframe[url_column].tolist()]
    return _load_urls_from_values(
        raw_values,
        exclude_patterns=exclude_patterns,
        source_type="xlsx",
        ignore_empty=False,
        exclude_pdf=exclude_pdf,
    )


def load_urls_from_text(
    raw_text: str,
    *,
    exclude_patterns: list[str] | None = None,
    max_urls: int | None = None,
    exclude_pdf: bool = True,
) -> UrlLoadResult:
    """改行区切りテキストから URL 一覧を読み込み、正規化・除外判定を行う。"""
    raw_values = [line.strip() for line in raw_text.splitlines()]
    return _load_urls_from_values(
        raw_values,
        exclude_patterns=exclude_patterns or [],
        source_type="url_list",
        max_urls=max_urls,
        ignore_empty=True,
        exclude_pdf=exclude_pdf,
    )


def save_url_load_results(result: UrlLoadResult, output_dir) -> tuple:
    """読み込み結果を CSV に保存する。"""
    target_path = output_dir / "target_urls.csv"
    skipped_path = output_dir / "url_load_skipped.csv"

    pd.DataFrame(result.target_records, columns=["original_url", "normalized_url", "source_type"]).to_csv(
        target_path,
        index=False,
        encoding="utf-8-sig",
    )
    pd.DataFrame(result.skipped_records, columns=["original_url", "normalized_url", "skip_reason"]).to_csv(
        skipped_path,
        index=False,
        encoding="utf-8-sig",
    )

    return target_path, skipped_path


def normalize_url(raw_url: str, *, exclude_pdf: bool = True) -> tuple[str, str | None]:
    """URL を正規化し、除外理由があれば返す。"""
    value = raw_url.strip()
    if not value:
        return "", "empty_value"

    parsed = urlparse(value)
    scheme = parsed.scheme.lower()
    if scheme in EXPLICIT_INVALID_SCHEMES:
        return "", f"unsupported_scheme:{scheme}"

    if scheme not in {"http", "https"} or not parsed.netloc:
        return "", "invalid_url"

    normalized_netloc = parsed.netloc.lower()
    normalized_netloc = _strip_default_port(scheme, normalized_netloc)
    normalized_path = _normalize_path(parsed.path)

    if _has_non_html_extension(normalized_path, exclude_pdf=exclude_pdf):
        normalized_url = urlunparse((scheme, normalized_netloc, normalized_path, "", "", ""))
        return normalized_url, "non_html_extension"

    filtered_query_pairs = [
        (key, value)
        for key, value in parse_qsl(parsed.query, keep_blank_values=True)
        if not _is_tracking_query_param(key)
    ]
    normalized_query = urlencode(filtered_query_pairs, doseq=True)

    normalized_url = urlunparse(
        (
            scheme,
            normalized_netloc,
            normalized_path,
            "",
            normalized_query,
            "",
        )
    )
    return normalized_url, None


def _read_excel_sheet(excel_path, sheet_name: str) -> pd.DataFrame:
    try:
        excel_file = pd.ExcelFile(excel_path, engine="openpyxl")
    except FileNotFoundError as exc:
        raise UrlLoadError(f"{excel_path} が見つかりません") from exc
    except OSError as exc:
        raise UrlLoadError(f"Excelファイルを開けませんでした: {exc}") from exc
    except Exception as exc:  # pragma: no cover - openpyxl 由来の例外吸収
        raise UrlLoadError(f"Excelファイルの読み込みに失敗しました: {exc}") from exc

    if sheet_name not in excel_file.sheet_names:
        raise UrlLoadError(
            f"シート名 '{sheet_name}' が見つかりません。利用可能なシート名: {excel_file.sheet_names}"
        )

    try:
        return pd.read_excel(
            excel_file,
            sheet_name=sheet_name,
            engine="openpyxl",
            dtype=object,
        )
    except Exception as exc:  # pragma: no cover - openpyxl 由来の例外吸収
        raise UrlLoadError(f"シート '{sheet_name}' の読み込みに失敗しました: {exc}") from exc


def _ensure_url_column_exists(dataframe: pd.DataFrame, url_column: str) -> None:
    columns = [str(column) for column in dataframe.columns.tolist()]
    if url_column not in dataframe.columns:
        raise UrlLoadError(f"URL 列が見つかりません。存在する列名: {columns}")


def _stringify_cell_value(value: Any) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _coerce_exclude_patterns(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return []


def _load_urls_from_values(
    raw_values: Iterable[str],
    *,
    exclude_patterns: list[str],
    source_type: str,
    max_urls: int | None = None,
    ignore_empty: bool,
    exclude_pdf: bool = True,
) -> UrlLoadResult:
    target_records: list[dict[str, str]] = []
    skipped_records: list[dict[str, str]] = []
    seen_normalized_urls: set[str] = set()
    effective_max_urls = _coerce_max_urls(max_urls)

    for raw_value in raw_values:
        original_url = str(raw_value).strip()
        if ignore_empty and not original_url:
            continue

        normalized_url, skip_reason = normalize_url(original_url, exclude_pdf=exclude_pdf)
        if skip_reason is not None:
            skipped_records.append(
                {
                    "original_url": original_url,
                    "normalized_url": normalized_url,
                    "skip_reason": skip_reason,
                }
            )
            continue

        matched_pattern = _find_matched_exclude_pattern(normalized_url, exclude_patterns)
        if matched_pattern is not None:
            skipped_records.append(
                {
                    "original_url": original_url,
                    "normalized_url": normalized_url,
                    "skip_reason": f"exclude_pattern:{matched_pattern}",
                }
            )
            continue

        if normalized_url in seen_normalized_urls:
            skipped_records.append(
                {
                    "original_url": original_url,
                    "normalized_url": normalized_url,
                    "skip_reason": "duplicate_url",
                }
            )
            continue

        if effective_max_urls is not None and len(target_records) >= effective_max_urls:
            skipped_records.append(
                {
                    "original_url": original_url,
                    "normalized_url": normalized_url,
                    "skip_reason": f"max_urls_exceeded:{effective_max_urls}",
                }
            )
            continue

        seen_normalized_urls.add(normalized_url)
        target_records.append(
            {
                "original_url": original_url,
                "normalized_url": normalized_url,
                "source_type": source_type,
            }
        )

    return UrlLoadResult(target_records=target_records, skipped_records=skipped_records)


def _coerce_max_urls(value: int | None) -> int | None:
    if value is None:
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _find_matched_exclude_pattern(normalized_url: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        if pattern in normalized_url:
            return pattern
    return None


def _strip_default_port(scheme: str, netloc: str) -> str:
    if scheme == "http" and netloc.endswith(":80"):
        return netloc[:-3]
    if scheme == "https" and netloc.endswith(":443"):
        return netloc[:-4]
    return netloc


def _normalize_path(path: str) -> str:
    if not path or path == "/":
        return ""

    normalized_path = path
    if normalized_path.endswith("/"):
        normalized_path = normalized_path.rstrip("/")

    if not normalized_path.startswith("/"):
        normalized_path = f"/{normalized_path}"

    return normalized_path


def _has_non_html_extension(path: str, *, exclude_pdf: bool = True) -> bool:
    if not path:
        return False
    suffix = PurePosixPath(path).suffix.lower()
    if suffix == ".pdf":
        return exclude_pdf
    return suffix in NON_HTML_EXTENSIONS


def _is_tracking_query_param(key: str) -> bool:
    return key.lower() in TRACKING_QUERY_PARAMS
