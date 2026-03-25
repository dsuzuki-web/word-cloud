from __future__ import annotations

import html as html_lib
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime

import pandas as pd
from bs4 import BeautifulSoup

try:
    import trafilatura
except ImportError:  # pragma: no cover
    trafilatura = None

from src.config_loader import AppConfig
from src.crawler import CrawlBatchResult, FetchedPageResult

EXCLUDED_TAGS = [
    "header",
    "footer",
    "nav",
    "aside",
    "script",
    "style",
    "noscript",
    "svg",
    "iframe",
    "form",
    "button",
    "input",
    "select",
    "option",
]
FALLBACK_SELECTORS = ["article", "main", '[role="main"]', "section", "div"]
URL_PATTERN = re.compile(r"https?://[^\s]+", re.IGNORECASE)
EMAIL_PATTERN = re.compile(r"\b[A-Z0-9._%+-]+@[A-Z0-9.-]+\.[A-Z]{2,}\b", re.IGNORECASE)
SPACE_PATTERN = re.compile(r"[\t\x0b\x0c\u00a0 ]+")
NEWLINE_PATTERN = re.compile(r"\n{3,}")

URL_AUDIT_COLUMNS = [
    "url",
    "final_url",
    "status_code",
    "robots_txt",
    "noindex_status",
    "noindex_source",
    "content_type",
    "extracted_chars",
    "extraction_success",
    "included_in_analysis",
    "excluded_reason",
]
NOINDEX_COLUMNS = [
    "url",
    "final_url",
    "status_code",
    "noindex_source",
    "noindex_value",
    "included_in_analysis",
    "detected_at",
]
EXTRACTED_TEXT_COLUMNS = [
    "url",
    "final_url",
    "status_code",
    "extracted_chars",
    "extraction_method",
    "included_in_analysis",
    "extracted_text",
]


@dataclass(slots=True)
class TextAuditRecord:
    """1ページ分の本文抽出・noindex監査結果。"""

    url: str
    final_url: str
    status_code: int | None
    robots_txt: str
    noindex_status: str
    noindex_source: str
    noindex_value: str
    content_type: str
    extracted_text: str
    extracted_chars: int
    extraction_success: bool
    extraction_method: str
    included_in_analysis: bool
    excluded_reason: str
    detected_at: str

    @property
    def is_noindex(self) -> bool:
        return self.noindex_status == "noindex"


@dataclass(slots=True)
class TextExtractionBatchResult:
    """本文抽出と監査CSV生成のための結果集合。"""

    records: list[TextAuditRecord]

    @property
    def included_records(self) -> list[TextAuditRecord]:
        return [record for record in self.records if record.included_in_analysis]

    @property
    def included_count(self) -> int:
        return len(self.included_records)

    @property
    def extraction_success_count(self) -> int:
        return sum(1 for record in self.records if record.extraction_success)

    @property
    def noindex_records(self) -> list[TextAuditRecord]:
        return [record for record in self.records if record.is_noindex]

    @property
    def noindex_count(self) -> int:
        return len(self.noindex_records)

    def to_url_audit_dataframe(self) -> pd.DataFrame:
        rows = [
            {
                "url": record.url,
                "final_url": record.final_url,
                "status_code": record.status_code,
                "robots_txt": record.robots_txt,
                "noindex_status": record.noindex_status,
                "noindex_source": record.noindex_source,
                "content_type": record.content_type,
                "extracted_chars": record.extracted_chars,
                "extraction_success": record.extraction_success,
                "included_in_analysis": record.included_in_analysis,
                "excluded_reason": record.excluded_reason,
            }
            for record in self.records
        ]
        return pd.DataFrame(rows, columns=URL_AUDIT_COLUMNS)

    def to_noindex_dataframe(self) -> pd.DataFrame:
        rows = [
            {
                "url": record.url,
                "final_url": record.final_url,
                "status_code": record.status_code,
                "noindex_source": record.noindex_source,
                "noindex_value": record.noindex_value,
                "included_in_analysis": record.included_in_analysis,
                "detected_at": record.detected_at,
            }
            for record in self.noindex_records
        ]
        return pd.DataFrame(rows, columns=NOINDEX_COLUMNS)

    def to_extracted_texts_dataframe(self) -> pd.DataFrame:
        rows = [
            {
                "url": record.url,
                "final_url": record.final_url,
                "status_code": record.status_code,
                "extracted_chars": record.extracted_chars,
                "extraction_method": record.extraction_method,
                "included_in_analysis": record.included_in_analysis,
                "extracted_text": record.extracted_text,
            }
            for record in self.records
            if record.extracted_text
        ]
        return pd.DataFrame(rows, columns=EXTRACTED_TEXT_COLUMNS)


class TextExtractionError(Exception):
    """本文抽出処理の共通例外。"""


def analyze_crawl_results(crawl_result: CrawlBatchResult, config: AppConfig, logger=None) -> TextExtractionBatchResult:
    """クロール結果から本文抽出と noindex 監査を行う。"""
    min_text_length = max(0, int(config.get("min_text_length", 100)))
    detect_noindex = bool(config.get("detect_noindex", True))
    exclude_noindex_pages = bool(config.get("exclude_noindex_pages", False))
    include_title = bool(config.get("include_title", False))

    records: list[TextAuditRecord] = []

    for crawl_record in crawl_result.page_results:
        try:
            records.append(
                _analyze_single_record(
                    crawl_record,
                    min_text_length=min_text_length,
                    detect_noindex=detect_noindex,
                    exclude_noindex_pages=exclude_noindex_pages,
                    include_title=include_title,
                    logger=logger,
                )
            )
        except Exception as exc:  # pragma: no cover
            if logger is not None:
                logger.exception("本文抽出中に予期しないエラーが発生しました: %s", crawl_record.target_url)
            records.append(
                TextAuditRecord(
                    url=crawl_record.target_url,
                    final_url=crawl_record.final_url,
                    status_code=crawl_record.status_code,
                    robots_txt=_map_robots_txt_status(crawl_record),
                    noindex_status="unknown",
                    noindex_source="",
                    noindex_value="",
                    content_type=crawl_record.content_type,
                    extracted_text="",
                    extracted_chars=0,
                    extraction_success=False,
                    extraction_method="",
                    included_in_analysis=False,
                    excluded_reason=f"extract_exception:{type(exc).__name__}",
                    detected_at=_now_iso(),
                )
            )

    return TextExtractionBatchResult(records=records)


def _analyze_single_record(
    crawl_record: FetchedPageResult,
    *,
    min_text_length: int,
    detect_noindex: bool,
    exclude_noindex_pages: bool,
    include_title: bool,
    logger=None,
) -> TextAuditRecord:
    detected_at = _now_iso()
    robots_txt = _map_robots_txt_status(crawl_record)

    if crawl_record.is_robots_blocked:
        return TextAuditRecord(
            url=crawl_record.target_url,
            final_url=crawl_record.final_url,
            status_code=crawl_record.status_code,
            robots_txt="blocked",
            noindex_status="unknown",
            noindex_source="",
            noindex_value="",
            content_type=crawl_record.content_type,
            extracted_text="",
            extracted_chars=0,
            extraction_success=False,
            extraction_method="",
            included_in_analysis=False,
            excluded_reason="robots_blocked",
            detected_at=detected_at,
        )

    if crawl_record.error_kind is not None:
        return TextAuditRecord(
            url=crawl_record.target_url,
            final_url=crawl_record.final_url,
            status_code=crawl_record.status_code,
            robots_txt=robots_txt,
            noindex_status="unknown",
            noindex_source="",
            noindex_value="",
            content_type=crawl_record.content_type,
            extracted_text="",
            extracted_chars=0,
            extraction_success=False,
            extraction_method="",
            included_in_analysis=False,
            excluded_reason=_map_error_kind_to_excluded_reason(crawl_record.error_kind),
            detected_at=detected_at,
        )

    noindex_status = "not_noindex"
    noindex_source = ""
    noindex_value = ""
    if detect_noindex:
        noindex_detected = _detect_noindex(crawl_record)
        if noindex_detected is not None:
            noindex_status = "noindex"
            noindex_source = noindex_detected[0]
            noindex_value = noindex_detected[1]
    else:
        noindex_status = "unknown"

    if not _is_html_like(crawl_record.content_type, crawl_record.html):
        return TextAuditRecord(
            url=crawl_record.target_url,
            final_url=crawl_record.final_url,
            status_code=crawl_record.status_code,
            robots_txt=robots_txt,
            noindex_status=noindex_status,
            noindex_source=noindex_source,
            noindex_value=noindex_value,
            content_type=crawl_record.content_type,
            extracted_text="",
            extracted_chars=0,
            extraction_success=False,
            extraction_method="",
            included_in_analysis=False,
            excluded_reason="non_html",
            detected_at=detected_at,
        )

    extracted_text, extraction_method = _extract_text_from_html(
        crawl_record.html or "",
        min_text_length=min_text_length,
        logger=logger,
    )
    if include_title:
        title_text = _extract_title_from_html(crawl_record.html or "")
        extracted_text, title_added = _prepend_title_text(title_text, extracted_text)
        if title_added:
            extraction_method = f"{extraction_method}+title" if extraction_method else "title"
    extracted_chars = len(extracted_text)
    extraction_success = bool(extracted_text)

    excluded_reason = ""
    included_in_analysis = False
    if not extraction_success:
        excluded_reason = "extract_failed"
    elif extracted_chars < min_text_length:
        excluded_reason = "low_text"
    elif noindex_status == "noindex" and exclude_noindex_pages:
        excluded_reason = "noindex_excluded"
    else:
        included_in_analysis = True

    return TextAuditRecord(
        url=crawl_record.target_url,
        final_url=crawl_record.final_url,
        status_code=crawl_record.status_code,
        robots_txt=robots_txt,
        noindex_status=noindex_status,
        noindex_source=noindex_source,
        noindex_value=noindex_value,
        content_type=crawl_record.content_type,
        extracted_text=extracted_text,
        extracted_chars=extracted_chars,
        extraction_success=extraction_success,
        extraction_method=extraction_method,
        included_in_analysis=included_in_analysis,
        excluded_reason=excluded_reason,
        detected_at=detected_at,
    )


def _detect_noindex(crawl_record: FetchedPageResult) -> tuple[str, str] | None:
    header_match = _detect_noindex_from_headers(crawl_record.headers)
    if header_match is not None:
        return header_match

    if not crawl_record.html:
        return None

    try:
        soup = BeautifulSoup(crawl_record.html, "lxml")
    except Exception:
        return None

    for meta in soup.find_all("meta"):
        name = str(meta.get("name") or "").strip().lower()
        content = str(meta.get("content") or "").strip()
        if not content:
            continue
        lowered_content = content.lower()
        if "noindex" not in lowered_content:
            continue
        if name == "robots":
            return "meta_robots", content
        if name == "googlebot":
            return "meta_googlebot", content
    return None


def _detect_noindex_from_headers(headers: dict[str, str]) -> tuple[str, str] | None:
    for key, value in headers.items():
        if str(key).lower() == "x-robots-tag":
            header_value = str(value).strip()
            if "noindex" in header_value.lower():
                return "x_robots_tag", header_value
    return None


def _extract_title_from_html(html_text: str) -> str:
    if not html_text:
        return ""
    try:
        soup = BeautifulSoup(html_text, "lxml")
    except Exception:
        return ""

    title_tag = soup.find("title")
    if title_tag is None:
        return ""
    return _clean_extracted_text(title_tag.get_text(" ", strip=True))


def _prepend_title_text(title_text: str, body_text: str) -> tuple[str, bool]:
    cleaned_title = _clean_extracted_text(title_text)
    cleaned_body = _clean_extracted_text(body_text)
    if not cleaned_title:
        return cleaned_body, False
    if not cleaned_body:
        return cleaned_title, True

    first_line = cleaned_body.splitlines()[0].strip() if cleaned_body.splitlines() else cleaned_body[:120]
    if cleaned_title == first_line or cleaned_title in first_line:
        return cleaned_body, False

    return f"{cleaned_title}\n{cleaned_body}", True


def _extract_text_from_html(html_text: str, *, min_text_length: int, logger=None) -> tuple[str, str]:
    trafilatura_text = ""
    if trafilatura is not None:
        try:
            raw_text = trafilatura.extract(
                html_text,
                output_format="txt",
                include_comments=False,
                include_links=False,
                include_images=False,
                include_tables=False,
                favor_recall=True,
            )
        except Exception as exc:  # pragma: no cover
            if logger is not None:
                logger.warning(
                    "trafilatura による本文抽出に失敗しました。BeautifulSoup にフォールバックします: %s",
                    exc,
                )
            raw_text = None
        trafilatura_text = _clean_extracted_text(raw_text or "")
        if len(trafilatura_text) >= min_text_length:
            return trafilatura_text, "trafilatura"

    bs4_text = _extract_text_with_beautifulsoup(html_text)
    if len(bs4_text) > len(trafilatura_text):
        return bs4_text, "beautifulsoup" if bs4_text else ""
    if trafilatura_text:
        return trafilatura_text, "trafilatura"
    return "", ""


def _extract_text_with_beautifulsoup(html_text: str) -> str:
    try:
        soup = BeautifulSoup(html_text, "lxml")
    except Exception:
        return ""

    for tag_name in EXCLUDED_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    selected_text = ""
    for selector in FALLBACK_SELECTORS:
        candidates = soup.select(selector)
        if not candidates:
            continue
        texts = []
        for candidate in candidates:
            text = candidate.get_text("\n", strip=True)
            cleaned = _clean_extracted_text(text)
            if cleaned:
                texts.append(cleaned)
        if texts:
            selected_text = max(texts, key=len)
            break

    if not selected_text:
        body = soup.body or soup
        selected_text = _clean_extracted_text(body.get_text("\n", strip=True))

    return selected_text


def _clean_extracted_text(text: str) -> str:
    if not text:
        return ""
    text = html_lib.unescape(text)
    text = unicodedata.normalize("NFKC", text)
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = URL_PATTERN.sub(" ", text)
    text = EMAIL_PATTERN.sub(" ", text)
    text = SPACE_PATTERN.sub(" ", text)
    text = re.sub(r" *\n *", "\n", text)
    text = NEWLINE_PATTERN.sub("\n\n", text)
    return text.strip()


def _is_html_like(content_type: str, html_text: str | None) -> bool:
    lowered = (content_type or "").lower()
    if "text/html" in lowered or "application/xhtml+xml" in lowered:
        return True
    if not lowered and bool(html_text):
        return True
    return False


def _map_robots_txt_status(crawl_record: FetchedPageResult) -> str:
    if not crawl_record.robots_checked:
        return "not_checked"
    if crawl_record.robots_allowed is False:
        return "blocked"
    return "allowed"


def _map_error_kind_to_excluded_reason(error_kind: str) -> str:
    mapping = {
        "http_403": "status_403",
        "http_404": "status_404",
        "gone": "status_410",
        "http_429": "status_429",
        "http_5xx": "status_5xx",
        "timeout": "timeout",
        "ssl_error": "ssl_error",
        "connection_error": "connection_error",
        "redirect_error": "redirect_error",
        "request_exception": "request_error",
        "request_error": "request_error",
        "server_error": "status_5xx",
        "too_many_requests": "status_429",
        "forbidden": "status_403",
        "not_found": "status_404",
    }
    return mapping.get(error_kind, error_kind or "crawl_error")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")
