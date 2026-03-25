from __future__ import annotations

import random
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import requests

from src.config_loader import AppConfig
from src.robots_utils import RobotsCheckResult, RobotsTxtManager
from src.utils import write_dataframe_csv


class CrawlError(Exception):
    """クロール処理の共通例外。"""


@dataclass(slots=True)
class FetchedPageResult:
    """1URLごとの取得結果。"""

    target_url: str
    final_url: str
    status_code: int | None
    headers: dict[str, str]
    content_type: str
    html: str
    error_kind: str | None
    error_message: str | None
    robots_checked: bool
    robots_allowed: bool | None
    robots_decision: str
    robots_url: str | None
    fetched_at: str
    elapsed_sec: float
    redirect_happened: bool
    content_length: int | None

    @property
    def is_success(self) -> bool:
        return self.error_kind is None and self.robots_allowed is not False

    @property
    def is_robots_blocked(self) -> bool:
        return self.robots_allowed is False


@dataclass(slots=True)
class CrawlBatchResult:
    """一括クロール結果。"""

    page_results: list[FetchedPageResult]

    @property
    def success_results(self) -> list[FetchedPageResult]:
        return [result for result in self.page_results if result.is_success]

    @property
    def error_results(self) -> list[FetchedPageResult]:
        return [
            result
            for result in self.page_results
            if not result.is_success and not result.is_robots_blocked
        ]

    @property
    def robots_blocked_results(self) -> list[FetchedPageResult]:
        return [result for result in self.page_results if result.is_robots_blocked]

    @property
    def success_count(self) -> int:
        return len(self.success_results)

    @property
    def error_count(self) -> int:
        return len(self.error_results)

    @property
    def robots_blocked_count(self) -> int:
        return len(self.robots_blocked_results)

    def count_errors_by_kind(self, error_kind: str) -> int:
        return sum(1 for result in self.error_results if result.error_kind == error_kind)

    def to_success_dataframe(self) -> pd.DataFrame:
        records = [
            {
                "target_url": result.target_url,
                "final_url": result.final_url,
                "status_code": result.status_code,
                "content_type": result.content_type,
                "content_length": result.content_length,
                "elapsed_sec": result.elapsed_sec,
                "redirect_happened": result.redirect_happened,
                "fetched_at": result.fetched_at,
            }
            for result in self.success_results
        ]
        return pd.DataFrame(
            records,
            columns=[
                "target_url",
                "final_url",
                "status_code",
                "content_type",
                "content_length",
                "elapsed_sec",
                "redirect_happened",
                "fetched_at",
            ],
        )

    def to_error_dataframe(self) -> pd.DataFrame:
        records = [
            {
                "target_url": result.target_url,
                "final_url": result.final_url,
                "status_code": result.status_code,
                "error_kind": result.error_kind,
                "error_message": result.error_message,
                "fetched_at": result.fetched_at,
            }
            for result in self.error_results
        ]
        return pd.DataFrame(
            records,
            columns=[
                "target_url",
                "final_url",
                "status_code",
                "error_kind",
                "error_message",
                "fetched_at",
            ],
        )

    def to_robots_blocked_dataframe(self) -> pd.DataFrame:
        records = [
            {
                "url": result.target_url,
                "normalized_url": result.target_url,
                "robots_decision": result.robots_decision,
                "fetched_at": result.fetched_at,
            }
            for result in self.robots_blocked_results
        ]
        return pd.DataFrame(
            records,
            columns=["url", "normalized_url", "robots_decision", "fetched_at"],
        )


@dataclass(slots=True)
class CrawlReportPaths:
    crawl_success_path: Path
    crawl_errors_path: Path
    robots_blocked_path: Path


class Crawler:
    """main.py から扱いやすいクロール実行クラス。"""

    def __init__(self, *, config: AppConfig, logger=None) -> None:
        self.config = config
        self.logger = logger

    def crawl(self, target_source: pd.DataFrame | Path | str | Iterable[dict[str, Any]]) -> CrawlBatchResult:
        return crawl_targets(target_source, self.config, logger=self.logger)


def crawl_urls(
    target_source: pd.DataFrame | Path | str | Iterable[dict[str, Any]],
    config: AppConfig,
    logger=None,
) -> CrawlBatchResult:
    """既存呼び出しとの互換関数。"""
    return crawl_targets(target_source, config, logger=logger)


def crawl_targets(
    target_source: pd.DataFrame | Path | str | Iterable[dict[str, Any]],
    config: AppConfig,
    logger=None,
) -> CrawlBatchResult:
    """URL一覧を順番に取得する。"""
    target_records = _coerce_target_records(target_source)

    user_agent = str(config.get("user_agent", "SiteKeywordAnalyzer/1.0"))
    timeout = float(config.get("request_timeout_sec", 15))
    retry_count = max(0, int(config.get("retry_count", 2)))
    sleep_min = float(config.get("sleep_min_sec", 2.0))
    sleep_max = float(config.get("sleep_max_sec", 3.0))
    respect_robots = bool(config.get("respect_robots_txt", True))

    if sleep_max < sleep_min:
        sleep_min, sleep_max = sleep_max, sleep_min

    session = requests.Session()
    session.headers.update({"User-Agent": user_agent})

    robots_manager = None
    if respect_robots:
        robots_manager = RobotsTxtManager(
            session=session,
            user_agent=user_agent,
            timeout=timeout,
            logger=logger,
        )

    page_results: list[FetchedPageResult] = []
    robots_cache_initialized: set[str] = set()

    for record in target_records:
        target_url = str(
            record.get("normalized_url")
            or record.get("target_url")
            or record.get("url")
            or ""
        ).strip()
        if not target_url:
            continue

        prefetched_result = record.get("_prefetched_result") or record.get("prefetched_result")
        if isinstance(prefetched_result, FetchedPageResult):
            if logger is not None:
                logger.info("事前取得済みの結果を再利用します: %s", target_url)
            page_results.append(replace(prefetched_result, target_url=target_url))
            continue

        robots_check: RobotsCheckResult | None = None
        if robots_manager is not None:
            origin = _get_origin(target_url)
            if origin is not None and origin not in robots_cache_initialized:
                _sleep_before_request(sleep_min, sleep_max)
                robots_cache_initialized.add(origin)
            robots_check = robots_manager.check_url(target_url)
            if logger is not None:
                logger.info("robots.txt 判定: %s -> %s", target_url, robots_check.robots_decision)
            if not robots_check.allowed:
                page_results.append(
                    FetchedPageResult(
                        target_url=target_url,
                        final_url="",
                        status_code=None,
                        headers={},
                        content_type="",
                        html="",
                        error_kind=None,
                        error_message=robots_check.error_message,
                        robots_checked=True,
                        robots_allowed=False,
                        robots_decision=robots_check.robots_decision,
                        robots_url=robots_check.robots_url,
                        fetched_at=robots_check.fetched_at,
                        elapsed_sec=0.0,
                        redirect_happened=False,
                        content_length=None,
                    )
                )
                continue

        page_result = _fetch_single_url(
            session=session,
            target_url=target_url,
            timeout=timeout,
            retry_count=retry_count,
            sleep_min=sleep_min,
            sleep_max=sleep_max,
            logger=logger,
        )

        if robots_check is not None:
            page_result.robots_checked = True
            page_result.robots_allowed = True
            page_result.robots_decision = robots_check.robots_decision
            page_result.robots_url = robots_check.robots_url
        else:
            page_result.robots_checked = False
            page_result.robots_allowed = None
            page_result.robots_decision = "robots_not_checked"
            page_result.robots_url = None

        page_results.append(page_result)

    return CrawlBatchResult(page_results=page_results)


def save_crawl_reports(result: CrawlBatchResult, output_dir: Path) -> CrawlReportPaths:
    """成功・失敗・robots除外のCSVを保存する。"""
    crawl_success_path = write_dataframe_csv(result.to_success_dataframe(), output_dir / "crawl_success.csv")
    crawl_errors_path = write_dataframe_csv(result.to_error_dataframe(), output_dir / "crawl_errors.csv")
    robots_blocked_path = write_dataframe_csv(
        result.to_robots_blocked_dataframe(),
        output_dir / "robots_blocked_urls.csv",
    )
    return CrawlReportPaths(
        crawl_success_path=crawl_success_path,
        crawl_errors_path=crawl_errors_path,
        robots_blocked_path=robots_blocked_path,
    )


def _coerce_target_records(
    target_source: pd.DataFrame | Path | str | Iterable[dict[str, Any]],
) -> list[dict[str, Any]]:
    if isinstance(target_source, pd.DataFrame):
        return target_source.to_dict("records")

    if isinstance(target_source, (str, Path)):
        csv_path = Path(target_source)
        if not csv_path.exists():
            raise CrawlError(f"target_urls.csv が見つかりません: {csv_path}")
        try:
            dataframe = pd.read_csv(csv_path, dtype=str).fillna("")
        except Exception as exc:  # pragma: no cover
            raise CrawlError(f"target_urls.csv の読み込みに失敗しました: {exc}") from exc
        return dataframe.to_dict("records")

    try:
        return [dict(record) for record in target_source]
    except Exception as exc:  # pragma: no cover
        raise CrawlError(f"URL一覧の解釈に失敗しました: {exc}") from exc


def fetch_single_url(
    *,
    session: requests.Session,
    target_url: str,
    timeout: float,
    retry_count: int,
    sleep_min: float,
    sleep_max: float,
    logger=None,
) -> FetchedPageResult:
    """外部モジュールから再利用可能な単一URL取得関数。"""
    return _fetch_single_url(
        session=session,
        target_url=target_url,
        timeout=timeout,
        retry_count=retry_count,
        sleep_min=sleep_min,
        sleep_max=sleep_max,
        logger=logger,
    )


def _fetch_single_url(
    *,
    session: requests.Session,
    target_url: str,
    timeout: float,
    retry_count: int,
    sleep_min: float,
    sleep_max: float,
    logger=None,
) -> FetchedPageResult:
    started_at = time.monotonic()
    max_attempts = retry_count + 1

    for attempt in range(1, max_attempts + 1):
        _sleep_before_request(sleep_min, sleep_max)
        if logger is not None:
            logger.info("URL取得を開始します (%s/%s): %s", attempt, max_attempts, target_url)

        try:
            response = session.get(target_url, timeout=timeout, allow_redirects=True)
        except requests.exceptions.Timeout:
            if attempt < max_attempts:
                if logger is not None:
                    logger.warning("タイムアウトのため再試行します: %s", target_url)
                continue
            return _build_error_result(
                target_url=target_url,
                final_url="",
                status_code=None,
                error_kind="timeout",
                error_message="タイムアウトが発生しました",
                started_at=started_at,
            )
        except requests.exceptions.SSLError as exc:
            return _build_error_result(
                target_url=target_url,
                final_url="",
                status_code=None,
                error_kind="ssl_error",
                error_message=f"SSLエラーが発生しました: {exc}",
                started_at=started_at,
            )
        except requests.exceptions.TooManyRedirects as exc:
            final_url = _extract_response_url_from_exception(exc) or ""
            status_code = _extract_status_code_from_exception(exc)
            return _build_error_result(
                target_url=target_url,
                final_url=final_url,
                status_code=status_code,
                error_kind="redirect_error",
                error_message="リダイレクト回数が上限を超えました",
                started_at=started_at,
            )
        except requests.exceptions.ConnectionError as exc:
            if attempt < max_attempts:
                if logger is not None:
                    logger.warning("接続エラーのため再試行します: %s", target_url)
                continue
            return _build_error_result(
                target_url=target_url,
                final_url="",
                status_code=None,
                error_kind="connection_error",
                error_message=f"接続エラーが発生しました: {exc}",
                started_at=started_at,
            )
        except requests.RequestException as exc:
            final_url = _extract_response_url_from_exception(exc) or ""
            status_code = _extract_status_code_from_exception(exc)
            return _build_error_result(
                target_url=target_url,
                final_url=final_url,
                status_code=status_code,
                error_kind="request_error",
                error_message=f"HTTP取得中にエラーが発生しました: {exc}",
                started_at=started_at,
            )

        if response.status_code == 429:
            if attempt < max_attempts:
                retry_after_sec = _parse_retry_after_seconds(response.headers.get("Retry-After"))
                if retry_after_sec is not None:
                    time.sleep(retry_after_sec)
                if logger is not None:
                    logger.warning("429 のため再試行します: %s", target_url)
                continue
            return _build_error_result(
                target_url=target_url,
                final_url=response.url,
                status_code=response.status_code,
                error_kind="too_many_requests",
                error_message="リクエストが多すぎるため取得できませんでした",
                started_at=started_at,
            )

        if 500 <= response.status_code <= 599:
            if attempt < max_attempts:
                if logger is not None:
                    logger.warning("サーバーエラーのため再試行します: %s", target_url)
                continue
            return _build_error_result(
                target_url=target_url,
                final_url=response.url,
                status_code=response.status_code,
                error_kind="server_error",
                error_message=f"サーバーエラーが発生しました: HTTP {response.status_code}",
                started_at=started_at,
            )

        if response.status_code >= 400:
            error_kind, error_message = _map_http_error(response.status_code)
            return _build_error_result(
                target_url=target_url,
                final_url=response.url,
                status_code=response.status_code,
                error_kind=error_kind,
                error_message=error_message,
                started_at=started_at,
            )

        return _build_success_result(target_url=target_url, response=response, started_at=started_at)

    return _build_error_result(
        target_url=target_url,
        final_url="",
        status_code=None,
        error_kind="unknown_error",
        error_message="不明なエラーが発生しました",
        started_at=started_at,
    )


def _build_success_result(*, target_url: str, response: requests.Response, started_at: float) -> FetchedPageResult:
    content_type = response.headers.get("Content-Type", "")
    html = response.text if _is_html_response(content_type) else ""
    content_length = _get_content_length(response)
    return FetchedPageResult(
        target_url=target_url,
        final_url=response.url,
        status_code=response.status_code,
        headers={key: value for key, value in response.headers.items()},
        content_type=content_type,
        html=html,
        error_kind=None,
        error_message=None,
        robots_checked=False,
        robots_allowed=None,
        robots_decision="robots_not_checked",
        robots_url=None,
        fetched_at=datetime.now().isoformat(timespec="seconds"),
        elapsed_sec=round(time.monotonic() - started_at, 3),
        redirect_happened=bool(response.history) or response.url != target_url,
        content_length=content_length,
    )


def _build_error_result(
    *,
    target_url: str,
    final_url: str,
    status_code: int | None,
    error_kind: str,
    error_message: str,
    started_at: float,
) -> FetchedPageResult:
    return FetchedPageResult(
        target_url=target_url,
        final_url=final_url,
        status_code=status_code,
        headers={},
        content_type="",
        html="",
        error_kind=error_kind,
        error_message=error_message,
        robots_checked=False,
        robots_allowed=None,
        robots_decision="robots_not_checked",
        robots_url=None,
        fetched_at=datetime.now().isoformat(timespec="seconds"),
        elapsed_sec=round(time.monotonic() - started_at, 3),
        redirect_happened=bool(final_url and final_url != target_url),
        content_length=None,
    )


def _sleep_before_request(sleep_min: float, sleep_max: float) -> None:
    wait_seconds = random.uniform(sleep_min, sleep_max)
    if wait_seconds > 0:
        time.sleep(wait_seconds)


def _get_content_length(response: requests.Response) -> int | None:
    header_value = response.headers.get("Content-Length")
    if header_value is not None:
        try:
            return int(header_value)
        except (TypeError, ValueError):
            pass
    try:
        return len(response.content)
    except Exception:  # pragma: no cover
        return None


def _is_html_response(content_type: str) -> bool:
    lowered = content_type.lower()
    return "text/html" in lowered or "application/xhtml+xml" in lowered or lowered == ""


def _map_http_error(status_code: int) -> tuple[str, str]:
    if status_code == 403:
        return "forbidden", "アクセスが拒否されました"
    if status_code == 404:
        return "not_found", "ページが見つかりません"
    if status_code == 410:
        return "gone", "削除済みページ"
    return f"http_{status_code}", f"HTTP {status_code} エラーが発生しました"


def _extract_response_url_from_exception(exc: requests.RequestException) -> str | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    return getattr(response, "url", None)


def _extract_status_code_from_exception(exc: requests.RequestException) -> int | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    return getattr(response, "status_code", None)


def _parse_retry_after_seconds(value: str | None) -> float | None:
    if not value:
        return None

    stripped = value.strip()
    if not stripped:
        return None

    try:
        seconds = float(stripped)
        return max(0.0, seconds)
    except ValueError:
        pass

    try:
        retry_at = parsedate_to_datetime(stripped)
    except (TypeError, ValueError, IndexError, OverflowError):
        return None

    now = datetime.now(timezone.utc)
    if retry_at.tzinfo is None:
        retry_at = retry_at.replace(tzinfo=timezone.utc)
    delta = (retry_at - now).total_seconds()
    return max(0.0, delta)


def _get_origin(url: str) -> str | None:
    try:
        parsed = requests.utils.urlparse(url)
    except Exception:  # pragma: no cover
        return None
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"
