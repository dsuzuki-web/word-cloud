from __future__ import annotations

import gzip
import io
import random
import time
import xml.etree.ElementTree as ET
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests

try:  # pragma: no cover - 実行環境によっては未導入
    import tldextract
except ModuleNotFoundError:  # pragma: no cover
    tldextract = None

from src.config_loader import AppConfig
from src.crawler import FetchedPageResult, fetch_single_url
from src.link_extractor import LinkExtractionError, extract_links_from_html
from src.robots_utils import RobotsCheckResult, RobotsTxtManager
from src.url_loader import normalize_url

TARGET_URL_COLUMNS = ["original_url", "normalized_url", "source_type", "discovered_from"]
SITEMAP_SOURCE_COLUMNS = ["sitemap_url", "discovered_from", "fetch_status", "url_count"]
DISCOVERED_URL_COLUMNS = ["normalized_url", "source_type", "discovered_from", "depth"]
WP_API_SOURCE_COLUMNS = ["api_url", "discovered_from", "fetch_status", "url_count", "page_count"]

if tldextract is not None:  # pragma: no cover - 外部依存の有無で分岐
    _TLD_EXTRACTOR = tldextract.TLDExtract(suffix_list_urls=None)
else:  # pragma: no cover
    _TLD_EXTRACTOR = None


class UrlCollectionError(Exception):
    """domain モードの URL 収集エラー。"""


@dataclass(slots=True)
class DomainUrlCollectionResult:
    """domain モードの URL 収集結果。"""

    target_records: list[dict[str, Any]]
    sitemap_source_records: list[dict[str, str | int]]
    discovered_records: list[dict[str, Any]]
    prefetched_results: dict[str, FetchedPageResult]
    wp_api_source_records: list[dict[str, str | int]] = field(default_factory=list)
    fallback_used: bool = False
    fallback_message: str = ""

    @property
    def target_count(self) -> int:
        return len(self.target_records)

    @property
    def sitemap_source_count(self) -> int:
        return len(self.sitemap_source_records)

    @property
    def discovered_count(self) -> int:
        return len(self.discovered_records)

    @property
    def wp_api_source_count(self) -> int:
        return len(self.wp_api_source_records)

    @property
    def wp_api_target_count(self) -> int:
        return sum(1 for record in self.target_records if str(record.get("source_type", "")) == "wp_rest_api")

    @property
    def wp_api_used(self) -> bool:
        return self.wp_api_target_count > 0

    def to_target_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.target_records, columns=TARGET_URL_COLUMNS)

    def to_sitemap_sources_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.sitemap_source_records, columns=SITEMAP_SOURCE_COLUMNS)

    def to_discovered_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.discovered_records, columns=DISCOVERED_URL_COLUMNS)

    def to_wp_api_sources_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame(self.wp_api_source_records, columns=WP_API_SOURCE_COLUMNS)


@dataclass(slots=True)
class _DomainContext:
    domain_url: str
    start_normalized_url: str
    origin: str
    root_host: str
    root_registered_domain: str
    include_subdomains: bool
    exclude_pdf: bool
    exclude_patterns: list[str]
    max_pages: int
    max_depth: int
    timeout: float
    retry_count: int
    user_agent: str
    sleep_min: float
    sleep_max: float
    respect_robots_txt: bool
    prefer_wordpress_api: bool


@dataclass(slots=True)
class _SitemapFetchResult:
    sitemap_url: str
    discovered_from: str
    fetch_status: str
    locs: list[str]
    document_type: str


@dataclass(slots=True)
class _WordPressApiProbeResult:
    api_url: str
    discovered_from: str
    fetch_status: str
    post_urls: list[str]
    page_count: int

    @property
    def url_count(self) -> int:
        return len(self.post_urls)


@dataclass(slots=True)
class _QueueItem:
    normalized_url: str
    depth: int


def collect_urls_from_domain(config: AppConfig, logger=None) -> DomainUrlCollectionResult:
    """WordPress REST API 優先 + sitemap 優先 + BFS 補完で domain 配下の URL を収集する。"""
    context = _build_context(config)

    session = requests.Session()
    session.headers.update({"User-Agent": context.user_agent})

    target_map: dict[str, dict[str, Any]] = {}
    discovered_map: dict[str, dict[str, Any]] = {}
    sitemap_source_records: list[dict[str, str | int]] = []
    prefetched_results: dict[str, FetchedPageResult] = {}
    wp_api_source_records: list[dict[str, str | int]] = []

    if context.prefer_wordpress_api:
        _collect_urls_from_wordpress_api(
            session=session,
            context=context,
            target_map=target_map,
            discovered_map=discovered_map,
            wp_api_source_records=wp_api_source_records,
            logger=logger,
        )

    if len(target_map) < context.max_pages:
        initial_sitemaps = _discover_initial_sitemaps(session, context, logger=logger)
        _collect_urls_from_sitemaps(
            session=session,
            context=context,
            initial_sitemaps=initial_sitemaps,
            target_map=target_map,
            discovered_map=discovered_map,
            sitemap_source_records=sitemap_source_records,
            logger=logger,
        )

    fallback_used = False
    fallback_message = ""
    if len(target_map) < context.max_pages:
        if context.start_normalized_url not in target_map:
            added = _add_target_record(
                target_map=target_map,
                discovered_map=discovered_map,
                normalized_url=context.start_normalized_url,
                original_url=context.domain_url,
                source_type="fallback_root",
                discovered_from=context.domain_url,
                depth=0,
                max_pages=context.max_pages,
            )
            if added:
                fallback_used = True
                if wp_api_source_records and any(str(record.get("fetch_status", "")).startswith("ok") for record in wp_api_source_records):
                    fallback_message = "WordPress API / sitemap から十分なURLを取得できなかったため、開始URLから内部リンク探索を行います"
                else:
                    fallback_message = "sitemap から十分なURLを取得できなかったため、開始URLから内部リンク探索を行います"
        _supplement_urls_with_bfs(
            session=session,
            context=context,
            target_map=target_map,
            discovered_map=discovered_map,
            prefetched_results=prefetched_results,
            logger=logger,
        )

    for normalized_url, prefetched_result in prefetched_results.items():
        if normalized_url in target_map:
            target_map[normalized_url]["_prefetched_result"] = prefetched_result

    return DomainUrlCollectionResult(
        target_records=list(target_map.values()),
        sitemap_source_records=sitemap_source_records,
        discovered_records=list(discovered_map.values()),
        prefetched_results=prefetched_results,
        wp_api_source_records=wp_api_source_records,
        fallback_used=fallback_used,
        fallback_message=fallback_message,
    )


def save_domain_collection_results(result: DomainUrlCollectionResult, output_dir) -> tuple:
    """domain モードの収集結果を CSV に保存する。"""
    target_path = output_dir / "target_urls.csv"
    sitemap_sources_path = output_dir / "sitemap_sources.csv"
    discovered_urls_path = output_dir / "discovered_urls.csv"

    result.to_target_dataframe().to_csv(target_path, index=False, encoding="utf-8-sig")
    result.to_sitemap_sources_dataframe().to_csv(sitemap_sources_path, index=False, encoding="utf-8-sig")
    result.to_discovered_dataframe().to_csv(discovered_urls_path, index=False, encoding="utf-8-sig")
    return target_path, sitemap_sources_path, discovered_urls_path


def _build_context(config: AppConfig) -> _DomainContext:
    domain_url = str(config.get("domain_url", "")).strip()
    same_domain_only_value = config.get("same_domain_only", None)
    if same_domain_only_value is None:
        include_subdomains = bool(config.get("include_subdomains", False))
    else:
        include_subdomains = not bool(same_domain_only_value)
    exclude_pdf = bool(config.get("exclude_pdf", True))

    normalized_domain_url, skip_reason = normalize_url(domain_url, exclude_pdf=exclude_pdf)
    if skip_reason is not None:
        raise UrlCollectionError(f"domain_url の形式が不正です: {domain_url}")

    parsed_root = urlparse(normalized_domain_url)
    origin = f"{parsed_root.scheme}://{parsed_root.netloc}"
    root_host = (parsed_root.hostname or "").lower()

    sleep_min = float(config.get("sleep_min_sec", 2.0))
    sleep_max = float(config.get("sleep_max_sec", 3.0))
    if sleep_max < sleep_min:
        sleep_min, sleep_max = sleep_max, sleep_min

    return _DomainContext(
        domain_url=domain_url,
        start_normalized_url=normalized_domain_url,
        origin=origin,
        root_host=root_host,
        root_registered_domain=_get_registered_domain(root_host),
        include_subdomains=include_subdomains,
        exclude_pdf=exclude_pdf,
        exclude_patterns=_coerce_exclude_patterns(config.get("exclude_url_patterns", [])),
        max_pages=max(1, int(config.get("max_pages", 200))),
        max_depth=max(0, int(config.get("max_depth", 3))),
        timeout=float(config.get("request_timeout_sec", 15)),
        retry_count=max(0, int(config.get("retry_count", 2))),
        user_agent=str(config.get("user_agent", "SiteKeywordAnalyzer/1.0")),
        sleep_min=sleep_min,
        sleep_max=sleep_max,
        respect_robots_txt=bool(config.get("respect_robots_txt", True)),
        prefer_wordpress_api=bool(config.get("prefer_wordpress_api", True)),
    )


def _collect_urls_from_wordpress_api(
    *,
    session: requests.Session,
    context: _DomainContext,
    target_map: dict[str, dict[str, Any]],
    discovered_map: dict[str, dict[str, Any]],
    wp_api_source_records: list[dict[str, str | int]],
    logger=None,
) -> None:
    candidates = _discover_wordpress_api_candidates(context)
    if logger is not None and candidates:
        logger.info("WordPress REST API の候補を確認します: %s", ", ".join(api_url for api_url, _ in candidates))

    for api_url, discovered_from in candidates:
        if len(target_map) >= context.max_pages:
            break

        probe_result = _fetch_wordpress_posts_api(
            session=session,
            api_url=api_url,
            discovered_from=discovered_from,
            max_urls=context.max_pages,
            timeout=context.timeout,
            sleep_min=context.sleep_min,
            sleep_max=context.sleep_max,
            logger=logger,
        )
        wp_api_source_records.append(
            {
                "api_url": probe_result.api_url,
                "discovered_from": probe_result.discovered_from,
                "fetch_status": probe_result.fetch_status,
                "url_count": probe_result.url_count,
                "page_count": probe_result.page_count,
            }
        )

        if not probe_result.fetch_status.startswith("ok"):
            continue

        added_count = 0
        for raw_url in probe_result.post_urls:
            candidate_url = urljoin(context.origin + "/", raw_url.strip())
            normalized_url, skip_reason = normalize_url(candidate_url, exclude_pdf=context.exclude_pdf)
            if skip_reason is not None:
                continue
            if _find_matched_exclude_pattern(normalized_url, context.exclude_patterns) is not None:
                continue
            if not _is_allowed_target_url(normalized_url, context):
                continue

            added = _add_target_record(
                target_map=target_map,
                discovered_map=discovered_map,
                normalized_url=normalized_url,
                original_url=candidate_url,
                source_type="wp_rest_api",
                discovered_from=probe_result.api_url,
                depth=0,
                max_pages=context.max_pages,
            )
            if added:
                added_count += 1
                if logger is not None:
                    logger.info("WordPress REST API でURLを追加しました: %s", normalized_url)
            if len(target_map) >= context.max_pages:
                break

        if logger is not None and probe_result.fetch_status.startswith("ok"):
            logger.info(
                "WordPress REST API の確認結果: %s (取得=%s件, 追加=%s件)",
                probe_result.api_url,
                probe_result.url_count,
                added_count,
            )

        if probe_result.url_count > 0:
            break


def _discover_wordpress_api_candidates(context: _DomainContext) -> list[tuple[str, str]]:
    parsed = urlparse(context.start_normalized_url)
    path_segments = [segment for segment in parsed.path.split("/") if segment]

    base_prefixes: list[str] = [""]
    current_segments: list[str] = []
    for segment in path_segments[:3]:
        current_segments.append(segment)
        base_prefixes.append("/" + "/".join(current_segments))

    candidates: list[tuple[str, str]] = []
    seen: set[str] = set()
    for base_prefix in base_prefixes:
        base_url = _compose_site_url(context.origin, base_prefix)
        endpoint_candidates = [
            (_compose_site_url(base_url, "/wp-json/wp/v2/posts"), f"base:{base_prefix or '/'}"),
            (f"{_compose_site_url(base_url, '/index.php')}?rest_route=/wp/v2/posts", f"base:{base_prefix or '/'}:rest_route"),
        ]
        for api_url, discovered_from in endpoint_candidates:
            cleaned = api_url.strip()
            if not cleaned or cleaned in seen:
                continue
            seen.add(cleaned)
            candidates.append((cleaned, discovered_from))
    return candidates


def _compose_site_url(base_url: str, path: str) -> str:
    base = base_url.rstrip("/")
    normalized_path = path.strip()
    if not normalized_path:
        return base
    if normalized_path.startswith("?"):
        return f"{base}{normalized_path}"
    return f"{base}/{normalized_path.lstrip('/')}"


def _fetch_wordpress_posts_api(
    *,
    session: requests.Session,
    api_url: str,
    discovered_from: str,
    max_urls: int,
    timeout: float,
    sleep_min: float,
    sleep_max: float,
    logger=None,
) -> _WordPressApiProbeResult:
    post_urls: list[str] = []
    page_count = 0
    page = 1

    while len(post_urls) < max_urls:
        remaining = max_urls - len(post_urls)
        per_page = min(100, max(1, remaining))
        params = {"per_page": per_page, "page": page, "_fields": "link"}

        try:
            _sleep_before_request(sleep_min, sleep_max)
            response = session.get(api_url, params=params, timeout=timeout, allow_redirects=True)
        except requests.RequestException as exc:
            if logger is not None:
                logger.info("WordPress REST API の取得に失敗しました: %s (%s)", api_url, exc)
            return _WordPressApiProbeResult(
                api_url=api_url,
                discovered_from=discovered_from,
                fetch_status="request_error",
                post_urls=[],
                page_count=page_count,
            )

        if response.status_code >= 400:
            error_code = _extract_wp_error_code(response)
            if page > 1 and response.status_code == 400 and error_code == "rest_post_invalid_page_number":
                break
            status = f"rest_error:{error_code}" if error_code else f"http_{response.status_code}"
            return _WordPressApiProbeResult(
                api_url=api_url,
                discovered_from=discovered_from,
                fetch_status=status,
                post_urls=post_urls,
                page_count=page_count,
            )

        try:
            payload = response.json()
        except ValueError:
            return _WordPressApiProbeResult(
                api_url=api_url,
                discovered_from=discovered_from,
                fetch_status="non_json_response",
                post_urls=post_urls,
                page_count=page_count,
            )

        if isinstance(payload, dict):
            error_code = str(payload.get("code", "")).strip()
            if page > 1 and error_code == "rest_post_invalid_page_number":
                break
            status = f"rest_error:{error_code}" if error_code else "unexpected_json_object"
            return _WordPressApiProbeResult(
                api_url=api_url,
                discovered_from=discovered_from,
                fetch_status=status,
                post_urls=post_urls,
                page_count=page_count,
            )

        if not isinstance(payload, list):
            return _WordPressApiProbeResult(
                api_url=api_url,
                discovered_from=discovered_from,
                fetch_status="unexpected_json_type",
                post_urls=post_urls,
                page_count=page_count,
            )

        if not _looks_like_wordpress_posts_response(response, payload):
            return _WordPressApiProbeResult(
                api_url=api_url,
                discovered_from=discovered_from,
                fetch_status="non_wordpress_response",
                post_urls=post_urls,
                page_count=page_count,
            )

        page_count += 1
        page_links = [str(item.get("link", "")).strip() for item in payload if isinstance(item, dict) and str(item.get("link", "")).strip()]
        post_urls.extend(page_links)

        total_pages = _coerce_positive_int(response.headers.get("X-WP-TotalPages"))
        if not payload:
            break
        if total_pages is not None and page >= total_pages:
            break
        if len(payload) < per_page:
            break
        page += 1

    fetch_status = "ok_posts" if post_urls else "ok_empty"
    return _WordPressApiProbeResult(
        api_url=api_url,
        discovered_from=discovered_from,
        fetch_status=fetch_status,
        post_urls=post_urls[:max_urls],
        page_count=page_count,
    )


def _extract_wp_error_code(response: requests.Response) -> str | None:
    try:
        payload = response.json()
    except ValueError:
        return None
    if isinstance(payload, dict):
        code = str(payload.get("code", "")).strip()
        return code or None
    return None


def _looks_like_wordpress_posts_response(response: requests.Response, payload: list[Any]) -> bool:
    if any(header_name.lower().startswith("x-wp-") for header_name in response.headers):
        return True
    first_item = next((item for item in payload if isinstance(item, dict)), None)
    if first_item is None:
        return False
    return "link" in first_item or "id" in first_item or "slug" in first_item


def _coerce_positive_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _collect_urls_from_sitemaps(
    *,
    session: requests.Session,
    context: _DomainContext,
    initial_sitemaps: list[tuple[str, str]],
    target_map: dict[str, dict[str, Any]],
    discovered_map: dict[str, dict[str, Any]],
    sitemap_source_records: list[dict[str, str | int]],
    logger=None,
) -> None:
    seen_sitemap_urls: set[str] = set()
    queue = deque(initial_sitemaps)

    while queue and len(target_map) < context.max_pages:
        sitemap_url, discovered_from = queue.popleft()
        sitemap_url = sitemap_url.strip()
        if not sitemap_url or sitemap_url in seen_sitemap_urls:
            continue
        seen_sitemap_urls.add(sitemap_url)

        fetch_result = _fetch_sitemap_document(
            session=session,
            sitemap_url=sitemap_url,
            discovered_from=discovered_from,
            timeout=context.timeout,
            sleep_min=context.sleep_min,
            sleep_max=context.sleep_max,
            logger=logger,
        )
        sitemap_source_records.append(
            {
                "sitemap_url": fetch_result.sitemap_url,
                "discovered_from": fetch_result.discovered_from,
                "fetch_status": fetch_result.fetch_status,
                "url_count": len(fetch_result.locs),
            }
        )

        if fetch_result.fetch_status.startswith("ok_sitemapindex"):
            for loc in fetch_result.locs:
                nested_url = urljoin(fetch_result.sitemap_url, loc.strip())
                if nested_url and nested_url not in seen_sitemap_urls:
                    queue.append((nested_url, fetch_result.sitemap_url))
            continue

        if not fetch_result.fetch_status.startswith("ok_urlset"):
            continue

        for raw_url in fetch_result.locs:
            candidate_url = urljoin(fetch_result.sitemap_url, raw_url.strip())
            normalized_url, skip_reason = normalize_url(candidate_url, exclude_pdf=context.exclude_pdf)
            if skip_reason is not None:
                continue
            if _find_matched_exclude_pattern(normalized_url, context.exclude_patterns) is not None:
                continue
            if not _is_allowed_target_url(normalized_url, context):
                continue
            added = _add_target_record(
                target_map=target_map,
                discovered_map=discovered_map,
                normalized_url=normalized_url,
                original_url=candidate_url,
                source_type="sitemap",
                discovered_from=fetch_result.sitemap_url,
                depth=0,
                max_pages=context.max_pages,
            )
            if len(target_map) >= context.max_pages:
                break
            if added and logger is not None:
                logger.info("sitemap でURLを追加しました: %s", normalized_url)


def _supplement_urls_with_bfs(
    *,
    session: requests.Session,
    context: _DomainContext,
    target_map: dict[str, dict[str, Any]],
    discovered_map: dict[str, dict[str, Any]],
    prefetched_results: dict[str, FetchedPageResult],
    logger=None,
) -> None:
    if context.max_depth < 0 or not target_map:
        return

    robots_manager = None
    robots_cache_initialized: set[str] = set()
    if context.respect_robots_txt:
        robots_manager = RobotsTxtManager(
            session=session,
            user_agent=context.user_agent,
            timeout=context.timeout,
            logger=logger,
        )

    seed_items = _build_bfs_seed_items(target_map, context.start_normalized_url)
    queue = deque(seed_items)
    queued_urls = {item.normalized_url for item in seed_items}
    visited_urls: set[str] = set()

    while queue and len(target_map) < context.max_pages:
        item = queue.popleft()
        queued_urls.discard(item.normalized_url)
        current_url = item.normalized_url
        current_depth = item.depth

        if current_depth > context.max_depth:
            continue
        if current_url in visited_urls:
            continue
        visited_urls.add(current_url)

        robots_check: RobotsCheckResult | None = None
        if robots_manager is not None:
            origin = _get_origin(current_url)
            if origin is not None and origin not in robots_cache_initialized:
                _sleep_before_request(context.sleep_min, context.sleep_max)
                robots_cache_initialized.add(origin)
            robots_check = robots_manager.check_url(current_url)
            if logger is not None:
                logger.info("BFS robots.txt 判定: %s -> %s", current_url, robots_check.robots_decision)
            if not robots_check.allowed:
                _remove_target_record(current_url, target_map=target_map, discovered_map=discovered_map)
                prefetched_results[current_url] = _build_robots_blocked_result(current_url, robots_check)
                continue

        fetch_result = fetch_single_url(
            session=session,
            target_url=current_url,
            timeout=context.timeout,
            retry_count=context.retry_count,
            sleep_min=context.sleep_min,
            sleep_max=context.sleep_max,
            logger=logger,
        )
        if robots_check is not None:
            fetch_result.robots_checked = True
            fetch_result.robots_allowed = True
            fetch_result.robots_decision = robots_check.robots_decision
            fetch_result.robots_url = robots_check.robots_url
        prefetched_results[current_url] = fetch_result

        if current_url in target_map:
            target_map[current_url]["_prefetched_result"] = fetch_result

        if not fetch_result.is_success:
            continue
        if not _is_html_like(fetch_result.content_type, fetch_result.html):
            continue
        if current_depth >= context.max_depth:
            continue

        try:
            extracted_links = extract_links_from_html(
                fetch_result.html,
                fetch_result.final_url or current_url,
                exclude_pdf=context.exclude_pdf,
            )
        except LinkExtractionError as exc:
            if logger is not None:
                logger.warning("内部リンク抽出に失敗しました: %s (%s)", current_url, exc)
            continue

        next_depth = current_depth + 1
        for absolute_url, normalized_url in extracted_links:
            if _find_matched_exclude_pattern(normalized_url, context.exclude_patterns) is not None:
                continue
            if not _is_allowed_target_url(normalized_url, context):
                continue
            if normalized_url in discovered_map:
                continue

            _add_discovered_record(
                discovered_map=discovered_map,
                normalized_url=normalized_url,
                source_type="bfs",
                discovered_from=current_url,
                depth=next_depth,
            )

            if robots_manager is not None:
                child_robots_check = robots_manager.check_url(normalized_url)
                if not child_robots_check.allowed:
                    if logger is not None:
                        logger.info("BFS で robots.txt により除外しました: %s", normalized_url)
                    prefetched_results.setdefault(normalized_url, _build_robots_blocked_result(normalized_url, child_robots_check))
                    continue

            if len(target_map) >= context.max_pages:
                break

            added = _add_target_record(
                target_map=target_map,
                discovered_map=discovered_map,
                normalized_url=normalized_url,
                original_url=absolute_url,
                source_type="bfs",
                discovered_from=current_url,
                depth=next_depth,
                max_pages=context.max_pages,
            )
            if not added:
                continue
            if logger is not None:
                logger.info("BFS でURLを追加しました: %s (depth=%s)", normalized_url, next_depth)
            if next_depth <= context.max_depth and normalized_url not in queued_urls:
                queue.append(_QueueItem(normalized_url=normalized_url, depth=next_depth))
                queued_urls.add(normalized_url)


def _build_bfs_seed_items(target_map: dict[str, dict[str, Any]], start_normalized_url: str) -> list[_QueueItem]:
    seed_items: list[_QueueItem] = []
    seen: set[str] = set()

    if start_normalized_url in target_map:
        seed_items.append(_QueueItem(normalized_url=start_normalized_url, depth=0))
        seen.add(start_normalized_url)

    for normalized_url, record in target_map.items():
        if normalized_url in seen:
            continue
        depth = int(record.get("_depth", 0) or 0)
        seed_items.append(_QueueItem(normalized_url=normalized_url, depth=depth))
        seen.add(normalized_url)

    return seed_items


def _add_target_record(
    *,
    target_map: dict[str, dict[str, Any]],
    discovered_map: dict[str, dict[str, Any]],
    normalized_url: str,
    original_url: str,
    source_type: str,
    discovered_from: str,
    depth: int,
    max_pages: int,
) -> bool:
    _add_discovered_record(
        discovered_map=discovered_map,
        normalized_url=normalized_url,
        source_type=source_type,
        discovered_from=discovered_from,
        depth=depth,
    )
    if normalized_url in target_map:
        return False
    if len(target_map) >= max_pages:
        return False

    target_map[normalized_url] = {
        "original_url": original_url,
        "normalized_url": normalized_url,
        "source_type": source_type,
        "discovered_from": discovered_from,
        "_depth": depth,
    }
    return True


def _add_discovered_record(
    *,
    discovered_map: dict[str, dict[str, Any]],
    normalized_url: str,
    source_type: str,
    discovered_from: str,
    depth: int,
) -> None:
    if normalized_url in discovered_map:
        return
    discovered_map[normalized_url] = {
        "normalized_url": normalized_url,
        "source_type": source_type,
        "discovered_from": discovered_from,
        "depth": depth,
    }


def _remove_target_record(
    normalized_url: str,
    *,
    target_map: dict[str, dict[str, Any]],
    discovered_map: dict[str, dict[str, Any]],
) -> None:
    target_map.pop(normalized_url, None)



def _discover_initial_sitemaps(session: requests.Session, context: _DomainContext, logger=None) -> list[tuple[str, str]]:
    robots_url = urljoin(context.origin + "/", "robots.txt")
    candidates: list[tuple[str, str]] = []

    try:
        _sleep_before_request(context.sleep_min, context.sleep_max)
        response = session.get(robots_url, timeout=context.timeout, allow_redirects=True)
    except requests.RequestException as exc:
        if logger is not None:
            logger.warning("robots.txt の取得に失敗したため、既定の sitemap パスを試します: %s", exc)
        response = None

    if response is not None and response.status_code < 400:
        for line in response.text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.lower().startswith("sitemap:"):
                sitemap_value = stripped.split(":", 1)[1].strip()
                if sitemap_value:
                    candidates.append((urljoin(response.url, sitemap_value), "robots_txt"))

    if not candidates:
        candidates.extend(
            [
                (urljoin(context.origin + "/", "sitemap.xml"), "default_guess"),
                (urljoin(context.origin + "/", "sitemap_index.xml"), "default_guess"),
            ]
        )

    deduped: list[tuple[str, str]] = []
    seen: set[str] = set()
    for sitemap_url, discovered_from in candidates:
        cleaned = sitemap_url.strip()
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append((cleaned, discovered_from))
    return deduped



def _fetch_sitemap_document(
    *,
    session: requests.Session,
    sitemap_url: str,
    discovered_from: str,
    timeout: float,
    sleep_min: float,
    sleep_max: float,
    logger=None,
) -> _SitemapFetchResult:
    try:
        _sleep_before_request(sleep_min, sleep_max)
        response = session.get(sitemap_url, timeout=timeout, allow_redirects=True)
    except requests.RequestException as exc:
        if logger is not None:
            logger.warning("sitemap の取得に失敗しました: %s (%s)", sitemap_url, exc)
        return _SitemapFetchResult(
            sitemap_url=sitemap_url,
            discovered_from=discovered_from,
            fetch_status="request_error",
            locs=[],
            document_type="",
        )

    if response.status_code >= 400:
        return _SitemapFetchResult(
            sitemap_url=response.url,
            discovered_from=discovered_from,
            fetch_status=f"http_{response.status_code}",
            locs=[],
            document_type="",
        )

    try:
        xml_text = _decode_sitemap_response(response)
        document_type, locs = _parse_sitemap_xml(xml_text)
    except UrlCollectionError:
        if logger is not None:
            logger.warning("sitemap XML の解析に失敗しました: %s", response.url)
        return _SitemapFetchResult(
            sitemap_url=response.url,
            discovered_from=discovered_from,
            fetch_status="parse_error",
            locs=[],
            document_type="",
        )

    return _SitemapFetchResult(
        sitemap_url=response.url,
        discovered_from=discovered_from,
        fetch_status=f"ok_{document_type}",
        locs=locs,
        document_type=document_type,
    )



def _decode_sitemap_response(response: requests.Response) -> str:
    content = response.content
    content_type = response.headers.get("Content-Type", "").lower()
    url_lower = response.url.lower()

    if url_lower.endswith(".gz") or "application/gzip" in content_type or "application/x-gzip" in content_type:
        try:
            content = gzip.decompress(content)
        except OSError as exc:
            raise UrlCollectionError(f"gzip sitemap の展開に失敗しました: {response.url}") from exc

    encoding = response.encoding or response.apparent_encoding or "utf-8"
    try:
        return content.decode(encoding, errors="replace")
    except LookupError:
        return content.decode("utf-8", errors="replace")



def _parse_sitemap_xml(xml_text: str) -> tuple[str, list[str]]:
    if not xml_text.strip():
        raise UrlCollectionError("空の sitemap です")

    try:
        tree = ET.parse(io.StringIO(xml_text))
    except ET.ParseError as exc:
        raise UrlCollectionError("sitemap XML を解析できません") from exc

    root = tree.getroot()
    root_name = _local_name(root.tag)

    if root_name == "urlset":
        locs = [elem.text.strip() for elem in root.findall(".//{*}url/{*}loc") if elem.text and elem.text.strip()]
        return "urlset", locs

    if root_name == "sitemapindex":
        locs = [elem.text.strip() for elem in root.findall(".//{*}sitemap/{*}loc") if elem.text and elem.text.strip()]
        return "sitemapindex", locs

    url_locs = [elem.text.strip() for elem in root.findall(".//{*}url/{*}loc") if elem.text and elem.text.strip()]
    if url_locs:
        return "urlset", url_locs

    sitemap_locs = [elem.text.strip() for elem in root.findall(".//{*}sitemap/{*}loc") if elem.text and elem.text.strip()]
    if sitemap_locs:
        return "sitemapindex", sitemap_locs

    raise UrlCollectionError("対応していない sitemap 形式です")



def _local_name(tag_name: str) -> str:
    if tag_name.startswith("{") and "}" in tag_name:
        return tag_name.split("}", 1)[1]
    return tag_name



def _coerce_exclude_patterns(value) -> list[str]:
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    return []



def _find_matched_exclude_pattern(normalized_url: str, patterns: list[str]) -> str | None:
    for pattern in patterns:
        if pattern in normalized_url:
            return pattern
    return None



def _get_registered_domain(host: str) -> str:
    normalized_host = (host or "").strip().lower().rstrip(".")
    if not normalized_host:
        return ""

    if _TLD_EXTRACTOR is not None:
        extracted = _TLD_EXTRACTOR(normalized_host)
        registered_domain = getattr(extracted, "registered_domain", "") or ""
        if registered_domain:
            return registered_domain
        domain = getattr(extracted, "domain", "") or ""
        suffix = getattr(extracted, "suffix", "") or ""
        if domain and suffix:
            return f"{domain}.{suffix}"

    return _fallback_registered_domain(normalized_host)



def _fallback_registered_domain(host: str) -> str:
    if not host:
        return ""
    if host.replace(".", "").isdigit():
        return host
    labels = [label for label in host.split(".") if label]
    if len(labels) <= 2:
        return host

    common_second_level_suffixes = {
        "ac.uk",
        "co.jp",
        "co.uk",
        "com.au",
        "com.br",
        "com.cn",
        "com.hk",
        "com.sg",
        "com.tr",
        "edu.au",
        "go.jp",
        "gov.uk",
        "ne.jp",
        "or.jp",
    }
    suffix_pair = ".".join(labels[-2:])
    suffix_triplet = ".".join(labels[-3:])
    if suffix_pair in common_second_level_suffixes and len(labels) >= 3:
        return suffix_triplet
    return ".".join(labels[-2:])



def _is_allowed_target_url(url: str, context: _DomainContext) -> bool:
    parsed = urlparse(url)
    host = (parsed.hostname or "").lower()
    if not host:
        return False

    if not context.include_subdomains:
        return host == context.root_host

    if context.root_registered_domain:
        return _get_registered_domain(host) == context.root_registered_domain

    return host == context.root_host or host.endswith(f".{context.root_host}")



def _build_robots_blocked_result(target_url: str, robots_check: RobotsCheckResult) -> FetchedPageResult:
    return FetchedPageResult(
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



def _get_origin(url: str) -> str | None:
    parsed = urlparse(url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return None
    return f"{parsed.scheme}://{parsed.netloc}"



def _sleep_before_request(sleep_min: float, sleep_max: float) -> None:
    wait_seconds = random.uniform(sleep_min, sleep_max)
    if wait_seconds > 0:
        time.sleep(wait_seconds)



def _is_html_like(content_type: str, html_text: str | None) -> bool:
    lowered = (content_type or "").lower()
    if "text/html" in lowered or "application/xhtml+xml" in lowered:
        return True
    if not lowered and bool(html_text):
        return True
    return False
