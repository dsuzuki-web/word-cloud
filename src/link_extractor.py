from __future__ import annotations

from typing import Iterable
from urllib.parse import urljoin

from bs4 import BeautifulSoup

from src.url_loader import normalize_url


class LinkExtractionError(Exception):
    """内部リンク抽出に関する例外。"""


def extract_links_from_html(html_text: str, base_url: str, *, exclude_pdf: bool = True) -> list[tuple[str, str]]:
    """HTML から a[href] を抽出し、絶対URL化・正規化して返す。"""
    if not html_text:
        return []

    try:
        soup = BeautifulSoup(html_text, "lxml")
    except Exception as exc:  # pragma: no cover
        raise LinkExtractionError(f"HTMLの解析に失敗しました: {exc}") from exc

    discovered: list[tuple[str, str]] = []
    seen_normalized_urls: set[str] = set()

    for anchor in soup.find_all("a", href=True):
        raw_href = str(anchor.get("href") or "").strip()
        if not raw_href:
            continue

        absolute_url = urljoin(base_url, raw_href)
        normalized_url, skip_reason = normalize_url(absolute_url, exclude_pdf=exclude_pdf)
        if skip_reason is not None:
            continue
        if not normalized_url or normalized_url in seen_normalized_urls:
            continue

        seen_normalized_urls.add(normalized_url)
        discovered.append((absolute_url, normalized_url))

    return discovered
