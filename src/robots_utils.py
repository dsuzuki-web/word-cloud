from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

import requests


@dataclass(slots=True)
class RobotsCheckResult:
    """robots.txt 判定結果。"""

    url: str
    normalized_url: str
    allowed: bool
    robots_decision: str
    fetched_at: str
    robots_url: str | None = None
    source_status: int | None = None
    error_message: str | None = None


@dataclass(slots=True)
class _RobotsPolicy:
    """ホストごとの robots.txt ポリシー。"""

    parser: RobotFileParser | None
    robots_url: str
    source_status: int | None
    allow_all: bool = False
    disallow_all: bool = False
    error_message: str | None = None


class RobotsTxtManager:
    """robots.txt をホスト単位で取得・キャッシュして判定する。"""

    def __init__(
        self,
        *,
        session: requests.Session,
        user_agent: str,
        timeout: float,
        logger=None,
    ) -> None:
        self.session = session
        self.user_agent = user_agent
        self.timeout = timeout
        self.logger = logger
        self._cache: dict[str, _RobotsPolicy] = {}

    def check_url(self, url: str) -> RobotsCheckResult:
        normalized_url = url.strip()
        fetched_at = datetime.now().isoformat(timespec="seconds")
        origin = self._get_origin(normalized_url)
        if origin is None:
            return RobotsCheckResult(
                url=url,
                normalized_url=normalized_url,
                allowed=True,
                robots_decision="robots_not_checked_invalid_url",
                fetched_at=fetched_at,
                error_message="robots.txt 判定対象外のURLです",
            )

        policy = self._cache.get(origin)
        if policy is None:
            policy = self._fetch_policy(origin)
            self._cache[origin] = policy

        if policy.disallow_all:
            return RobotsCheckResult(
                url=url,
                normalized_url=normalized_url,
                allowed=False,
                robots_decision="blocked_by_robots_txt",
                fetched_at=fetched_at,
                robots_url=policy.robots_url,
                source_status=policy.source_status,
                error_message=policy.error_message,
            )

        if policy.allow_all or policy.parser is None:
            return RobotsCheckResult(
                url=url,
                normalized_url=normalized_url,
                allowed=True,
                robots_decision="allowed_by_robots_txt",
                fetched_at=fetched_at,
                robots_url=policy.robots_url,
                source_status=policy.source_status,
                error_message=policy.error_message,
            )

        allowed = policy.parser.can_fetch(self.user_agent, normalized_url)
        return RobotsCheckResult(
            url=url,
            normalized_url=normalized_url,
            allowed=allowed,
            robots_decision="allowed_by_robots_txt" if allowed else "blocked_by_robots_txt",
            fetched_at=fetched_at,
            robots_url=policy.robots_url,
            source_status=policy.source_status,
            error_message=policy.error_message,
        )

    def _fetch_policy(self, origin: str) -> _RobotsPolicy:
        robots_url = f"{origin}/robots.txt"
        try:
            response = self.session.get(
                robots_url,
                headers={"User-Agent": self.user_agent},
                timeout=self.timeout,
                allow_redirects=True,
            )
        except requests.RequestException as exc:
            if self.logger is not None:
                self.logger.warning("robots.txt の取得に失敗しました: %s (%s)", robots_url, exc)
            return _RobotsPolicy(
                parser=None,
                robots_url=robots_url,
                source_status=None,
                allow_all=True,
                error_message=f"robots.txt の取得に失敗しました: {exc}",
            )

        if response.status_code == 404:
            return _RobotsPolicy(
                parser=None,
                robots_url=response.url,
                source_status=response.status_code,
                allow_all=True,
            )

        if response.status_code == 403:
            return _RobotsPolicy(
                parser=None,
                robots_url=response.url,
                source_status=response.status_code,
                disallow_all=True,
                error_message="robots.txt へのアクセスが拒否されました",
            )

        if response.status_code >= 400:
            return _RobotsPolicy(
                parser=None,
                robots_url=response.url,
                source_status=response.status_code,
                allow_all=True,
                error_message=f"robots.txt の取得に失敗しました: HTTP {response.status_code}",
            )

        parser = RobotFileParser()
        parser.set_url(response.url)
        parser.parse(response.text.splitlines())
        return _RobotsPolicy(
            parser=parser,
            robots_url=response.url,
            source_status=response.status_code,
        )

    @staticmethod
    def _get_origin(url: str) -> str | None:
        parsed = urlparse(url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return None
        return f"{parsed.scheme}://{parsed.netloc}"
