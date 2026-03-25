from __future__ import annotations

from pathlib import Path
from typing import Iterable

# プロジェクト内にユーザーが置いたフォントを最優先に探し、
# 見つからなければ各 OS / Community Cloud でよく使われる日本語フォントへフォールバックする。
PROJECT_FONT_CANDIDATES = (
    "fonts/NotoSansJP-Regular.otf",
    "fonts/NotoSansJP-Regular.ttf",
    "fonts/NotoSansCJK-Regular.ttc",
    "assets/fonts/NotoSansJP-Regular.otf",
    "assets/fonts/NotoSansJP-Regular.ttf",
    "assets/fonts/NotoSansCJK-Regular.ttc",
)

SYSTEM_FONT_CANDIDATES = (
    "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/opentype/noto/NotoSansJP-Regular.otf",
    "/usr/share/fonts/truetype/noto/NotoSansCJK-Regular.ttc",
    "/usr/share/fonts/truetype/noto/NotoSansJP-Regular.ttf",
    "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc",
    "/System/Library/Fonts/Hiragino Sans GB.ttc",
    "C:/Windows/Fonts/msgothic.ttc",
    "C:/Windows/Fonts/meiryo.ttc",
    "C:/Windows/Fonts/YuGothM.ttc",
)


def iter_font_candidates(base_dir: str | Path | None = None) -> Iterable[Path]:
    base_path = Path(base_dir).resolve() if base_dir is not None else None

    if base_path is not None:
        for relative_path in PROJECT_FONT_CANDIDATES:
            yield (base_path / relative_path).resolve()

    for candidate in SYSTEM_FONT_CANDIDATES:
        yield Path(candidate)


def resolve_font_path(*, preferred: str | Path | None = None, base_dir: str | Path | None = None) -> Path | None:
    """日本語ワードクラウド用フォントを解決する。"""
    candidate_paths: list[Path] = []

    if preferred:
        preferred_path = Path(preferred).expanduser()
        if not preferred_path.is_absolute() and base_dir is not None:
            preferred_path = (Path(base_dir).resolve() / preferred_path).resolve()
        else:
            preferred_path = preferred_path.resolve()
        candidate_paths.append(preferred_path)

    candidate_paths.extend(iter_font_candidates(base_dir))

    seen: set[Path] = set()
    for path in candidate_paths:
        normalized = path.resolve() if path.is_absolute() else path
        if normalized in seen:
            continue
        seen.add(normalized)
        if normalized.exists() and normalized.is_file():
            return normalized
    return None
