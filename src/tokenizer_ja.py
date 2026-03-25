from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Iterable

from src.config_loader import AppConfig

if TYPE_CHECKING:  # pragma: no cover
    from janome.tokenizer import Tokenizer as JanomeTokenizer
else:  # pragma: no cover - 実行時は get_tokenizer() で遅延 import する
    JanomeTokenizer = Any

HIRAGANA_ONLY_PATTERN = re.compile(r"^[ぁ-んー]+$")
ALLOWED_PRIMARY_POS = "名詞"
EXCLUDED_SINGLE_NOUN_SUBTYPES = {"数", "非自立", "代名詞", "接尾"}
EXCLUDED_COMPOUND_NOUN_SUBTYPES = {"数", "非自立", "代名詞"}


class TokenizerJaError(Exception):
    """日本語トークン処理に関する例外。"""


@dataclass(slots=True)
class StopwordSets:
    """既定ストップワードとユーザー追加ストップワード。"""

    default_stopwords: set[str]
    user_stopwords: set[str]


@dataclass(slots=True)
class MorphToken:
    """Janome のトークンを扱いやすくした軽量表現。"""

    surface: str
    base_form: str
    normalized_surface: str
    normalized_base_form: str
    primary_pos: str
    subtype: str
    pos_parts: tuple[str, ...]

    @property
    def is_noun(self) -> bool:
        return self.primary_pos == ALLOWED_PRIMARY_POS

    @property
    def is_suffix(self) -> bool:
        return self.is_noun and self.subtype == "接尾"

    @property
    def is_proper_noun(self) -> bool:
        return self.is_noun and self.subtype == "固有名詞"


@dataclass(slots=True)
class ForcedCompoundMatcher:
    """強制複合語辞書のマッチャー。"""

    phrases: set[str]
    prefixes: set[str]
    max_phrase_length: int
    first_chars: set[str]


_TOKENIZER: JanomeTokenizer | None = None


def get_tokenizer() -> JanomeTokenizer:
    """Janome Tokenizer を遅延初期化して返す。"""
    global _TOKENIZER
    if _TOKENIZER is None:
        try:
            from janome.tokenizer import Tokenizer
        except ModuleNotFoundError as exc:  # pragma: no cover - 環境依存
            raise TokenizerJaError(
                "janome がインストールされていません。`pip install -r requirements.txt` を実行してください。"
            ) from exc
        _TOKENIZER = Tokenizer()
    return _TOKENIZER


def load_stopword_sets(config: AppConfig) -> StopwordSets:
    """設定ファイルと実行時設定からストップワードを読み込む。"""
    default_stopwords: set[str] = set()
    user_stopwords: set[str] = set()

    if bool(config.get("use_default_stopwords", True)):
        default_path = config.get_path("stopwords_default_file")
        if default_path is None:
            raise TokenizerJaError("stopwords_default_file の設定が不正です")
        default_stopwords = _load_stopwords_from_file(default_path)

    if bool(config.get("use_user_stopwords_file", True)):
        user_path = config.get_path("stopwords_user_file")
        if user_path is None:
            raise TokenizerJaError("stopwords_user_file の設定が不正です")
        user_stopwords |= _load_stopwords_from_file(user_path)

    user_stopwords |= _load_runtime_stopwords_from_config(config)
    return StopwordSets(default_stopwords=default_stopwords, user_stopwords=user_stopwords)


def tokenize_text(
    text: str,
    *,
    tokenizer: JanomeTokenizer | None = None,
    config: AppConfig | None = None,
    compound_mode: bool | None = None,
    forced_compounds: Iterable[str] | None = None,
) -> list[str]:
    """本文から解析対象の名詞トークンを抽出する。"""
    if not text:
        return []

    tokenizer = tokenizer or get_tokenizer()
    morph_tokens = _to_morph_tokens(text, tokenizer=tokenizer)

    resolved_compound_mode = bool(
        compound_mode if compound_mode is not None else (config.get("compound_mode", False) if config else False)
    )
    resolved_forced_compounds = _load_runtime_compounds_from_config(config, forced_compounds)
    matcher = _build_forced_compound_matcher(resolved_forced_compounds)

    tokens: list[str] = []
    index = 0
    while index < len(morph_tokens):
        forced_match = _match_forced_compound(morph_tokens, start=index, matcher=matcher)
        if forced_match is not None:
            next_index, forced_phrase = forced_match
            if not _should_skip_basic_token(forced_phrase):
                tokens.append(forced_phrase)
            index = next_index
            continue

        if resolved_compound_mode:
            compound_match = _consume_auto_compound(morph_tokens, start=index)
            if compound_match is not None:
                next_index, compound_phrase = compound_match
                if not _should_skip_basic_token(compound_phrase):
                    tokens.append(compound_phrase)
                index = next_index
                continue

        single_token = _extract_single_token(morph_tokens[index])
        if single_token is not None:
            tokens.append(single_token)
        index += 1

    return tokens


def normalize_token(token: str) -> str:
    """表記ゆれを抑えるための簡易正規化。"""
    normalized = unicodedata.normalize("NFKC", token or "").strip()
    normalized = re.sub(r"\s+", "", normalized)
    return normalized.lower()


def apply_stopwords(tokens: list[str], stopwords: set[str]) -> list[str]:
    """ストップワードを除外する。"""
    if not stopwords:
        return list(tokens)
    return [token for token in tokens if token not in stopwords]


def parse_stopwords_text(text: str) -> list[str]:
    """カンマ区切り・改行区切りのストップワード入力を正規化して返す。"""
    return _parse_delimited_terms(text)


def parse_compounds_text(text: str) -> list[str]:
    """カンマ区切り・改行区切りの複合語入力を正規化して返す。"""
    return _parse_delimited_terms(text)


def _parse_delimited_terms(text: str) -> list[str]:
    if not text:
        return []

    candidates = re.split(r"[\r\n,，、]+", text)
    normalized: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        stripped = normalize_token(candidate)
        if not stripped or stripped in seen:
            continue
        seen.add(stripped)
        normalized.append(stripped)
    return normalized


def _load_runtime_stopwords_from_config(config: AppConfig) -> set[str]:
    runtime_stopwords: set[str] = set()

    additional_stopwords = config.get("additional_stopwords", [])
    if isinstance(additional_stopwords, Iterable) and not isinstance(additional_stopwords, (str, bytes)):
        runtime_stopwords |= _normalize_term_values(additional_stopwords)

    additional_stopwords_text = str(config.get("additional_stopwords_text", ""))
    runtime_stopwords |= set(parse_stopwords_text(additional_stopwords_text))
    return runtime_stopwords


def _load_runtime_compounds_from_config(
    config: AppConfig | None,
    forced_compounds: Iterable[str] | None = None,
) -> list[str]:
    normalized: set[str] = set()
    ordered: list[str] = []

    def _add_values(values: Iterable[Any]) -> None:
        for value in values:
            normalized_value = normalize_token(str(value))
            if not normalized_value or normalized_value in normalized:
                continue
            normalized.add(normalized_value)
            ordered.append(normalized_value)

    if forced_compounds is not None and not isinstance(forced_compounds, (str, bytes)):
        _add_values(forced_compounds)

    if config is not None:
        configured_compounds = config.get("forced_compounds", [])
        if isinstance(configured_compounds, Iterable) and not isinstance(configured_compounds, (str, bytes)):
            _add_values(configured_compounds)

        configured_text = str(config.get("forced_compounds_text", ""))
        _add_values(parse_compounds_text(configured_text))

    return ordered


def _normalize_term_values(values: Iterable[Any]) -> set[str]:
    normalized_values: set[str] = set()
    for value in values:
        normalized = normalize_token(str(value))
        if normalized:
            normalized_values.add(normalized)
    return normalized_values


def _load_stopwords_from_file(path: Path) -> set[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError as exc:
        raise TokenizerJaError(f"ストップワードファイルを読み込めませんでした: {path}") from exc

    stopwords: set[str] = set()
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        normalized = normalize_token(stripped)
        if normalized:
            stopwords.add(normalized)
    return stopwords


def _to_morph_tokens(text: str, *, tokenizer: JanomeTokenizer) -> list[MorphToken]:
    morph_tokens: list[MorphToken] = []
    for token in tokenizer.tokenize(text):
        surface = str(getattr(token, "surface", "") or "")
        base_form = str(getattr(token, "base_form", "") or "")
        if not base_form or base_form == "*":
            base_form = surface

        part_of_speech = str(getattr(token, "part_of_speech", "") or "")
        pos_parts = tuple(part_of_speech.split(",")) if part_of_speech else tuple()
        primary_pos = pos_parts[0] if pos_parts else ""
        subtype = pos_parts[1] if len(pos_parts) > 1 else ""

        morph_tokens.append(
            MorphToken(
                surface=surface,
                base_form=base_form,
                normalized_surface=normalize_token(surface),
                normalized_base_form=normalize_token(base_form),
                primary_pos=primary_pos,
                subtype=subtype,
                pos_parts=pos_parts,
            )
        )
    return morph_tokens


def _extract_single_token(token: MorphToken) -> str | None:
    if not token.is_noun:
        return None
    if token.subtype in EXCLUDED_SINGLE_NOUN_SUBTYPES:
        return None

    normalized = token.normalized_base_form or token.normalized_surface
    if _should_skip_basic_token(normalized):
        return None
    return normalized


def _consume_auto_compound(morph_tokens: list[MorphToken], *, start: int) -> tuple[int, str] | None:
    first = morph_tokens[start]
    if not _is_compound_start_token(first):
        return None

    end = start + 1
    while end < len(morph_tokens) and _is_compound_extension_token(morph_tokens[end]):
        end += 1

    if end - start <= 1:
        return None

    compound_text = normalize_token("".join(token.surface for token in morph_tokens[start:end]))
    if _should_skip_basic_token(compound_text):
        return None
    return end, compound_text


def _is_compound_start_token(token: MorphToken) -> bool:
    if not token.is_noun:
        return False
    if token.subtype in EXCLUDED_COMPOUND_NOUN_SUBTYPES:
        return False
    if token.is_suffix:
        return False
    return bool(token.normalized_surface)


def _is_compound_extension_token(token: MorphToken) -> bool:
    if not token.is_noun:
        return False
    if token.subtype in EXCLUDED_COMPOUND_NOUN_SUBTYPES:
        return False
    return bool(token.normalized_surface)


def _build_forced_compound_matcher(forced_compounds: Iterable[str]) -> ForcedCompoundMatcher | None:
    normalized_phrases: list[str] = []
    for phrase in forced_compounds:
        normalized_phrase = normalize_token(str(phrase))
        if normalized_phrase:
            normalized_phrases.append(normalized_phrase)
    unique_phrases = sorted(set(normalized_phrases), key=len, reverse=True)
    if not unique_phrases:
        return None

    phrases = set(unique_phrases)
    prefixes: set[str] = set()
    first_chars: set[str] = set()
    max_phrase_length = 0
    for phrase in unique_phrases:
        max_phrase_length = max(max_phrase_length, len(phrase))
        first_chars.add(phrase[0])
        for index in range(1, len(phrase) + 1):
            prefixes.add(phrase[:index])

    return ForcedCompoundMatcher(
        phrases=phrases,
        prefixes=prefixes,
        max_phrase_length=max_phrase_length,
        first_chars=first_chars,
    )


def _match_forced_compound(
    morph_tokens: list[MorphToken],
    *,
    start: int,
    matcher: ForcedCompoundMatcher | None,
) -> tuple[int, str] | None:
    if matcher is None or start >= len(morph_tokens):
        return None

    first = morph_tokens[start].normalized_surface
    if not first or first[0] not in matcher.first_chars:
        return None

    combined = ""
    matched_end: int | None = None
    matched_phrase: str | None = None

    for index in range(start, len(morph_tokens)):
        part = morph_tokens[index].normalized_surface
        if not part:
            break
        combined += part
        if len(combined) > matcher.max_phrase_length:
            break
        if combined not in matcher.prefixes:
            break
        if combined in matcher.phrases:
            matched_end = index + 1
            matched_phrase = combined

    if matched_end is None or matched_phrase is None:
        return None
    return matched_end, matched_phrase


def _should_skip_basic_token(token: str) -> bool:
    if not token:
        return True
    if len(token) <= 1:
        return True
    if len(token) <= 2 and HIRAGANA_ONLY_PATTERN.fullmatch(token):
        return True
    if _is_numeric_only(token):
        return True
    if _is_symbol_only(token):
        return True
    return False


def _is_numeric_only(token: str) -> bool:
    return all(unicodedata.category(char).startswith("N") for char in token)


def _is_symbol_only(token: str) -> bool:
    return all(unicodedata.category(char)[0] in {"P", "S"} for char in token)
