from __future__ import annotations

from collections import Counter
from dataclasses import dataclass

import pandas as pd

from src.config_loader import AppConfig
from src.text_extractor import TextExtractionBatchResult
from src.tokenizer_ja import TokenizerJaError, apply_stopwords, get_tokenizer, load_stopword_sets, tokenize_text

FREQUENCY_COLUMNS = ["順位", "キーワード", "出現数"]


class AnalysisError(Exception):
    """形態素解析・頻度集計に関する例外。"""


@dataclass(slots=True)
class KeywordAnalysisResult:
    """頻度集計結果。"""

    raw_counter: Counter[str]
    final_counter: Counter[str]
    analyzed_page_count: int
    total_raw_token_count: int
    total_final_token_count: int

    @property
    def has_keywords(self) -> bool:
        return bool(self.final_counter)

    @property
    def raw_keyword_count(self) -> int:
        return len(self.raw_counter)

    @property
    def final_keyword_count(self) -> int:
        return len(self.final_counter)

    def to_raw_dataframe(self) -> pd.DataFrame:
        return _counter_to_dataframe(self.raw_counter)

    def to_final_dataframe(self) -> pd.DataFrame:
        return _counter_to_dataframe(self.final_counter)


@dataclass(slots=True)
class _TokenizationAccumulator:
    raw_tokens: list[str]
    final_tokens: list[str]
    analyzed_page_count: int


def analyze_keywords(text_result: TextExtractionBatchResult, config: AppConfig, logger=None) -> KeywordAnalysisResult:
    """本文抽出済みのページからキーワード頻度を集計する。"""
    try:
        stopword_sets = load_stopword_sets(config)
    except TokenizerJaError as exc:
        raise AnalysisError(str(exc)) from exc

    tokenizer = get_tokenizer()
    accumulator = _collect_tokens(text_result, tokenizer=tokenizer, stopword_sets=stopword_sets, config=config, logger=logger)

    raw_counter = Counter(accumulator.raw_tokens)
    final_counter = Counter(accumulator.final_tokens)
    return KeywordAnalysisResult(
        raw_counter=raw_counter,
        final_counter=final_counter,
        analyzed_page_count=accumulator.analyzed_page_count,
        total_raw_token_count=sum(raw_counter.values()),
        total_final_token_count=sum(final_counter.values()),
    )


def _collect_tokens(text_result: TextExtractionBatchResult, *, tokenizer, stopword_sets, config: AppConfig, logger=None) -> _TokenizationAccumulator:
    raw_tokens: list[str] = []
    final_tokens: list[str] = []
    analyzed_page_count = 0

    for record in text_result.records:
        if not record.included_in_analysis or not record.extracted_text:
            continue

        analyzed_page_count += 1
        page_tokens = tokenize_text(record.extracted_text, tokenizer=tokenizer, config=config)
        page_tokens_before_user = apply_stopwords(page_tokens, stopword_sets.default_stopwords)
        page_tokens_after_user = apply_stopwords(page_tokens_before_user, stopword_sets.user_stopwords)

        raw_tokens.extend(page_tokens_before_user)
        final_tokens.extend(page_tokens_after_user)

        if logger is not None:
            logger.info(
                "形態素解析: %s -> default適用後=%s語, user適用後=%s語",
                record.url,
                len(page_tokens_before_user),
                len(page_tokens_after_user),
            )

    return _TokenizationAccumulator(
        raw_tokens=raw_tokens,
        final_tokens=final_tokens,
        analyzed_page_count=analyzed_page_count,
    )


def _counter_to_dataframe(counter: Counter[str]) -> pd.DataFrame:
    sorted_items = sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    rows = [
        {"順位": index, "キーワード": keyword, "出現数": count}
        for index, (keyword, count) in enumerate(sorted_items, start=1)
    ]
    return pd.DataFrame(rows, columns=FREQUENCY_COLUMNS)
