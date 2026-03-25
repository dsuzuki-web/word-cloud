from __future__ import annotations

from dataclasses import dataclass
from io import BytesIO
from pathlib import Path
from typing import TYPE_CHECKING

import pandas as pd
from PIL import Image, ImageDraw, ImageFont
from wordcloud import WordCloud

from src.config_loader import AppConfig

if TYPE_CHECKING:  # pragma: no cover
    from src.analyzer import KeywordAnalysisResult
from src.utils import write_dataframe_csv


class ReportError(Exception):
    """レポート出力に関する例外。"""


class WordcloudGenerationError(ReportError):
    """ワードクラウド生成時の例外。"""


@dataclass(slots=True)
class AnalysisReportPaths:
    raw_frequency_csv_path: Path
    keyword_frequency_csv_path: Path
    keyword_frequency_xlsx_path: Path
    wordcloud_path: Path


@dataclass(slots=True)
class AnalysisReportPayloads:
    """画面表示やダウンロードにそのまま使えるレポート本体。"""

    raw_frequency_df: pd.DataFrame
    keyword_frequency_df: pd.DataFrame
    raw_frequency_csv_bytes: bytes
    keyword_frequency_csv_bytes: bytes
    keyword_frequency_xlsx_bytes: bytes
    wordcloud_bytes: bytes


def save_analysis_reports(
    analysis_result: KeywordAnalysisResult,
    *,
    config: AppConfig,
    output_dir: Path,
    logger=None,
) -> AnalysisReportPaths:
    """頻度表とワードクラウドを保存する。"""
    raw_df = analysis_result.to_raw_dataframe()
    final_df = analysis_result.to_final_dataframe()

    raw_frequency_csv_path = write_dataframe_csv(raw_df, output_dir / "raw_frequency_before_user_stopwords.csv")
    keyword_frequency_csv_path = write_dataframe_csv(final_df, output_dir / "keyword_frequency.csv")
    keyword_frequency_xlsx_path = _write_dataframe_xlsx(final_df, output_dir / "keyword_frequency.xlsx")
    wordcloud_path = output_dir / "wordcloud.png"

    font_path = config.get_path("font_path")
    if font_path is None:
        raise ReportError("font_path の設定が不正です。config.yaml を確認してください。")

    _validate_font(font_path)

    max_words = max(1, int(config.get("top_n_wordcloud", 200)))
    if analysis_result.final_counter:
        _generate_wordcloud(
            frequencies=dict(analysis_result.final_counter),
            font_path=font_path,
            max_words=max_words,
            destination=wordcloud_path,
        )
        if logger is not None:
            logger.info("ワードクラウド画像を生成しました: %s", wordcloud_path)
    else:
        _generate_empty_wordcloud(
            font_path=font_path,
            destination=wordcloud_path,
            message="有効なキーワードが抽出できませんでした",
        )
        if logger is not None:
            logger.info("抽出キーワードが0件のため、空のワードクラウド画像を生成しました: %s", wordcloud_path)

    return AnalysisReportPaths(
        raw_frequency_csv_path=raw_frequency_csv_path,
        keyword_frequency_csv_path=keyword_frequency_csv_path,
        keyword_frequency_xlsx_path=keyword_frequency_xlsx_path,
        wordcloud_path=wordcloud_path,
    )


def build_analysis_report_payloads(
    analysis_result: KeywordAnalysisResult,
    *,
    report_paths: AnalysisReportPaths,
) -> AnalysisReportPayloads:
    """画面表示用の DataFrame とダウンロード用 bytes を構築する。"""
    raw_df = analysis_result.to_raw_dataframe()
    final_df = analysis_result.to_final_dataframe()
    return AnalysisReportPayloads(
        raw_frequency_df=raw_df,
        keyword_frequency_df=final_df,
        raw_frequency_csv_bytes=dataframe_to_csv_bytes(raw_df),
        keyword_frequency_csv_bytes=dataframe_to_csv_bytes(final_df),
        keyword_frequency_xlsx_bytes=dataframe_to_xlsx_bytes(final_df),
        wordcloud_bytes=read_binary_file(report_paths.wordcloud_path),
    )


def dataframe_to_csv_bytes(dataframe: pd.DataFrame) -> bytes:
    """DataFrame を UTF-8 BOM 付き CSV bytes に変換する。"""
    try:
        return dataframe.to_csv(index=False).encode("utf-8-sig")
    except Exception as exc:  # pragma: no cover
        raise ReportError(f"CSV データの作成に失敗しました: {exc}") from exc


def dataframe_to_xlsx_bytes(dataframe: pd.DataFrame) -> bytes:
    """DataFrame を XLSX bytes に変換する。"""
    buffer = BytesIO()
    try:
        with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
            dataframe.to_excel(writer, index=False)
    except Exception as exc:  # pragma: no cover
        raise ReportError(f"Excel データの作成に失敗しました: {exc}") from exc
    return buffer.getvalue()


def read_binary_file(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise ReportError(f"ファイルの読み込みに失敗しました: {path}") from exc


def _validate_font(font_path: Path) -> None:
    try:
        ImageFont.truetype(str(font_path), size=24)
    except OSError as exc:
        raise WordcloudGenerationError(f"指定したフォントファイルを読み込めません: {font_path}") from exc


def _write_dataframe_xlsx(dataframe: pd.DataFrame, destination: Path) -> Path:
    try:
        dataframe.to_excel(destination, index=False)
    except OSError as exc:
        raise ReportError(f"Excelファイルの保存に失敗しました: {destination}") from exc
    return destination


def _generate_wordcloud(*, frequencies: dict[str, int], font_path: Path, max_words: int, destination: Path) -> None:
    try:
        cloud = WordCloud(
            font_path=str(font_path),
            width=1600,
            height=900,
            background_color="white",
            max_words=max_words,
            collocations=False,
        ).generate_from_frequencies(frequencies)
        cloud.to_file(str(destination))
    except Exception as exc:  # pragma: no cover
        raise WordcloudGenerationError(f"ワードクラウドの生成に失敗しました: {exc}") from exc


def _generate_empty_wordcloud(*, font_path: Path, destination: Path, message: str) -> None:
    try:
        image = Image.new("RGB", (1600, 900), color="white")
        draw = ImageDraw.Draw(image)
        font = ImageFont.truetype(str(font_path), size=36)
        bbox = draw.textbbox((0, 0), message, font=font)
        text_width = bbox[2] - bbox[0]
        text_height = bbox[3] - bbox[1]
        x = max(20, (1600 - text_width) // 2)
        y = max(20, (900 - text_height) // 2)
        draw.text((x, y), message, fill="black", font=font)
        image.save(destination)
    except Exception as exc:  # pragma: no cover
        raise WordcloudGenerationError(f"空のワードクラウド画像の生成に失敗しました: {exc}") from exc
