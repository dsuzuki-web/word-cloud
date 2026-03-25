from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

import pandas as pd

from src.config_loader import AppConfig, ConfigError, build_config_from_settings, validate_config
from src.utils import create_run_output_dir, setup_logging, write_dataframe_csv, write_run_config

if TYPE_CHECKING:  # pragma: no cover
    from src.analyzer import KeywordAnalysisResult
    from src.crawler import CrawlBatchResult
    from src.reporter import AnalysisReportPaths, AnalysisReportPayloads
    from src.text_extractor import TextExtractionBatchResult
    from src.url_collector import DomainUrlCollectionResult
    from src.url_loader import UrlLoadResult

PROJECT_ROOT = Path(__file__).resolve().parents[1]


class PipelineError(Exception):
    """services.pipeline 全体の共通例外。"""


class PipelineValidationError(PipelineError):
    """settings / config の検証エラー。"""

    def __init__(self, errors: list[str]) -> None:
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass(slots=True)
class PipelineArtifacts:
    """実行時に出力したアーティファクトのパス一覧。"""

    output_dir: Path
    config_snapshot_path: Path
    target_csv_path: Path | None = None
    skipped_csv_path: Path | None = None
    sitemap_sources_path: Path | None = None
    discovered_urls_path: Path | None = None
    crawl_success_path: Path | None = None
    crawl_errors_path: Path | None = None
    robots_blocked_path: Path | None = None
    url_audit_path: Path | None = None
    noindex_path: Path | None = None
    extracted_texts_path: Path | None = None
    raw_frequency_csv_path: Path | None = None
    keyword_frequency_csv_path: Path | None = None
    keyword_frequency_xlsx_path: Path | None = None
    wordcloud_path: Path | None = None

    def to_dict(self) -> dict[str, Path | None]:
        return {
            "output_dir": self.output_dir,
            "config_snapshot_path": self.config_snapshot_path,
            "target_csv_path": self.target_csv_path,
            "skipped_csv_path": self.skipped_csv_path,
            "sitemap_sources_path": self.sitemap_sources_path,
            "discovered_urls_path": self.discovered_urls_path,
            "crawl_success_path": self.crawl_success_path,
            "crawl_errors_path": self.crawl_errors_path,
            "robots_blocked_path": self.robots_blocked_path,
            "url_audit_path": self.url_audit_path,
            "noindex_path": self.noindex_path,
            "extracted_texts_path": self.extracted_texts_path,
            "raw_frequency_csv_path": self.raw_frequency_csv_path,
            "keyword_frequency_csv_path": self.keyword_frequency_csv_path,
            "keyword_frequency_xlsx_path": self.keyword_frequency_xlsx_path,
            "wordcloud_path": self.wordcloud_path,
        }


@dataclass(slots=True)
class PipelineResult:
    """run_analysis() / rerun_analysis_from_text_result() の戻り値。"""

    config: AppConfig
    summary: dict[str, Any]
    frequency_df: pd.DataFrame
    url_audit_df: pd.DataFrame
    wordcloud_path_or_bytes: Path | bytes
    artifacts: PipelineArtifacts
    crawl_result: CrawlBatchResult
    text_result: TextExtractionBatchResult
    analysis_result: KeywordAnalysisResult
    report_paths: AnalysisReportPaths
    report_payloads: AnalysisReportPayloads
    load_result: UrlLoadResult | None = None
    collection_result: DomainUrlCollectionResult | None = None
    raw_frequency_df: pd.DataFrame | None = None


@dataclass(slots=True)
class DomainPreviewResult:
    """domain モードのURL収集プレビュー結果。"""

    config: AppConfig
    collection_result: DomainUrlCollectionResult
    target_urls_df: pd.DataFrame
    sitemap_sources_df: pd.DataFrame
    discovered_urls_df: pd.DataFrame
    wp_api_sources_df: pd.DataFrame
    summary: dict[str, Any]


# NOTE: Streamlit などの UI から辞書を渡して実行する想定。
def run_analysis(
    settings: Mapping[str, Any] | AppConfig,
    *,
    base_dir: str | Path | None = None,
    config_path: str | Path | None = None,
    return_wordcloud_bytes: bool = False,
) -> PipelineResult:
    """設定辞書または AppConfig から一連の分析処理を実行する。"""
    config = _coerce_config(settings, base_dir=base_dir, config_path=config_path)

    errors = validate_config(config)
    if errors:
        raise PipelineValidationError(errors)

    output_dir, logger, artifacts = _prepare_pipeline_runtime(config)

    try:
        from src.analyzer import AnalysisError
        from src.crawler import CrawlError, crawl_urls
        from src.text_extractor import TextExtractionError, analyze_crawl_results
    except ModuleNotFoundError as exc:
        raise PipelineError(_format_missing_dependency_message(exc.name)) from exc

    input_mode = str(config.get("input_mode", "domain"))
    load_result: UrlLoadResult | None = None
    collection_result: DomainUrlCollectionResult | None = None

    if input_mode == "xlsx":
        try:
            from src.url_loader import UrlLoadError, load_urls_from_excel, save_url_load_results
        except ModuleNotFoundError as exc:
            raise PipelineError(_format_missing_dependency_message(exc.name)) from exc

        try:
            logger.info("ExcelからURL一覧を読み込みます")
            load_result = load_urls_from_excel(config)
            artifacts.target_csv_path, artifacts.skipped_csv_path = save_url_load_results(load_result, output_dir)
        except (UrlLoadError, OSError) as exc:
            raise PipelineError(f"Excel URL 読み込みに失敗しました: {exc}") from exc

        target_records = load_result.target_records
        logger.info("target_urls.csv を保存しました: %s", artifacts.target_csv_path)
        logger.info("url_load_skipped.csv を保存しました: %s", artifacts.skipped_csv_path)
        logger.info(
            "URL前処理が完了しました。採用件数=%s, 除外件数=%s",
            load_result.target_count,
            load_result.skipped_count,
        )
    elif input_mode == "url_list":
        try:
            from src.url_loader import UrlLoadError, load_urls_from_text, save_url_load_results
        except ModuleNotFoundError as exc:
            raise PipelineError(_format_missing_dependency_message(exc.name)) from exc

        url_list_text = _coerce_url_list_text(config)
        max_list_urls = _coerce_optional_int(config.get("max_list_urls", 50))
        exclude_patterns = config.get("exclude_url_patterns", [])
        try:
            logger.info("URL一覧テキストからURL前処理を開始します")
            load_result = load_urls_from_text(
                url_list_text,
                exclude_patterns=exclude_patterns if isinstance(exclude_patterns, list) else [],
                max_urls=max_list_urls,
            )
            artifacts.target_csv_path, artifacts.skipped_csv_path = save_url_load_results(load_result, output_dir)
        except (UrlLoadError, OSError) as exc:
            raise PipelineError(f"URL一覧の読み込みに失敗しました: {exc}") from exc

        target_records = load_result.target_records
        logger.info("target_urls.csv を保存しました: %s", artifacts.target_csv_path)
        logger.info("url_load_skipped.csv を保存しました: %s", artifacts.skipped_csv_path)
        logger.info(
            "URL一覧の前処理が完了しました。採用件数=%s, 除外件数=%s",
            load_result.target_count,
            load_result.skipped_count,
        )
    elif input_mode == "domain":
        try:
            from src.url_collector import (
                UrlCollectionError,
                collect_urls_from_domain,
                save_domain_collection_results,
            )
        except ModuleNotFoundError as exc:
            raise PipelineError(_format_missing_dependency_message(exc.name)) from exc

        try:
            logger.info("domain モードのURL収集を開始します")
            collection_result = collect_urls_from_domain(config, logger=logger)
            (
                artifacts.target_csv_path,
                artifacts.sitemap_sources_path,
                artifacts.discovered_urls_path,
            ) = save_domain_collection_results(collection_result, output_dir)
        except (UrlCollectionError, OSError) as exc:
            raise PipelineError(f"domain モードのURL収集に失敗しました: {exc}") from exc

        target_records = collection_result.target_records
        logger.info("target_urls.csv を保存しました: %s", artifacts.target_csv_path)
        logger.info("sitemap_sources.csv を保存しました: %s", artifacts.sitemap_sources_path)
        logger.info("discovered_urls.csv を保存しました: %s", artifacts.discovered_urls_path)
        logger.info(
            "domain モードのURL収集が完了しました。採用件数=%s, sitemap調査件数=%s, 発見URL件数=%s",
            collection_result.target_count,
            collection_result.sitemap_source_count,
            collection_result.discovered_count,
        )
        if collection_result.fallback_used and collection_result.fallback_message:
            logger.info(collection_result.fallback_message)
    else:
        raise PipelineError('input_mode は "xlsx" / "domain" / "url_list" のいずれかを指定してください')

    try:
        logger.info("URLのHTTP取得を開始します")
        crawl_result = crawl_urls(target_records, config, logger=logger)
        _persist_crawl_artifacts(crawl_result=crawl_result, output_dir=output_dir, artifacts=artifacts)
    except (CrawlError, OSError) as exc:
        raise PipelineError(f"HTTP取得の前処理またはCSV保存に失敗しました: {exc}") from exc

    logger.info(
        "HTTP取得が完了しました。成功=%s, 失敗=%s, robots除外=%s",
        crawl_result.success_count,
        crawl_result.error_count,
        crawl_result.robots_blocked_count,
    )

    try:
        logger.info("本文抽出と noindex 判定を開始します")
        text_result = analyze_crawl_results(crawl_result, config, logger=logger)
        url_audit_df = _persist_text_artifacts(
            text_result,
            config=config,
            output_dir=output_dir,
            artifacts=artifacts,
        )
    except (TextExtractionError, OSError) as exc:
        raise PipelineError(f"本文抽出またはCSV保存に失敗しました: {exc}") from exc

    logger.info(
        "本文抽出が完了しました。noindex検出=%s, 集計対象=%s",
        text_result.noindex_count,
        text_result.included_count,
    )

    return _finalize_analysis_from_text_result(
        config=config,
        input_mode=input_mode,
        load_result=load_result,
        collection_result=collection_result,
        crawl_result=crawl_result,
        text_result=text_result,
        url_audit_df=url_audit_df,
        artifacts=artifacts,
        output_dir=output_dir,
        logger=logger,
        return_wordcloud_bytes=return_wordcloud_bytes,
        execution_mode="fresh",
    )


def rerun_analysis_from_text_result(
    settings: Mapping[str, Any] | AppConfig,
    *,
    text_result: TextExtractionBatchResult,
    crawl_result: CrawlBatchResult,
    load_result: UrlLoadResult | None = None,
    collection_result: DomainUrlCollectionResult | None = None,
    base_dir: str | Path | None = None,
    config_path: str | Path | None = None,
    return_wordcloud_bytes: bool = False,
) -> PipelineResult:
    """抽出済み本文を再利用し、クロールを省略して再解析だけ実行する。"""
    config = _coerce_config(settings, base_dir=base_dir, config_path=config_path)

    errors = _validate_reanalysis_config(config)
    if errors:
        raise PipelineValidationError(errors)

    output_dir, logger, artifacts = _prepare_pipeline_runtime(config)
    input_mode = str(config.get("input_mode", "domain"))

    _persist_cached_input_artifacts(
        load_result=load_result,
        collection_result=collection_result,
        output_dir=output_dir,
        artifacts=artifacts,
        logger=logger,
    )

    try:
        _persist_crawl_artifacts(crawl_result=crawl_result, output_dir=output_dir, artifacts=artifacts)
        url_audit_df = _persist_text_artifacts(
            text_result,
            config=config,
            output_dir=output_dir,
            artifacts=artifacts,
        )
    except OSError as exc:
        raise PipelineError(f"再解析用の監査CSV保存に失敗しました: {exc}") from exc

    logger.info("抽出済みテキストから再解析を開始します")
    logger.info(
        "再利用する本文抽出結果: 集計対象=%s, noindex=%s",
        text_result.included_count,
        text_result.noindex_count,
    )

    return _finalize_analysis_from_text_result(
        config=config,
        input_mode=input_mode,
        load_result=load_result,
        collection_result=collection_result,
        crawl_result=crawl_result,
        text_result=text_result,
        url_audit_df=url_audit_df,
        artifacts=artifacts,
        output_dir=output_dir,
        logger=logger,
        return_wordcloud_bytes=return_wordcloud_bytes,
        execution_mode="cached_text",
    )


def preview_domain_collection(
    settings: Mapping[str, Any] | AppConfig,
    *,
    base_dir: str | Path | None = None,
    config_path: str | Path | None = None,
) -> DomainPreviewResult:
    """domain モードの URL 収集だけを実行してプレビュー結果を返す。"""
    config = _coerce_config(settings, base_dir=base_dir, config_path=config_path)

    if str(config.get("input_mode", "domain")) != "domain":
        raise PipelineValidationError(["domain プレビューは input_mode='domain' で実行してください"])

    domain_url = str(config.get("domain_url", "")).strip()
    if not domain_url:
        raise PipelineValidationError(["domain_url を入力してください"])

    try:
        from src.url_collector import UrlCollectionError, collect_urls_from_domain
    except ModuleNotFoundError as exc:
        raise PipelineError(_format_missing_dependency_message(exc.name)) from exc

    try:
        collection_result = collect_urls_from_domain(config, logger=None)
    except UrlCollectionError as exc:
        raise PipelineError(f"domain モードのURLプレビューに失敗しました: {exc}") from exc

    summary = {
        "input_mode": "domain",
        "target_count": collection_result.target_count,
        "sitemap_source_count": collection_result.sitemap_source_count,
        "discovered_count": collection_result.discovered_count,
        "wp_api_source_count": collection_result.wp_api_source_count,
        "wp_api_target_count": collection_result.wp_api_target_count,
        "wp_api_used": collection_result.wp_api_used,
        "fallback_used": collection_result.fallback_used,
        "fallback_message": collection_result.fallback_message,
    }

    return DomainPreviewResult(
        config=config,
        collection_result=collection_result,
        target_urls_df=collection_result.to_target_dataframe(),
        sitemap_sources_df=collection_result.to_sitemap_sources_dataframe(),
        discovered_urls_df=collection_result.to_discovered_dataframe(),
        wp_api_sources_df=collection_result.to_wp_api_sources_dataframe(),
        summary=summary,
    )


def _prepare_pipeline_runtime(config: AppConfig):
    output_root = config.get_path("output_root")
    if output_root is None:
        raise PipelineError("output_root の設定が不正です。config.yaml を確認してください。")

    try:
        output_dir = create_run_output_dir(output_root)
        logger = setup_logging(output_dir / "run.log")
        config_snapshot_path = write_run_config(config, output_dir)
    except OSError as exc:
        raise PipelineError(f"出力フォルダまたはログファイルの作成に失敗しました: {exc}") from exc

    artifacts = PipelineArtifacts(
        output_dir=output_dir,
        config_snapshot_path=config_snapshot_path,
    )

    logger.info("site_keyword_analyzer を初期化しました")
    logger.info("設定ファイル: %s", config.config_path)
    logger.info("出力フォルダ: %s", output_dir)
    logger.info("設定スナップショット: %s", config_snapshot_path)
    return output_dir, logger, artifacts


def _persist_cached_input_artifacts(
    *,
    load_result: UrlLoadResult | None,
    collection_result: DomainUrlCollectionResult | None,
    output_dir: Path,
    artifacts: PipelineArtifacts,
    logger,
) -> None:
    if load_result is not None:
        try:
            from src.url_loader import save_url_load_results
        except ModuleNotFoundError as exc:
            raise PipelineError(_format_missing_dependency_message(exc.name)) from exc
        try:
            artifacts.target_csv_path, artifacts.skipped_csv_path = save_url_load_results(load_result, output_dir)
        except OSError as exc:
            raise PipelineError(f"URL前処理結果の保存に失敗しました: {exc}") from exc
        logger.info("既存の URL 前処理結果を再利用しました")

    if collection_result is not None:
        try:
            from src.url_collector import save_domain_collection_results
        except ModuleNotFoundError as exc:
            raise PipelineError(_format_missing_dependency_message(exc.name)) from exc
        try:
            (
                artifacts.target_csv_path,
                artifacts.sitemap_sources_path,
                artifacts.discovered_urls_path,
            ) = save_domain_collection_results(collection_result, output_dir)
        except OSError as exc:
            raise PipelineError(f"domain URL収集結果の保存に失敗しました: {exc}") from exc
        logger.info("既存の domain URL収集結果を再利用しました")


def _persist_crawl_artifacts(*, crawl_result: CrawlBatchResult, output_dir: Path, artifacts: PipelineArtifacts) -> None:
    artifacts.crawl_success_path = write_dataframe_csv(
        crawl_result.to_success_dataframe(),
        output_dir / "crawl_success.csv",
    )
    artifacts.crawl_errors_path = write_dataframe_csv(
        crawl_result.to_error_dataframe(),
        output_dir / "crawl_errors.csv",
    )
    artifacts.robots_blocked_path = write_dataframe_csv(
        crawl_result.to_robots_blocked_dataframe(),
        output_dir / "robots_blocked_urls.csv",
    )


def _persist_text_artifacts(
    text_result: TextExtractionBatchResult,
    *,
    config: AppConfig,
    output_dir: Path,
    artifacts: PipelineArtifacts,
) -> pd.DataFrame:
    url_audit_df = text_result.to_url_audit_dataframe()
    artifacts.url_audit_path = write_dataframe_csv(url_audit_df, output_dir / "url_audit.csv")
    artifacts.noindex_path = write_dataframe_csv(text_result.to_noindex_dataframe(), output_dir / "noindex_urls.csv")
    if bool(config.get("save_extracted_text", False)):
        artifacts.extracted_texts_path = write_dataframe_csv(
            text_result.to_extracted_texts_dataframe(),
            output_dir / "extracted_texts.csv",
        )
    return url_audit_df


def _finalize_analysis_from_text_result(
    *,
    config: AppConfig,
    input_mode: str,
    load_result: UrlLoadResult | None,
    collection_result: DomainUrlCollectionResult | None,
    crawl_result: CrawlBatchResult,
    text_result: TextExtractionBatchResult,
    url_audit_df: pd.DataFrame,
    artifacts: PipelineArtifacts,
    output_dir: Path,
    logger,
    return_wordcloud_bytes: bool,
    execution_mode: str,
) -> PipelineResult:
    try:
        from src.analyzer import AnalysisError, analyze_keywords
        from src.reporter import ReportError, build_analysis_report_payloads, save_analysis_reports
    except ModuleNotFoundError as exc:
        raise PipelineError(_format_missing_dependency_message(exc.name)) from exc

    try:
        logger.info("形態素解析と頻度集計を開始します")
        analysis_result = analyze_keywords(text_result, config, logger=logger)
        logger.info("頻度表とワードクラウドの保存を開始します")
        report_paths = save_analysis_reports(
            analysis_result,
            config=config,
            output_dir=output_dir,
            logger=logger,
        )
        report_payloads = build_analysis_report_payloads(
            analysis_result,
            report_paths=report_paths,
        )
    except (AnalysisError, ReportError) as exc:
        raise PipelineError(f"形態素解析またはレポート出力に失敗しました: {exc}") from exc

    artifacts.raw_frequency_csv_path = report_paths.raw_frequency_csv_path
    artifacts.keyword_frequency_csv_path = report_paths.keyword_frequency_csv_path
    artifacts.keyword_frequency_xlsx_path = report_paths.keyword_frequency_xlsx_path
    artifacts.wordcloud_path = report_paths.wordcloud_path

    logger.info("raw_frequency_before_user_stopwords.csv を保存しました: %s", artifacts.raw_frequency_csv_path)
    logger.info("keyword_frequency.csv を保存しました: %s", artifacts.keyword_frequency_csv_path)
    logger.info("keyword_frequency.xlsx を保存しました: %s", artifacts.keyword_frequency_xlsx_path)
    logger.info("wordcloud.png を保存しました: %s", artifacts.wordcloud_path)
    logger.info(
        "形態素解析が完了しました。解析ページ数=%s, finalキーワード数=%s",
        analysis_result.analyzed_page_count,
        analysis_result.final_keyword_count,
    )

    frequency_df = report_payloads.keyword_frequency_df.copy()
    raw_frequency_df = report_payloads.raw_frequency_df.copy()
    summary = _build_summary(
        config=config,
        input_mode=input_mode,
        load_result=load_result,
        collection_result=collection_result,
        crawl_result=crawl_result,
        text_result=text_result,
        analysis_result=analysis_result,
        output_dir=output_dir,
        execution_mode=execution_mode,
    )

    wordcloud_path_or_bytes: Path | bytes = report_paths.wordcloud_path
    if return_wordcloud_bytes:
        wordcloud_path_or_bytes = report_payloads.wordcloud_bytes

    return PipelineResult(
        config=config,
        summary=summary,
        frequency_df=frequency_df,
        url_audit_df=url_audit_df,
        wordcloud_path_or_bytes=wordcloud_path_or_bytes,
        artifacts=artifacts,
        crawl_result=crawl_result,
        text_result=text_result,
        analysis_result=analysis_result,
        report_paths=report_paths,
        report_payloads=report_payloads,
        load_result=load_result,
        collection_result=collection_result,
        raw_frequency_df=raw_frequency_df,
    )


def _validate_reanalysis_config(config: AppConfig) -> list[str]:
    errors: list[str] = []

    input_mode = str(config.get("input_mode", "domain"))
    if input_mode not in {"xlsx", "domain", "url_list"}:
        errors.append('input_mode は "xlsx" / "domain" / "url_list" のいずれかを指定してください')

    font_path = config.get_path("font_path")
    if font_path is None or not font_path.exists() or not font_path.is_file():
        display_value = config.get("font_path", "<未設定>")
        errors.append(f"日本語フォントを解決できませんでした: {display_value}。font_path を指定するか、packages.txt で fonts-noto-cjk を導入してください")

    use_default_stopwords = bool(config.get("use_default_stopwords", True))
    if use_default_stopwords:
        stopwords_default = config.get_path("stopwords_default_file")
        if stopwords_default is None or not stopwords_default.exists() or not stopwords_default.is_file():
            display_value = config.get("stopwords_default_file", "<未設定>")
            errors.append(f"ストップワードファイルが見つかりません: {display_value}")

    use_user_stopwords_file = bool(config.get("use_user_stopwords_file", True))
    if use_user_stopwords_file:
        stopwords_user = config.get_path("stopwords_user_file")
        if stopwords_user is None or not stopwords_user.exists() or not stopwords_user.is_file():
            display_value = config.get("stopwords_user_file", "<未設定>")
            errors.append(f"ユーザー追加用ストップワードファイルが見つかりません: {display_value}")

    return errors


def _count_runtime_stopwords(config: AppConfig) -> int:
    additional_stopwords = config.get("additional_stopwords", [])
    normalized: set[str] = set()
    if isinstance(additional_stopwords, list):
        normalized |= {str(item).strip() for item in additional_stopwords if str(item).strip()}

    additional_stopwords_text = str(config.get("additional_stopwords_text", ""))
    for part in additional_stopwords_text.replace("，", ",").replace("、", ",").replace("\r", "\n").split("\n"):
        for item in part.split(","):
            stripped = str(item).strip()
            if stripped:
                normalized.add(stripped)
    return len(normalized)


def _count_forced_compounds(config: AppConfig) -> int:
    normalized: set[str] = set()

    forced_compounds = config.get("forced_compounds", [])
    if isinstance(forced_compounds, list):
        normalized |= {str(item).strip() for item in forced_compounds if str(item).strip()}

    forced_compounds_text = str(config.get("forced_compounds_text", ""))
    for part in forced_compounds_text.replace("，", ",").replace("、", ",").replace("\r", "\n").split("\n"):
        for item in part.split(","):
            stripped = str(item).strip()
            if stripped:
                normalized.add(stripped)
    return len(normalized)


def _coerce_config(
    settings: Mapping[str, Any] | AppConfig,
    *,
    base_dir: str | Path | None,
    config_path: str | Path | None,
) -> AppConfig:
    if isinstance(settings, AppConfig):
        return settings

    try:
        return build_config_from_settings(
            settings,
            base_dir=base_dir or PROJECT_ROOT,
            config_path=config_path,
        )
    except ConfigError:
        raise
    except Exception as exc:  # pragma: no cover - 設定入力の異常系吸収
        raise PipelineError(f"settings から AppConfig を構築できませんでした: {exc}") from exc


def _coerce_url_list_text(config: AppConfig) -> str:
    raw_text = str(config.get("url_list_text", "")).strip()
    if raw_text:
        return raw_text

    input_urls = config.get("input_urls", [])
    if isinstance(input_urls, list):
        return "\n".join(str(item).strip() for item in input_urls if str(item).strip())

    return ""


def _coerce_optional_int(value: Any) -> int | None:
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    if parsed <= 0:
        return None
    return parsed


def _build_summary(
    *,
    config: AppConfig,
    input_mode: str,
    load_result: UrlLoadResult | None,
    collection_result: DomainUrlCollectionResult | None,
    crawl_result: CrawlBatchResult,
    text_result: TextExtractionBatchResult,
    analysis_result: KeywordAnalysisResult,
    output_dir: Path,
    execution_mode: str,
) -> dict[str, Any]:
    total_target_count = 0
    if input_mode in {"xlsx", "url_list"} and load_result is not None:
        total_target_count = load_result.target_count
    if input_mode == "domain" and collection_result is not None:
        total_target_count = collection_result.target_count

    count_404 = crawl_result.count_errors_by_kind("http_404") + crawl_result.count_errors_by_kind("not_found")
    count_410 = crawl_result.count_errors_by_kind("gone")

    summary: dict[str, Any] = {
        "input_mode": input_mode,
        "execution_mode": execution_mode,
        "total_target_count": total_target_count,
        "crawl_success_count": crawl_result.success_count,
        "crawl_error_count": crawl_result.error_count,
        "count_404": count_404,
        "count_410": count_410,
        "robots_blocked_count": crawl_result.robots_blocked_count,
        "noindex_count": text_result.noindex_count,
        "included_count": text_result.included_count,
        "final_keyword_count": analysis_result.final_keyword_count,
        "final_keyword_total": analysis_result.total_final_token_count,
        "use_default_stopwords": bool(config.get("use_default_stopwords", True)),
        "runtime_stopword_count": _count_runtime_stopwords(config),
        "compound_mode": bool(config.get("compound_mode", True)),
        "forced_compound_count": _count_forced_compounds(config),
        "resolved_font_path": str(config.get_path("font_path") or ""),
        "raw_keyword_count": analysis_result.raw_keyword_count,
        "raw_keyword_total": analysis_result.total_raw_token_count,
        "analyzed_page_count": analysis_result.analyzed_page_count,
        "has_keywords": analysis_result.has_keywords,
        "output_dir": output_dir,
        "config_path": config.config_path,
    }

    if load_result is not None:
        summary.update(
            {
                "input_count": load_result.input_count,
                "skipped_count": load_result.skipped_count,
            }
        )
    if collection_result is not None:
        summary.update(
            {
                "sitemap_source_count": collection_result.sitemap_source_count,
                "discovered_count": collection_result.discovered_count,
                "wp_api_source_count": collection_result.wp_api_source_count,
                "wp_api_target_count": collection_result.wp_api_target_count,
                "wp_api_used": collection_result.wp_api_used,
                "fallback_used": collection_result.fallback_used,
                "fallback_message": collection_result.fallback_message,
            }
        )

    return summary


def _format_missing_dependency_message(module_name: str | None) -> str:
    module_label = module_name or "不明なモジュール"
    return (
        f"必要なライブラリが不足しています: {module_label}\n"
        "先に仮想環境を有効化し、次を実行してください。\n"
        "pip install -r requirements.txt"
    )
