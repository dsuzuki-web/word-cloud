from __future__ import annotations

import argparse
import sys
from pathlib import Path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "site_keyword_analyzer は、公開ページの本文テキストを取得し、\n"
            "日本語の頻出キーワード頻度表とワードクラウドを作成する CLI ツールです。"
        ),
        epilog=(
            "実行例:\n"
            "  設定チェックだけ行う\n"
            "    python main.py --check-config\n\n"
            "  xlsx モードで実行する\n"
            "    1) config.yaml の input_mode を \"xlsx\" にする\n"
            "    2) input_xlsx / sheet_name / url_column を合わせる\n"
            "    3) python main.py\n\n"
            "  domain モードで実行する\n"
            "    1) config.yaml の input_mode を \"domain\" にする\n"
            "    2) domain_url / max_pages / max_depth を設定する\n"
            "    3) python main.py"
        ),
        formatter_class=argparse.RawTextHelpFormatter,
        add_help=False,
    )
    parser._positionals.title = "位置引数"
    parser._optionals.title = "オプション"
    parser.add_argument(
        "-h",
        "--help",
        action="help",
        help="このヘルプを表示して終了する",
    )
    parser.add_argument(
        "--config",
        default="config.yaml",
        help="読み込む設定ファイルのパス（デフォルト: config.yaml）",
    )
    parser.add_argument(
        "--check-config",
        action="store_true",
        help="設定ファイルの妥当性だけを確認して終了する",
    )
    return parser


def _format_missing_dependency_message(module_name: str | None) -> str:
    module_label = module_name or "不明なモジュール"
    return (
        f"必要なライブラリが不足しています: {module_label}\n"
        "先に仮想環境を有効化し、次を実行してください。\n"
        "pip install -r requirements.txt"
    )


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    try:
        from src.config_loader import ConfigError, load_config, validate_config
    except ModuleNotFoundError as exc:
        print(_format_missing_dependency_message(exc.name), file=sys.stderr)
        return 1

    try:
        config = load_config(args.config)
    except ConfigError as exc:
        print(f"設定ファイルの読み込みに失敗しました: {exc}", file=sys.stderr)
        return 1

    errors = validate_config(config)
    if errors:
        print("設定エラーが見つかりました。以下を修正してください。", file=sys.stderr)
        for error in errors:
            print(f"- {error}", file=sys.stderr)
        return 1

    if args.check_config:
        print("設定チェックOK")
        return 0

    try:
        from src.analyzer import AnalysisError, analyze_keywords
        from src.crawler import CrawlError, crawl_urls
        from src.reporter import ReportError, save_analysis_reports
        from src.text_extractor import TextExtractionError, analyze_crawl_results
        from src.url_collector import UrlCollectionError, collect_urls_from_domain, save_domain_collection_results
        from src.url_loader import UrlLoadError, load_urls_from_excel, save_url_load_results
        from src.utils import create_run_output_dir, setup_logging, write_dataframe_csv, write_run_config
    except ModuleNotFoundError as exc:
        print(_format_missing_dependency_message(exc.name), file=sys.stderr)
        return 1

    output_root = config.get_path("output_root")
    if output_root is None:
        print("output_root の設定が不正です。config.yaml を確認してください。", file=sys.stderr)
        return 1

    try:
        output_dir = create_run_output_dir(output_root)
        logger = setup_logging(output_dir / "run.log")
        config_snapshot_path = write_run_config(config, output_dir)
    except OSError as exc:
        print(f"出力フォルダまたはログファイルの作成に失敗しました: {exc}", file=sys.stderr)
        return 1

    logger.info("site_keyword_analyzer を初期化しました")
    logger.info("設定ファイル: %s", config.config_path)
    logger.info("出力フォルダ: %s", output_dir)
    logger.info("設定スナップショット: %s", config_snapshot_path)

    input_mode = config.get("input_mode")
    load_result = None
    collection_result = None
    target_csv_path = None
    skipped_csv_path = None
    sitemap_sources_path = None
    discovered_urls_path = None

    if input_mode == "xlsx":
        try:
            logger.info("ExcelからURL一覧を読み込みます")
            load_result = load_urls_from_excel(config)
        except UrlLoadError as exc:
            logger.error("Excel URL 読み込みに失敗しました: %s", exc)
            print(f"Excel URL 読み込みに失敗しました: {exc}", file=sys.stderr)
            return 1

        try:
            target_csv_path, skipped_csv_path = save_url_load_results(load_result, output_dir)
        except OSError as exc:
            logger.error("URL一覧CSVの保存に失敗しました: %s", exc)
            print(f"URL一覧CSVの保存に失敗しました: {exc}", file=sys.stderr)
            return 1

        target_records = load_result.target_records
        logger.info("target_urls.csv を保存しました: %s", target_csv_path)
        logger.info("url_load_skipped.csv を保存しました: %s", skipped_csv_path)
        logger.info(
            "URL前処理が完了しました。採用件数=%s, 除外件数=%s",
            load_result.target_count,
            load_result.skipped_count,
        )
    elif input_mode == "domain":
        try:
            logger.info("domain モードのURL収集を開始します")
            collection_result = collect_urls_from_domain(config, logger=logger)
        except UrlCollectionError as exc:
            logger.error("domain モードのURL収集に失敗しました: %s", exc)
            print(f"domain モードのURL収集に失敗しました: {exc}", file=sys.stderr)
            return 1

        try:
            target_csv_path, sitemap_sources_path, discovered_urls_path = save_domain_collection_results(
                collection_result,
                output_dir,
            )
        except OSError as exc:
            logger.error("domain モードのCSV保存に失敗しました: %s", exc)
            print(f"domain モードのCSV保存に失敗しました: {exc}", file=sys.stderr)
            return 1

        target_records = collection_result.target_records
        logger.info("target_urls.csv を保存しました: %s", target_csv_path)
        logger.info("sitemap_sources.csv を保存しました: %s", sitemap_sources_path)
        logger.info("discovered_urls.csv を保存しました: %s", discovered_urls_path)
        logger.info(
            "domain モードのURL収集が完了しました。採用件数=%s, sitemap調査件数=%s, 発見URL件数=%s",
            collection_result.target_count,
            collection_result.sitemap_source_count,
            collection_result.discovered_count,
        )
        if collection_result.fallback_used and collection_result.fallback_message:
            logger.info(collection_result.fallback_message)
            print(collection_result.fallback_message)
    else:
        print('input_mode は "xlsx" または "domain" を指定してください', file=sys.stderr)
        return 1

    try:
        logger.info("URLのHTTP取得を開始します")
        crawl_result = crawl_urls(target_records, config, logger=logger)
    except CrawlError as exc:
        logger.error("HTTP取得の前処理に失敗しました: %s", exc)
        print(f"HTTP取得の前処理に失敗しました: {exc}", file=sys.stderr)
        return 1

    try:
        crawl_success_path = write_dataframe_csv(crawl_result.to_success_dataframe(), output_dir / "crawl_success.csv")
        crawl_errors_path = write_dataframe_csv(crawl_result.to_error_dataframe(), output_dir / "crawl_errors.csv")
        robots_blocked_path = write_dataframe_csv(
            crawl_result.to_robots_blocked_dataframe(),
            output_dir / "robots_blocked_urls.csv",
        )
    except OSError as exc:
        logger.error("クロール結果CSVの保存に失敗しました: %s", exc)
        print(f"クロール結果CSVの保存に失敗しました: {exc}", file=sys.stderr)
        return 1

    logger.info("crawl_success.csv を保存しました: %s", crawl_success_path)
    logger.info("crawl_errors.csv を保存しました: %s", crawl_errors_path)
    logger.info("robots_blocked_urls.csv を保存しました: %s", robots_blocked_path)
    logger.info(
        "HTTP取得が完了しました。成功=%s, 失敗=%s, robots除外=%s",
        crawl_result.success_count,
        crawl_result.error_count,
        crawl_result.robots_blocked_count,
    )

    try:
        logger.info("本文抽出と noindex 判定を開始します")
        text_result = analyze_crawl_results(crawl_result, config, logger=logger)
    except TextExtractionError as exc:
        logger.error("本文抽出に失敗しました: %s", exc)
        print(f"本文抽出に失敗しました: {exc}", file=sys.stderr)
        return 1

    try:
        url_audit_path = write_dataframe_csv(text_result.to_url_audit_dataframe(), output_dir / "url_audit.csv")
        noindex_path = write_dataframe_csv(text_result.to_noindex_dataframe(), output_dir / "noindex_urls.csv")
        extracted_texts_path = None
        if bool(config.get("save_extracted_text", False)):
            extracted_texts_path = write_dataframe_csv(
                text_result.to_extracted_texts_dataframe(),
                output_dir / "extracted_texts.csv",
            )
    except OSError as exc:
        logger.error("本文抽出結果CSVの保存に失敗しました: %s", exc)
        print(f"本文抽出結果CSVの保存に失敗しました: {exc}", file=sys.stderr)
        return 1

    logger.info("url_audit.csv を保存しました: %s", url_audit_path)
    logger.info("noindex_urls.csv を保存しました: %s", noindex_path)
    if extracted_texts_path is not None:
        logger.info("extracted_texts.csv を保存しました: %s", extracted_texts_path)
    logger.info(
        "本文抽出が完了しました。noindex検出=%s, 集計対象=%s",
        text_result.noindex_count,
        text_result.included_count,
    )

    try:
        logger.info("形態素解析と頻度集計を開始します")
        analysis_result = analyze_keywords(text_result, config, logger=logger)
    except AnalysisError as exc:
        logger.error("形態素解析に失敗しました: %s", exc)
        print(f"形態素解析に失敗しました: {exc}", file=sys.stderr)
        return 1

    try:
        logger.info("頻度表とワードクラウドの保存を開始します")
        report_paths = save_analysis_reports(
            analysis_result,
            config=config,
            output_dir=output_dir,
            logger=logger,
        )
    except ReportError as exc:
        logger.error("頻度表またはワードクラウドの出力に失敗しました: %s", exc)
        print(f"頻度表またはワードクラウドの出力に失敗しました: {exc}", file=sys.stderr)
        return 1

    logger.info("raw_frequency_before_user_stopwords.csv を保存しました: %s", report_paths.raw_frequency_csv_path)
    logger.info("keyword_frequency.csv を保存しました: %s", report_paths.keyword_frequency_csv_path)
    logger.info("keyword_frequency.xlsx を保存しました: %s", report_paths.keyword_frequency_xlsx_path)
    logger.info("wordcloud.png を保存しました: %s", report_paths.wordcloud_path)
    logger.info(
        "形態素解析が完了しました。解析ページ数=%s, finalキーワード数=%s",
        analysis_result.analyzed_page_count,
        analysis_result.final_keyword_count,
    )

    _print_output_paths(
        output_dir=output_dir,
        config_snapshot_path=config_snapshot_path,
        target_csv_path=target_csv_path,
        skipped_csv_path=skipped_csv_path,
        sitemap_sources_path=sitemap_sources_path,
        discovered_urls_path=discovered_urls_path,
        crawl_success_path=crawl_success_path,
        crawl_errors_path=crawl_errors_path,
        robots_blocked_path=robots_blocked_path,
        url_audit_path=url_audit_path,
        noindex_path=noindex_path,
        extracted_texts_path=extracted_texts_path,
        raw_frequency_csv_path=report_paths.raw_frequency_csv_path,
        keyword_frequency_csv_path=report_paths.keyword_frequency_csv_path,
        keyword_frequency_xlsx_path=report_paths.keyword_frequency_xlsx_path,
        wordcloud_path=report_paths.wordcloud_path,
    )

    total_target_count = load_result.target_count if input_mode == "xlsx" else collection_result.target_count
    count_404 = crawl_result.count_errors_by_kind("http_404") + crawl_result.count_errors_by_kind("not_found")
    count_410 = crawl_result.count_errors_by_kind("gone")

    _print_summary(
        input_mode=input_mode,
        total_target_count=total_target_count,
        crawl_success_count=crawl_result.success_count,
        count_404=count_404,
        count_410=count_410,
        robots_blocked_count=crawl_result.robots_blocked_count,
        noindex_count=text_result.noindex_count,
        included_count=text_result.included_count,
        output_dir=output_dir,
        analysis_result=analysis_result,
        load_result=load_result,
        collection_result=collection_result,
    )
    return 0


def _print_output_paths(
    *,
    output_dir: Path,
    config_snapshot_path: Path,
    target_csv_path: Path | None,
    skipped_csv_path: Path | None,
    sitemap_sources_path: Path | None,
    discovered_urls_path: Path | None,
    crawl_success_path: Path,
    crawl_errors_path: Path,
    robots_blocked_path: Path,
    url_audit_path: Path,
    noindex_path: Path,
    extracted_texts_path: Path | None,
    raw_frequency_csv_path: Path,
    keyword_frequency_csv_path: Path,
    keyword_frequency_xlsx_path: Path,
    wordcloud_path: Path,
) -> None:
    print("\n=== 出力ファイル ===")
    print(f"出力先フォルダ: {output_dir}")
    print(f"run.log: {output_dir / 'run.log'}")
    print(f"run_config.json: {config_snapshot_path}")
    if target_csv_path is not None:
        print(f"target_urls.csv: {target_csv_path}")
    if skipped_csv_path is not None:
        print(f"url_load_skipped.csv: {skipped_csv_path}")
    if sitemap_sources_path is not None:
        print(f"sitemap_sources.csv: {sitemap_sources_path}")
    if discovered_urls_path is not None:
        print(f"discovered_urls.csv: {discovered_urls_path}")
    print(f"crawl_success.csv: {crawl_success_path}")
    print(f"crawl_errors.csv: {crawl_errors_path}")
    print(f"robots_blocked_urls.csv: {robots_blocked_path}")
    print(f"url_audit.csv: {url_audit_path}")
    print(f"noindex_urls.csv: {noindex_path}")
    if extracted_texts_path is not None:
        print(f"extracted_texts.csv: {extracted_texts_path}")
    print(f"raw_frequency_before_user_stopwords.csv: {raw_frequency_csv_path}")
    print(f"keyword_frequency.csv: {keyword_frequency_csv_path}")
    print(f"keyword_frequency.xlsx: {keyword_frequency_xlsx_path}")
    print(f"wordcloud.png: {wordcloud_path}")


def _print_summary(
    *,
    input_mode: str,
    total_target_count: int,
    crawl_success_count: int,
    count_404: int,
    count_410: int,
    robots_blocked_count: int,
    noindex_count: int,
    included_count: int,
    output_dir: Path,
    analysis_result,
    load_result=None,
    collection_result=None,
) -> None:
    print("\n=== 実行サマリー ===")
    if input_mode == "xlsx":
        print("モード: xlsx")
        print(f"入力URL総数: {load_result.input_count}")
        print(f"除外URL件数: {load_result.skipped_count}")
    else:
        print("モード: domain")
        print(f"sitemap調査件数: {collection_result.sitemap_source_count}")
        print(f"発見URL件数: {collection_result.discovered_count}")
    print(f"対象URL総数: {total_target_count}")
    print(f"取得成功件数: {crawl_success_count}")
    print(f"404件数: {count_404}")
    print(f"410件数: {count_410}")
    print(f"robots.txt で除外した件数: {robots_blocked_count}")
    print(f"noindex 検出件数: {noindex_count}")
    print(f"集計対象件数: {included_count}")
    print(f"final キーワード種類数: {analysis_result.final_keyword_count}")
    print(f"final キーワード総数: {analysis_result.total_final_token_count}")
    print(f"出力先フォルダ: {output_dir}")
    if not analysis_result.has_keywords:
        print("有効なキーワードが抽出できませんでした")


if __name__ == "__main__":
    raise SystemExit(main())
