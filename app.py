from __future__ import annotations

from pathlib import Path
from datetime import datetime, timezone
import json
import re
from typing import Any

import pandas as pd

try:
    import streamlit as st
except ModuleNotFoundError as exc:  # pragma: no cover
    raise SystemExit(
        "streamlit がインストールされていません。\n"
        "先に `pip install -r requirements.txt` を実行してください。"
    ) from exc

from services.pipeline import (
    PipelineError,
    PipelineValidationError,
    preview_domain_collection,
    rerun_analysis_from_text_result,
    run_analysis,
)
from src.config_loader import AppConfig, ConfigError, build_config_from_settings, load_config
from src.font_utils import resolve_font_path
from src.url_loader import UrlLoadResult, load_urls_from_text

PROJECT_ROOT = Path(__file__).resolve().parent
CONFIG_PATH = PROJECT_ROOT / "config.yaml"
DEFAULT_STOPWORDS_PATH = PROJECT_ROOT / "config" / "stopwords_ja.txt"
USER_STOPWORDS_PATH = PROJECT_ROOT / "config" / "user_stopwords.txt"
DEFAULT_STOPWORD_PREVIEW_LIMIT = 12

MODE_LABEL_TO_VALUE = {
    "ドメインモード": "domain",
    "URL一覧モード": "url_list",
}
MODE_VALUE_TO_LABEL = {value: label for label, value in MODE_LABEL_TO_VALUE.items()}
PRESET_SCHEMA_VERSION = 1

EMPTY_FREQUENCY_DF = pd.DataFrame(columns=["順位", "キーワード", "出現数"])
EMPTY_AUDIT_DF = pd.DataFrame(columns=["url", "status", "included_in_analysis", "text_length"])
EMPTY_TARGET_URLS_DF = pd.DataFrame(columns=["original_url", "normalized_url", "source_type"])
EMPTY_SKIPPED_URLS_DF = pd.DataFrame(columns=["original_url", "normalized_url", "skip_reason"])
EMPTY_DOMAIN_TARGET_DF = pd.DataFrame(columns=["original_url", "normalized_url", "source_type", "discovered_from"])
EMPTY_SITEMAP_SOURCES_DF = pd.DataFrame(columns=["sitemap_url", "discovered_from", "fetch_status", "url_count"])
EMPTY_DISCOVERED_URLS_DF = pd.DataFrame(columns=["normalized_url", "source_type", "discovered_from", "depth"])
EMPTY_WP_API_SOURCES_DF = pd.DataFrame(columns=["api_url", "discovered_from", "fetch_status", "url_count", "page_count"])
EMPTY_SUMMARY_DF = pd.DataFrame(columns=["項目", "値"])


def main() -> None:
    st.set_page_config(
        page_title="サイトキーワードアナライザー",
        page_icon="☁️",
        layout="centered",
        initial_sidebar_state="collapsed",
    )
    _apply_mobile_friendly_style()
    defaults = _load_default_settings()
    _init_session_state(defaults)
    _apply_pending_preset_import()

    st.title("サイトキーワードアナライザー")
    st.caption("スマホでも扱いやすい 1ページ完結 UI。上から入力し、そのまま結果を確認できます。")

    mode_label = st.radio(
        "入力モード",
        options=list(MODE_LABEL_TO_VALUE.keys()),
        horizontal=True,
        key="ui_mode_label",
    )
    input_mode = MODE_LABEL_TO_VALUE[mode_label]

    st.subheader("入力")
    _render_input_section(input_mode)

    st.subheader("詳細設定")
    with st.expander("詳細設定を開く", expanded=False):
        _render_detail_section(input_mode)

    st.subheader("設定プリセット")
    _render_preset_section()

    st.caption("再取得はクロールからやり直し、再生成は前回抽出した本文を再利用します。")
    action_cols = st.columns(2)
    rerun_clicked = action_cols[0].button("再取得して実行", type="primary", width="stretch")
    regenerate_clicked = action_cols[1].button(
        "抽出済みテキストから再生成",
        width="stretch",
        disabled=not _has_reanalysis_cache(),
    )
    if rerun_clicked:
        _handle_run(input_mode=input_mode)
    if regenerate_clicked:
        _handle_regenerate()

    st.subheader("結果")
    _render_result_section()



def _apply_mobile_friendly_style() -> None:
    st.markdown(
        """
        <style>
          html, body, [data-testid="stAppViewContainer"], [data-testid="stVerticalBlock"] {
            overflow-x: hidden;
          }
          .block-container {
            max-width: 720px;
            padding-top: 1rem;
            padding-bottom: 3rem;
            padding-left: 1rem;
            padding-right: 1rem;
          }
          div[data-testid="stButton"] > button,
          div[data-testid="stDownloadButton"] > button {
            width: 100%;
            min-height: 3rem;
            border-radius: 0.75rem;
            font-size: 1rem;
          }
          textarea, input {
            font-size: 16px !important;
          }
          [data-testid="stMetric"] {
            background: rgba(49, 51, 63, 0.04);
            padding: 0.75rem;
            border-radius: 0.75rem;
          }
          @media (max-width: 640px) {
            div[data-testid="stHorizontalBlock"] {
              flex-direction: column;
              gap: 0.75rem;
            }
            div[data-testid="column"] {
              width: 100% !important;
              flex: 1 1 100% !important;
            }
          }
        </style>
        """,
        unsafe_allow_html=True,
    )



def _load_default_settings() -> dict[str, Any]:
    try:
        loaded: AppConfig = load_config(CONFIG_PATH)
        return dict(loaded.data)
    except ConfigError:
        fallback = build_config_from_settings({}, base_dir=PROJECT_ROOT)
        return dict(fallback.data)



def _init_session_state(defaults: dict[str, Any]) -> None:
    include_subdomains_default = bool(defaults.get("include_subdomains", False))
    same_domain_only_default = bool(defaults.get("same_domain_only", not include_subdomains_default))
    initial_values: dict[str, Any] = {
        "ui_mode_label": "ドメインモード",
        "domain_url": str(defaults.get("domain_url", "https://example.com/")),
        "url_list_text": str(defaults.get("url_list_text", "")),
        "max_pages": int(defaults.get("max_pages", 200)),
        "max_depth": int(defaults.get("max_depth", 3)),
        "max_list_urls": int(defaults.get("max_list_urls", 50)),
        "same_domain_only": same_domain_only_default,
        "include_subdomains": include_subdomains_default,
        "exclude_pdf": bool(defaults.get("exclude_pdf", True)),
        "include_title": bool(defaults.get("include_title", False)),
        "prefer_wordpress_api": bool(defaults.get("prefer_wordpress_api", True)),
        "respect_robots_txt": bool(defaults.get("respect_robots_txt", True)),
        "request_timeout_sec": float(defaults.get("request_timeout_sec", 15.0)),
        "min_text_length": int(defaults.get("min_text_length", 100)),
        "detect_noindex": bool(defaults.get("detect_noindex", True)),
        "exclude_noindex_pages": bool(defaults.get("exclude_noindex_pages", False)),
        "top_n_wordcloud": int(defaults.get("top_n_wordcloud", 200)),
        "font_path": str(defaults.get("font_path", "")),
        "exclude_patterns_text": _list_to_multiline(defaults.get("exclude_url_patterns", [])),
        "use_default_stopwords": bool(defaults.get("use_default_stopwords", True)),
        "additional_stopwords_text": _load_initial_user_stopwords_text(defaults),
        "compound_mode": bool(defaults.get("compound_mode", True)),
        "forced_compounds_text": _load_initial_forced_compounds_text(defaults),
        "preset_notice": None,
        "preset_error": None,
        "pending_preset_import_bytes": None,
        "pending_preset_import_name": None,
        "last_result": None,
        "last_error": None,
        "last_notice": None,
        "last_domain_preview": None,
        "last_preview_error": None,
        "last_preview_notice": None,
        "analysis_cache": None,
    }
    for key, value in initial_values.items():
        if key not in st.session_state:
            st.session_state[key] = value



def _render_input_section(input_mode: str) -> None:
    if input_mode == "domain":
        st.text_input(
            "ドメインまたは開始URL",
            key="domain_url",
            placeholder="https://example.com/",
            help="指定した URL を起点に URL を収集し、その本文を分析します。",
        )
        st.caption("例: https://example.com/ または https://example.com/blog/")

        preview_clicked = st.button("取得対象URLをプレビューする", width="stretch", key="preview_domain_urls")
        if preview_clicked:
            _handle_domain_preview()

        _render_domain_preview_section()
        return

    st.text_area(
        "URL一覧（1行に1URL）",
        key="url_list_text",
        height=180,
        placeholder="https://example.com/article-1\nhttps://example.com/article-2",
        help="1行に1URLで貼り付けると、正規化・重複除外・除外判定を行ってから分析します。",
    )

    preview_result = _build_url_list_preview()
    non_empty_input_count = _count_non_empty_lines(st.session_state.get("url_list_text", ""))
    st.caption(
        f"入力行数: {non_empty_input_count} / 採用予定: {preview_result.target_count} / 除外予定: {preview_result.skipped_count}"
    )

    with st.expander("URL前処理プレビュー", expanded=False):
        st.markdown("**採用予定URL**")
        st.dataframe(
            _result_records_to_dataframe(preview_result.target_records, kind="target"),
            width="stretch",
            hide_index=True,
        )
        st.markdown("**除外URL**")
        st.dataframe(
            _result_records_to_dataframe(preview_result.skipped_records, kind="skipped"),
            width="stretch",
            hide_index=True,
        )



def _render_domain_preview_section() -> None:
    preview_error = st.session_state.get("last_preview_error")
    if preview_error:
        st.error(preview_error)

    preview_notice = st.session_state.get("last_preview_notice")
    if preview_notice:
        st.info(preview_notice)

    preview = st.session_state.get("last_domain_preview")
    if not preview:
        st.caption("プレビューを実行すると、取得対象URL一覧がここに表示されます。")
        return

    summary = preview.get("summary", {})
    st.caption(
        " / ".join(
            [
                f"採用URL: {summary.get('target_count', 0)}件",
                f"WP API: {summary.get('wp_api_target_count', 0)}件",
                f"sitemap確認: {summary.get('sitemap_source_count', 0)}件",
                f"発見URL: {summary.get('discovered_count', 0)}件",
            ]
        )
    )
    if summary.get("fallback_used") and summary.get("fallback_message"):
        st.caption(f"補足: {summary.get('fallback_message')}")

    with st.expander("取得対象URLプレビュー", expanded=False):
        st.markdown("**採用URL**")
        st.dataframe(preview.get("target_urls_df", EMPTY_DOMAIN_TARGET_DF), width="stretch", hide_index=True)
        if not preview.get("wp_api_sources_df", EMPTY_WP_API_SOURCES_DF).empty:
            st.markdown("**WP REST API 調査結果**")
            st.dataframe(preview.get("wp_api_sources_df", EMPTY_WP_API_SOURCES_DF), width="stretch", hide_index=True)
        st.markdown("**sitemap 調査結果**")
        st.dataframe(preview.get("sitemap_sources_df", EMPTY_SITEMAP_SOURCES_DF), width="stretch", hide_index=True)
        st.markdown("**発見URL一覧**")
        st.dataframe(preview.get("discovered_urls_df", EMPTY_DISCOVERED_URLS_DF), width="stretch", hide_index=True)



def _render_detail_section(input_mode: str) -> None:
    if input_mode == "domain":
        st.number_input("最大取得件数", min_value=1, step=1, key="max_pages")
        st.number_input("クロール深さ", min_value=0, step=1, key="max_depth")
        st.checkbox(
            "同一ドメインのみを対象にする",
            key="same_domain_only",
            help="ON なら example.com のみ。OFF にするとサブドメインも収集対象にします。",
        )
        st.checkbox(
            "PDF を除外する",
            key="exclude_pdf",
            help="ON の場合、.pdf を収集対象から外します。",
        )
        st.checkbox(
            "タイトルを本文に含める",
            key="include_title",
            help="ON にすると HTML の title 要素を本文テキストの先頭へ付与します。",
        )
        st.checkbox(
            "WordPress REST API を優先してURL収集する",
            key="prefer_wordpress_api",
            help="ON の場合、wp-json/wp/v2/posts を先に試し、取得できなければ sitemap / BFS にフォールバックします。",
        )
    else:
        st.number_input("URL一覧モード最大件数", min_value=1, step=1, key="max_list_urls")

    st.checkbox("robots.txt を尊重する", key="respect_robots_txt")
    st.number_input("リクエストタイムアウト（秒）", min_value=1.0, step=1.0, key="request_timeout_sec")
    st.number_input("最小本文文字数", min_value=1, step=10, key="min_text_length")
    st.checkbox("noindex を検出する", key="detect_noindex")
    st.checkbox("noindex ページを集計から除外する", key="exclude_noindex_pages")
    st.number_input("ワードクラウド最大語数", min_value=1, step=10, key="top_n_wordcloud")

    st.markdown("**不要語設定**")
    default_stopwords = _read_stopwords_file(DEFAULT_STOPWORDS_PATH)
    st.checkbox(
        "デフォルト不要語を使用する",
        key="use_default_stopwords",
        help="config/stopwords_ja.txt を読み込んで不要語として除外します。",
    )
    if default_stopwords:
        preview_values = "、".join(default_stopwords[:DEFAULT_STOPWORD_PREVIEW_LIMIT])
        st.caption(
            f"デフォルト不要語: {len(default_stopwords)}語 / 例: {preview_values}"
            + (" ..." if len(default_stopwords) > DEFAULT_STOPWORD_PREVIEW_LIMIT else "")
        )
    else:
        st.caption("config/stopwords_ja.txt の不要語を読み込めませんでした。")

    st.text_area(
        "追加不要語（カンマ区切り・改行区切り両対応）",
        key="additional_stopwords_text",
        height=120,
        help="この実行で追加したい不要語を入力してください。カンマ区切りと改行区切りの両方に対応しています。",
    )
    st.caption(
        f"追加不要語: {len(_parse_stopwords_text(str(st.session_state.get('additional_stopwords_text', ''))))}語"
    )

    st.markdown("**複合語設定**")
    st.checkbox(
        "複合語優先モード",
        key="compound_mode",
        help="ON の場合、連続する名詞や名詞+接尾をなるべく 1語として再結合します。",
    )
    st.text_area(
        "強制複合語辞書（カンマ区切り・改行区切り両対応）",
        key="forced_compounds_text",
        height=120,
        help="ここで入力した語は、複合語優先モードの ON/OFF に関係なく、常に 1語として扱います。",
    )
    st.caption(
        f"強制複合語: {len(_parse_compounds_text(str(st.session_state.get('forced_compounds_text', ''))))}語"
    )

    st.text_area(
        "除外URLパターン（1行に1パターン）",
        key="exclude_patterns_text",
        height=120,
        help="例: /tag/ や /category/ のような、最初から除外したい URL パターンを指定します。",
    )
    st.text_input(
        "フォントパス（空欄で自動選択）",
        key="font_path",
        help="空欄なら、プロジェクト内フォントや OS の日本語フォントを自動で探します。",
        placeholder="未入力なら自動選択",
    )
    st.caption(f"利用予定フォント: {_resolve_font_display()}")


def _resolve_font_display() -> str:
    preferred = str(st.session_state.get("font_path", "")).strip()
    resolved = resolve_font_path(preferred=preferred, base_dir=PROJECT_ROOT)
    if resolved is None:
        return "未検出（font_path を指定するか、packages.txt の fonts-noto-cjk を利用してください）"
    return str(resolved)



def _render_preset_section() -> None:
    preset_error = st.session_state.get("preset_error")
    if preset_error:
        st.error(preset_error)

    preset_notice = st.session_state.get("preset_notice")
    if preset_notice:
        st.info(preset_notice)

    uploaded_preset = st.file_uploader(
        "JSON プリセットをアップロード",
        type=["json"],
        key="preset_import_file",
        accept_multiple_files=False,
        help="書き出した設定 JSON を読み込むと、現在の UI 設定へ反映します。",
    )

    preset_cols = st.columns(2)
    preset_cols[0].download_button(
        "現在設定を JSON 保存",
        data=_build_preset_download_bytes(),
        file_name=_build_preset_filename(),
        mime="application/json",
        width="stretch",
        key="download_preset_json",
    )

    apply_disabled = uploaded_preset is None
    if preset_cols[1].button(
        "JSON を読み込んで設定に反映",
        width="stretch",
        disabled=apply_disabled,
        key="apply_preset_json",
    ):
        if uploaded_preset is None:
            st.session_state["preset_error"] = "読み込む JSON ファイルを選択してください。"
            st.session_state["preset_notice"] = None
        else:
            _queue_preset_import(uploaded_preset.getvalue(), uploaded_preset.name)
            _rerun_streamlit()

    st.caption("JSON の保存と読込だけで復元できるため、Streamlit Community Cloud でもそのまま使えます。")


def _apply_pending_preset_import() -> None:
    payload = st.session_state.get("pending_preset_import_bytes")
    if not payload:
        return

    preset_name = str(st.session_state.get("pending_preset_import_name", "")).strip()
    st.session_state["pending_preset_import_bytes"] = None
    st.session_state["pending_preset_import_name"] = None

    try:
        preset_settings = _load_preset_from_bytes(payload)
        _apply_preset_settings(preset_settings)
    except ValueError as exc:
        st.session_state["preset_error"] = f"設定プリセットの読込に失敗しました: {exc}"
        st.session_state["preset_notice"] = None
        return

    st.session_state["preset_error"] = None
    st.session_state["preset_notice"] = f"設定プリセットを読み込みました: {preset_name or 'preset.json'}"
    st.session_state["last_domain_preview"] = None
    st.session_state["last_preview_error"] = None
    st.session_state["last_preview_notice"] = None


def _queue_preset_import(payload: bytes, filename: str | None = None) -> None:
    st.session_state["pending_preset_import_bytes"] = payload
    st.session_state["pending_preset_import_name"] = filename or "preset.json"
    st.session_state["preset_error"] = None
    st.session_state["preset_notice"] = None


def _build_preset_download_bytes() -> bytes:
    payload = _build_preset_payload_from_state()
    return json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")


def _build_preset_payload_from_state() -> dict[str, Any]:
    mode_label = str(st.session_state.get("ui_mode_label", "ドメインモード"))
    input_mode = MODE_LABEL_TO_VALUE.get(mode_label, "domain")
    settings = {
        "input_mode": input_mode,
        "max_pages": int(st.session_state.get("max_pages", 200)),
        "max_depth": int(st.session_state.get("max_depth", 3)),
        "max_list_urls": int(st.session_state.get("max_list_urls", 50)),
        "exclude_url_patterns": _multiline_to_list(str(st.session_state.get("exclude_patterns_text", ""))),
        "use_default_stopwords": bool(st.session_state.get("use_default_stopwords", True)),
        "additional_stopwords": _parse_stopwords_text(str(st.session_state.get("additional_stopwords_text", ""))),
        "compound_mode": bool(st.session_state.get("compound_mode", True)),
        "forced_compounds": _parse_compounds_text(str(st.session_state.get("forced_compounds_text", ""))),
        "min_text_length": int(st.session_state.get("min_text_length", 100)),
        "include_title": bool(st.session_state.get("include_title", False)),
        "exclude_pdf": bool(st.session_state.get("exclude_pdf", True)),
        "prefer_wordpress_api": bool(st.session_state.get("prefer_wordpress_api", True)),
    }
    return {
        "app": "site_keyword_analyzer",
        "schema_version": PRESET_SCHEMA_VERSION,
        "exported_at": datetime.now(timezone.utc).isoformat(),
        "settings": settings,
    }


def _load_preset_from_bytes(payload: bytes) -> dict[str, Any]:
    try:
        decoded = payload.decode("utf-8-sig")
    except UnicodeDecodeError as exc:
        raise ValueError("UTF-8 の JSON ファイルを指定してください。") from exc

    try:
        data = json.loads(decoded)
    except json.JSONDecodeError as exc:
        raise ValueError(f"JSON の解析に失敗しました: {exc}") from exc

    settings = _extract_preset_settings(data)
    stopwords_group = settings.get("stopwords") if isinstance(settings.get("stopwords"), dict) else {}
    compound_group = settings.get("compound") if isinstance(settings.get("compound"), dict) else {}

    current_mode = MODE_LABEL_TO_VALUE.get(str(st.session_state.get("ui_mode_label", "ドメインモード")), "domain")
    current_max_pages = int(st.session_state.get("max_pages", 200))
    current_max_depth = int(st.session_state.get("max_depth", 3))
    current_max_list_urls = int(st.session_state.get("max_list_urls", 50))
    current_patterns = _multiline_to_list(str(st.session_state.get("exclude_patterns_text", "")))
    current_stopwords_text = str(st.session_state.get("additional_stopwords_text", ""))
    current_forced_compounds_text = str(st.session_state.get("forced_compounds_text", ""))
    current_min_text_length = int(st.session_state.get("min_text_length", 100))
    current_include_title = bool(st.session_state.get("include_title", False))
    current_exclude_pdf = bool(st.session_state.get("exclude_pdf", True))
    current_prefer_wordpress_api = bool(st.session_state.get("prefer_wordpress_api", True))
    current_use_default_stopwords = bool(st.session_state.get("use_default_stopwords", True))
    current_compound_mode = bool(st.session_state.get("compound_mode", True))

    return {
        "input_mode": _normalize_preset_input_mode(settings.get("input_mode", settings.get("mode", current_mode))),
        "max_pages": _coerce_int_setting(settings.get("max_pages", current_max_pages), default=current_max_pages, minimum=1),
        "max_depth": _coerce_int_setting(settings.get("max_depth", current_max_depth), default=current_max_depth, minimum=0),
        "max_list_urls": _coerce_int_setting(settings.get("max_list_urls", current_max_list_urls), default=current_max_list_urls, minimum=1),
        "exclude_url_patterns": _normalize_text_list(settings.get("exclude_url_patterns", current_patterns)),
        "use_default_stopwords": _coerce_bool_setting(
            settings.get("use_default_stopwords", stopwords_group.get("use_default", current_use_default_stopwords)),
            default=current_use_default_stopwords,
        ),
        "additional_stopwords": _normalize_text_list(
            settings.get(
                "additional_stopwords",
                settings.get("additional_stopwords_text", stopwords_group.get("additional", stopwords_group.get("additional_text", current_stopwords_text))),
            )
        ),
        "compound_mode": _coerce_bool_setting(
            settings.get("compound_mode", compound_group.get("mode", current_compound_mode)),
            default=current_compound_mode,
        ),
        "forced_compounds": _normalize_text_list(
            settings.get(
                "forced_compounds",
                settings.get("forced_compounds_text", compound_group.get("forced", compound_group.get("forced_text", current_forced_compounds_text))),
            )
        ),
        "min_text_length": _coerce_int_setting(
            settings.get("min_text_length", current_min_text_length),
            default=current_min_text_length,
            minimum=1,
        ),
        "include_title": _coerce_bool_setting(settings.get("include_title", current_include_title), default=current_include_title),
        "exclude_pdf": _coerce_bool_setting(settings.get("exclude_pdf", current_exclude_pdf), default=current_exclude_pdf),
        "prefer_wordpress_api": _coerce_bool_setting(settings.get("prefer_wordpress_api", current_prefer_wordpress_api), default=current_prefer_wordpress_api),
    }


def _extract_preset_settings(data: Any) -> dict[str, Any]:
    if not isinstance(data, dict):
        raise ValueError("JSON の最上位はオブジェクト形式で指定してください。")

    settings = data.get("settings", data)
    if not isinstance(settings, dict):
        raise ValueError("settings はオブジェクト形式で指定してください。")
    return settings


def _apply_preset_settings(preset_settings: dict[str, Any]) -> None:
    input_mode = str(preset_settings.get("input_mode", "domain"))
    mode_label = MODE_VALUE_TO_LABEL.get(input_mode, MODE_VALUE_TO_LABEL["domain"])

    st.session_state["ui_mode_label"] = mode_label
    st.session_state["max_pages"] = int(preset_settings.get("max_pages", st.session_state.get("max_pages", 200)))
    st.session_state["max_depth"] = int(preset_settings.get("max_depth", st.session_state.get("max_depth", 3)))
    st.session_state["max_list_urls"] = int(preset_settings.get("max_list_urls", st.session_state.get("max_list_urls", 50)))
    st.session_state["exclude_patterns_text"] = _list_to_multiline(preset_settings.get("exclude_url_patterns", []))
    st.session_state["use_default_stopwords"] = bool(preset_settings.get("use_default_stopwords", True))
    st.session_state["additional_stopwords_text"] = _list_to_multiline(preset_settings.get("additional_stopwords", []))
    st.session_state["compound_mode"] = bool(preset_settings.get("compound_mode", True))
    st.session_state["forced_compounds_text"] = _list_to_multiline(preset_settings.get("forced_compounds", []))
    st.session_state["min_text_length"] = int(preset_settings.get("min_text_length", st.session_state.get("min_text_length", 100)))
    st.session_state["include_title"] = bool(preset_settings.get("include_title", st.session_state.get("include_title", False)))
    st.session_state["exclude_pdf"] = bool(preset_settings.get("exclude_pdf", st.session_state.get("exclude_pdf", True)))
    st.session_state["prefer_wordpress_api"] = bool(preset_settings.get("prefer_wordpress_api", st.session_state.get("prefer_wordpress_api", True)))


def _normalize_text_list(value: Any) -> list[str]:
    if isinstance(value, list):
        parts = [str(item).strip() for item in value if str(item).strip()]
    elif isinstance(value, str):
        parts = _parse_stopwords_text(value)
    else:
        parts = []

    deduped: list[str] = []
    seen: set[str] = set()
    for part in parts:
        lowered = part.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        deduped.append(part)
    return deduped


def _coerce_bool_setting(value: Any, *, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return default
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        normalized = value.strip().casefold()
        if normalized in {"1", "true", "yes", "y", "on"}:
            return True
        if normalized in {"0", "false", "no", "n", "off"}:
            return False
    return default


def _coerce_int_setting(value: Any, *, default: int, minimum: int) -> int:
    try:
        coerced = int(value)
    except (TypeError, ValueError):
        return default
    return max(minimum, coerced)


def _normalize_preset_input_mode(value: Any) -> str:
    if value is None:
        return "domain"

    raw = str(value).strip()
    if not raw:
        return "domain"

    if raw in MODE_VALUE_TO_LABEL:
        return raw
    if raw in MODE_LABEL_TO_VALUE:
        return MODE_LABEL_TO_VALUE[raw]

    normalized = raw.casefold().replace("-", "_").replace(" ", "_")
    alias_map = {
        "domain": "domain",
        "url_list": "url_list",
        "urllist": "url_list",
        "url一覧モード": "url_list",
        "url一覧": "url_list",
    }
    if normalized in alias_map:
        return alias_map[normalized]

    raise ValueError('input_mode は "domain" または "url_list" を指定してください。')


def _build_preset_filename() -> str:
    mode_label = str(st.session_state.get("ui_mode_label", "ドメインモード"))
    input_mode = MODE_LABEL_TO_VALUE.get(mode_label, "domain")
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"site_keyword_preset_{input_mode}_{timestamp}.json"


def _rerun_streamlit() -> None:
    rerun_callable = getattr(st, "rerun", None) or getattr(st, "experimental_rerun", None)
    if callable(rerun_callable):
        rerun_callable()



def _handle_domain_preview() -> None:
    st.session_state["last_preview_error"] = None
    st.session_state["last_preview_notice"] = None

    settings = _build_domain_settings()
    try:
        with st.spinner("取得対象URLをプレビューしています..."):
            preview_result = preview_domain_collection(settings, base_dir=PROJECT_ROOT)
    except PipelineValidationError as exc:
        st.session_state["last_domain_preview"] = None
        st.session_state["last_preview_error"] = "\n".join(exc.errors)
        return
    except (PipelineError, ConfigError) as exc:
        st.session_state["last_domain_preview"] = None
        st.session_state["last_preview_error"] = str(exc)
        return

    st.session_state["last_domain_preview"] = {
        "summary": dict(preview_result.summary),
        "target_urls_df": preview_result.target_urls_df,
        "sitemap_sources_df": preview_result.sitemap_sources_df,
        "discovered_urls_df": preview_result.discovered_urls_df,
        "wp_api_sources_df": preview_result.wp_api_sources_df,
    }
    st.session_state["last_preview_notice"] = "取得対象URLプレビューを更新しました。"



def _handle_run(*, input_mode: str) -> None:
    st.session_state["last_error"] = None
    st.session_state["last_notice"] = None

    settings = _build_domain_settings() if input_mode == "domain" else _build_url_list_settings()
    try:
        with st.spinner("URL収集からワードクラウド生成まで実行しています..."):
            result = run_analysis(settings, base_dir=PROJECT_ROOT, return_wordcloud_bytes=True)
    except PipelineValidationError as exc:
        st.session_state["last_result"] = None
        st.session_state["last_error"] = "\n".join(exc.errors)
        return
    except (PipelineError, ConfigError) as exc:
        st.session_state["last_result"] = None
        st.session_state["last_error"] = str(exc)
        return

    _store_pipeline_result(result)
    _store_analysis_cache(result)
    st.session_state["last_notice"] = "分析が完了しました。下に結果を表示しています。"



def _handle_regenerate() -> None:
    st.session_state["last_error"] = None
    st.session_state["last_notice"] = None

    cache = st.session_state.get("analysis_cache")
    if not isinstance(cache, dict) or cache.get("text_result") is None or cache.get("crawl_result") is None:
        st.session_state["last_error"] = "再生成できる抽出済みテキストがありません。先に『再取得して実行』を行ってください。"
        return

    settings = _build_reanalysis_settings_from_cache(cache)
    try:
        with st.spinner("抽出済みテキストから再生成しています..."):
            result = rerun_analysis_from_text_result(
                settings,
                text_result=cache["text_result"],
                crawl_result=cache["crawl_result"],
                load_result=cache.get("load_result"),
                collection_result=cache.get("collection_result"),
                base_dir=PROJECT_ROOT,
                return_wordcloud_bytes=True,
            )
    except PipelineValidationError as exc:
        st.session_state["last_result"] = None
        st.session_state["last_error"] = "\n".join(exc.errors)
        return
    except (PipelineError, ConfigError) as exc:
        st.session_state["last_result"] = None
        st.session_state["last_error"] = str(exc)
        return

    _store_pipeline_result(result)
    _store_analysis_cache(result)
    cache_mode = _input_mode_label(str(cache.get("input_mode", result.summary.get("input_mode", "domain"))))
    st.session_state["last_notice"] = f"抽出済みテキストから再生成しました。前回の本文抽出結果（{cache_mode}）を再利用しています。"



def _store_pipeline_result(result) -> None:
    target_urls_df = EMPTY_TARGET_URLS_DF.copy()
    skipped_urls_df = EMPTY_SKIPPED_URLS_DF.copy()
    sitemap_sources_df = EMPTY_SITEMAP_SOURCES_DF.copy()
    discovered_urls_df = EMPTY_DISCOVERED_URLS_DF.copy()
    wp_api_sources_df = EMPTY_WP_API_SOURCES_DF.copy()

    if result.load_result is not None:
        target_urls_df = _result_records_to_dataframe(result.load_result.target_records, kind="target")
        skipped_urls_df = _result_records_to_dataframe(result.load_result.skipped_records, kind="skipped")
    elif result.collection_result is not None:
        target_urls_df = result.collection_result.to_target_dataframe()
        sitemap_sources_df = result.collection_result.to_sitemap_sources_dataframe()
        discovered_urls_df = result.collection_result.to_discovered_dataframe()
        wp_api_sources_df = result.collection_result.to_wp_api_sources_dataframe()

    sorted_frequency_df = _sort_frequency_df(result.frequency_df)
    output_stem = _build_download_stem(result.summary)
    report_payloads = result.report_payloads

    st.session_state["last_result"] = {
        "summary": _serialize_summary(result.summary),
        "frequency_df": sorted_frequency_df,
        "url_audit_df": result.url_audit_df,
        "wordcloud_bytes": report_payloads.wordcloud_bytes,
        "keyword_csv_bytes": report_payloads.keyword_frequency_csv_bytes,
        "keyword_xlsx_bytes": report_payloads.keyword_frequency_xlsx_bytes,
        "target_urls_df": target_urls_df,
        "skipped_urls_df": skipped_urls_df,
        "sitemap_sources_df": sitemap_sources_df,
        "discovered_urls_df": discovered_urls_df,
        "wp_api_sources_df": wp_api_sources_df,
        "download_stem": output_stem,
    }



def _store_analysis_cache(result) -> None:
    st.session_state["analysis_cache"] = {
        "input_mode": str(result.summary.get("input_mode", result.config.get("input_mode", "domain"))),
        "config_data": dict(result.config.data),
        "crawl_result": result.crawl_result,
        "text_result": result.text_result,
        "load_result": result.load_result,
        "collection_result": result.collection_result,
    }



def _has_reanalysis_cache() -> bool:
    cache = st.session_state.get("analysis_cache")
    return isinstance(cache, dict) and cache.get("text_result") is not None and cache.get("crawl_result") is not None



def _build_reanalysis_settings_from_cache(cache: dict[str, Any]) -> dict[str, Any]:
    cached_settings = dict(cache.get("config_data", {}))
    cached_settings.update(_build_runtime_analysis_overrides())
    if "input_mode" not in cached_settings:
        cached_settings["input_mode"] = str(cache.get("input_mode", "domain"))
    return cached_settings



def _build_runtime_analysis_overrides() -> dict[str, Any]:
    return {
        "use_default_stopwords": bool(st.session_state.get("use_default_stopwords", True)),
        "use_user_stopwords_file": False,
        "additional_stopwords_text": str(st.session_state.get("additional_stopwords_text", "")),
        "additional_stopwords": _parse_stopwords_text(str(st.session_state.get("additional_stopwords_text", ""))),
        "compound_mode": bool(st.session_state.get("compound_mode", True)),
        "forced_compounds_text": str(st.session_state.get("forced_compounds_text", "")),
        "forced_compounds": _parse_compounds_text(str(st.session_state.get("forced_compounds_text", ""))),
        "top_n_wordcloud": int(st.session_state.get("top_n_wordcloud", 200)),
        "font_path": str(st.session_state.get("font_path", "")).strip(),
        "output_root": "output",
    }



def _build_domain_settings() -> dict[str, Any]:
    exclude_patterns = _multiline_to_list(st.session_state.get("exclude_patterns_text", ""))
    same_domain_only = bool(st.session_state.get("same_domain_only", True))
    return {
        "input_mode": "domain",
        "domain_url": str(st.session_state.get("domain_url", "")).strip(),
        "same_domain_only": same_domain_only,
        "include_subdomains": not same_domain_only,
        "exclude_pdf": bool(st.session_state.get("exclude_pdf", True)),
        "include_title": bool(st.session_state.get("include_title", False)),
        "prefer_wordpress_api": bool(st.session_state.get("prefer_wordpress_api", True)),
        "respect_robots_txt": bool(st.session_state.get("respect_robots_txt", True)),
        "max_pages": int(st.session_state.get("max_pages", 200)),
        "max_depth": int(st.session_state.get("max_depth", 3)),
        "request_timeout_sec": float(st.session_state.get("request_timeout_sec", 15.0)),
        "min_text_length": int(st.session_state.get("min_text_length", 100)),
        "detect_noindex": bool(st.session_state.get("detect_noindex", True)),
        "exclude_noindex_pages": bool(st.session_state.get("exclude_noindex_pages", False)),
        "top_n_wordcloud": int(st.session_state.get("top_n_wordcloud", 200)),
        "font_path": str(st.session_state.get("font_path", "")).strip(),
        "exclude_url_patterns": exclude_patterns,
        "use_default_stopwords": bool(st.session_state.get("use_default_stopwords", True)),
        "use_user_stopwords_file": False,
        "additional_stopwords_text": str(st.session_state.get("additional_stopwords_text", "")),
        "additional_stopwords": _parse_stopwords_text(str(st.session_state.get("additional_stopwords_text", ""))),
        "compound_mode": bool(st.session_state.get("compound_mode", True)),
        "forced_compounds_text": str(st.session_state.get("forced_compounds_text", "")),
        "forced_compounds": _parse_compounds_text(str(st.session_state.get("forced_compounds_text", ""))),
        "stopwords_default_file": "config/stopwords_ja.txt",
        "stopwords_user_file": "config/user_stopwords.txt",
        "output_root": "output",
    }



def _build_url_list_settings() -> dict[str, Any]:
    exclude_patterns = _multiline_to_list(st.session_state.get("exclude_patterns_text", ""))
    return {
        "input_mode": "url_list",
        "url_list_text": str(st.session_state.get("url_list_text", "")),
        "max_list_urls": int(st.session_state.get("max_list_urls", 50)),
        "respect_robots_txt": bool(st.session_state.get("respect_robots_txt", True)),
        "request_timeout_sec": float(st.session_state.get("request_timeout_sec", 15.0)),
        "min_text_length": int(st.session_state.get("min_text_length", 100)),
        "detect_noindex": bool(st.session_state.get("detect_noindex", True)),
        "exclude_noindex_pages": bool(st.session_state.get("exclude_noindex_pages", False)),
        "top_n_wordcloud": int(st.session_state.get("top_n_wordcloud", 200)),
        "font_path": str(st.session_state.get("font_path", "")).strip(),
        "exclude_url_patterns": exclude_patterns,
        "use_default_stopwords": bool(st.session_state.get("use_default_stopwords", True)),
        "use_user_stopwords_file": False,
        "additional_stopwords_text": str(st.session_state.get("additional_stopwords_text", "")),
        "additional_stopwords": _parse_stopwords_text(str(st.session_state.get("additional_stopwords_text", ""))),
        "compound_mode": bool(st.session_state.get("compound_mode", True)),
        "forced_compounds_text": str(st.session_state.get("forced_compounds_text", "")),
        "forced_compounds": _parse_compounds_text(str(st.session_state.get("forced_compounds_text", ""))),
        "stopwords_default_file": "config/stopwords_ja.txt",
        "stopwords_user_file": "config/user_stopwords.txt",
        "output_root": "output",
    }



def _render_result_section() -> None:
    last_error = st.session_state.get("last_error")
    if last_error:
        st.error(last_error)

    last_notice = st.session_state.get("last_notice")
    if last_notice:
        st.info(last_notice)

    result = st.session_state.get("last_result")
    if not result:
        st.markdown("**実行サマリー**")
        st.dataframe(EMPTY_SUMMARY_DF, width="stretch", hide_index=True)
        st.markdown("**ワードクラウド**")
        st.caption("分析後にここへワードクラウド画像が表示されます。")
        st.markdown("**キーワード頻度**")
        st.dataframe(EMPTY_FREQUENCY_DF, width="stretch", hide_index=True)
        st.markdown("**成功 / 失敗 / 集計対象**")
        st.caption("分析後に URL 件数サマリーが表示されます。")
        st.markdown("**ダウンロード**")
        st.caption("分析後に CSV / XLSX / PNG をダウンロードできます。")
        st.markdown("**URL監査プレビュー**")
        st.dataframe(EMPTY_AUDIT_DF, width="stretch", hide_index=True)
        st.markdown("**対象URLプレビュー**")
        st.dataframe(EMPTY_DOMAIN_TARGET_DF, width="stretch", hide_index=True)
        return

    summary = result["summary"]
    st.markdown("**実行サマリー**")
    summary_rows = [
        ("入力モード", _input_mode_label(str(summary.get("input_mode", "-")))),
        ("実行方式", _execution_mode_label(str(summary.get("execution_mode", "fresh")))),
        ("対象URL総数", summary.get("total_target_count", 0)),
        ("前処理除外件数", summary.get("skipped_count", 0)),
        ("取得成功件数", summary.get("crawl_success_count", 0)),
        ("取得失敗件数", summary.get("crawl_error_count", 0)),
        ("robots.txt 除外件数", summary.get("robots_blocked_count", 0)),
        ("noindex 検出件数", summary.get("noindex_count", 0)),
        ("集計対象件数", summary.get("included_count", 0)),
        ("キーワード種類数", summary.get("final_keyword_count", 0)),
        ("キーワード総数", summary.get("final_keyword_total", 0)),
        ("デフォルト不要語", "ON" if summary.get("use_default_stopwords", True) else "OFF"),
        ("追加不要語数", summary.get("runtime_stopword_count", 0)),
        ("複合語優先モード", "ON" if summary.get("compound_mode", True) else "OFF"),
        ("強制複合語数", summary.get("forced_compound_count", 0)),
        ("WP REST API 使用", "ON" if summary.get("wp_api_used", False) else "OFF"),
        ("WP REST API URL数", summary.get("wp_api_target_count", 0)),
        ("使用フォント", summary.get("resolved_font_path", "自動選択失敗")),
        ("出力フォルダ", summary.get("output_dir", "-")),
    ]
    summary_df = pd.DataFrame(summary_rows, columns=["項目", "値"])
    st.dataframe(summary_df, width="stretch", hide_index=True)

    st.markdown("**ワードクラウド**")
    st.image(result["wordcloud_bytes"], width="stretch")

    st.markdown("**キーワード頻度**")
    st.dataframe(result["frequency_df"], width="stretch", hide_index=True)

    st.markdown("**成功 / 失敗 / 集計対象**")
    metric_cols = st.columns(3)
    metric_cols[0].metric("成功URL数", int(summary.get("crawl_success_count", 0)))
    metric_cols[1].metric("失敗URL数", int(summary.get("crawl_error_count", 0)))
    metric_cols[2].metric("集計対象URL数", int(summary.get("included_count", 0)))

    st.markdown("**ダウンロード**")
    download_stem = str(result.get("download_stem", "site_keyword_analysis"))
    download_cols = st.columns(3)
    download_cols[0].download_button(
        "CSV をダウンロード",
        data=result["keyword_csv_bytes"],
        file_name=f"{download_stem}_keyword_frequency.csv",
        mime="text/csv",
        width="stretch",
        key="download_keyword_csv",
    )
    download_cols[1].download_button(
        "XLSX をダウンロード",
        data=result["keyword_xlsx_bytes"],
        file_name=f"{download_stem}_keyword_frequency.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        width="stretch",
        key="download_keyword_xlsx",
    )
    download_cols[2].download_button(
        "PNG をダウンロード",
        data=result["wordcloud_bytes"],
        file_name=f"{download_stem}_wordcloud.png",
        mime="image/png",
        width="stretch",
        key="download_wordcloud_png",
    )

    with st.expander("URL監査プレビュー", expanded=False):
        st.dataframe(result["url_audit_df"], width="stretch", hide_index=True)

    with st.expander("対象URLプレビュー", expanded=False):
        st.markdown("**採用URL**")
        st.dataframe(result.get("target_urls_df", EMPTY_DOMAIN_TARGET_DF), width="stretch", hide_index=True)
        if not result.get("skipped_urls_df", EMPTY_SKIPPED_URLS_DF).empty:
            st.markdown("**除外URL**")
            st.dataframe(result.get("skipped_urls_df", EMPTY_SKIPPED_URLS_DF), width="stretch", hide_index=True)
        if not result.get("wp_api_sources_df", EMPTY_WP_API_SOURCES_DF).empty:
            st.markdown("**WP REST API 調査結果**")
            st.dataframe(result.get("wp_api_sources_df", EMPTY_WP_API_SOURCES_DF), width="stretch", hide_index=True)
        if not result.get("sitemap_sources_df", EMPTY_SITEMAP_SOURCES_DF).empty:
            st.markdown("**sitemap 調査結果**")
            st.dataframe(result.get("sitemap_sources_df", EMPTY_SITEMAP_SOURCES_DF), width="stretch", hide_index=True)
        if not result.get("discovered_urls_df", EMPTY_DISCOVERED_URLS_DF).empty:
            st.markdown("**発見URL一覧**")
            st.dataframe(result.get("discovered_urls_df", EMPTY_DISCOVERED_URLS_DF), width="stretch", hide_index=True)



def _build_url_list_preview() -> UrlLoadResult:
    exclude_patterns = _multiline_to_list(st.session_state.get("exclude_patterns_text", ""))
    return load_urls_from_text(
        str(st.session_state.get("url_list_text", "")),
        exclude_patterns=exclude_patterns,
        max_urls=int(st.session_state.get("max_list_urls", 50)),
    )



def _result_records_to_dataframe(records: list[dict[str, str]], *, kind: str) -> pd.DataFrame:
    if kind == "target":
        if not records:
            return EMPTY_TARGET_URLS_DF.copy()
        return pd.DataFrame(records, columns=["original_url", "normalized_url", "source_type"])

    if not records:
        return EMPTY_SKIPPED_URLS_DF.copy()
    return pd.DataFrame(records, columns=["original_url", "normalized_url", "skip_reason"])



def _serialize_summary(summary: dict[str, Any]) -> dict[str, Any]:
    serialized: dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, Path):
            serialized[key] = str(value)
        else:
            serialized[key] = value
    return serialized



def _list_to_multiline(value: Any) -> str:
    if isinstance(value, list):
        return "\n".join(str(item) for item in value if str(item).strip())
    return ""



def _multiline_to_list(text: str) -> list[str]:
    return [line.strip() for line in text.splitlines() if line.strip()]



def _count_non_empty_lines(text: str) -> int:
    return len(_multiline_to_list(text))



def _sort_frequency_df(dataframe: pd.DataFrame) -> pd.DataFrame:
    if dataframe.empty:
        return EMPTY_FREQUENCY_DF.copy()

    sortable_columns = [column for column in ["出現数", "キーワード"] if column in dataframe.columns]
    if sortable_columns:
        ascending = [False if column == "出現数" else True for column in sortable_columns]
        sorted_df = dataframe.sort_values(sortable_columns, ascending=ascending, kind="mergesort").reset_index(drop=True)
    else:
        sorted_df = dataframe.reset_index(drop=True)

    if "順位" in sorted_df.columns:
        sorted_df = sorted_df.copy()
        sorted_df["順位"] = range(1, len(sorted_df) + 1)
    return sorted_df





def _load_initial_user_stopwords_text(defaults: dict[str, Any]) -> str:
    configured_path = str(defaults.get("stopwords_user_file", USER_STOPWORDS_PATH)).strip()
    candidate = Path(configured_path).expanduser()
    if not candidate.is_absolute():
        candidate = (PROJECT_ROOT / candidate).resolve()
    return "\n".join(_read_stopwords_file(candidate))


def _load_initial_forced_compounds_text(defaults: dict[str, Any]) -> str:
    configured_text = str(defaults.get("forced_compounds_text", "")).strip()
    if configured_text:
        return configured_text

    configured_list = defaults.get("forced_compounds", [])
    if isinstance(configured_list, list):
        return "\n".join(str(item) for item in configured_list if str(item).strip())
    return ""


def _read_stopwords_file(path: Path) -> list[str]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []

    stopwords: list[str] = []
    seen: set[str] = set()
    for line in lines:
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        normalized = stripped.casefold()
        if normalized in seen:
            continue
        seen.add(normalized)
        stopwords.append(stripped)
    return stopwords


def _parse_stopwords_text(text: str) -> list[str]:
    if not text:
        return []

    parts = re.split(r"[\r\n,，、]+", text)
    stopwords: list[str] = []
    seen: set[str] = set()
    for part in parts:
        normalized = part.strip()
        if not normalized:
            continue
        lowered = normalized.casefold()
        if lowered in seen:
            continue
        seen.add(lowered)
        stopwords.append(normalized)
    return stopwords


def _parse_compounds_text(text: str) -> list[str]:
    return _parse_stopwords_text(text)


def _input_mode_label(value: str) -> str:
    return {
        "domain": "ドメインモード",
        "url_list": "URL一覧モード",
        "xlsx": "Excelモード",
    }.get(value, value or "-")


def _execution_mode_label(value: str) -> str:
    return {
        "fresh": "再取得して実行",
        "cached_text": "抽出済みテキストから再生成",
    }.get(value, value or "-")


def _build_download_stem(summary: dict[str, Any]) -> str:
    input_mode = str(summary.get("input_mode", "analysis"))
    output_dir = str(summary.get("output_dir", "")).strip()
    output_name = Path(output_dir).name if output_dir else "latest"
    safe_mode = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in input_mode)
    safe_name = "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in output_name)
    return f"{safe_mode}_{safe_name}".strip("_") or "site_keyword_analysis"


if __name__ == "__main__":  # pragma: no cover
    main()
