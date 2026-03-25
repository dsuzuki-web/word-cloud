# site_keyword_analyzer

公開されているWebページから本文テキストを取得し、日本語の頻出キーワードを集計して、ワードクラウドと頻度表を作るための Python CLI ツールです。

主な用途:
- 競合サイトのトピカルオーソリティ調査
- 自社サイトの頻出語チェック
- noindex や robots.txt を含む URL 監査
- 記事群・サイト群の専門性のざっくり把握

## できること

- `xlsx` モード
  - Excel に書いた URL 一覧を読み込んで分析します。
- `domain` モード
  - WordPress サイトでは `wp-json/wp/v2/posts` を先に試し、記事URLを直接取得します。
  - 取得できなかった場合だけ、従来どおり `robots.txt` / `sitemap.xml` / BFS へフォールバックします。
- 本文抽出
  - `trafilatura` を優先し、必要に応じて BeautifulSoup でフォールバックします。
- noindex 判定
  - `meta robots` / `meta googlebot` / `X-Robots-Tag` を確認します。
- 形態素解析
  - Janome で日本語名詞を抽出し、ストップワードを除外して集計します。
  - 複合語優先モードを ON にすると、連続名詞や名詞+接尾をなるべく 1語へ再結合します。
  - 強制複合語辞書に登録した語は、常に 1語として扱います。
- 出力
  - `keyword_frequency.csv`
  - `keyword_frequency.xlsx`
  - `wordcloud.png`
  - `url_audit.csv`
  - `robots_blocked_urls.csv`
  - `noindex_urls.csv`
  - ほか監査用 CSV 一式

---

## ディレクトリ構成

```text
site_keyword_analyzer/
├─ main.py
├─ requirements.txt
├─ README.md
├─ config.yaml
├─ config.example.yaml
├─ config/
│  ├─ stopwords_ja.txt
│  └─ user_stopwords.txt
├─ input/
│  └─ urls_sample.xlsx        # サンプル入力（初回確認用）
├─ output/
│  └─ YYYYMMDD_HHMMSS/
├─ tools/
│  └─ create_sample_input.py
└─ src/
   ├─ analyzer.py
   ├─ config_loader.py
   ├─ crawler.py
   ├─ link_extractor.py
   ├─ reporter.py
   ├─ robots_utils.py
   ├─ text_extractor.py
   ├─ tokenizer_ja.py
   ├─ url_collector.py
   ├─ url_loader.py
   └─ utils.py
```

---


## Streamlit UI を使う

1ページ完結の UI でも実行できます。

```bash
streamlit run app.py
```

`streamlit` コマンドが見つからない環境では、次でも起動できます。

```bash
python -m streamlit run app.py
```

現在の画面仕様:
- 最上部で入力モード切替
- 下に入力欄 / 詳細設定 / 実行ボタン / 結果表示
- スマホでも横スクロールしにくい中央寄せレイアウト
- 狭い画面では横並び要素を縦積みにしやすい CSS を適用
- 結果画面で実行サマリー / ワードクラウド / キーワード頻度表を確認可能
- 成功URL数 / 失敗URL数 / 集計対象URL数を画面表示
- CSV / XLSX / PNG をその場でダウンロード可能

URL一覧モードでは、改行区切りのURLをそのまま貼り付けて実行できます。
前処理プレビューで「採用URL」と「除外URL / 除外理由」も確認できます。

ドメインモードでは、UI から次を変更できます。
- 最大取得件数
- クロール深さ
- 同一ドメインのみ / サブドメインも含める
- PDF 除外
- タイトルを本文に含める
- WordPress REST API を優先してURL収集する
- 除外URLパターン

また、`取得対象URLをプレビューする` ボタンで、分析実行前に収集対象 URL 一覧を確認できます。

詳細設定では、不要語の ON/OFF と追加不要語に加えて、次も UI から操作できます。
- 複合語優先モード ON/OFF
- 強制複合語辞書（カンマ区切り / 改行区切り両対応）

実行ボタンは 2種類あります。
- `再取得して実行`
  - URL収集 / クロール / 本文抽出からやり直します。
- `抽出済みテキストから再生成`
  - 前回抽出した本文を `session_state` に保持し、不要語や複合語辞書を変えても再クロールせず再解析だけ実行します。

設定プリセットも UI から扱えます。
- `現在設定を JSON 保存`
  - 入力モード / 最大取得件数 / クロール深さ / URL除外パターン / stopwords 設定 / 追加 stopwords / 複合語優先モード / 強制複合語辞書 / 最小文字数 / タイトル抽出 / PDF除外 を JSON で保存します。
- `JSON を読み込んで設定に反映`
  - 書き出した JSON をアップロードすると、現在の UI 設定へ復元できます。
  - ダウンロードとアップロードだけで完結するので、Streamlit Community Cloud でも使えます。

---

## 事前準備

### 1. 仮想環境を作成する

Windows:

```bash
python -m venv .venv
.venv\Scripts\activate
```

macOS / Linux:

```bash
python -m venv .venv
source .venv/bin/activate
```

### 2. ライブラリをインストールする

```bash
pip install -r requirements.txt
```

### 3. 日本語フォントについて

- `font_path` を空欄のままにすると、日本語フォントを自動検出します。
- Streamlit Community Cloud では `packages.txt` の `fonts-noto-cjk` を使う前提です。
- ローカルで別フォントを使いたい場合だけ、`config.yaml` または UI の `フォントパス` に明示してください。

---

## 最初に試すおすすめ手順

初回は `xlsx` モードで、サンプル入力を使って動作確認するのが安全です。

### 手順

1. サンプル Excel を作る

```bash
python tools/create_sample_input.py
```

2. `config.example.yaml` を見ながら `config.yaml` を調整する
   - 最初は `input_mode: "xlsx"`
   - `input_xlsx: "input/urls_sample.xlsx"`
   - `sheet_name: "Sheet1"`
   - `url_column: "URL"`

3. 設定チェックをする

```bash
python main.py --check-config
```

4. 実行する

```bash
python main.py
```

---

## config.yaml の使い方

### まず覚える設定

- `input_mode`
  - `xlsx`: Excel の URL 一覧を使う
  - `domain`: ドメインを自動収集して分析する
- `input_xlsx`
  - Excel ファイルの場所
- `sheet_name`
  - 読み込むシート名
- `url_column`
  - URL が入っている列名
- `domain_url`
  - domain モードの開始 URL
- `font_path`
  - 日本語ワードクラウド用フォント
- `max_pages`
  - 最大取得ページ数
- `max_depth`
  - BFS での内部リンク探索深さ
- `exclude_url_patterns`
  - 除外したい URL パターン
- `stopwords_default_file`
  - 既定ストップワード
- `stopwords_user_file`
  - 後から追加するストップワード

設定例は `config.example.yaml` に日本語コメント付きで入っています。

---

## xlsx モードの実行手順

### Excel の形式

- シート名: `Sheet1` など
- URL 列名: `URL`

例:

| URL |
|---|
| https://example.com/ |
| https://www.iana.org/domains/reserved |
| https://www.iana.org/help/example-domains |

### 実行例

```bash
python main.py
```

### よくあるポイント

- URL 列名が `URL` でない場合は `config.yaml` の `url_column` を合わせてください。
- `xlsx` モードでは、空欄・重複・非 HTTP URL・PDF などは自動除外されます。

---

## domain モードの実行手順

`config.yaml` の例:

```yaml
input_mode: "domain"
domain_url: "https://example.com/"
prefer_wordpress_api: true
include_subdomains: false
max_pages: 50
max_depth: 2
```

実行:

```bash
python main.py
```

### domain モードの流れ

1. `prefer_wordpress_api: true` の場合、まず `wp-json/wp/v2/posts` を試す
2. WordPress API で不足または取得失敗なら `robots.txt` を確認
3. `Sitemap:` を探す
4. 見つからなければ `sitemap.xml` / `sitemap_index.xml` を試す
5. sitemap だけでは不足する場合は BFS で内部リンクを補完
6. 本文抽出 → 形態素解析 → 頻度表出力

### 初回のおすすめ設定

最初は負荷と確認コストを抑えるため、以下がおすすめです。

```yaml
max_pages: 30
max_depth: 1
respect_robots_txt: true
```

---

## --check-config の使い方

設定ファイルだけ先に確認したいときは、次を実行します。

```bash
python main.py --check-config
```

問題がなければ次のように表示されます。

```text
設定チェックOK
```

---

## 日本語フォントの設定例

ワードクラウドは日本語フォントが正しく設定されていないと豆腐化します。

### Windows

```yaml
font_path: "C:/Windows/Fonts/msgothic.ttc"
```

または

```yaml
font_path: "C:/Windows/Fonts/YuGothM.ttc"
```

### macOS

```yaml
font_path: "/System/Library/Fonts/ヒラギノ角ゴシック W3.ttc"
```

### Linux

Noto CJK 系フォントの例:

```yaml
font_path: "/usr/share/fonts/opentype/noto/NotoSansCJK-Regular.ttc"
```

必要ならインストール例:

```bash
sudo apt-get install fonts-noto-cjk
```

---

## 出力ファイルの意味

実行ごとに `output/YYYYMMDD_HHMMSS/` フォルダが作られます。

### 主要成果物

- `wordcloud.png`
  - ワードクラウド画像
- `keyword_frequency.csv`
  - 最終的な頻度表（UTF-8 with BOM）
- `keyword_frequency.xlsx`
  - Excel 形式の頻度表
- `raw_frequency_before_user_stopwords.csv`
  - ユーザー追加ストップワード適用前の頻度表

### URL 監査・クロール関連

- `target_urls.csv`
  - 実際に分析対象として採用した URL 一覧
- `url_load_skipped.csv`
  - `xlsx` モードで除外した URL 一覧
- `sitemap_sources.csv`
  - `domain` モードで調査した sitemap 一覧
- `discovered_urls.csv`
  - `domain` モードで発見した URL 一覧（BFS を含む）
- `crawl_success.csv`
  - 取得成功 URL 一覧
- `crawl_errors.csv`
  - 取得失敗 URL 一覧
- `robots_blocked_urls.csv`
  - robots.txt により取得しなかった URL 一覧
- `noindex_urls.csv`
  - noindex が付いていた URL 一覧
- `url_audit.csv`
  - 各 URL の監査結果を一覧化したもの
- `extracted_texts.csv`
  - 抽出本文（`save_extracted_text: true` のときのみ）
- `run.log`
  - 実行ログ
- `run_config.json`
  - 実行時設定のスナップショット

---

## robots_blocked_urls.csv の見方

主な列:

- `url`
- `normalized_url`
- `robots_decision`
- `fetched_at`

このファイルを見ると、**なぜ本文抽出も noindex 判定もされていないのか** がわかります。
`robots_decision` が `blocked_by_robots_txt` の URL は、取得禁止のため本文解析の対象外です。

---

## noindex_urls.csv の見方

主な列:

- `url`
- `final_url`
- `status_code`
- `noindex_source`
- `noindex_value`
- `included_in_analysis`
- `detected_at`

### `noindex_source` の例

- `meta_robots`
- `meta_googlebot`
- `x_robots_tag`

`included_in_analysis=false` なら、`exclude_noindex_pages: true` によって集計から除外されています。

---

## url_audit.csv の見方

主な列:

- `url`
- `final_url`
- `status_code`
- `robots_txt`
- `noindex_status`
- `noindex_source`
- `content_type`
- `extracted_chars`
- `extraction_success`
- `included_in_analysis`
- `excluded_reason`

このファイルを見れば、各 URL が最終的にどう扱われたかを一括で追えます。

### `excluded_reason` の代表例

- `robots_blocked`
- `status_404`
- `status_410`
- `timeout`
- `non_html`
- `extract_failed`
- `low_text`
- `noindex_excluded`

---

## 404 / 410 / robots / noindex / low_text の意味

- `404`
  - ページが見つからない URL です。
- `410`
  - 削除済みページです。404 よりも「意図的に削除した」意味が強いです。
- `robots`
  - `robots.txt` で取得禁止のため、本文取得していません。
- `noindex`
  - 検索エンジンにインデックスさせない指定です。
- `low_text`
  - 本文は取れたが、文字数が少なすぎるため集計対象外です。

---

## ストップワード調整の考え方

このツールで一番大事なのは、**不要語を少しずつ減らしていくこと**です。

### 既定ストップワード

- `config/stopwords_ja.txt`
  - 最初から入れておく不要語

### ユーザー追加ストップワード

- `config/user_stopwords.txt`
  - 実行結果を見て後から足す不要語

例えば以下のような語が目立つなら、`config/user_stopwords.txt` に1行1語で追加します。

```text
ホーム
メニュー
ログイン
検索
お知らせ
```

変更後は、もう一度 `python main.py` を実行してください。

---

## よくあるエラーと対処法

### 1. 指定したフォントファイルが存在しません

原因:
- `font_path` が OS に合っていない

対処:
- Windows / macOS / Linux のフォント例を参考に `config.yaml` を修正する

### 2. input/urls.xlsx が見つかりません

原因:
- `xlsx` モードで指定した Excel がない

対処:
- ファイル名と場所を確認する
- 初回は `python tools/create_sample_input.py` でサンプルを作る

### 3. URL 列が見つかりません

原因:
- Excel の列名が `URL` ではない

対処:
- Excel 側の列名を `URL` にする
- もしくは `config.yaml` の `url_column` を合わせる

### 4. シート名が見つかりません

原因:
- `sheet_name` の値が違う

対処:
- Excel のシート名を確認して `config.yaml` を修正する

### 5. 有効なキーワードが抽出できませんでした

原因の例:
- noindex 除外が多い
- low_text が多い
- ストップワードを入れすぎた
- 対象ページ数が少ない

対処:
- `url_audit.csv` を確認する
- `config/user_stopwords.txt` を見直す
- `max_pages` を少し増やす
- 除外パターンを見直す

---

## サンプル入力の作り方

```bash
python tools/create_sample_input.py
```

既定では次のファイルが作成されます。

```text
input/urls_sample.xlsx
```

別の場所に作りたい場合:

```bash
python tools/create_sample_input.py --output input/my_sample.xlsx
```

---

## 注意点

- 公開ページだけを対象にしてください。
- サイトの利用規約・robots.txt を尊重してください。
- JavaScript 依存ページでは本文取得がうまくいかないことがあります。
- まずは少ないページ数で確認してから対象を増やすのがおすすめです。



## Streamlit UI の不要語設定

- `config/stopwords_ja.txt` をデフォルト不要語として ON/OFF できます。
- 追加不要語はテキストエリアに入力すると、その実行時にだけ反映されます。
- 追加不要語はカンマ区切りと改行区切りの両方に対応しています。

---

## Streamlit Community Cloud へ載せる

このリポジトリは `app.py` をそのままエントリーポイントにできます。

### 必要ファイル
- `app.py`
- `requirements.txt`
- `packages.txt`
- `config/stopwords_ja.txt`
- `config/user_stopwords.txt`

### デプロイ時のポイント
- Community Cloud のエントリーポイントに `app.py` を指定してください。
- `requirements.txt` は `app.py` と同じ階層に置いてあります。
- `packages.txt` に `fonts-noto-cjk` を入れてあるため、日本語ワードクラウド用フォントを自動解決できます。
- Python 3.12 を選ぶと、Community Cloud の既定値に合わせやすいです。

### ローカル確認コマンド
```bash
streamlit run app.py
```

### よくある確認項目
- 画面は開くが分析時に失敗する場合は、`pip install -r requirements.txt` が完了しているか確認してください。
- フォントエラーが出る場合は `packages.txt` が反映されているか、または `font_path` を明示してください。
- iPhone で横幅が窮屈な場合でも、主要ボタンとダウンロード操作は縦積みに崩れにくい構成にしています。
