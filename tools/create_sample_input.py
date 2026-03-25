from __future__ import annotations

import argparse
from pathlib import Path

from openpyxl import Workbook


SAMPLE_ROWS = [
    {
        "URL": "https://www.iana.org/domains/reserved",
        "メモ": "サンプル1: 実在する公開ページ",
    },
    {
        "URL": "https://www.iana.org/help/example-domains",
        "メモ": "サンプル2: 実在する公開ページ",
    },
    {
        "URL": "https://example.com/",
        "メモ": "サンプル3: 形式確認用のページ",
    },
]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="site_keyword_analyzer 用のサンプル Excel を作成します。",
    )
    parser.add_argument(
        "--output",
        default="input/urls_sample.xlsx",
        help="出力する Excel ファイルのパス（デフォルト: input/urls_sample.xlsx）",
    )
    return parser


def create_sample_workbook(output_path: Path) -> Path:
    output_path.parent.mkdir(parents=True, exist_ok=True)

    workbook = Workbook()
    worksheet = workbook.active
    worksheet.title = "Sheet1"

    worksheet.append(["URL", "メモ"])
    for row in SAMPLE_ROWS:
        worksheet.append([row["URL"], row["メモ"]])

    worksheet.column_dimensions["A"].width = 55
    worksheet.column_dimensions["B"].width = 28
    workbook.save(output_path)
    return output_path


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()

    output_path = Path(args.output).expanduser()
    if not output_path.is_absolute():
        output_path = (Path.cwd() / output_path).resolve()
    else:
        output_path = output_path.resolve()

    created_path = create_sample_workbook(output_path)
    print(f"サンプル入力ファイルを作成しました: {created_path}")
    print("シート名: Sheet1")
    print("URL列名: URL")
    print("次に config.yaml を xlsx モードへ合わせて python main.py を実行してください")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
