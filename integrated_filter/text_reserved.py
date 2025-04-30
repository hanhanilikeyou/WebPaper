#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import re
import argparse
from bs4 import BeautifulSoup

# ===== 配置区：英文文本专用 =====

# 样板区域（导航、侧栏、页眉、页脚等）匹配器
BOILERPLATE_SELECTORS = [
    {'name': 'header', 'attrs': {}},
    {'name': 'footer', 'attrs': {}},
    {'name': 'nav',    'attrs': {}},
    {'name': 'div',    'attrs': {'class': re.compile(r'navbar|sidebar|banner|ad|cookie|modal', re.I)}},
]

# 最小有效段落长度（字符数）
MIN_PARAGRAPH_LENGTH = 30

# 纯符号段落正则
SYMBOL_ONLY_PATTERN = re.compile(r'^[\W_]+$')

# 英文参考文献常见模式
REFERENCE_PATTERNS = [
    r'^\s*References\s*$',           # 单独行 “References”
    r'^\s*\[\d+\]\s*.+',             # “[1] ...”
    r'^\s*\(\d+\)\s*.+',             # “(1) ...”
    r'.+\(\d{4}[,;]?\s*[A-Za-z]+.*\)' # “Smith (2020)” 等
]

# 英文作者信息关键词（不区分大小写）
AUTHOR_INFO_KEYWORDS = [
    'Keywords', 'Corresponding author', 'Affiliation',
    'Funding', 'Acknowledgment', 'Author information'
]

# 英文页脚/脚注关键字
FOOTER_KEYWORDS = [
    'Privacy Policy', 'Terms of Service', 'Contact Us',
    '©', 'All rights reserved', 'Unsubscribe'
]


# ===== 清洗函数 =====

def clean_html(raw_html: str) -> str:
    """用 BeautifulSoup 去除 <script>/<style> 及样板元素，返回纯文本。"""
    soup = BeautifulSoup(raw_html, 'html5lib')
    # 删除脚本与样式
    for tag in soup(['script', 'style']):
        tag.decompose()
    # 删除样板区块
    for sel in BOILERPLATE_SELECTORS:
        for tag in soup.find_all(sel['name'], sel.get('attrs', {})):
            tag.decompose()
    # 提取纯文本，以换行为分隔符
    return soup.get_text('\n')


def filter_paragraphs(text: str) -> str:
    """
    拆行、去重、剔除短行/纯符号/参考文献/作者信息/页脚等，
    以及尾部脚注样式。
    """
    lines = [ln.strip() for ln in text.splitlines()]
    seen = set()
    filtered = []

    for line in lines:
        if not line or line in seen:
            continue
        seen.add(line)

        # 删除极短或纯符号的段落
        if len(line) < MIN_PARAGRAPH_LENGTH or SYMBOL_ONLY_PATTERN.match(line):
            continue

        # 删除参考文献相关行
        if any(re.match(pat, line) for pat in REFERENCE_PATTERNS):
            continue

        # 删除作者信息相关行
        low = line.lower()
        if any(kw.lower() in low for kw in AUTHOR_INFO_KEYWORDS):
            continue

        # 删除页脚/脚注相关行
        if any(kw in line for kw in FOOTER_KEYWORDS):
            continue

        filtered.append(line)

    # 删除尾部可能的脚注：若末尾两行都很短或以数字开头，则连续删掉
    while len(filtered) >= 2 and all(
        len(l) < MIN_PARAGRAPH_LENGTH or re.match(r'^\d+\s', l)
        for l in filtered[-2:]
    ):
        filtered = filtered[:-2]

    return '\n\n'.join(filtered)


def process_record(record: dict) -> dict:
    """
    对单条记录的 'text' 字段执行清洗，并返回更新后的记录。
    """
    raw = record.get('text', '')
    cleaned = clean_html(raw)
    cleaned = filter_paragraphs(cleaned)
    record['text'] = cleaned
    return record


# ===== 批量处理函数 =====

def batch_clean_jsonl(input_path: str, output_path: str, output_jsonl: bool = False):
    """
    按行读取 JSONL 文件，逐条清洗，并写入新文件。
    :param input_path:  原始 JSONL 文件路径
    :param output_path: 输出文件路径
    :param output_jsonl: True 则输出 JSONL；False 则输出标准 JSON 数组
    """
    cleaned_records = []
    with open(input_path, 'r', encoding='utf-8') as fin:
        for lineno, line in enumerate(fin, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"[Warning] 行 {lineno} 解析失败，跳过：{e}")
                continue
            cleaned = process_record(record)
            cleaned_records.append(cleaned)

    with open(output_path, 'w', encoding='utf-8') as fout:
        if output_jsonl:
            for rec in cleaned_records:
                fout.write(json.dumps(rec, ensure_ascii=False) + '\n')
        else:
            json.dump(cleaned_records, fout, ensure_ascii=False, indent=2)


# ===== 命令行入口 =====

def main():
    parser = argparse.ArgumentParser(
        description='批量清洗英文网页抓取的 JSONL/JSON 文件中的 HTML 文本字段'
    )
    parser.add_argument('input',  help='输入文件路径（JSONL 或 JSON 数组）')
    parser.add_argument('output', help='输出文件路径')
    parser.add_argument(
        '--jsonl', action='store_true',
        help='输出为 JSONL（每行一条），否则输出标准 JSON 数组'
    )
    args = parser.parse_args()

    # 先尝试整体加载为标准 JSON 数组
    try:
        with open(args.input, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if isinstance(data, list):
            cleaned = [process_record(rec) for rec in data]
            with open(args.output, 'w', encoding='utf-8') as fout:
                if args.jsonl:
                    for rec in cleaned:
                        fout.write(json.dumps(rec, ensure_ascii=False) + '\n')
                else:
                    json.dump(cleaned, fout, ensure_ascii=False, indent=2)
            return
    except json.JSONDecodeError:
        pass  # 不是标准数组则继续按 JSONL 处理

    # 按 JSONL 处理
    batch_clean_jsonl(args.input, args.output, output_jsonl=args.jsonl)


if __name__ == '__main__':
    main()