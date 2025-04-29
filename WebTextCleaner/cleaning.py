'''from bs4 import BeautifulSoup

def clean_html(html):
    soup = BeautifulSoup(html, "html.parser")

    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()

    return soup.get_text(separator="\n")'''

import re
from bs4 import BeautifulSoup

def clean_html(html):
    soup = BeautifulSoup(html, "html.parser")

    # 移除不必要的 HTML 元素
    for tag in soup(["script", "style", "nav", "header", "footer", "aside"]):
        tag.decompose()

    return soup.get_text(separator="\n")

def filter_text_blocks(text):
    lines = text.splitlines()
    filtered = []
    for line in lines:
        line = line.strip()
        if not line:
            continue
        # 忽略广告、版权声明
        if re.search(r"(广告|隐私|cookies|版权|all rights reserved)", line, re.I):
            continue
        # 忽略参考文献、作者信息、脚注、DOI、链接等
        if re.search(r"(参考文献|References|作者|footnote|corresponding author|doi:|https?://\\S+)", line, re.I):
            continue
        # 忽略过短或无意义段落
        if len(line) < 20:
            continue
        filtered.append(line)
    return filtered

def clean_and_filter_json_entry(entry, html_key="html"):
    html = entry.get(html_key, "")
    text = clean_html(html)
    paragraphs = filter_text_blocks(text)
    return paragraphs

import json

def clean_json(json):

# 从文件读取 JSON
with open("data.json", "r", encoding="utf-8") as f:
    data = json.load(f)

# 从字符串解析 JSON
json_str = '{"name": "Alice", "age": 30}'
data = json.loads(json_str)

# 访问数据
print(data["name"])  # 输出: Alice