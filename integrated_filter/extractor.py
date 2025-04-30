import json
import re
from pathlib import Path


class JSONStreamParser:
    def __init__(self):
        self.buffer = ""
        self.in_object = False
        self.object_count = 0
        self.entry_count = 0
        self.error_count = 0

    def feed(self, line: str):
        """处理单行输入并返回完整JSON对象"""
        line = line.strip()
        if not line:
            return None

        # 检测可能的JSON起始
        if re.match(r'^\s*\{[\s"]*text["\']?\s*:', line):
            self.in_object = True
            self.object_count = 0
            self.buffer = ""

        if self.in_object:
            self.buffer += line
            self.object_count += line.count('{')
            self.object_count -= line.count('}')

            # 当括号平衡时尝试解析
            if self.object_count == 0:
                self.in_object = False
                return self._parse_buffer()
        return None

    def _parse_buffer(self):
        """解析缓冲区并返回清洗后的文本"""
        self.entry_count += 1

        try:
            # 修复常见格式错误
            sanitized = self.buffer \
                .replace('“', '"') \
                .replace('”', '"') \
                .replace("'", '"') \
                .replace('\\u2019', "'")

            # 处理尾部逗号
            if re.search(r',\s*}$', sanitized):
                sanitized = re.sub(r',\s*}$', '}', sanitized)

            data = json.loads(sanitized)
            return data.get('text', '')
        except Exception as e:
            self.error_count += 1
            print(f"解析错误（条目#{self.entry_count}）: {str(e)}")
            return None


def process_file(input_path, output_path):
    parser = JSONStreamParser()
    extracted = []

    with open(input_path, 'r', encoding='utf-8', errors='replace') as f:
        for line in f:
            result = parser.feed(line)
            if result is not None and result.strip():
                extracted.append(result.strip())

    # 保存结果
    output_file = Path(output_path)
    output_file.parent.mkdir(exist_ok=True)

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("\n\n".join(extracted))

    print(f"处理完成！成功提取: {len(extracted)} 条，失败: {parser.error_count} 条")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("input", help="sample_174x10.jsonl")
    parser.add_argument("output", help="out_put.json")
    args = parser.parse_args()

    process_file(args.input, args.output)

