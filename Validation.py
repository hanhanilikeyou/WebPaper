import json
import re
from typing import Dict, List
from collections import defaultdict


class FilterValidator:
    def __init__(self, original_path: str, filtered_path: str, rules: Dict):
        self.original = self._load_json(original_path)
        self.filtered = self._load_json(filtered_path)
        self.rules = rules
        self.stats = defaultdict(int)
        self.errors = []

    def _load_json(self, path: str) -> Union[Dict, List]:
        with open(path, 'r') as f:
            return json.load(f)

    def validate_all(self):
        """执行全量验证流程"""
        self._validate_structure()
        self._validate_blacklist_rules()
        self._validate_whitelist_content()
        self._generate_report()

    def _validate_structure(self):
        """结构完整性验证"""
        if isinstance(self.original, dict) and isinstance(self.filtered, dict):
            self._compare_dicts(self.original, self.filtered, path="root")
        elif isinstance(self.original, list) and isinstance(self.filtered, list):
            self._compare_lists(self.original, self.filtered, path="root")
        else:
            self._log_error("结构类型不匹配")

    def _compare_dicts(self, orig: Dict, filt: Dict, path: str):
        for key in orig:
            if key in self.rules.get('key_blacklist', []):
                if key in filt:
                    self._log_error(f"黑名单键未过滤: {path}.{key}")
                continue

            if key not in filt:
                self._log_error(f"键丢失: {path}.{key}")
                continue

            if isinstance(orig[key], dict):
                self._compare_dicts(orig[key], filt[key], f"{path}.{key}")
            elif isinstance(orig[key], list):
                self._compare_lists(orig[key], filt[key], f"{path}.{key}")
            else:
                self._compare_values(orig[key], filt[key], f"{path}.{key}")

    def _compare_lists(self, orig: List, filt: List, path: str):
        if len(orig) != len(filt):
            self.stats['list_length_change'] += 1

        for i, (o_item, f_item) in enumerate(zip(orig, filt)):
            current_path = f"{path}[{i}]"
            if isinstance(o_item, dict):
                self._compare_dicts(o_item, f_item, current_path)
            elif isinstance(o_item, list):
                self._compare_lists(o_item, f_item, current_path)
            else:
                self._compare_values(o_item, f_item, current_path)

    def _compare_values(self, orig_val, filt_val, path: str):
        if isinstance(orig_val, str):
            self._validate_string_rules(orig_val, filt_val, path)

    def _validate_blacklist_rules(self):
        """验证黑名单规则"""
        # 广告关键词检测
        self._scan_for_patterns(
            self.filtered,
            patterns=self.rules['ad_keywords'],
            rule_type="blacklist"
        )

        # 参考文献格式检测
        self._scan_for_patterns(
            self.filtered,
            patterns=self.rules['reference_patterns'],
            rule_type="blacklist"
        )

    def _validate_whitelist_content(self):
        """白名单内容完整性验证"""
        # 确保公式等核心内容保留
        self._scan_for_patterns(
            self.filtered,
            patterns=self.rules['whitelist_patterns'],
            rule_type="whitelist"
        )

    def _scan_for_patterns(self, data, patterns: List[str], rule_type: str):
        """递归扫描JSON结构中的字符串"""
        if isinstance(data, dict):
            for v in data.values():
                self._scan_for_patterns(v, patterns, rule_type)
        elif isinstance(data, list):
            for item in data:
                self._scan_for_patterns(item, patterns, rule_type)
        elif isinstance(data, str):
            for pattern in patterns:
                if re.search(pattern, data, re.IGNORECASE):
                    self.stats[f'{rule_type}_hits'] += 1
                    if rule_type == "blacklist":
                        self._log_error(f"黑名单内容残留: {pattern} -> {data[:50]}...")
                    else:
                        if not re.search(pattern, data):
                            self._log_error(f"白名单内容丢失: {pattern}")

    def _log_error(self, msg: str):
        self.errors.append(msg)
        self.stats['total_errors'] += 1

    def _generate_report(self):
        """生成验证报告"""
        print(f"\n=== 过滤有效性验证报告 ===")
        print(f"原始文件条目数: {self._count_items(self.original)}")
        print(f"过滤后条目数: {self._count_items(self.filtered)}")
        print(f"检测到黑名单违规: {self.stats['blacklist_hits']}次")
        print(f"白名单内容丢失: {self.stats['whitelist_hits']}次")
        print(f"结构变化警告: {self.stats['list_length_change']}处")

        if self.errors:
            print("\n=== 错误详情（前10条） ===")
            for err in self.errors[:10]:
                print(f"• {err}")
        else:
            print("\n✅ 所有校验通过")

    def _count_items(self, data) -> int:
        """统计JSON结构中的叶子节点数"""
        if isinstance(data, dict):
            return sum(self._count_items(v) for v in data.values())
        elif isinstance(data, list):
            return sum(self._count_items(item) for item in data)
        else:
            return 1


# 使用示例
if __name__ == "__main__":
    validator = FilterValidator(
        original_path="raw_data.json",
        filtered_path="cleaned_data.json",
        rules={
            "ad_keywords": ["sponsored", "click here"],
            "reference_patterns": [r'\breferences?\b', r'\[\d+\]'],
            "whitelist_patterns": [r'\$[^$]+\$'],  # LaTeX公式
            "key_blacklist": ["footnotes"]
        }
    )
    validator.validate_all()