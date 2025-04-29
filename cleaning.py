import json
import re
import os
import sys
import argparse
import logging
from typing import Dict, List, Any, Union
from concurrent.futures import ThreadPoolExecutor
from functools import lru_cache, partial
import hashlib
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
import psutil

# ----------------- 初始化配置 -----------------
nltk.download('punkt', quiet=True)
nltk.download('stopwords', quiet=True)

# 日志配置
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('filter.log'),
        logging.StreamHandler(sys.stdout)
    ]
)


class AcademicContentFilter:
    def __init__(self, config: Dict[str, Any] = None):
        # 硬件感知初始化
        self.cpu_cores = max(1, psutil.cpu_count(logical=False))  # 物理核心数
        self.mem_gb = psutil.virtual_memory().total // (1024 ** 3)

        # 默认配置（动态调整硬件参数）
        self.config = {
            "min_paragraph_length": 50,
            "max_stopword_ratio": 0.6,
            "reference_patterns": [
                r'\b(?:references|bibliography)\b',
                r'\[\d+\]',
                r'\bdoi:\s*\d+\.\d+/[\w.-]+'
            ],
            "author_info_patterns": [
                r'\b(?:corresponding author|email|@\w+\.\w{2,3})\b',
                r'\b(?:prof\.|dr\.|ph\.d\.)\b'
            ],
            "ad_keywords": ['sponsored', 'advertisement'],
            "parallel_workers": self._auto_configure_workers(),
            "enable_cache": self.mem_gb >= 16  # 内存充足时启用缓存
        }

        # 合并自定义配置
        if config:
            self.config.update(config)
            logging.info(f"加载自定义配置: {config}")

        # 初始化组件
        self._compile_regex()
        self.stop_words = stopwords.words('english')
        self.logger = logging.getLogger(self.__class__.__name__)

        # 硬件优化提示
        self.logger.info(f"硬件环境: {self.cpu_cores}物理核心/{self.mem_gb}GB内存")
        self.logger.info(f"并行工作线程: {self.config['parallel_workers']}")
        self.logger.info(f"缓存状态: {'启用' if self.config['enable_cache'] else '禁用'}")

    def _auto_configure_workers(self) -> int:
        """根据硬件资源自动配置并行度"""
        if self.mem_gb < 4:
            return 1  # 低内存设备禁用并行
        return min(self.cpu_cores * 2, 16)  # 最大不超过16线程

    def _compile_regex(self):
        """预编译正则表达式（带性能监控）"""
        try:
            self.ref_regex = re.compile(
                '|'.join(self.config['reference_patterns']),
                flags=re.IGNORECASE
            )
            self.author_regex = re.compile(
                '|'.join(self.config['author_info_patterns']),
                flags=re.IGNORECASE
            )
        except re.error as e:
            self.logger.error(f"正则表达式编译错误: {str(e)}")
            sys.exit(1)

    # ----------------- 核心过滤逻辑 -----------------
    #@lru_cache(maxsize=10000) if self.config['enable_cache'] else (lambda func: func)
    def _filter_text(self, text: str) -> bool:
        """带缓存的文本过滤核心方法"""
        try:
            # 规则1: 广告关键词
            if any(kw in text.lower() for kw in self.config['ad_keywords']):
                return True

            # 规则2: 参考文献/作者信息
            if self.ref_regex.search(text) or self.author_regex.search(text):
                return True

            # 规则3: 质量检测
            if self._is_low_quality(text):
                return True

            # 规则4: 无意义内容
            if self._is_nonsense(text):
                return True

            return False
        except Exception as e:
            self.logger.error(f"文本过滤异常: {str(e)}")
            return False

    def _is_low_quality(self, text: str) -> bool:
        """质量检测（带资源监控）"""
        try:
            tokens = word_tokenize(text.lower())
            words = [w for w in tokens if w.isalpha()]

            # 内存保护机制
            if len(words) > 10000:
                self.logger.warning(f"超长文本检测: {len(words)}词 (已跳过)")
                return False

            return (
                    len(words) < self.config['min_paragraph_length'] or
                    (sum(1 for w in words if w in self.stop_words) / len(words))
                    > self.config['max_stopword_ratio']
            )
        except RuntimeError as e:
            self.logger.error(f"NLTK处理失败: {str(e)}")
            return False

    def _is_nonsense(self, text: str) -> bool:
        """无意义内容检测"""
        if not text:
            return True
        special_chars = re.findall(r'[^\w\s.,;:!?]', text)
        return (
                len(special_chars) / len(text) > 0.3 or
                re.search(r'(\w)\1{3,}', text
                          ))

        # ----------------- 行业场景支持 -----------------

    def filter_content(self, data: Union[Dict, List]) -> Union[Dict, List]:
        """递归过滤入口"""
        mem_usage = psutil.Process().memory_info().rss / 1024 ** 2
        if mem_usage > self.mem_gb * 900:  # 内存超过90%时报警
            self.logger.critical(f"内存使用告警: {mem_usage:.1f}MB")

        if isinstance(data, dict):
            return {k: self.filter_content(v) for k, v in data.items()
                    if not self._is_key_blacklisted(k)}
        elif isinstance(data, list):
            return [self.filter_content(item) for item in data
                    if not (isinstance(item, str) and self._filter_text(item))]
        return data

    def _is_key_blacklisted(self, key: str) -> bool:
        """行业特定键过滤"""
        return key.lower() in self.config.get('blacklist_keys', [])

    # ----------------- 性能优化方法 -----------------
    def parallel_filter(self, data_list: List[Dict]) -> List[Dict]:
        """并行批量处理"""
        with ThreadPoolExecutor(max_workers=self.config['parallel_workers']) as executor:
            results = list(executor.map(self.filter_content, data_list))
            self.logger.info(f"批量处理完成: {len(results)}条")
            return results

    def stream_process(self, input_path: str, output_path: str, batch_size: int = 1000):
        """流式处理大文件"""
        self.logger.info(f"启动流式处理: {input_path}")
        with open(input_path) as fin, open(output_path, 'w') as fout:
            buffer = []
            for i, line in enumerate(fin):
                try:
                    buffer.append(json.loads(line))
                    if len(buffer) >= batch_size:
                        self._process_batch(buffer, fout, i)
                        buffer = []
                except json.JSONDecodeError:
                    self.logger.error(f"JSON解析失败: 第{i + 1}行")

            if buffer:  # 处理剩余数据
                self._process_batch(buffer, fout, i)

    def _process_batch(self, buffer: List, fout, line_num: int):
        """带异常处理的批次处理"""
        try:
            for cleaned in self.parallel_filter(buffer):
                fout.write(json.dumps(cleaned) + '\n')
            self.logger.debug(f"已处理到第{line_num}行")
        except Exception as e:
            self.logger.error(f"批次处理异常: {str(e)}")


# ----------------- 行业场景配置 -----------------
CONFIG_PROFILES = {
    # 医学
    "medical": {
        "min_paragraph_length": 150,
        "ad_keywords": ["clinical trial", "drug discount"],
        "reference_patterns": [r'\b(?:pmid|nih)\s*\d+\b'],
        "blacklist_keys": ["patient_metadata"]
    },

    # 法律
    "legal": {
        "reference_patterns": [r'\b\d+\s[A-Z]+\s\d+\b'],
        "author_info_patterns": [],
        "blacklist_keys": ["case_history"]
    },

    # 社科
    "social": {
        "min_paragraph_length": 20,
        "max_stopword_ratio": 0.8,
        "ad_keywords": ["#ad", "sponsored"],
        "parallel_workers": 8
    }
}

# ----------------- 命令行接口 -----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='智能学术内容过滤器')
    parser.add_argument('-i', '--input', required=True, help='sample_174x10.jsonl')
    parser.add_argument('-o', '--output', required=True, help='cleaned.json')
    parser.add_argument('-p', '--profile', choices=CONFIG_PROFILES.keys(),
                        help='行业配置方案')
    parser.add_argument('-v', '--verbose', action='store_true',
                        help='启用调试日志')
    args = parser.parse_args()

    # 动态日志级别
    logging.getLogger().setLevel(
        logging.DEBUG if args.verbose else logging.INFO
    )

    # 加载行业配置
    config = CONFIG_PROFILES.get(args.profile, {})
    if args.profile:
        logging.info(f"激活行业配置: {args.profile}")

    # 实例化并运行
    try:
        processor = AcademicContentFilter(config)
        processor.stream_process(
            input_path=args.input,
            output_path=args.output,
            batch_size=1000
        )
    except KeyboardInterrupt:
        logging.warning("用户中断操作")
        sys.exit(130)
    except Exception as e:
        logging.critical(f"致命错误: {str(e)}")
        sys.exit(1)