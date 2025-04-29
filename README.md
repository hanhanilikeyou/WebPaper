# WebTextCleaner

用于批量处理网页数据，提取学术正文内容的清洗工具链。支持 jusText 内容提取、FastText + SciBERT 筛选、MinHash 去重等功能。

## 使用方法

```bash
python run_pipeline.py ./data/sample.html ./output/result.txt ./models/fasttext_model.bin
```
