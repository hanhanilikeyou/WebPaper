import json
from multiprocessing import Pool
from typing import List, Callable

def load_json(path: str) -> List[dict]:
    '''with open(path, "r", encoding="utf-8") as f:
        return json.load(f)'''


    valid_data = []
    with open(path, "r",  encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    valid_data.append(json.loads(line))
                except json.JSONDecodeError:
                    print(f"忽略无效行: {line}")

def save_json(data: List[dict], path: str):
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

def run_parallel(data: List[dict], func: Callable, workers: int) -> List[dict]:
    if workers == 1:
        return [func(item) for item in data]
    with Pool(processes=workers) as pool:
        return pool.map(func, data)