from typing import List

def split_list(l: List, k: int = 5) -> List[List]:
    """将列表进行切片, 长度不满的直接输出, 最终输出嵌套列表"""
    return [l[i:i + k] for i in range(0, len(l), k)]