"""运行配置：全部通过环境变量覆盖。"""
from __future__ import annotations

import os


def db_path() -> str:
    """SQLite 数据库文件路径。"""
    return os.environ.get("NAVLIGHT_DB_PATH", "data/navlight.db")


def grid_max_steps() -> int:
    """钟漂网格搜索的最大步数。"""
    return int(os.environ.get("NAVLIGHT_GRID_MAX_STEPS", "201"))


def seed_top_k() -> int:
    """进入细化阶段的相位/钟漂种子数量。"""
    return int(os.environ.get("NAVLIGHT_SEED_TOP_K", "5"))
