"""轻量持久化存储：保存最近一次成功提取到的站点地址、已处理邮件 UID 等。

数据统一存放于 AstrBot 的插件数据目录（``StarTools.get_data_dir()``），
避免插件更新/重装时被覆盖。
"""

from __future__ import annotations

import json
import os
import threading
from pathlib import Path
from typing import Any

from astrbot.api import logger

try:  # pragma: no cover - 运行时才可用
    from astrbot.api.star import StarTools
except Exception:  # pragma: no cover - 兼容旧版本
    StarTools = None  # type: ignore


def resolve_data_dir(plugin_name: str = "astrbot_plugin_mailvideo") -> Path:
    """获取插件数据目录，尽量使用框架规范接口，失败时回退。"""
    if StarTools is not None:
        try:
            return Path(StarTools.get_data_dir(plugin_name))
        except Exception as e:  # pragma: no cover
            logger.warning(f"[mailvideo] StarTools.get_data_dir 失败，回退默认目录: {e}")
    fallback = Path("data") / "plugin_data" / plugin_name
    fallback.mkdir(parents=True, exist_ok=True)
    return fallback.resolve()


class StateStore:
    """一个极简的 JSON 状态文件读写器（线程安全）。"""

    def __init__(self, data_dir: Path) -> None:
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)
        self.state_path = self.data_dir / "state.json"
        self.temp_dir = self.data_dir / "tmp"
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._cache: dict[str, Any] = self._load()

    # ------------------------------------------------------------------ #
    def _load(self) -> dict[str, Any]:
        if not self.state_path.exists():
            return {}
        try:
            with open(self.state_path, "r", encoding="utf-8") as f:
                data = json.load(f)
            return data if isinstance(data, dict) else {}
        except Exception as e:  # pragma: no cover
            logger.warning(f"[mailvideo] 读取状态文件失败: {e}")
            return {}

    def _flush(self) -> None:
        try:
            tmp = self.state_path.with_suffix(".json.tmp")
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
            os.replace(tmp, self.state_path)
        except Exception as e:  # pragma: no cover
            logger.warning(f"[mailvideo] 写入状态文件失败: {e}")

    # ------------------------------------------------------------------ #
    def get(self, key: str, default: Any = None) -> Any:
        with self._lock:
            return self._cache.get(key, default)

    def set(self, key: str, value: Any) -> None:
        with self._lock:
            self._cache[key] = value
            self._flush()

    # 常见字段便捷方法 -------------------------------------------------- #
    def get_last_address(self) -> str:
        return str(self.get("last_address", "") or "")

    def set_last_address(self, url: str) -> None:
        if url:
            self.set("last_address", url)
            self.set("last_address_ts", _now_ts())

    def get_processed_uids(self) -> list[str]:
        val = self.get("processed_uids", [])
        return list(val) if isinstance(val, list) else []

    def add_processed_uids(self, uids: list[str]) -> None:
        if not uids:
            return
        merged = self.get_processed_uids()
        for u in uids:
            if u not in merged:
                merged.append(u)
        # 只保留最近 500 条，防止无限增长
        self.set("processed_uids", merged[-500:])

    # 待投递（发送失败自动补发）字段 -------------------------------- #
    def get_pending_map(self) -> dict[str, Any]:
        val = self.get("pending_deliveries", {})
        return val if isinstance(val, dict) else {}

    def set_pending(self, umo: str, item: dict[str, Any]) -> None:
        """记录指定会话的待投递（同会话覆盖旧记录）。"""
        pending = self.get_pending_map()
        pending[umo] = {**item, "ts": _now_ts()}
        self.set("pending_deliveries", pending)

    def pop_pending(self, umo: str) -> dict[str, Any] | None:
        """取出指定会话的待投递，没有则返回 None。"""
        pending = self.get_pending_map()
        item = pending.pop(umo, None)
        self.set("pending_deliveries", pending)
        return item if isinstance(item, dict) else None

    # 候选选择会话（列候选 → 用户回序号 → 只下载该条） ---------------- #
    def get_selection_map(self) -> dict[str, Any]:
        val = self.get("selection_sessions", {})
        return val if isinstance(val, dict) else {}

    def set_selection(self, umo: str, data: dict[str, Any]) -> None:
        """记录指定会话的待选候选（同会话覆盖旧记录）。"""
        sessions = self.get_selection_map()
        sessions[umo] = {**data, "ts": _now_ts()}
        self.set("selection_sessions", sessions)

    def get_selection(self, umo: str) -> dict[str, Any] | None:
        """读取指定会话的待选候选（不删除）。"""
        item = self.get_selection_map().get(umo)
        return item if isinstance(item, dict) else None

    def pop_selection(self, umo: str) -> dict[str, Any] | None:
        """取出并删除指定会话的待选候选，没有则返回 None。"""
        sessions = self.get_selection_map()
        item = sessions.pop(umo, None)
        self.set("selection_sessions", sessions)
        return item if isinstance(item, dict) else None


def _now_ts() -> int:
    import time

    return int(time.time())
