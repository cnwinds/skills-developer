"""Adapters：把各种外部信号格式转成 engine 要的通用 trade table。

两种用法：

    # 按 slug 发现（扫 adapters/ 目录）
    from adapters import load_adapter, list_adapters
    adapter = load_adapter("930_00c")

    # 按文件路径加载（adapter 没落入正式目录时也能用）
    adapter = load_adapter("/abs/path/to/my_strategy_adapter.py")

每个 adapter 的契约见 ``_contract.py``。
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
from pathlib import Path
from types import ModuleType

# 向后兼容老代码：旧 run_backtest.py 直接 from adapters import load_generic/load_qinglong
from adapters.generic import load_generic  # noqa: F401
from adapters.qinglong import load_qinglong  # noqa: F401


_ADAPTERS_DIR = Path(__file__).resolve().parent
_SCRIPTS_DIR = _ADAPTERS_DIR.parent


def _ensure_scripts_on_path() -> None:
    if str(_SCRIPTS_DIR) not in sys.path:
        sys.path.insert(0, str(_SCRIPTS_DIR))


def list_adapters() -> list[dict[str, str]]:
    """扫 adapters/ 目录下所有 adapter 模块。

    忽略 ``_`` 开头的文件（如 ``_contract.py``）和 ``__init__.py``。
    返回 [{"name", "description", "module"}]，按 ``NAME`` 字典序。
    """
    _ensure_scripts_on_path()
    results: list[dict[str, str]] = []
    for p in sorted(_ADAPTERS_DIR.glob("*.py")):
        if p.name.startswith("_") or p.name == "__init__.py":
            continue
        try:
            mod = importlib.import_module(f"adapters.{p.stem}")
        except Exception as exc:  # noqa: BLE001
            # 某个 adapter 加载失败不影响列其它的
            results.append({
                "name": p.stem,
                "description": f"[load error] {exc}",
                "module": str(p),
            })
            continue
        results.append({
            "name": getattr(mod, "NAME", p.stem),
            "description": getattr(mod, "DESCRIPTION", ""),
            "module": str(p),
        })
    results.sort(key=lambda r: r["name"])
    return results


def load_adapter(spec: str) -> ModuleType:
    """接受 slug 或 .py 路径，返回 adapter 模块对象。

    查找优先级：
    1. ``spec`` 作为 NAME 匹配 ``adapters/`` 下已注册 adapter。
    2. ``adapters/{spec}.py`` 文件存在 → 按 slug 加载。
    3. ``spec`` 是有效的 ``.py`` 文件路径 → 按路径加载（importlib.util）。

    加载后验证 adapter 暴露 ``NAME``、``add_arguments``、``load`` 三样东西。
    """
    _ensure_scripts_on_path()

    # 1) 按 NAME 匹配
    for info in list_adapters():
        if info["name"] == spec:
            return _load_by_path(Path(info["module"]), prefer_package=True)

    # 2) 按文件名匹配
    slug_path = _ADAPTERS_DIR / f"{spec}.py"
    if slug_path.exists() and not spec.startswith("_"):
        return _load_by_path(slug_path, prefer_package=True)

    # 3) 外部路径
    as_path = Path(spec).expanduser()
    if as_path.is_file() and as_path.suffix == ".py":
        mod = _load_by_path(as_path, prefer_package=False)
        _validate_adapter(mod, as_path.stem)
        return mod

    available = ", ".join(a["name"] for a in list_adapters()) or "(空)"
    raise SystemExit(
        f"找不到 adapter {spec!r}。已知 adapter: {available}。"
        f"\n提示：也可以把 --adapter 指向某个 .py 文件的绝对路径。"
    )


def _load_by_path(path: Path, prefer_package: bool) -> ModuleType:
    """加载一个 adapter 文件。

    - ``prefer_package=True``：路径在 adapters/ 里，走 ``importlib.import_module``
      以便相对/绝对 import 都能工作。
    - 否则走 ``spec_from_file_location``，外部 adapter 只能用绝对 import。
    """
    if prefer_package and path.parent == _ADAPTERS_DIR:
        mod = importlib.import_module(f"adapters.{path.stem}")
        _validate_adapter(mod, path.stem)
        return mod
    spec = importlib.util.spec_from_file_location(
        f"capital_backtest_adapter_{_sanitize(path.stem)}", path
    )
    if spec is None or spec.loader is None:
        raise ImportError(f"无法加载 adapter 文件: {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _sanitize(stem: str) -> str:
    # 让 spec_from_file_location 的模块名合法（不要以数字开头）
    return stem if not stem[:1].isdigit() else f"p_{stem}"


def _validate_adapter(mod: ModuleType, hint: str) -> None:
    for attr in ("NAME", "add_arguments", "load"):
        if not hasattr(mod, attr):
            raise SystemExit(
                f"adapter {hint!r} 缺少必需属性 {attr}（见 references/adapter_contract.md）"
            )
