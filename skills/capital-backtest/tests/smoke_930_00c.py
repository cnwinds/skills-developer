"""930-00c adapter 的 smoke test。

跑法（在含 ``04_outputs/`` 的策略项目根目录下，例如 930-00c；或将本 skill 放在
``~/.claude/skills/capital-backtest`` 后从项目根执行）：

    python skills/capital-backtest/tests/smoke_930_00c.py

若从本仓库根目录克隆 standalone 运行且上层无 ``04_outputs/``，脚本会 ``[SKIP]`` 并退出 0。

做三件事：

1. **vendored infer_board 一致性**：对项目里实际出现过的 code，
   用 `adapters._contract.infer_board` 和 `02_core.board_classifier.infer_board`
   逐行对比，任何不一致都算失败。
2. **end-to-end run**：拿 2 个月的 `04_outputs/930_00c_buy_signals_*_en.csv`
   跑 `run --adapter 930_00c --no-plots --no-xlsx`，断言：
   - 返回码 = 0
   - 输出目录存在 {bucket}_trade_log.csv / monthly.csv / yearly.csv
   - summary.json 里的 adapter=='930_00c'，buckets 非空
3. **公共 flag 生效**：再跑一次，`--date-from 20190501 --date-to 20190630`
   收窄一下，断言 summary.json 的 date_from / date_to 写对了，
   并且 buckets 里至少有一个桶能产出 closed_sells > 0。

任何一步失败打印可读的错误并退出 non-zero。非 930-00c 项目（找不到 02_core 或
对应的 buy_signals 文件）会打印 [SKIP] 并退出 0。
"""
from __future__ import annotations

import importlib.util
import json
import subprocess
import sys
import tempfile
from pathlib import Path

import pandas as pd


HERE = Path(__file__).resolve()
SKILL_DIR = HERE.parents[1]
SCRIPT = SKILL_DIR / "scripts" / "run_backtest.py"


def _find_workspace_root(skill_dir: Path) -> Path | None:
    """从 skill 目录向上找第一个含 ``04_outputs/`` 的目录（策略项目根）。"""
    cur = skill_dir.parent
    for _ in range(16):
        if (cur / "04_outputs").is_dir():
            return cur
        if cur.parent == cur:
            return None
        cur = cur.parent
    return None


PACKAGE_DIR: Path | None = _find_workspace_root(SKILL_DIR)

# 取 2 个月的输入。这两个月在实际数据里有十几条 is_buy=True 的行。
SAMPLE_MONTHS = ("201905", "201906")


def _skip(msg: str) -> int:
    print(f"[SKIP] {msg}")
    return 0


# ---------------------------------------------------------------------------
# (1) vendored infer_board 一致性
# ---------------------------------------------------------------------------

def check_infer_board_consistency() -> bool:
    """对比 adapter 里 vendored 的 infer_board 与项目版本。"""
    if PACKAGE_DIR is None:
        print("[SKIP-part1] 无项目根；跳过 infer_board 一致性校验。")
        return True
    proj_file = PACKAGE_DIR / "02_core" / "board_classifier.py"
    if not proj_file.exists():
        print(f"[SKIP-part1] 找不到 {proj_file}；跳过 infer_board 一致性校验。")
        return True

    # 动态加载项目内的 board_classifier（不污染 sys.path）
    spec = importlib.util.spec_from_file_location("_proj_board_classifier", proj_file)
    assert spec and spec.loader
    proj_mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(proj_mod)

    # 加载 adapter 侧的 infer_board
    sys.path.insert(0, str(SKILL_DIR / "scripts"))
    try:
        from adapters._contract import infer_board as vendored_infer
    finally:
        sys.path.pop(0)

    # 收集一份"真实出现过的 code"样本
    codes: set[str] = set()
    for m in SAMPLE_MONTHS + ("201512", "202001", "202604"):
        p = PACKAGE_DIR / "04_outputs" / f"930_00c_buy_signals_{m}_en.csv"
        if not p.exists():
            continue
        try:
            col = pd.read_csv(p, encoding="utf-8-sig", usecols=["code"], dtype={"code": str})
        except Exception:
            continue
        codes.update(col["code"].dropna().astype(str).str.strip().tolist())

    # 再塞点边界样本（6 位数字、BJ、带错误后缀等）
    codes.update({
        "688006.SH", "300258.SZ", "600519.SH", "000001.SZ", "002001.SZ",
        "001234.SZ", "870866.BJ", "430047.BJ",
        "688006", "300258", "600519",
        "abc", "",
    })

    mismatches: list[tuple[str, str, str]] = []
    for c in sorted(codes):
        a = vendored_infer(c)
        b = proj_mod.infer_board(c)
        if a != b:
            mismatches.append((c, a, b))

    print(f"[part1] infer_board 一致性：检查 {len(codes)} 个 code，差异 {len(mismatches)}")
    for c, a, b in mismatches[:20]:
        print(f"  MISMATCH {c!r}: vendored={a} project={b}")
    return not mismatches


# ---------------------------------------------------------------------------
# (2) end-to-end run
# ---------------------------------------------------------------------------

def _collect_sample_inputs() -> list[Path]:
    if PACKAGE_DIR is None:
        return []
    paths = [
        PACKAGE_DIR / "04_outputs" / f"930_00c_buy_signals_{m}_en.csv"
        for m in SAMPLE_MONTHS
    ]
    return [p for p in paths if p.exists()]


def run_end_to_end(tmp: Path) -> tuple[bool, dict | None]:
    inputs = _collect_sample_inputs()
    if not inputs:
        print(f"[SKIP-part2] 找不到 {SAMPLE_MONTHS} 的 buy_signals；跳过 end-to-end。")
        return True, None

    out = tmp / "e2e"
    cmd = [sys.executable, str(SCRIPT), "run", "--adapter", "930_00c"]
    for p in inputs:
        cmd += ["--input", str(p)]
    cmd += [
        "--exit-horizon", "8d",
        "--initial-cash", "200000", "--per-stock-cap", "150000",
        "--no-plots", "--no-xlsx",
        "--out-dir", str(out),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True,
                       cwd=str(PACKAGE_DIR))
    if r.returncode != 0:
        print("[FAIL-part2] run --adapter 930_00c 返回非 0")
        print("STDOUT:", r.stdout)
        print("STDERR:", r.stderr)
        return False, None

    summary_path = out / "summary.json"
    if not summary_path.exists():
        print(f"[FAIL-part2] 找不到 {summary_path}")
        return False, None
    sj = json.loads(summary_path.read_text(encoding="utf-8"))

    ok = True
    if sj.get("adapter") != "930_00c":
        print(f"[FAIL-part2] summary.adapter 期望 930_00c，实际 {sj.get('adapter')!r}")
        ok = False
    if not sj.get("buckets"):
        print(f"[FAIL-part2] summary.buckets 为空")
        ok = False
    for b in sj.get("buckets", []):
        for suffix in ("_trade_log.csv", "_monthly.csv", "_yearly.csv", "_equity.csv"):
            f = out / f"{b}{suffix}"
            if not f.exists():
                print(f"[FAIL-part2] 缺输出文件: {f}")
                ok = False
    if ok:
        print(f"[part2] end-to-end OK：buckets={sj['buckets']}, "
              f"adapter={sj['adapter']}, 文件齐全。")
    return ok, sj


# ---------------------------------------------------------------------------
# (3) 公共 flag（date-from / date-to）
# ---------------------------------------------------------------------------

def run_with_date_window(tmp: Path) -> bool:
    inputs = _collect_sample_inputs()
    if not inputs:
        print("[SKIP-part3] 没样本输入，跳过。")
        return True

    out = tmp / "date_window"
    cmd = [sys.executable, str(SCRIPT), "run", "--adapter", "930_00c"]
    for p in inputs:
        cmd += ["--input", str(p)]
    cmd += [
        "--date-from", "20190501",
        "--date-to", "20190630",
        "--initial-cash", "200000", "--per-stock-cap", "150000",
        "--no-plots", "--no-xlsx",
        "--out-dir", str(out),
    ]
    r = subprocess.run(cmd, capture_output=True, text=True,
                       cwd=str(PACKAGE_DIR))
    if r.returncode != 0:
        print("[FAIL-part3] date window 运行失败")
        print("STDOUT:", r.stdout); print("STDERR:", r.stderr)
        return False
    sj = json.loads((out / "summary.json").read_text(encoding="utf-8"))
    ok = True
    if sj.get("date_from") != "20190501":
        print(f"[FAIL-part3] date_from 期望 20190501，实际 {sj.get('date_from')!r}")
        ok = False
    if sj.get("date_to") != "20190630":
        print(f"[FAIL-part3] date_to 期望 20190630，实际 {sj.get('date_to')!r}")
        ok = False
    any_closed = any(
        (pb.get("summary") or {}).get("closed_sells", 0) > 0
        for pb in (sj.get("per_bucket") or {}).values()
    )
    if not any_closed:
        print("[FAIL-part3] date window 里所有 bucket 的 closed_sells 都是 0，"
              "过滤可能把数据都切没了。")
        ok = False
    if ok:
        print(f"[part3] date-window OK：{sj['date_from']}~{sj['date_to']}, "
              f"buckets={sj['buckets']}。")
    return ok


# ---------------------------------------------------------------------------
# 入口
# ---------------------------------------------------------------------------

def main() -> int:
    if not SCRIPT.exists():
        print(f"[SKIP] 找不到 run_backtest.py: {SCRIPT}")
        return 0
    if PACKAGE_DIR is None:
        return _skip(
            "未在上级目录找到含 04_outputs/ 的策略项目根；本 smoke 仅在完整工作区（如 930-00c）内运行。"
        )
    # 检测一下有没有 buy_signals 输入。没有就退化成只跑 part1。
    inputs_exist = bool(_collect_sample_inputs())
    has_proj_board = (PACKAGE_DIR / "02_core" / "board_classifier.py").exists()
    if not inputs_exist and not has_proj_board:
        return _skip("既没有 02_core/board_classifier.py 也没有样本 buy_signals，"
                     "此 smoke 只在原 930-00c 项目里生效。")

    ok_1 = check_infer_board_consistency()

    with tempfile.TemporaryDirectory() as td:
        tmp = Path(td)
        ok_2, _ = run_end_to_end(tmp)
        ok_3 = run_with_date_window(tmp) if ok_2 else False

    all_ok = ok_1 and ok_2 and ok_3
    print("ALL PASSED" if all_ok else "SMOKE FAILED")
    return 0 if all_ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
