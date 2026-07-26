#!/usr/bin/env python3
"""check_go_coverage.py 的回归测试（免 Go，守护「覆盖率门槛门禁自身」不退化）。

用 fixture profile 文本直接驱动纯函数 parse_profile / summarize / coverage_pct，并
对 main 的高层行为做端到端断言（profile 缺失 / 达门槛 / 跌破门槛 / 刷新门槛）。

覆盖：
  - parse_profile 解析 block 并正确归包（去模块前缀 raftkv/）
  - coverage_pct 边界（total==0 -> 100%）
  - summarize 聚合正确（covered/total 与整体一致）
  - main：整体达 min_total 通过(0)；跌破 FAIL(1)；profile 缺失 FAIL(2)
  - --update-baseline 写回实测值
"""
import json
import os
import sys
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_go_coverage as cg  # noqa: E402


PROFILE = """mode: atomic
raftkv/src/foo/foo.go:10.20,12.3 3 1
raftkv/src/foo/foo.go:14.20,16.3 2 0
raftkv/src/bar/bar.go:5.10,7.2 4 2
raftkv/src/bar/bar.go:9.1,11.1 1 0
"""


def test_parse_profile_and_pkg():
    blocks = cg.parse_profile(PROFILE)
    assert len(blocks) == 4, blocks
    pkgs = {b["pkg"] for b in blocks}
    assert pkgs == {"src/foo", "src/bar"}, pkgs
    # 首行 mode: 被跳过，count>0 视为覆盖
    assert blocks[0]["count"] == 1 and blocks[0]["stmts"] == 3


def test_coverage_pct_boundary():
    assert cg.coverage_pct(0, 0) == 100.0
    assert cg.coverage_pct(0, 10) == 0.0
    assert cg.coverage_pct(5, 10) == 50.0


def test_summarize_aggregates():
    summ = cg.summarize(cg.parse_profile(PROFILE))
    # foo: 3(覆盖)+2(未覆盖)=5 总, 3 覆盖；bar: 4+1=5 总, 4 覆盖
    assert summ["packages"]["src/foo"] == {"covered": 3, "total": 5}
    assert summ["packages"]["src/bar"] == {"covered": 4, "total": 5}
    assert summ["total"] == {"covered": 7, "total": 10}
    assert cg.coverage_pct(7, 10) == 70.0


def test_main_passes_when_above_floor(tmp_path):
    prof = tmp_path / "cover.out"
    prof.write_text(PROFILE, encoding="utf-8")
    cfg = tmp_path / "cfg.json"
    cfg.write_text(json.dumps({"min_total": 60.0, "min_package": 0.0, "packages": {}}),
                   encoding="utf-8")
    rc = cg.main(["--profile", str(prof), "--config", str(cfg)])
    assert rc == 0, f"应达门槛通过, got {rc}"


def test_main_fails_when_below_floor(tmp_path):
    prof = tmp_path / "cover.out"
    prof.write_text(PROFILE, encoding="utf-8")
    cfg = tmp_path / "cfg.json"
    cfg.write_text(json.dumps({"min_total": 90.0, "min_package": 0.0, "packages": {}}),
                   encoding="utf-8")
    rc = cg.main(["--profile", str(prof), "--config", str(cfg)])
    assert rc == 1, f"应跌破门槛 FAIL, got {rc}"


def test_main_missing_profile():
    rc = cg.main(["--profile", "/nonexistent/cover.out"])
    assert rc == 2, f"profile 缺失应 FAIL(2), got {rc}"


def test_update_baseline_writes_measured(tmp_path):
    prof = tmp_path / "cover.out"
    prof.write_text(PROFILE, encoding="utf-8")
    cfg = tmp_path / "cfg.json"
    cfg.write_text(json.dumps({"min_total": 0.0, "min_package": 0.0, "packages": {}}),
                   encoding="utf-8")
    rc = cg.main(["--profile", str(prof), "--config", str(cfg), "--update-baseline"])
    assert rc == 0
    refreshed = json.load(open(cfg, encoding="utf-8"))
    assert refreshed["min_total"] == 70.0, refreshed


def main() -> int:
    import inspect
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    tmp_path = Path(tempfile.mkdtemp(prefix="covtest_", dir=cg.ROOT))
    failed = 0
    for t in tests:
        try:
            if len(inspect.signature(t).parameters) == 1:
                t(tmp_path)
            else:
                t()
            print(f"[PASS] {t.__name__}")
        except AssertionError as e:
            failed += 1
            print(f"[FAIL] {t.__name__}: {e}")
    if failed:
        print(f"\n{len(tests) - failed}/{len(tests)} 通过，{failed} 失败。")
        return 1
    print(f"\n全部 {len(tests)} 例通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
