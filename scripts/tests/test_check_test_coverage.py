#!/usr/bin/env python3
"""check_test_coverage.py 的单元测试（免 Go，fixture 驱动）。

仓库的免 Go 静态自检门禁自身此前没有任何测试：一旦某次改动令某一校验器在「干净仓库」
下误报或崩溃，CI 会在无人察觉时静默失败（这正是 cycle 110 修复的那类问题在「门禁自身」
上的变种）。本测试用临时 fixture 仓库驱动 check_test_coverage 的纯函数 analyze()，
断言其包级缺口判定与导出符号引用识别行为正确，使该护栏可被回归守护。

运行：python3 scripts/tests/test_check_test_coverage.py
"""
import importlib.util
import os
import sys
import tempfile
import unittest

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPTS = os.path.dirname(HERE)
SPEC = os.path.join(SCRIPTS, "check_test_coverage.py")

spec = importlib.util.spec_from_file_location("check_test_coverage_ut", SPEC)
ctc = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ctc)


def _write(path: str, text: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


class CheckTestCoverageTest(unittest.TestCase):
    def setUp(self):
        self.root = tempfile.mkdtemp(prefix="ctc-")

    def tearDown(self):
        import shutil
        shutil.rmtree(self.root, ignore_errors=True)

    def _mk_pkg(self, name, go_src, test_src=None):
        d = os.path.join(self.root, name)
        _write(os.path.join(d, name + ".go"), go_src)
        if test_src is not None:
            _write(os.path.join(d, name + "_test.go"), test_src)
        return d

    def test_no_gap_when_func_referenced(self):
        self._mk_pkg("p", "package p\n\nfunc Used() {}\n",
                     "package p\n\nfunc TestX(t *testing.T) { Used() }\n")
        rep, hard = ctc.analyze(root=self.root)
        self.assertEqual(hard, 0)
        self.assertEqual(rep["summary"]["unref_funcs_total"], 0)

    def test_orphan_func_reported(self):
        self._mk_pkg("p",
                     "package p\n\nfunc Orphan() {}\nfunc Refd() {}\n",
                     "package p\n\nfunc TestX(t *testing.T) { Refd() }\n")
        rep, _ = ctc.analyze(root=self.root)
        funcs = rep["packages"][0]["unref_funcs"]
        self.assertIn("Orphan", funcs)
        self.assertNotIn("Refd", funcs)

    def test_package_without_test_is_hard_gap(self):
        self._mk_pkg("orphan", "package orphan\n\nfunc Foo() {}\n")
        rep, hard = ctc.analyze(root=self.root)
        self.assertEqual(hard, 1)
        self.assertEqual(rep["summary"]["packages_without_test"], 1)

    def test_strict_mode_returns_nonzero_exit(self):
        # main() 在 strict 且存在硬缺口时应返回 2
        self._mk_pkg("orphan", "package orphan\n\nfunc Foo() {}\n")
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = ctc.main(argv=["--strict", "--root", self.root])
        self.assertEqual(rc, 2)

    def test_returns_zero_when_clean(self):
        self._mk_pkg("p", "package p\n\nfunc Used() {}\n",
                     "package p\n\nfunc TestX(t *testing.T) { Used() }\n")
        import io
        from contextlib import redirect_stdout
        buf = io.StringIO()
        with redirect_stdout(buf):
            rc = ctc.main(argv=["--root", self.root])
        self.assertEqual(rc, 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
