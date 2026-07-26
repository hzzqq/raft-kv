#!/usr/bin/env python3
"""check_secrets.py 的回归测试（免 Go，守护「安全门禁自身」不退化）。

用 fixture 文本直接驱动 check_secrets.scan_text，断言：
  - HARD：PEM 私钥块 / AWS AKIA / AWS SK 字面量 必须被检出
  - WARN：明文口令 / Slack token / GitHub token 必须被检出
  - 干净代码：无任何命中
  - 策略：WARN 不应污染 HARD（误升级）
"""
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
import check_secrets as cs  # noqa: E402


def test_hard_pem_private_key():
    text = "key := `-----BEGIN RSA PRIVATE KEY-----\nMIIBOgIBAAJBAK\n-----END RSA PRIVATE KEY-----`"
    hard, warn = cs.scan_text(text, "fixture.go")
    assert any("PEM" in label for _, _, label, _ in hard), f"PEM 私钥未被 HARD 检出: {hard}"


def test_hard_aws_access_key_id():
    text = 'ak := "AKIAIOSFODNN7EXAMPLE"'
    hard, _ = cs.scan_text(text, "fixture.go")
    assert any("AWS Access Key ID" == label for _, _, label, _ in hard), f"AKIA 未被检出: {hard}"


def test_hard_aws_secret_literal():
    text = 'aws_secret_access_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"'
    hard, _ = cs.scan_text(text, "fixture.py")
    assert any("AWS Secret" in label for _, _, label, _ in hard), f"AWS SK 未被检出: {hard}"


def test_warn_plaintext_password():
    text = 'cfg.Password = "supersecret123"'
    hard, warn = cs.scan_text(text, "fixture.go")
    assert not hard, f"明文口令不应进 HARD: {hard}"
    assert any("明文" in label for _, _, label, _ in warn), f"明文口令未被 WARN: {warn}"


def test_warn_slack_token():
    text = 'token = "xoxb-1234567890-abcdefghijkl"'
    hard, warn = cs.scan_text(text, "fixture.py")
    assert not hard
    assert any("Slack" in label for _, _, label, _ in warn), f"Slack token 未被 WARN: {warn}"


def test_warn_github_token():
    text = 'tok = "ghp_0123456789abcdefghijklmnopqrstuvwxyz"'
    hard, warn = cs.scan_text(text, "fixture.py")
    assert not hard
    assert any("GitHub" in label for _, _, label, _ in warn), f"GitHub token 未被 WARN: {warn}"


def test_clean_code_no_hits():
    text = (
        "package kv\n"
        "func Put(k string, v []byte) error { return nil }\n"
        'const prefix = "AKIA-" // 仅作普通字符串前缀示例\n'
    )
    hard, warn = cs.scan_text(text, "clean.go")
    assert not hard, f"干净代码误报 HARD: {hard}"
    assert not warn, f"干净代码误报 WARN: {warn}"


def main() -> int:
    tests = [v for k, v in sorted(globals().items()) if k.startswith("test_")]
    failed = 0
    for t in tests:
        try:
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
