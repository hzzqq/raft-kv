// gateway_version_fallback_test.go —— /debug/version 构建信息回退的 cluster-free 单测（#197）。
package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"raftkv/src/version"

	"raftkv/src/util"
)

// TestGatewayDebugVersionFallback：未 SetVersion 时 version 字段回退到构建期注入的
// version.BuildVersion（默认 "dev"），且新增 commit/build_time 字段透传。
func TestGatewayDebugVersionFallback(t *testing.T) {
	s := &Server{
		sem:            util.NewSemaphore(maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		startedAt:      time.Now(),
		// version 留空 → 应回退
	}
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/version")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Version   string `json:"version"`
		Commit    string `json:"commit"`
		BuildTime string `json:"build_time"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Version != version.BuildVersion {
		t.Fatalf("version = %q, want fallback %q", out.Version, version.BuildVersion)
	}
	if out.Commit != version.Commit || out.BuildTime != version.BuildTime {
		t.Fatalf("commit/build_time not passed through: %+v", out)
	}
}

// TestGatewayDebugVersionExplicitWins：显式 SetVersion 后不回退。
func TestGatewayDebugVersionExplicitWins(t *testing.T) {
	s := &Server{
		sem:            util.NewSemaphore(maxConcurrent),
		accessCap:      256,
		logCap:         256,
		requestTimeout: 30 * time.Second,
		startedAt:      time.Now(),
	}
	s.SetVersion("v9.9.9")
	ts := httptest.NewServer(s.Handler())
	defer ts.Close()

	resp, err := http.Get(ts.URL + "/debug/version")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out struct {
		Version string `json:"version"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatal(err)
	}
	if out.Version != "v9.9.9" {
		t.Fatalf("version = %q, want v9.9.9", out.Version)
	}
}
