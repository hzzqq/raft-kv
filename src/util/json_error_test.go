package util

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestJSONErrorImplements(t *testing.T) {
	var err error = NewJSONError("E_TEST", "boom")
	if err.Error() != "[E_TEST] boom" {
		t.Fatalf("unexpected error string: %q", err.Error())
	}
}

func TestJSONErrorMarshal(t *testing.T) {
	e := NewJSONErrorf("E_BAD", "bad %s", "x").WithDetails(map[string]int{"n": 1})
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	s := string(b)
	if !strings.Contains(s, `"code":"E_BAD"`) || !strings.Contains(s, `"message":"bad x"`) {
		t.Fatalf("marshal missing fields: %s", s)
	}
	if !strings.Contains(s, `"details"`) {
		t.Fatalf("details should be present: %s", s)
	}
}

func TestJSONErrorMarshalNoDetails(t *testing.T) {
	e := NewJSONError("E_X", "msg")
	b, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(b), "details") {
		t.Fatalf("details should be omitted when nil: %s", b)
	}
}

func TestJSONErrorAsError(t *testing.T) {
	e := NewJSONError("E_Y", "y")
	if !strings.Contains(e.Error(), "E_Y") {
		t.Fatalf("error string should contain code")
	}
}
