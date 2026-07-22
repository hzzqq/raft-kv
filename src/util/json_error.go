package util

import (
	"encoding/json"
	"fmt"
)

// JSONError 是带机器可读 code 的结构化错误，可直接序列化为 JSON 给调用方。
// 用于网关/API 统一错误体：{"code":"...","message":"...","details":...}。
// 同时实现 error 接口，可像普通 error 一样在调用链中传递。
type JSONError struct {
	Code    string      `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details,omitempty"`
}

// NewJSONError 创建结构化错误。
func NewJSONError(code, message string) *JSONError {
	return &JSONError{Code: code, Message: message}
}

// NewJSONErrorf 创建带格式化 message 的结构化错误。
func NewJSONErrorf(code, format string, args ...interface{}) *JSONError {
	return &JSONError{Code: code, Message: fmt.Sprintf(format, args...)}
}

// WithDetails 附加结构化细节并返回自身（链式）。
func (e *JSONError) WithDetails(details interface{}) *JSONError {
	e.Details = details
	return e
}

// Error 实现 error 接口。
func (e *JSONError) Error() string {
	if e.Details != nil {
		return fmt.Sprintf("[%s] %s (%v)", e.Code, e.Message, e.Details)
	}
	return fmt.Sprintf("[%s] %s", e.Code, e.Message)
}

// MarshalJSON 输出稳定的错误体（details 为零值时省略该字段）。
func (e *JSONError) MarshalJSON() ([]byte, error) {
	type alias JSONError
	return json.Marshal((*alias)(e))
}
