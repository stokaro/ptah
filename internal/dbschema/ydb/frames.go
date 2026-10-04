package ydb

import "regexp"

// sdkFrame matches one stack frame ydb-go-sdk writes into an error's text.
// The SDK wraps an error at each layer it passes through and appends the
// caller to the message every time:
//
//	operation/GENERIC_ERROR (code = 400080, ...) at `github.com/ydb-platform/ydb-go-sdk/v3/internal/conn.(*grpcClientStream).RecvMsg(grpc_client_stream.go:180)` at `...`
//
// Measured on YDB 26.2.1.14 and 25.1.4.7 through ydb-go-sdk v3.153.2, one
// failed statement carried nine of them. A frame is always the SDK's own
// package path inside backticks after " at ", and a server message does not
// take that shape, so the pattern names the path rather than any backticked
// text.
var sdkFrame = regexp.MustCompile(" at `github\\.com/ydb-platform/ydb-go-sdk/v3/[^`]*`")

// WithoutStackFrames returns err with the stack frames ydb-go-sdk writes into
// its text taken out, and everything else kept: the status, the status code,
// the server's address and its issues with their codes and messages.
//
// Only the text changes. The result wraps err, so errors.Is and errors.As --
// and the SDK's own predicates such as ydb.IsOperationError, which use them --
// answer as they did. An error that carries no frame is returned as it is,
// which keeps the sentinels database/sql compares by identity, such as io.EOF
// and driver.ErrSkip, identical. A nil error stays nil.
func WithoutStackFrames(err error) error {
	if err == nil {
		return nil
	}
	text := err.Error()
	stripped := sdkFrame.ReplaceAllString(text, "")
	if stripped == text {
		return err
	}
	return &framelessError{err: err, text: stripped}
}

// framelessError is an SDK error read without its stack frames.
type framelessError struct {
	err  error
	text string
}

func (e *framelessError) Error() string { return e.text }

func (e *framelessError) Unwrap() error { return e.err }
