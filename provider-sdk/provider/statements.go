package provider

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"unicode"
	"unicode/utf8"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
)

// StatementsFeature advertises the statements service: running a statement
// the operator wrote, such as a check, a test assertion or a script step, on
// a live session, and returning what it read in pages. Each supporting engine
// also declares it. The host sends no statement it composes.
const StatementsFeature = "sessions.statements"

// MaxStatementErrorBytes bounds the server's message a failed statement
// carries.
const MaxStatementErrorBytes = 4096

// maxStatementErrorCode bounds the server's code for a failed statement.
const maxStatementErrorCode = 64

// StatementRunner runs the operator's statements on a live session. A result
// larger than one page stays open until its last page is read, CloseStatement
// releases it, or the transaction or the session it was read in ends.
type StatementRunner interface {
	// RunStatement runs one statement. A READ_ONLY statement runs only when
	// the owner proves, before running it, that it is one query that changes
	// nothing and reads only objects of the session's database, and runs where
	// a write is refused; otherwise the reply is REFUSED and nothing ran.
	RunStatement(context.Context, *protocol.RunStatementRequest) (*protocol.StatementResult, error)
	// ReadStatementRows returns the next page of an open result. A read that
	// cannot finish is an error and no page.
	ReadStatementRows(context.Context, *protocol.ReadStatementRowsRequest) (*protocol.StatementRows, error)
	// CloseStatement releases an open result. A token whose result is already
	// released succeeds.
	CloseStatement(context.Context, *protocol.CloseStatementRequest) (*protocol.Empty, error)
}

// ValidateRunStatementRequest refuses a statement without a session, a
// request ID or text, text that is not UTF-8 or holds a NUL, an argument that
// is unset or invalid, an unspecified access, a READ_ONLY statement in a
// transaction, and a page bound outside 1 to MaxRowsPerPage.
func ValidateRunStatementRequest(request *protocol.RunStatementRequest) error {
	if request.GetSessionId() == "" || request.GetRequestId() == "" {
		return statementError("a statement requires a session and a request ID")
	}
	if strings.TrimSpace(request.GetSql()) == "" || !utf8.ValidString(request.GetSql()) || strings.ContainsRune(request.GetSql(), 0) {
		return statementError("a statement requires UTF-8 text without a NUL")
	}
	for _, argument := range request.GetArguments() {
		if argument.GetValue() == nil {
			return statementError("a statement argument is unset")
		}
		if err := ValidateDataValue(argument); err != nil {
			return err
		}
	}
	switch request.GetAccess() {
	case protocol.RunStatementRequest_READ_WRITE:
	case protocol.RunStatementRequest_READ_ONLY:
		if request.GetTransactionId() != "" {
			return statementError("a read-only statement runs in a transaction of its own")
		}
	default:
		return statementError("a statement requires a known access")
	}
	return validateStatementPageBound(request.GetMaxRows())
}

// ValidateStatementResult binds a reply to its request and checks the
// outcome's shape. Only SUCCEEDED carries columns, rows, a row count or a page
// token; its rows have one set value per column, of the column's kind, and no
// more rows than the bound. Only FAILED carries the server's message, at most
// MaxStatementErrorBytes of UTF-8 without a NUL, and its code, one token of at
// most 64 bytes. Only REFUSED carries a refusal, one line of text.
func ValidateStatementResult(request *protocol.RunStatementRequest, result *protocol.StatementResult) error {
	if err := ValidateRunStatementRequest(request); err != nil {
		return err
	}
	// The request ID is never empty, so a missing reply answers another one.
	if result.GetRequestId() != request.GetRequestId() {
		return statementError("a statement result changed its request binding")
	}
	succeeded := result.GetOutcome() == protocol.StatementResult_SUCCEEDED
	if !succeeded && (len(result.GetColumns()) != 0 || len(result.GetRows()) != 0 || result.RowsAffected != nil || result.GetNextPageToken() != "") {
		return statementError("only a statement that succeeded returns rows, a row count or a page token")
	}
	if (result.GetOutcome() == protocol.StatementResult_FAILED) != (result.GetError() != "") || result.GetErrorCode() != "" && result.GetError() == "" {
		return statementError("exactly a failed statement carries the server's message")
	}
	if (result.GetOutcome() == protocol.StatementResult_REFUSED) != (result.GetRefusal() != "") {
		return statementError("exactly a refused statement carries a refusal")
	}
	switch result.GetOutcome() {
	case protocol.StatementResult_SUCCEEDED:
		return validateStatementRows(result, request.GetMaxRows())
	case protocol.StatementResult_FAILED:
		return validateServerError(result.GetError(), result.GetErrorCode())
	case protocol.StatementResult_REFUSED:
		if !diagnosticLine(result.GetRefusal()) {
			return statementError("a statement refusal is not one line of text")
		}
		return nil
	case protocol.StatementResult_UNKNOWN:
		return nil
	default:
		return statementError("unknown statement outcome")
	}
}

// validateStatementRows checks the first page of a result that succeeded.
func validateStatementRows(result *protocol.StatementResult, maxRows uint32) error {
	for _, column := range result.GetColumns() {
		if !utf8.ValidString(column.GetName()) {
			return statementError("a result column's name is not UTF-8")
		}
		if err := validateColumnType(column); err != nil {
			return err
		}
	}
	if len(result.GetColumns()) == 0 && (len(result.GetRows()) != 0 || result.GetNextPageToken() != "") {
		return statementError("a statement that returns no columns returns no rows")
	}
	if err := validatePageToken(result.GetNextPageToken()); err != nil {
		return err
	}
	if err := validatePageRows(result.GetRows(), len(result.GetColumns()), maxRows); err != nil {
		return err
	}
	return validateRowKinds(result.GetRows(), result.GetColumns())
}

// validateServerError checks the server's message and code for a failed
// statement.
func validateServerError(message, code string) error {
	if len(message) > MaxStatementErrorBytes || !utf8.ValidString(message) || strings.ContainsRune(message, 0) {
		return statementError("a failed statement's message is over its bound, not UTF-8 or holds a NUL")
	}
	if len(code) > maxStatementErrorCode || !utf8.ValidString(code) ||
		strings.ContainsFunc(code, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }) {
		return statementError("a failed statement's code is not one token")
	}
	return nil
}

// ValidateReadStatementRowsRequest refuses a page read without a session, a
// page token, a page bound from 1 to MaxRowsPerPage or the result's column
// count.
func ValidateReadStatementRowsRequest(request *protocol.ReadStatementRowsRequest) error {
	if request.GetSessionId() == "" {
		return statementError("a page read requires a session")
	}
	if request.GetPageToken() == "" {
		return statementError("a page read requires a page token")
	}
	if err := validatePageToken(request.GetPageToken()); err != nil {
		return err
	}
	if request.GetColumnCount() == 0 {
		return statementError("a page read requires the result's column count")
	}
	return validateStatementPageBound(request.GetMaxRows())
}

// ValidateStatementRows checks a page of an open result: no more rows than
// the bound, one set value per column the request counts, and a valid next
// page token.
func ValidateStatementRows(request *protocol.ReadStatementRowsRequest, result *protocol.StatementRows) error {
	if err := ValidateReadStatementRowsRequest(request); err != nil {
		return err
	}
	if result == nil {
		return statementError("a statement page is missing")
	}
	if err := validatePageToken(result.GetNextPageToken()); err != nil {
		return err
	}
	return validatePageRows(result.GetRows(), int(request.GetColumnCount()), request.GetMaxRows())
}

// ValidateCloseStatementRequest refuses a release without a session or a page
// token.
func ValidateCloseStatementRequest(request *protocol.CloseStatementRequest) error {
	if request.GetSessionId() == "" || request.GetPageToken() == "" {
		return statementError("a release requires a session and a page token")
	}
	return validatePageToken(request.GetPageToken())
}

// ValidateClosedStatement checks the reply of a release.
func ValidateClosedStatement(request *protocol.CloseStatementRequest, result *protocol.Empty) error {
	if err := ValidateCloseStatementRequest(request); err != nil {
		return err
	}
	if result == nil {
		return statementError("a release reply is missing")
	}
	return nil
}

// StatementError records a statement that did not succeed. UNKNOWN, and a
// reply that did not arrive, wrap ErrOutcomeUnknown; FAILED and REFUSED wrap
// ErrExecution. Provider and server text is not included.
type StatementError struct {
	Outcome protocol.StatementResult_Outcome
}

// Error describes the outcome without provider or server text.
func (e *StatementError) Error() string {
	return fmt.Sprintf("provider statement outcome is %s", e.Outcome)
}

// Unwrap distinguishes an uncertain statement from a confirmed one.
func (e *StatementError) Unwrap() error {
	switch e.Outcome {
	case protocol.StatementResult_FAILED, protocol.StatementResult_REFUSED:
		return ErrExecution
	default:
		return ErrOutcomeUnknown
	}
}

// CheckStatement accepts only SUCCEEDED. A malformed reply leaves the outcome
// unknown, and the caller must not send the statement again.
func CheckStatement(request *protocol.RunStatementRequest, result *protocol.StatementResult) error {
	if err := ValidateStatementResult(request, result); err != nil {
		return errors.Join(err, &StatementError{Outcome: protocol.StatementResult_UNKNOWN})
	}
	if result.GetOutcome() == protocol.StatementResult_SUCCEEDED {
		return nil
	}
	return &StatementError{Outcome: result.GetOutcome()}
}

func validateStatementPageBound(maxRows uint32) error {
	if maxRows == 0 || maxRows > MaxRowsPerPage {
		return fmt.Errorf("%w: a statement page requires a bound of at most %d rows", ErrProtocol, MaxRowsPerPage)
	}
	return nil
}

func validatePageToken(token string) error {
	if len(token) > maxPageToken || !utf8.ValidString(token) {
		return statementError("invalid page token")
	}
	return nil
}

// validatePageRows checks one page of rows: at most maxRows, each with one set
// and valid value per column.
func validatePageRows(rows []*protocol.Row, width int, maxRows uint32) error {
	if len(rows) > int(maxRows) {
		return statementError("a statement page holds more rows than its bound")
	}
	if err := validateRows(rows, width); err != nil {
		return err
	}
	return requireSetValues(rows)
}

func statementError(reason string) error {
	return fmt.Errorf("%w: %s", ErrProtocol, reason)
}
