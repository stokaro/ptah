package runtime

import (
	"context"

	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
)

var (
	statementRows = sessionCall[*protocol.ReadStatementRowsRequest, *protocol.StatementRows]{
		feature: provider.StatementsFeature, operation: "read statement rows", timeout: CallTimeout,
		validateInput: provider.ValidateReadStatementRowsRequest, validateOutput: provider.ValidateStatementRows,
	}
	statementClose = sessionCall[*protocol.CloseStatementRequest, *protocol.Empty]{
		feature: provider.StatementsFeature, operation: "close statement", timeout: CallTimeout,
		validateInput: provider.ValidateCloseStatementRequest, validateOutput: provider.ValidateClosedStatement,
	}
)

// statementRun is the call of one statement. A READ_ONLY statement changes
// nothing, so a reply that does not arrive leaves nothing uncertain; any
// other leaves the statement's effect unknown.
func statementRun(request *protocol.RunStatementRequest) sessionCall[*protocol.RunStatementRequest, *protocol.StatementResult] {
	call := sessionCall[*protocol.RunStatementRequest, *protocol.StatementResult]{
		feature: provider.StatementsFeature, operation: "run statement", timeout: CallTimeout, transaction: request.GetTransactionId(),
		validateInput: provider.ValidateRunStatementRequest, validateOutput: provider.ValidateStatementResult,
	}
	if request.GetAccess() != protocol.RunStatementRequest_READ_ONLY {
		call.uncertain = unknownStatement
	}
	return call
}

func unknownStatement() error {
	return &provider.StatementError{Outcome: protocol.StatementResult_UNKNOWN}
}

// RunStatement runs one statement the operator wrote on a live session. The
// reply says what the statement did; a reply that does not arrive or cannot be
// read leaves a statement that may write unknown, and the caller must not send
// it again.
func (c *Client) RunStatement(ctx context.Context, request *protocol.RunStatementRequest) (*protocol.StatementResult, error) {
	return statementRun(request).call(ctx, c, request, c.rpc.RunStatement)
}

// ReadStatementRows reads the next page of an open result. The caller resumes
// with the page's next page token, never by repeating a request whose reply it
// did not get.
func (c *Client) ReadStatementRows(ctx context.Context, request *protocol.ReadStatementRowsRequest) (*protocol.StatementRows, error) {
	return statementRows.call(ctx, c, request, c.rpc.ReadStatementRows)
}

// CloseStatement releases an open result before its last page.
func (c *Client) CloseStatement(ctx context.Context, request *protocol.CloseStatementRequest) (*protocol.Empty, error) {
	return statementClose.call(ctx, c, request, c.rpc.CloseStatement)
}

func (s *serverAdapter) RunStatement(ctx context.Context, request *protocol.RunStatementRequest) (*protocol.StatementResult, error) {
	if nilService(s.server.Statements) {
		return nil, unavailable("statements")
	}
	return statementRun(request).serve(ctx, s, request, s.server.Statements.RunStatement)
}

func (s *serverAdapter) ReadStatementRows(ctx context.Context, request *protocol.ReadStatementRowsRequest) (*protocol.StatementRows, error) {
	if nilService(s.server.Statements) {
		return nil, unavailable("statements")
	}
	return statementRows.serve(ctx, s, request, s.server.Statements.ReadStatementRows)
}

func (s *serverAdapter) CloseStatement(ctx context.Context, request *protocol.CloseStatementRequest) (*protocol.Empty, error) {
	if nilService(s.server.Statements) {
		return nil, unavailable("statements")
	}
	return statementClose.serve(ctx, s, request, s.server.Statements.CloseStatement)
}
