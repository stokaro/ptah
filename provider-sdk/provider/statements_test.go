package provider_test

import (
	"errors"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	protocol "github.com/stokaro/ptah/provider-sdk/protocol/experimental"
	"github.com/stokaro/ptah/provider-sdk/provider"
	"google.golang.org/protobuf/proto"
)

func statementRequest() *protocol.RunStatementRequest {
	return &protocol.RunStatementRequest{SessionId: "session", RequestId: "request", Sql: "SELECT code, rate FROM rates WHERE code = ?",
		Arguments: []*protocol.DataValue{text("CZ")}, Access: protocol.RunStatementRequest_READ_WRITE, MaxRows: 2}
}

func statementColumns() []*protocol.DataColumn {
	return []*protocol.DataColumn{{Name: "code", Type: "Utf8", Kind: protocol.DataKind_DATA_KIND_STRING}, {Name: "rate"}}
}

func statementResult() *protocol.StatementResult {
	return &protocol.StatementResult{RequestId: "request", Outcome: protocol.StatementResult_SUCCEEDED, Columns: statementColumns(),
		Rows: []*protocol.Row{row(text("CZ"), decimal("150", 10, 2)), row(text("CZ"), null())}, NextPageToken: "page-2"}
}

func pageRequest() *protocol.ReadStatementRowsRequest {
	return &protocol.ReadStatementRowsRequest{SessionId: "session", PageToken: "page-2", MaxRows: 2, ColumnCount: 2}
}

func TestRunStatement_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := statementRequest()
	before := proto.CloneOf(request)
	c.Assert(provider.ValidateRunStatementRequest(request), qt.IsNil)
	c.Assert(provider.ValidateStatementResult(request, statementResult()), qt.IsNil)
	c.Assert(proto.Equal(before, request), qt.IsTrue)

	request.Access, request.Arguments = protocol.RunStatementRequest_READ_ONLY, nil
	c.Assert(provider.ValidateRunStatementRequest(request), qt.IsNil)
	request.Access, request.TransactionId = protocol.RunStatementRequest_READ_WRITE, "transaction"
	c.Assert(provider.ValidateRunStatementRequest(request), qt.IsNil)

	for _, result := range []*protocol.StatementResult{
		{RequestId: "request", Outcome: protocol.StatementResult_SUCCEEDED, RowsAffected: new(uint64(3))},
		{RequestId: "request", Outcome: protocol.StatementResult_SUCCEEDED, RowsAffected: new(uint64(0))},
		{RequestId: "request", Outcome: protocol.StatementResult_SUCCEEDED},
		{RequestId: "request", Outcome: protocol.StatementResult_SUCCEEDED, Columns: statementColumns()},
		{RequestId: "request", Outcome: protocol.StatementResult_FAILED, Error: "duplicate key value (code)=(CZ)\nDETAIL: key exists", ErrorCode: "23505"},
		{RequestId: "request", Outcome: protocol.StatementResult_FAILED, Error: strings.Repeat("x", provider.MaxStatementErrorBytes)},
		{RequestId: "request", Outcome: protocol.StatementResult_REFUSED, Refusal: "the statement writes, and a check may not"},
		{RequestId: "request", Outcome: protocol.StatementResult_UNKNOWN},
	} {
		c.Assert(provider.ValidateStatementResult(request, result), qt.IsNil, qt.Commentf("%v", result))
	}
}

func TestRunStatementRequest_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.RunStatementRequest)
		want   string
	}{
		{"no session", func(r *protocol.RunStatementRequest) { r.SessionId = "" }, "requires a session and a request ID"},
		{"no request ID", func(r *protocol.RunStatementRequest) { r.RequestId = "" }, "requires a session and a request ID"},
		{"blank text", func(r *protocol.RunStatementRequest) { r.Sql = " \n" }, "requires UTF-8 text without a NUL"},
		{"text that is not UTF-8", func(r *protocol.RunStatementRequest) { r.Sql = "SELECT '\xff'" }, "requires UTF-8 text without a NUL"},
		{"text with a NUL", func(r *protocol.RunStatementRequest) { r.Sql = "SELECT 1\x00" }, "requires UTF-8 text without a NUL"},
		{"an unset argument", func(r *protocol.RunStatementRequest) { r.Arguments = append(r.Arguments, &protocol.DataValue{}) }, "argument is unset"},
		{"an invalid argument", func(r *protocol.RunStatementRequest) { r.Arguments[0] = text("\xff") }, "a string value is not UTF-8"},
		{"an unspecified access", func(r *protocol.RunStatementRequest) { r.Access = protocol.RunStatementRequest_ACCESS_UNSPECIFIED }, "requires a known access"},
		{"an unknown access", func(r *protocol.RunStatementRequest) { r.Access = 9 }, "requires a known access"},
		{"a read-only statement in a transaction", func(r *protocol.RunStatementRequest) {
			r.Access, r.TransactionId = protocol.RunStatementRequest_READ_ONLY, "transaction"
		}, "runs in a transaction of its own"},
		{"no page bound", func(r *protocol.RunStatementRequest) { r.MaxRows = 0 }, "a bound of at most 10000 rows"},
		{"a page bound over the limit", func(r *protocol.RunStatementRequest) { r.MaxRows = provider.MaxRowsPerPage + 1 }, "a bound of at most 10000 rows"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := statementRequest()
			test.mutate(request)
			err := provider.ValidateRunStatementRequest(request)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
		})
	}
}

func TestStatementResult_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.StatementResult)
		want   string
	}{
		{"another request ID", func(r *protocol.StatementResult) { r.RequestId = "other" }, "changed its request binding"},
		{"an unspecified outcome", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken = protocol.StatementResult_OUTCOME_UNSPECIFIED, nil, nil, ""
		}, "unknown statement outcome"},
		{"rows on a failure", func(r *protocol.StatementResult) { r.Outcome, r.Error = protocol.StatementResult_FAILED, "no" },
			"only a statement that succeeded returns rows"},
		{"a row count on a refusal", func(r *protocol.StatementResult) {
			r.Outcome, r.Refusal, r.Columns, r.Rows, r.NextPageToken = protocol.StatementResult_REFUSED, "no", nil, nil, ""
			r.RowsAffected = new(uint64(1))
		}, "only a statement that succeeded returns rows"},
		{"a page token on an unknown outcome", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows = protocol.StatementResult_UNKNOWN, nil, nil
		}, "only a statement that succeeded returns rows"},
		{"a failure without a message", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken = protocol.StatementResult_FAILED, nil, nil, ""
		}, "exactly a failed statement carries the server's message"},
		{"a message on a success", func(r *protocol.StatementResult) { r.Error = "no" }, "exactly a failed statement carries the server's message"},
		{"a code without a message", func(r *protocol.StatementResult) { r.ErrorCode = "23505" }, "exactly a failed statement carries the server's message"},
		{"a refusal without a reason", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken = protocol.StatementResult_REFUSED, nil, nil, ""
		}, "exactly a refused statement carries a refusal"},
		{"a reason on a success", func(r *protocol.StatementResult) { r.Refusal = "no" }, "exactly a refused statement carries a refusal"},
		{"a refusal of two lines", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken, r.Refusal = protocol.StatementResult_REFUSED, nil, nil, "", "one\ntwo"
		}, "refusal is not one line of text"},
		{"a message over its bound", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken = protocol.StatementResult_FAILED, nil, nil, ""
			r.Error = strings.Repeat("x", provider.MaxStatementErrorBytes+1)
		}, "message is over its bound"},
		{"a message with a NUL", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken, r.Error = protocol.StatementResult_FAILED, nil, nil, "", "no\x00"
		}, "message is over its bound, not UTF-8 or holds a NUL"},
		{"a message that is not UTF-8", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken, r.Error = protocol.StatementResult_FAILED, nil, nil, "", "\xff"
		}, "message is over its bound, not UTF-8 or holds a NUL"},
		{"a code with a space", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken, r.Error, r.ErrorCode = protocol.StatementResult_FAILED, nil, nil, "", "no", "23 505"
		}, "code is not one token"},
		{"a code over its bound", func(r *protocol.StatementResult) {
			r.Outcome, r.Columns, r.Rows, r.NextPageToken, r.Error, r.ErrorCode = protocol.StatementResult_FAILED, nil, nil, "", "no", strings.Repeat("9", 65)
		}, "code is not one token"},
		{"a column name that is not UTF-8", func(r *protocol.StatementResult) { r.Columns[1].Name = "\xff" }, "column's name is not UTF-8"},
		{"a column with a type and no kind", func(r *protocol.StatementResult) { r.Columns[1].Type = "Decimal(10,2)" }, "needs a type and a kind, or neither"},
		{"rows without columns", func(r *protocol.StatementResult) { r.Columns = nil }, "returns no columns returns no rows"},
		{"a page token without columns", func(r *protocol.StatementResult) { r.Columns, r.Rows = nil, nil }, "returns no columns returns no rows"},
		{"a page token that is not UTF-8", func(r *protocol.StatementResult) { r.NextPageToken = "\xff" }, "invalid page token"},
		{"more rows than the bound", func(r *protocol.StatementResult) { r.Rows = append(r.Rows, row(text("SK"), null())) }, "more rows than its bound"},
		{"a row shorter than the columns", func(r *protocol.StatementResult) { r.Rows[0] = row(text("CZ")) }, "do not align with its columns"},
		{"an unset value", func(r *protocol.StatementResult) { r.Rows[1] = row(text("CZ"), &protocol.DataValue{}) }, "leaves a column unset"},
		{"an invalid value", func(r *protocol.StatementResult) { r.Rows[1] = row(text("\xff"), null()) }, "a string value is not UTF-8"},
		{"a value of another kind", func(r *protocol.StatementResult) { r.Rows[1] = row(decimal("1", 1, 0), null()) }, "kind disagrees with its column type"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result := statementResult()
			test.mutate(result)
			err := provider.ValidateStatementResult(statementRequest(), result)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
		})
	}
	c := qt.New(t)
	request := statementRequest()
	request.SessionId = ""
	c.Assert(provider.ValidateStatementResult(request, statementResult()), qt.ErrorMatches, ".*requires a session and a request ID")
	c.Assert(provider.ValidateStatementResult(statementRequest(), nil), qt.ErrorMatches, ".*changed its request binding")
}

func TestStatementRows_HappyPath(t *testing.T) {
	c := qt.New(t)
	request := pageRequest()
	c.Assert(provider.ValidateReadStatementRowsRequest(request), qt.IsNil)
	c.Assert(provider.ValidateStatementRows(request, &protocol.StatementRows{Rows: []*protocol.Row{row(text("SK"), null())}, NextPageToken: "page-3"}), qt.IsNil)
	c.Assert(provider.ValidateStatementRows(request, &protocol.StatementRows{}), qt.IsNil)
	closing := &protocol.CloseStatementRequest{SessionId: "session", PageToken: "page-2"}
	c.Assert(provider.ValidateCloseStatementRequest(closing), qt.IsNil)
	c.Assert(provider.ValidateClosedStatement(closing, &protocol.Empty{}), qt.IsNil)
}

func TestStatementRows_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		mutate func(*protocol.ReadStatementRowsRequest, *protocol.StatementRows)
		want   string
	}{
		{"no session", func(r *protocol.ReadStatementRowsRequest, _ *protocol.StatementRows) { r.SessionId = "" }, "page read requires a session"},
		{"no page token", func(r *protocol.ReadStatementRowsRequest, _ *protocol.StatementRows) { r.PageToken = "" }, "requires a page token"},
		{"a page token over its bound", func(r *protocol.ReadStatementRowsRequest, _ *protocol.StatementRows) {
			r.PageToken = strings.Repeat("p", 4097)
		}, "invalid page token"},
		{"no column count", func(r *protocol.ReadStatementRowsRequest, _ *protocol.StatementRows) { r.ColumnCount = 0 }, "result's column count"},
		{"no page bound", func(r *protocol.ReadStatementRowsRequest, _ *protocol.StatementRows) { r.MaxRows = 0 }, "a bound of at most 10000 rows"},
		{"a page bound over the limit", func(r *protocol.ReadStatementRowsRequest, _ *protocol.StatementRows) {
			r.MaxRows = provider.MaxRowsPerPage + 1
		}, "a bound of at most 10000 rows"},
		{"more rows than the bound", func(_ *protocol.ReadStatementRowsRequest, p *protocol.StatementRows) {
			p.Rows = append(p.Rows, row(text("SK"), null()), row(text("PL"), null()))
		}, "more rows than its bound"},
		{"a row of another width", func(_ *protocol.ReadStatementRowsRequest, p *protocol.StatementRows) {
			p.Rows = []*protocol.Row{row(text("SK"))}
		}, "do not align with its columns"},
		{"an unset value", func(_ *protocol.ReadStatementRowsRequest, p *protocol.StatementRows) {
			p.Rows = []*protocol.Row{row(text("SK"), &protocol.DataValue{})}
		}, "leaves a column unset"},
		{"a next page token that is not UTF-8", func(_ *protocol.ReadStatementRowsRequest, p *protocol.StatementRows) { p.NextPageToken = "\xff" },
			"invalid page token"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request, page := pageRequest(), &protocol.StatementRows{Rows: []*protocol.Row{row(text("SK"), null())}}
			test.mutate(request, page)
			err := provider.ValidateStatementRows(request, page)
			c.Assert(err, qt.ErrorIs, provider.ErrProtocol)
			c.Assert(err, qt.ErrorMatches, ".*"+regexp.QuoteMeta(test.want)+".*")
		})
	}
	c := qt.New(t)
	c.Assert(provider.ValidateStatementRows(pageRequest(), nil), qt.ErrorMatches, ".*statement page is missing")
	for _, closing := range []*protocol.CloseStatementRequest{{PageToken: "page-2"}, {SessionId: "session"}} {
		c.Assert(provider.ValidateCloseStatementRequest(closing), qt.ErrorMatches, ".*requires a session and a page token")
	}
	c.Assert(provider.ValidateCloseStatementRequest(&protocol.CloseStatementRequest{SessionId: "session", PageToken: "\xff"}), qt.ErrorMatches,
		".*invalid page token")
	c.Assert(provider.ValidateClosedStatement(&protocol.CloseStatementRequest{SessionId: "session", PageToken: "page-2"}, nil), qt.ErrorMatches,
		".*release reply is missing")
}

func TestCheckStatement_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(provider.CheckStatement(statementRequest(), statementResult()), qt.IsNil)
}

func TestCheckStatement_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		result *protocol.StatementResult
		wantIs error
	}{
		{"failed", &protocol.StatementResult{RequestId: "request", Outcome: protocol.StatementResult_FAILED, Error: "no"}, provider.ErrExecution},
		{"refused", &protocol.StatementResult{RequestId: "request", Outcome: protocol.StatementResult_REFUSED, Refusal: "no"}, provider.ErrExecution},
		{"unknown", &protocol.StatementResult{RequestId: "request", Outcome: protocol.StatementResult_UNKNOWN}, provider.ErrOutcomeUnknown},
		{"malformed", &protocol.StatementResult{RequestId: "other", Outcome: protocol.StatementResult_SUCCEEDED}, provider.ErrOutcomeUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := provider.CheckStatement(statementRequest(), test.result)
			c.Assert(err, qt.ErrorIs, test.wantIs)
			statement, isStatement := errors.AsType[*provider.StatementError](err)
			c.Assert(isStatement, qt.IsTrue)
			c.Assert(err.Error(), qt.Contains, "provider statement outcome is "+statement.Outcome.String())
		})
	}
	c := qt.New(t)
	c.Assert(provider.CheckStatement(statementRequest(), &protocol.StatementResult{RequestId: "other"}), qt.ErrorIs, provider.ErrProtocol)
}

// An engine lists the statements service only when its provider serves it.
func TestStatementsDescription_FailurePath(t *testing.T) {
	c := qt.New(t)
	d := description()
	d.Engines[0].Features = []string{provider.StatementsFeature}
	c.Assert(provider.ValidateDescription(d, nil), qt.ErrorMatches, `.*engine sessions\.statements requires the provider's`)
	d.Features = append(d.Features, provider.StatementsFeature)
	c.Assert(provider.ValidateDescription(d, nil), qt.IsNil)
}
