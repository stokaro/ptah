package statements_test

import (
	"context"
	"math"
	"testing"

	"example.com/ptah-fixture-provider/backend"
	"example.com/ptah-fixture-provider/statements"
	qt "github.com/frankban/quicktest"

	protocol "ptah.run/provider-sdk/protocol/experimental"
)

// noSessions fails a test that reaches a session: a refused statement runs
// nothing.
type noSessions struct{ c *qt.C }

func (s noSessions) WithSession(_ context.Context, _, _ string, _ func(backend.Access) error) error {
	s.c.Fatalf("a refused statement reached its session")
	return nil
}

func every(months, days int64) *protocol.DataValue {
	return &protocol.DataValue{Value: &protocol.DataValue_IntervalValue{IntervalValue: &protocol.IntervalValue{Months: months, Days: days, Nanoseconds: 1}}}
}

// An argument SQLite has no value for refuses the statement, plain or typed,
// rather than binding null.
func TestRunStatement_RefusesWhatSQLiteCannotBind(t *testing.T) {
	interval := &protocol.ArgumentType{Scalar: protocol.ArgumentType_INTERVAL}
	tests := []struct {
		name    string
		request *protocol.RunStatementRequest
		want    string
	}{
		{name: "an interval of months", request: &protocol.RunStatementRequest{Arguments: []*protocol.DataValue{every(1, 0)}},
			want: "the fixture binds an interval as nanoseconds, which cannot hold months or days"},
		{name: "a typed interval of days", request: &protocol.RunStatementRequest{TypedArguments: []*protocol.StatementArgument{{Type: interval, Value: every(0, 1)}}},
			want: "the fixture binds an interval as nanoseconds, which cannot hold months or days"},
		{name: "a decimal", request: &protocol.RunStatementRequest{Arguments: []*protocol.DataValue{{Value: &protocol.DataValue_DecimalValue{
			DecimalValue: &protocol.DecimalValue{Digits: "1"}}}}}, want: "the fixture binds no decimal, date or native statement argument"},
		{name: "a date", request: &protocol.RunStatementRequest{Arguments: []*protocol.DataValue{{Value: &protocol.DataValue_DateValue{DateValue: 1}}}},
			want: "the fixture binds no decimal, date or native statement argument"},
		{name: "an element of a list", request: &protocol.RunStatementRequest{TypedArguments: []*protocol.StatementArgument{{
			Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_INTERVAL, List: true}, Elements: []*protocol.DataValue{every(1, 0)}}}},
			want: "the fixture binds an interval as nanoseconds, which cannot hold months or days"},
		{name: "a list JSON cannot hold", request: &protocol.RunStatementRequest{TypedArguments: []*protocol.StatementArgument{{
			Type: &protocol.ArgumentType{Scalar: protocol.ArgumentType_FLOAT64, List: true}, Elements: []*protocol.DataValue{{
				Value: &protocol.DataValue_DoubleValue{DoubleValue: math.NaN()}}}}}},
			want: "the fixture binds a list as JSON, which cannot hold one of its elements"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.request.SessionId, test.request.RequestId, test.request.Sql = "session", "request", "SELECT ?"
			test.request.Access, test.request.MaxRows = protocol.RunStatementRequest_READ_WRITE, 1
			service := &statements.Service{Sessions: noSessions{c: c}}
			reply, err := service.RunStatement(c.Context(), test.request)
			c.Assert(err, qt.IsNil)
			c.Assert(reply.GetOutcome(), qt.Equals, protocol.StatementResult_REFUSED)
			c.Assert(reply.GetRefusal(), qt.Equals, test.want)
		})
	}
}
