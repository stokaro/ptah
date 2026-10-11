package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/routineext"
	"ptah.run/engine"
)

// echoReader answers each body with one statement of the given kind holding
// the body's text, and records the requests it was asked.
type echoReader struct {
	kind  routineext.StatementKind
	asked *[]routineext.ReadRequest
}

func (r echoReader) ReadBodies(_ context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	*r.asked = append(*r.asked, request)
	result := routineext.ReadResult{}
	for _, body := range request.Bodies {
		result.Bodies = append(result.Bodies, routineext.Reading{Statements: []routineext.Statement{{Kind: r.kind, SQL: body.SQL}}})
	}
	return result, nil
}

// routineRuntime holds target alpha (alias a) reading bodies with reader, and
// beta, which reads none.
func routineRuntime(c *qt.C, reader routineext.Reader) *engine.Runtime {
	c.Helper()
	return must.Must(engine.New(engine.Provider{ID: "example.org/one", Targets: []engine.Target{
		{Name: "alpha", Aliases: []string{"a"}, RoutineBodies: reader}, {Name: "beta"},
	}}))
}

// ReadRoutineBodies asks the target's reader once, under the canonical name,
// and returns its answer.
func TestRuntime_ReadRoutineBodies_HappyPath(t *testing.T) {
	c := qt.New(t)
	var asked []routineext.ReadRequest
	runtime := routineRuntime(c, echoReader{kind: routineext.StatementSQL, asked: &asked})

	result, err := runtime.ReadRoutineBodies(c.Context(), routineext.ReadRequest{Target: "a",
		Bodies: []routineext.Body{{SQL: "x"}, {SQL: "y"}}})

	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, routineext.ReadResult{Bodies: []routineext.Reading{
		{Statements: []routineext.Statement{{Kind: routineext.StatementSQL, SQL: "x"}}},
		{Statements: []routineext.Statement{{Kind: routineext.StatementSQL, SQL: "y"}}},
	}})
	c.Assert(asked, qt.DeepEquals, []routineext.ReadRequest{{Target: "alpha", Bodies: []routineext.Body{{SQL: "x"}, {SQL: "y"}}}})
}

func TestRuntime_ReadRoutineBodies_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		ctx     context.Context
		target  string
		kind    routineext.StatementKind
		wantIs  error
		wantErr string
	}{
		{name: "a target without a reader", ctx: context.Background(), target: "beta", kind: routineext.StatementSQL,
			wantIs: ptaherr.ErrUnsupportedDialect, wantErr: `.*no routine body reader for target "beta"`},
		{name: "an unregistered target", ctx: context.Background(), target: "gamma", kind: routineext.StatementSQL,
			wantIs: ptaherr.ErrUnsupportedDialect, wantErr: `.*no routine body reader for target "gamma"`},
		{name: "a reply that does not answer", ctx: context.Background(), target: "alpha", kind: "loud",
			wantIs: routineext.ErrInvalid, wantErr: `routine body reader of "example.org/one": body 0: .*unknown kind "loud"`},
		{name: "no context", target: "alpha", kind: routineext.StatementSQL,
			wantIs: routineext.ErrInvalid, wantErr: `.*reading routine bodies requires a context`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked []routineext.ReadRequest
			runtime := routineRuntime(c, echoReader{kind: test.kind, asked: &asked})

			result, err := runtime.ReadRoutineBodies(test.ctx, routineext.ReadRequest{Target: test.target,
				Bodies: []routineext.Body{{SQL: "x"}}})

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, routineext.ReadResult{})
		})
	}
}
