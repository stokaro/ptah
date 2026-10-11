package schemalineage_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/routineext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemalineage"
)

// countingBodies counts the calls it answers, passing each to the bundled
// runtime, or fails every one with failure.
type countingBodies struct {
	calls   *int
	bodies  *int
	failure error
}

func (b countingBodies) ReadRoutineBodies(ctx context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	*b.calls++
	*b.bodies += len(request.Bodies)
	result, err := builtintest.Runtime().ReadRoutineBodies(ctx, request)
	return result, errors.Join(err, b.failure)
}

// threeRoutines holds two procedural routines and one plain-SQL function.
func threeRoutines() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "C", Name: "customers"}},
		Fields: []schemamodel.Field{{StructName: "C", Name: "email", Type: "TEXT"}},
		Functions: []schemamodel.Function{
			{Name: "a", Language: "plpgsql", Body: "BEGIN DELETE FROM customers; END;"},
			{Name: "b", Language: "sql", Body: "SELECT email FROM customers"},
			{Name: "c", Language: "plpgsql", Body: "BEGIN UPDATE customers SET email = 'x'; END;"},
		},
	}
}

// The procedural bodies of a schema are read in one call, and a schema with
// none asks nothing.
func TestDeriveRoutines_ReadsEveryBodyInOneCall(t *testing.T) {
	for _, test := range []struct {
		name       string
		db         *schemamodel.Database
		wantCalls  int
		wantBodies int
		wantWrites []string
	}{
		{name: "two procedural bodies", db: threeRoutines(), wantCalls: 1, wantBodies: 2,
			wantWrites: []string{"customers:delete", "customers.email:update"}},
		{name: "no procedural body", db: &schemamodel.Database{Functions: []schemamodel.Function{
			{Name: "b", Language: "sql", Body: "SELECT 1"}}}, wantCalls: 0, wantBodies: 0, wantWrites: make([]string, 0)},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			calls, bodies := 0, 0

			result, err := schemalineage.DeriveRoutines(c.Context(), countingBodies{calls: &calls, bodies: &bodies}, test.db, "postgres")

			c.Assert(err, qt.IsNil)
			c.Assert([]int{calls, bodies}, qt.DeepEquals, []int{test.wantCalls, test.wantBodies})
			c.Assert(writeTargets(result), qt.DeepEquals, test.wantWrites)
		})
	}
}

// A target whose owner reads no bodies says so on each procedural routine,
// whichever dialect it is.
func TestDeriveRoutines_AnOwnerThatReadsNoBodiesSaysSo(t *testing.T) {
	c := qt.New(t)
	withoutReader := must.Must(engine.New(engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{{Name: "postgres"}}}))

	result, err := schemalineage.DeriveRoutines(c.Context(), withoutReader, proceduralRoutine("BEGIN DELETE FROM customers; END;"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(result.Writes, qt.HasLen, 0)
	c.Assert(result.Undecided, qt.DeepEquals, []schemalineage.UndecidedRoutine{
		{Routine: "r", Kind: "function", Reason: "no routine-body analysis exists for the postgres dialect"},
	})
}

func TestDeriveRoutines_FailurePath(t *testing.T) {
	calls, bodies := 0, 0
	for _, test := range []struct {
		name    string
		bodies  schemalineage.Bodies
		wantErr string
	}{
		{name: "no reader", wantErr: `routine lineage reads procedural bodies and was given no routine body reader`},
		{name: "a reader that fails", bodies: countingBodies{calls: &calls, bodies: &bodies, failure: errors.New("boom")},
			wantErr: `read the routine bodies: boom`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := schemalineage.DeriveRoutines(c.Context(), test.bodies, threeRoutines(), "postgres")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, schemalineage.RoutineResult{})
		})
	}
}

// columnBlindReader reads a body as one statement and says nothing about
// which names are columns.
type columnBlindReader struct{}

func (columnBlindReader) ReadBodies(_ context.Context, request routineext.ReadRequest) (routineext.ReadResult, error) {
	result := routineext.ReadResult{}
	for _, body := range request.Bodies {
		result.Bodies = append(result.Bodies, routineext.Reading{
			Statements: []routineext.Statement{{Kind: routineext.StatementSQL, SQL: body.SQL}},
		})
	}
	return result, nil
}

// Whether a body's reads are derived is the owner's to say: an owner that does
// not vouch for its names gets its writes and no reads, and the reason says
// why.
func TestDeriveRoutines_ReadsFollowTheOwnersWord(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(engine.New(engine.Provider{ID: "example.org/postgres", Targets: []engine.Target{
		{Name: "postgres", RoutineBodies: columnBlindReader{}}}}))

	result, err := schemalineage.DeriveRoutines(c.Context(), runtime,
		customersSchema("", "plpgsql", "UPDATE customers SET country = 'CZ' WHERE id = 1;"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(writeTargets(result), qt.DeepEquals, []string{"customers.country:update"})
	c.Assert(result.Reads, qt.HasLen, 0)
	c.Assert(result.Undecided[0].Reason, qt.Equals, "the body is plpgsql: every statement was classified, "+
		"so the writes are complete; the columns it reads are not resolved, because a declared variable takes "+
		"precedence over a column of the same name on this engine and the body does not distinguish them")
}
