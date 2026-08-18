//go:build integration

package integration_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/dbtarget"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// declaredRoutine is one routine as a schema declares it.
func declaredRoutine(name, parameters, returns, body string) goschema.Function {
	return goschema.Function{
		Name: name, Parameters: parameters, Returns: returns,
		Language: "sql", Security: "INVOKER", Volatility: "VOLATILE", Body: body,
	}
}

// TestPostgresRoutineOverloadIdentityE2E measures the defect stokaro/ptah#1664
// names against the only judge that can settle it: a server that actually holds
// two overloads.
//
// Two routines with one name and different argument types are two objects.
// Before this, the comparator kept one entry per name, so the second overwrote
// the first: a dropped overload was reported as a MODIFICATION of the survivor
// rather than as a removal, and a new overload was never created. Both answers
// were wrong and both exited 0.
//
// The signature comes from the catalog's own pg_get_function_identity_arguments,
// which is what makes this measurable rather than assumed — the declaration says
// `a int` where the catalog says `a integer`, and the normalizer is written to
// agree with the catalog rather than to reproduce it.
func TestPostgresRoutineOverloadIdentityE2E(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.PostgreSQL)

	tests := []struct {
		name        string
		declared    []goschema.Function
		wantAdded   int
		wantRemoved int
	}{
		{
			// The dangerous direction: the schema stops declaring one overload
			// and the plan has to drop exactly that one.
			name: "a dropped overload is removed rather than folded into the survivor",
			declared: []goschema.Function{
				declaredRoutine("greet", "a integer", "text", "SELECT 'int'"),
			},
			wantRemoved: 1,
		},
		{
			// Declared in the REVERSE of the order the catalog reports them,
			// which is what separates pairing by signature from pairing by
			// position. The bodies differ, so a positional pairing matches
			// greet(text)'s declaration to greet(integer)'s row and reports two
			// modifications; pairing on the signature reports none.
			name: "both overloads declared, in the opposite order, is no change at all",
			declared: []goschema.Function{
				declaredRoutine("greet", "a text", "text", "SELECT 'text'"),
				declaredRoutine("greet", "a integer", "text", "SELECT 'int'"),
			},
		},
		{
			// A third overload the database does not have is an addition, and
			// the two it does have stay paired.
			name: "a new overload is added and the existing pair is untouched",
			declared: []goschema.Function{
				declaredRoutine("greet", "a boolean", "text", "SELECT 'bool'"),
				declaredRoutine("greet", "a text", "text", "SELECT 'text'"),
				declaredRoutine("greet", "a integer", "text", "SELECT 'int'"),
			},
			wantAdded: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()

			adminDB, err := sql.Open("pgx", dbURL)
			c.Assert(err, qt.IsNil)
			defer adminDB.Close()

			testDBName := fmt.Sprintf("ptah_routine_overload_e2e_%d", time.Now().UnixNano())
			createE2EDatabase(c, ctx, adminDB, testDBName)
			defer dropE2EDatabase(c, context.Background(), adminDB, testDBName)

			scopedURL := replaceDatabaseName(c, dbURL, testDBName)
			setupDB, err := sql.Open("pgx", scopedURL)
			c.Assert(err, qt.IsNil)
			defer setupDB.Close()
			_, err = setupDB.ExecContext(ctx,
				`CREATE FUNCTION greet(a integer) RETURNS text LANGUAGE sql AS $$SELECT 'int'$$`)
			c.Assert(err, qt.IsNil)
			_, err = setupDB.ExecContext(ctx,
				`CREATE FUNCTION greet(a text) RETURNS text LANGUAGE sql AS $$SELECT 'text'$$`)
			c.Assert(err, qt.IsNil)

			conn, err := dbschema.ConnectToDatabase(ctx, scopedURL)
			c.Assert(err, qt.IsNil)
			defer dbschema.CloseAndWarn(conn)
			read, err := conn.Reader().ReadSchema()
			c.Assert(err, qt.IsNil)
			// The server really does hold two routines under one name, which is
			// the premise every row below rests on.
			c.Assert(countRoutinesNamed(read.Functions, "greet"), qt.Equals, 2)

			diff := schemadiff.CompareWithDialect(
				&goschema.Database{Functions: test.declared}, read, "postgres")

			c.Assert(diff.FunctionsAdded, qt.HasLen, test.wantAdded,
				qt.Commentf("added=%v", diff.FunctionsAdded))
			c.Assert(diff.FunctionsRemoved, qt.HasLen, test.wantRemoved,
				qt.Commentf("removed=%v", diff.FunctionsRemoved))
			// A pairing that matched the wrong overloads would surface here:
			// the bodies differ, so a mispaired routine reports a modification.
			c.Assert(diff.FunctionsModified, qt.HasLen, 0,
				qt.Commentf("modified=%v", diff.FunctionsModified))
		})
	}
}

func countRoutinesNamed(functions []dbschematypes.DBFunction, name string) int {
	count := 0
	for _, function := range functions {
		if function.Name == name {
			count++
		}
	}
	return count
}
