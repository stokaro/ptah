package oracle

// White-box testing required: three catalog views are joined into one
// description inside the reader, and the exported read returns the same shape
// whichever of them answered -- the symptom of a wrong join is a routine that
// differs from its own declaration on every run.

import (
	"database/sql/driver"
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/dbschema/dbtest"
)

// TestReadFunctions_JoinsTheThreeViewsIntoOneDescription is the reading half of
// the round trip.
//
// Every value below is what the catalog returned on 23.26.2.0.0 for routines
// created through the renderer, and the same values came back on 21.3.0.0.0.
// The procedure is in the fixture beside the function because the two differ in
// three places at once -- kind, the missing POSITION 0 row, and the header that
// has no RETURN clause -- and a fixture with only a function proves none of them.
func TestReadFunctions_JoinsTheThreeViewsIntoOneDescription(t *testing.T) {
	c := qt.New(t)
	db := dbtest.Open(t, answeringRoutineCatalog)
	reader := NewOracleReader(db.SQL, "APP")

	functions, err := reader.readFunctions(t.Context())

	c.Assert(err, qt.IsNil)
	c.Assert(functions, qt.DeepEquals, []catalog.Function{
		{
			Name:       "FN_DET",
			Parameters: "p in number",
			Returns:    "number",
			Language:   "plsql",
			Security:   "INVOKER",
			Volatility: "IMMUTABLE",
			Body:       "BEGIN\n  RETURN p;\nEND;",
		},
		{
			Name:       "FN_DOUBLE",
			Parameters: "p in number",
			Returns:    "number",
			Language:   "plsql",
			Security:   "DEFINER",
			Volatility: "VOLATILE",
			Body:       "BEGIN\n  RETURN p * 2;\nEND;",
		},
		{
			Name:       "PR_OUT",
			Kind:       goschema.FunctionKindProcedure,
			Parameters: "a in number, b out number, c in out varchar2",
			Language:   "plsql",
			Security:   "DEFINER",
			Volatility: "VOLATILE",
			Body:       "BEGIN\n  b := a;\nEND;",
		},
	})
}

// TestReadSchema_AsksForRoutinesOnlyWhereAPresetClaimsThem counts the queries
// SENT rather than the rows returned.
//
// A skipped read and a tolerated failure both produce an empty list, and only
// one of them leaves the connection usable. Counting is what separates them:
// the routine views exist on every supported line, so this gate is about not
// asking a target whose preset says it hosts no routine, and the count is the
// only observable difference.
func TestReadSchema_AsksForRoutinesOnlyWhereAPresetClaimsThem(t *testing.T) {
	tests := []struct {
		name      string
		caps      capability.Capabilities
		wantAsked bool
	}{
		{name: "both keys", caps: capability.Oracle23(), wantAsked: true},
		{
			name:      "functions alone",
			caps:      capability.Oracle23().With(capability.Procedures, false),
			wantAsked: true,
		},
		{
			name:      "procedures alone",
			caps:      capability.Oracle23().With(capability.Functions, false),
			wantAsked: true,
		},
		{
			name: "neither",
			caps: capability.Oracle23().
				With(capability.Functions, false).
				With(capability.Procedures, false),
			wantAsked: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var asked bool
			db := dbtest.Open(t, func(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
				asked = asked || strings.Contains(strings.ToLower(query), "from all_procedures")
				return emptyOrRoutineCatalog(query, args)
			})
			reader := NewOracleReaderWithCapabilities(db.SQL, "APP", test.caps)

			_, err := reader.ReadSchemaContext(t.Context())

			c.Assert(err, qt.IsNil)
			c.Assert(asked, qt.Equals, test.wantAsked)
		})
	}
}

// TestRoutineKind_SeparatesTheTwoObjectsOneViewReports pins the mapping the
// drop verb depends on.
func TestRoutineKind_SeparatesTheTwoObjectsOneViewReports(t *testing.T) {
	tests := []struct {
		name       string
		objectType string
		want       string
	}{
		{name: "function", objectType: "FUNCTION", want: ""},
		{name: "procedure", objectType: "PROCEDURE", want: goschema.FunctionKindProcedure},
		{name: "padded", objectType: "  PROCEDURE  ", want: goschema.FunctionKindProcedure},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(routineKind(test.objectType), qt.Equals, test.want)
		})
	}
}

// answeringRoutineCatalog answers each view with the projection its query asks
// for.
//
// Per projection rather than per call, for the reason the role fake records: a
// query that stopped selecting a column would scan the wrong number of values
// and fail here, where a fake keyed on call order would hand it the same rows
// and pass.
func answeringRoutineCatalog(query string, _ []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_procedures"):
		return routineRows(), nil
	case strings.Contains(folded, "from all_arguments"):
		return routineArgumentRows(), nil
	case strings.Contains(folded, "from all_source"):
		return routineSourceRows(), nil
	}
	return dbtest.QueryResult{}, fmt.Errorf("unexpected query: %s", query)
}

// emptyOrRoutineCatalog answers the routine views and hands every other read an
// empty result, so a whole ReadSchema completes without a fixture for each one.
func emptyOrRoutineCatalog(query string, args []driver.NamedValue) (dbtest.QueryResult, error) {
	folded := strings.ToLower(query)
	switch {
	case strings.Contains(folded, "from all_procedures"),
		strings.Contains(folded, "from all_arguments"),
		strings.Contains(folded, "from all_source"):
		return answeringRoutineCatalog(query, args)
	}
	return dbtest.QueryResult{Columns: []string{"unused"}}, nil
}

// routineRows is what ALL_PROCEDURES answered for three standalone routines.
func routineRows() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"OBJECT_NAME", "OBJECT_TYPE", "AUTHID", "DETERMINISTIC"},
		Rows: [][]driver.Value{
			{"FN_DET", "FUNCTION", "CURRENT_USER", "YES"},
			{"FN_DOUBLE", "FUNCTION", "DEFINER", "NO"},
			{"PR_OUT", "PROCEDURE", "DEFINER", "NO"},
		},
	}
}

// routineArgumentRows is what ALL_ARGUMENTS answered for them.
//
// POSITION 0 is the return value and has no name; a procedure has no such row
// at all, which is what leaves its Returns empty without a rule saying so.
func routineArgumentRows() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"OBJECT_NAME", "POSITION", "ARGUMENT_NAME", "DATA_TYPE", "IN_OUT"},
		Rows: [][]driver.Value{
			{"FN_DET", int64(0), " ", "NUMBER", "OUT"},
			{"FN_DET", int64(1), "P", "NUMBER", "IN"},
			{"FN_DOUBLE", int64(0), " ", "NUMBER", "OUT"},
			{"FN_DOUBLE", int64(1), "P", "NUMBER", "IN"},
			{"PR_OUT", int64(1), "A", "NUMBER", "IN"},
			{"PR_OUT", int64(2), "B", "NUMBER", "OUT"},
			{"PR_OUT", int64(3), "C", "VARCHAR2", "IN/OUT"},
		},
	}
}

// routineSourceRows is what ALL_SOURCE answered, one row per line, without the
// CREATE OR REPLACE prefix the statement carried.
func routineSourceRows() dbtest.QueryResult {
	return dbtest.QueryResult{
		Columns: []string{"NAME", "TYPE", "TEXT"},
		Rows: [][]driver.Value{
			{"FN_DET", "FUNCTION", "FUNCTION fn_det(p IN NUMBER) RETURN NUMBER DETERMINISTIC AUTHID CURRENT_USER IS\n"},
			{"FN_DET", "FUNCTION", "BEGIN\n"},
			{"FN_DET", "FUNCTION", "  RETURN p;\n"},
			{"FN_DET", "FUNCTION", "END;\n"},
			{"FN_DOUBLE", "FUNCTION", "FUNCTION fn_double(p IN NUMBER) RETURN NUMBER AUTHID DEFINER IS\n"},
			{"FN_DOUBLE", "FUNCTION", "BEGIN\n"},
			{"FN_DOUBLE", "FUNCTION", "  RETURN p * 2;\n"},
			{"FN_DOUBLE", "FUNCTION", "END;\n"},
			{"PR_OUT", "PROCEDURE", "PROCEDURE pr_out(a IN NUMBER, b OUT NUMBER, c IN OUT VARCHAR2) IS\n"},
			{"PR_OUT", "PROCEDURE", "BEGIN\n"},
			{"PR_OUT", "PROCEDURE", "  b := a;\n"},
			{"PR_OUT", "PROCEDURE", "END;\n"},
		},
	}
}
