package deporder_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/identifier"
	"ptah.run/internal/deporder"
)

// A table with the same bare name cannot make a routine call ambiguous: the
// call and relation namespaces remain separate even without a known session
// schema. Losing this edge creates the calling table before its function.
func TestCreationDependenciesIn_ResolvesTheCalledObjectKind(t *testing.T) {
	c := qt.New(t)
	objects := []deporder.ViewLike{
		{Name: "reports.total"},
		{Name: "helpers.total", Routine: true, Body: "RETURN 1"},
		{Name: "orders", Body: "total()"},
	}

	dependencies := deporder.CreationDependenciesIn(objects, "mysql", identifier.Semantics{})

	c.Assert(dependencies, qt.DeepEquals, [][]int{nil, nil, {1}})
}

func TestCreationDependenciesIn_SeparatesRelationsFromRoutineOperands(t *testing.T) {
	for _, row := range []struct {
		name, dialect, body string
		before              []int
	}{
		{name: "dynamic SQL binds a parameter", dialect: "oracle",
			body: "BEGIN EXECUTE IMMEDIATE 'SELECT :1 FROM dual' INTO n USING target_rows; END;"},
		{name: "a table variable is local", dialect: "sqlserver",
			body: "BEGIN DECLARE @target_rows TABLE (n int); RETURN (SELECT COUNT(*) FROM @target_rows); END;"},
		{name: "a temporary table is local", dialect: "sqlserver",
			body: "SELECT n FROM #target_rows;"},
		{name: "a collection bind is local", dialect: "oracle",
			body: "SELECT n FROM TABLE(:target_rows);"},
		{name: "a row type reads its table", dialect: "oracle",
			body: "DECLARE v target_rows % /* type qualifier */ ROWTYPE; BEGIN NULL; END;", before: []int{0}},
		{name: "a column type reads its table", dialect: "oracle",
			body: "DECLARE v target_rows.n%TYPE; BEGIN NULL; END;", before: []int{0}},
		{name: "an unqualified type anchor names a variable", dialect: "oracle",
			body: "DECLARE v target_rows%TYPE; BEGIN NULL; END;"},
		{name: "MERGE USING names a source table", dialect: "oracle",
			body: "MERGE INTO target_rows t USING source_rows s ON (t.n = s.n) WHEN MATCHED THEN UPDATE SET t.n = s.n;", before: []int{0, 1}},
		{name: "DELETE USING names a target table", dialect: "mysql",
			body: "DELETE FROM t USING target_rows AS t;", before: []int{0}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			objects := []deporder.ViewLike{{Name: "target_rows"}, {Name: "source_rows"}, {Name: "f", Routine: true, Body: row.body}}

			dependencies := deporder.CreationDependenciesIn(objects, row.dialect, identifier.Semantics{})

			c.Assert(dependencies[2], qt.DeepEquals, row.before)
		})
	}
}

func TestCreationDependenciesIn_QuotedRelationKeepsItsPrefix(t *testing.T) {
	c := qt.New(t)
	objects := []deporder.ViewLike{
		{Name: "@target_rows"},
		{Name: "f", Routine: true, Body: "SELECT n FROM [@target_rows]"},
	}

	dependencies := deporder.CreationDependenciesIn(objects, "sqlserver", identifier.Semantics{})

	c.Assert(dependencies[1], qt.DeepEquals, []int{0})
}
