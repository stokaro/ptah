package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// TestPlan_OraclePrimaryKeyOverColumnKey_HappyPath plans a named primary key
// over a column whose UNIQUE Oracle named, where the file declares the column
// without it. The read carries that key on the column, the column change drops
// it, and the primary key follows. Dropped a second time ahead of the
// constraint additions, it answers ORA-02442 and the migration stops there.
func TestPlan_OraclePrimaryKeyOverColumnKey_HappyPath(t *testing.T) {
	c := qt.New(t)
	runtime := must.Must(builtin.New())
	desired, _, err := sqlschema.ReadWith(c.Context(),
		[]byte("CREATE TABLE p (a NUMBER(10) NOT NULL, CONSTRAINT pg_pk PRIMARY KEY (a));"),
		platform.Oracle, nil, sqlschema.Options{Owners: runtime.SQL()})
	c.Assert(err, qt.IsNil)
	current := &catalog.Database{Tables: []catalog.Table{{Name: "P", Columns: []catalog.Column{
		{Name: "A", DataType: "NUMBER(10)", IsNullable: "NO", IsUnique: true},
	}}}}
	diff := must.Must(schemadiff.CompareWithDialect(c.Context(), &desired, current, platform.Oracle, runtime))

	plan, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, platform.Oracle,
		planext.Options{Capabilities: capability.ForDialect(platform.Oracle)})

	c.Assert(err, qt.IsNil)
	c.Assert(plan, qt.DeepEquals, []string{
		"-- Modify table: p\n-- Modify column p.a: unique: true -> false\nALTER TABLE p DROP UNIQUE (a)",
		"ALTER TABLE p ADD CONSTRAINT pg_pk PRIMARY KEY (a)",
	})
}
