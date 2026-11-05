package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// removedSequencePlan plans on dialect the removal of the sequence a column of
// owners draws from, with ownedBy as the column that owns it. The declaration
// keeps the column and holds no default on it.
func removedSequencePlan(c *qt.C, dialect, ownedBy string) []string {
	c.Helper()
	runtime := must.Must(builtin.New())
	held := styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns[1].ColumnDefault = new("nextval('public.s'::REGCLASS)")
		h.Sequences = []catalog.Sequence{{Name: "s", Schema: "public", DataType: "bigint", OwnedBy: ownedBy}}
	})
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), styleDeclared(), held,
		catalog.ServerInfo{Dialect: dialect, Schema: "public"}, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return statements
}

// onSequence is the statements of plan that name a sequence or a default, in plan
// order.
func onSequence(plan []string) []string {
	var named []string
	for _, statement := range plan {
		if strings.Contains(statement, "SEQUENCE") || strings.Contains(statement, "DEFAULT") {
			named = append(named, statement)
		}
	}
	return named
}

// TestRemovedSequence_TheDefaultLeavesBeforeTheSequence pins the order the server
// requires, and where CockroachDB also needs the sequence to leave its owner: there
// DROP DEFAULT on the owning column stops at an internal error.
func TestRemovedSequence_TheDefaultLeavesBeforeTheSequence(t *testing.T) {
	const (
		detach = `ALTER SEQUENCE "public"."s" OWNED BY NONE`
		retire = `ALTER TABLE "owners" ALTER COLUMN "name" DROP DEFAULT`
		remove = `DROP SEQUENCE "public"."s"`
	)
	tests := []struct {
		name, dialect, ownedBy string
		want                   []string
	}{
		{name: "PostgreSQL, a sequence no column owns", dialect: platform.Postgres, want: []string{retire, remove}},
		{name: "PostgreSQL, a sequence the column owns", dialect: platform.Postgres, ownedBy: "owners.name", want: []string{retire, remove}},
		{name: "CockroachDB, a sequence no column owns", dialect: platform.CockroachDB, want: []string{retire, remove}},
		{name: "CockroachDB, a sequence the column owns", dialect: platform.CockroachDB, ownedBy: "owners.name", want: []string{detach, retire, remove}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := removedSequencePlan(c, test.dialect, test.ownedBy)

			c.Assert(onSequence(got), qt.DeepEquals, test.want)
		})
	}
}
