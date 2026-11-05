package planner_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// ownedSequencePlan plans a change of the default of owners.code on dialect. The
// column draws from public.s in the database, s has the owner ownedBy, and the
// declaration gives the column the default declared.
func ownedSequencePlan(c *qt.C, dialect, ownedBy, declared string) []string {
	c.Helper()
	runtime := must.Must(builtin.New())
	desired := styleDeclaredWith(func(d *schemamodel.Database) {
		d.Fields = append(d.Fields, schemamodel.Field{StructName: "Owner", Name: "code", Type: "bigint", Nullable: true, DefaultExpr: declared})
		d.Sequences = []schemamodel.Sequence{{Name: "s", Schema: "public"}, {Name: "s2", Schema: "public"}}
	})
	held := styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Columns = append(h.Tables[0].Columns, catalog.Column{
			Name: "code", DataType: "bigint", UDTName: "int8", IsNullable: "YES", ColumnDefault: new("nextval('s'::regclass)"),
		})
		h.Sequences = []catalog.Sequence{{Name: "s", Schema: "public", OwnedBy: ownedBy}, {Name: "s2", Schema: "public"}}
	})
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, held,
		catalog.ServerInfo{Dialect: dialect, Schema: "public"}, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	// The fixture spells other columns as PostgreSQL reads them, which is no concern
	// of these rows on CockroachDB.
	return slices.DeleteFunc(statements, func(statement string) bool {
		return !strings.Contains(statement, `"code"`) && !strings.Contains(statement, "ALTER SEQUENCE")
	})
}

// TestOwnedSequenceDefault_HappyPath pins the plan of a default that leaves a
// sequence its column owns. CockroachDB answers the bare ALTER COLUMN with an
// internal error, so the sequence is detached around it and takes its owner back.
// PostgreSQL takes the change as it stands.
func TestOwnedSequenceDefault_HappyPath(t *testing.T) {
	const (
		detach = `ALTER SEQUENCE "public"."s" OWNED BY NONE`
		attach = `ALTER SEQUENCE "public"."s" OWNED BY "owners"."code"`
	)
	tests := []struct {
		name, dialect, ownedBy, declared string
		want                             []string
	}{
		{
			name: "CockroachDB, the default leaves", dialect: platform.CockroachDB, ownedBy: "owners.code",
			want: []string{detach, `ALTER TABLE "owners" ALTER COLUMN "code" DROP DEFAULT`, attach},
		},
		{
			name: "CockroachDB, the default moves to another sequence", dialect: platform.CockroachDB, ownedBy: "owners.code",
			declared: "nextval('s2')",
			want:     []string{detach, `ALTER TABLE "owners" ALTER COLUMN "code" SET DEFAULT nextval('s2')`, attach},
		},
		{
			name: "CockroachDB, the owner is written with its schema", dialect: platform.CockroachDB, ownedBy: "public.owners.code",
			want: []string{detach, `ALTER TABLE "owners" ALTER COLUMN "code" DROP DEFAULT`, attach},
		},
		{
			name: "CockroachDB, another column owns the sequence", dialect: platform.CockroachDB, ownedBy: "owners.name",
			want: []string{`ALTER TABLE "owners" ALTER COLUMN "code" DROP DEFAULT`},
		},
		{
			name: "CockroachDB, a column of the same name on another table owns the sequence", dialect: platform.CockroachDB, ownedBy: "items.code",
			want: []string{`ALTER TABLE "owners" ALTER COLUMN "code" DROP DEFAULT`},
		},
		{
			name: "CockroachDB, no column owns the sequence", dialect: platform.CockroachDB,
			want: []string{`ALTER TABLE "owners" ALTER COLUMN "code" DROP DEFAULT`},
		},
		{
			name: "CockroachDB, the default does not change", dialect: platform.CockroachDB, ownedBy: "owners.code", declared: "nextval('s')",
			want: []string{},
		},
		{
			name: "PostgreSQL takes the change as it stands", dialect: platform.Postgres, ownedBy: "owners.code",
			want: []string{`ALTER TABLE "owners" ALTER COLUMN "code" DROP DEFAULT`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ownedSequencePlan(c, test.dialect, test.ownedBy, test.declared), qt.DeepEquals, test.want)
		})
	}
}
