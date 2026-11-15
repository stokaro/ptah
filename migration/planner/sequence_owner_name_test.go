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

// The owner of a sequence is written as a column reference, a name quoted when
// it holds a dot: the readers write the column code of a table own.ers as
// `"own.ers".code`, and the column co.de of owners as `owners."co.de"`. Split
// on its last dot, the second owner names the column `de"`.

// namedOwnerPlan plans on dialect the change of a column of table, which draws
// from public.s in the database and owns it as ownedBy, to declared. The table
// is the first table of the style fixture under another name. The statements
// about the key columns the fixture spells differently are left out.
func namedOwnerPlan(c *qt.C, dialect, table, ownedBy string, declared schemamodel.Field, sequences []schemamodel.Sequence) []string {
	c.Helper()
	runtime := must.Must(builtin.New())
	desired := styleDeclaredWith(func(d *schemamodel.Database) {
		d.Tables[0].Name = table
		d.Fields = append(d.Fields, declared)
		d.Sequences = sequences
	})
	held := styleHeldWith(func(h *catalog.Database) {
		h.Tables[0].Name = table
		h.Tables[0].Columns = append(h.Tables[0].Columns, catalog.Column{
			Name: declared.Name, DataType: "bigint", UDTName: "int8", IsNullable: "NO",
			ColumnDefault: new("nextval('s'::regclass)"), OrdinalPosition: 3,
		})
		h.Sequences = []catalog.Sequence{{Name: "s", Schema: "public", DataType: "bigint", OwnedBy: ownedBy}}
	})
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, held,
		catalog.ServerInfo{Dialect: dialect, Schema: "public"}, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(c.Context(), runtime, diff, dialect,
		planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planext.StatementStyleExact})
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(statements, func(statement string) bool {
		return strings.Contains(statement, `COLUMN "id"`)
	})
}

// TestOwnedSequenceDefault_ANameHoldingADot plans, on CockroachDB, a default that
// leaves a sequence its column owns. The sequence is detached around the change and
// takes its owner back, whichever name holds the dot.
func TestOwnedSequenceDefault_ANameHoldingADot(t *testing.T) {
	tests := []struct {
		name, table, column, ownedBy string
		want                         []string
	}{
		{
			name: "the table", table: "own.ers", column: "code", ownedBy: `"own.ers".code`,
			want: []string{
				`ALTER SEQUENCE "public"."s" OWNED BY NONE`,
				`ALTER TABLE "own.ers" ALTER COLUMN "code" DROP DEFAULT`,
				`ALTER SEQUENCE "public"."s" OWNED BY "own.ers"."code"`,
			},
		},
		{
			name: "the column", table: "owners", column: "co.de", ownedBy: `owners."co.de"`,
			want: []string{
				`ALTER SEQUENCE "public"."s" OWNED BY NONE`,
				`ALTER TABLE "owners" ALTER COLUMN "co.de" DROP DEFAULT`,
				`ALTER SEQUENCE "public"."s" OWNED BY "owners"."co.de"`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := schemamodel.Field{StructName: "Owner", Name: test.column, Type: "bigint"}

			got := namedOwnerPlan(c, platform.CockroachDB, test.table, test.ownedBy, declared,
				[]schemamodel.Sequence{{Name: "s", Schema: "public"}})

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestIdentityForAnOwnedSequence_ANameHoldingADot plans an identity for a column
// that draws from a sequence it owns, which the declaration leaves out. The
// sequence is dropped ahead of the identity, whose sequence takes its name, so
// pg_get_serial_sequence answers with the identity's own sequence.
func TestIdentityForAnOwnedSequence_ANameHoldingADot(t *testing.T) {
	tests := []struct {
		name, table, column, ownedBy string
		want                         []string
	}{
		{
			name: "the table", table: "own.ers", column: "code", ownedBy: `"own.ers".code`,
			want: []string{
				`ALTER TABLE "own.ers" ALTER COLUMN "code" DROP DEFAULT`,
				`DROP SEQUENCE "public"."s"`,
				`ALTER TABLE "own.ers" ALTER COLUMN "code" ADD GENERATED ALWAYS AS IDENTITY`,
				`SELECT setval(pg_get_serial_sequence('"own.ers"', 'code'), max("code")) FROM "own.ers" HAVING max("code") >= 1`,
			},
		},
		{
			name: "the column", table: "owners", column: "co.de", ownedBy: `owners."co.de"`,
			want: []string{
				`ALTER TABLE "owners" ALTER COLUMN "co.de" DROP DEFAULT`,
				`DROP SEQUENCE "public"."s"`,
				`ALTER TABLE "owners" ALTER COLUMN "co.de" ADD GENERATED ALWAYS AS IDENTITY`,
				`SELECT setval(pg_get_serial_sequence('"owners"', 'co.de'), max("co.de")) FROM "owners" HAVING max("co.de") >= 1`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := schemamodel.Field{StructName: "Owner", Name: test.column, Type: "bigint", IdentityGeneration: "ALWAYS"}

			got := namedOwnerPlan(c, platform.Postgres, test.table, test.ownedBy, declared, nil)

			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

// TestSerialForAnIdentity_ANameHoldingADot declares a serial column over an
// identity generated ALWAYS, and the new sequence is given to the column. Written
// with a bare dot, `parent.n.m` renders as OWNED BY "parent"."n"."m", a column of
// a table n in a schema parent.
func TestSerialForAnIdentity_ANameHoldingADot(t *testing.T) {
	tests := []struct {
		name, table, column, want string
	}{
		{name: "the table", table: `"pa.rent"`, column: "n", want: `ALTER SEQUENCE "pa.rent_n_seq" OWNED BY "pa.rent"."n"`},
		{name: "the column", table: "parent", column: `"n.m"`, want: `ALTER SEQUENCE "parent_n.m_seq" OWNED BY "parent"."n.m"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := "CREATE TABLE " + test.table + " (id bigint PRIMARY KEY, " + test.column + " bigint GENERATED ALWAYS AS IDENTITY);"
			desired := "CREATE TABLE " + test.table + " (id bigint PRIMARY KEY, " + test.column + " bigserial NOT NULL);"

			got := columnKeyPlan(c, platform.Postgres, held, desired)

			c.Assert(slices.DeleteFunc(got, func(statement string) bool { return !strings.Contains(statement, "OWNED BY") }),
				qt.DeepEquals, []string{test.want})
		})
	}
}

// TestOwnedSequenceDefault_AnOwnerInAnotherSchema plans, on CockroachDB, a default
// that leaves the sequence public.s, which the column code of app.owners owns,
// from the column code of public.owners. CockroachDB accepts an owner in another
// schema, and a reader writes it with its schema. The column does not own the
// sequence, so the plan does not detach it and gives it no new owner.
func TestOwnedSequenceDefault_AnOwnerInAnotherSchema(t *testing.T) {
	c := qt.New(t)
	declared := schemamodel.Field{StructName: "Owner", Name: "code", Type: "bigint"}

	got := namedOwnerPlan(c, platform.CockroachDB, "owners", "app.owners.code", declared,
		[]schemamodel.Sequence{{Name: "s", Schema: "public"}})

	c.Assert(got, qt.DeepEquals, []string{`ALTER TABLE "owners" ALTER COLUMN "code" DROP DEFAULT`})
}
