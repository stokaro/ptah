package generator

// White-box testing required: every exported entry point that writes a
// migration file reads a live database first, and the property under test is
// decided in the SQL-only generation stage below them.

import (
	"context"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// engineOnPostgres is a diff that creates a table declaring an ENGINE, which
// the PostgreSQL family has no clause for.
func engineOnPostgres() (*difftypes.SchemaDiff, *schemamodel.Database) {
	desired := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", Engine: "InnoDB"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INTEGER", Primary: true}},
	}
	diff := must.Must(schemadiff.CompareWithDialect(context.Background(), desired, &catalog.Database{}, platform.Postgres, must.Must(builtin.New())))
	return diff, desired
}

// The PostgreSQL renderer writes its own `skipped` comment beside the CREATE
// TABLE, so the file does not open with a second comment that says the same.
func TestGenerateUpMigrationSQL_SaysAnOmissionTheRendererCommentedOnce(t *testing.T) {
	c := qt.New(t)
	diff, desired := engineOnPostgres()

	sql, err := upMigrationSQL(t.Context(), must.Must(builtin.New()), diff, desired, platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(sql, "ENGINE=InnoDB"), qt.Equals, 1, qt.Commentf("migration body:\n%s", sql))
	c.Assert(sql, qt.Contains, "-- POSTGRES: table option ENGINE=InnoDB is not supported by this target; skipped.")
	c.Assert(sql, qt.Not(qt.Contains), "was skipped (")
}
