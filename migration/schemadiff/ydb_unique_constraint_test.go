package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// ydbUniqueDeclaration declares a named UNIQUE and a column's own UNIQUE, which
// YDB holds as global unique indexes.
func ydbUniqueDeclaration() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "Account", Name: "email", Type: "TEXT", Nullable: true, Unique: true},
			{StructName: "Account", Name: "login", Type: "TEXT", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{
			{StructName: "Account", Name: "uq_accounts_login", Type: "UNIQUE", Columns: []string{"login"}},
		},
	}
}

// ydbUniqueCatalog is the table as the YDB reader reports it once a plan built
// it: the key, and the two unique indexes under the renderer's names.
func ydbUniqueCatalog() *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{Name: "accounts", Type: "TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
			{Name: "email", DataType: "Utf8", ColumnType: "Utf8", IsNullable: "YES", OrdinalPosition: 2},
			{Name: "login", DataType: "Utf8", ColumnType: "Utf8", IsNullable: "YES", OrdinalPosition: 3},
		}}},
		Constraints: []catalog.Constraint{{Name: "accounts_pkey", TableName: "accounts", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
		Indexes: []catalog.Index{
			{Name: "accounts_email_key", TableName: "accounts", Columns: []string{"email"}, IsUnique: true, Method: "GLOBAL SYNC"},
			{Name: "uq_accounts_login", TableName: "accounts", Columns: []string{"login"}, IsUnique: true, Method: "GLOBAL SYNC"},
		},
	}
}

// TestCompare_YDBUniqueConstraintIsItsIndex reads a declared UNIQUE as the
// unique index YDB holds for it, both against the database a plan built and
// against the same document on the other side of a file-to-file comparison:
// neither plans anything.
func TestCompare_YDBUniqueConstraintIsItsIndex(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "against the database", diff: must.Must(schemadiff.CompareWithDialect(t.Context(), ydbUniqueDeclaration(), ydbUniqueCatalog(), platform.YDB, must.Must(builtin.New())))},
		{name: "against the same document", diff: must.Must(schemadiff.CompareSchemas(t.Context(), ydbUniqueDeclaration(), ydbUniqueDeclaration(), platform.YDB, must.Must(builtin.New())))},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_YDBUniqueConstraintAddsItsIndex plans a declared UNIQUE the
// database does not hold as the unique index, never as a constraint, so the
// planner adds it through the key any unique index added to a table that
// exists needs.
func TestCompare_YDBUniqueConstraintAddsItsIndex(t *testing.T) {
	c := qt.New(t)
	database := ydbUniqueCatalog()
	database.Indexes = nil

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), ydbUniqueDeclaration(), database, platform.YDB, must.Must(builtin.New())))

	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "accounts_email_key", TableName: "accounts"},
		{Name: "uq_accounts_login", TableName: "accounts"},
	})
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
}
