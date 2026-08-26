package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
)

// A comment that differs between the declaration and the database is a change
// -- stokaro/ptah#2168.
//
// The readers carry a comment and the renderers write one, and the comparator
// had nowhere to put a difference between them. So a table or column whose
// comment was rewritten reported `Schema is synced, no changes to be made.`,
// on every run, with nothing able to fix it. Measured on PostgreSQL 17 before
// this existed, against a database holding 'people who buy' and a declaration
// asking for 'customers of record'.
func TestCompareWithDialect_CommentDifferenceIsAChange(t *testing.T) {
	tests := []struct {
		name         string
		declared     string
		inDatabase   string
		wantTable    *difftypes.CommentChange
		wantModified int
	}{
		{
			name:         "a comment rewritten",
			declared:     "customers of record",
			inDatabase:   "people who buy",
			wantTable:    &difftypes.CommentChange{Current: "people who buy", Desired: "customers of record"},
			wantModified: 1,
		},
		{
			// The direction the issue calls out: the declaration drops the
			// comment and the database still holds one. Without both sides a
			// planner cannot tell this from "no comment anywhere".
			name:         "a comment removed from the declaration",
			declared:     "",
			inDatabase:   "people who buy",
			wantTable:    &difftypes.CommentChange{Current: "people who buy", Desired: ""},
			wantModified: 1,
		},
		{
			name:         "a comment added",
			declared:     "people who buy",
			inDatabase:   "",
			wantTable:    &difftypes.CommentChange{Current: "", Desired: "people who buy"},
			wantModified: 1,
		},
		{
			// The control. Identical comments are not a change, and a table
			// with nothing else different must not reach TablesModified at all
			// -- otherwise every schema would report drift forever.
			name:         "the same comment on both sides",
			declared:     "people who buy",
			inDatabase:   "people who buy",
			wantTable:    nil,
			wantModified: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				commentedDeclaration(tt.declared, ""),
				commentedDatabase(tt.inDatabase, ""),
				"postgres",
			)

			c.Assert(diff.TablesModified, qt.HasLen, tt.wantModified)
			c.Assert(tableCommentChange(diff), qt.DeepEquals, tt.wantTable)
		})
	}
}

// A column's comment is compared the same way, and a column whose ONLY
// difference is its comment has to reach ColumnsModified: the gate there asks
// whether the change map is non-empty, and a comment has no entry in it.
func TestCompareWithDialect_AColumnCommentDifferenceIsAChange(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(
		commentedDeclaration("", "primary contact"),
		commentedDatabase("", "login address"),
		"postgres",
	)

	c.Assert(diff.TablesModified, qt.HasLen, 1)
	modified := diff.TablesModified[0].ColumnsModified
	c.Assert(modified, qt.HasLen, 1)
	c.Assert(modified[0].ColumnName, qt.Equals, "email")
	c.Assert(modified[0].Changes, qt.HasLen, 0)
	c.Assert(modified[0].CommentChange, qt.DeepEquals,
		&difftypes.CommentChange{Current: "login address", Desired: "primary contact"})
}

func commentedDeclaration(table, column string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "users", StructName: "User", Comment: table}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "integer", Primary: true},
			{StructName: "User", Name: "email", Type: "varchar(255)", Comment: column},
		},
	}
}

func commentedDatabase(table, column string) *catalog.Database {
	return &catalog.Database{
		Tables: []catalog.Table{{
			Name:    "users",
			Type:    "TABLE",
			Comment: table,
			Columns: []catalog.Column{
				{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "email", DataType: "varchar(255)", IsNullable: "NO", Comment: column},
			},
		}},
	}
}

func tableCommentChange(diff *difftypes.SchemaDiff) *difftypes.CommentChange {
	if len(diff.TablesModified) == 0 {
		return nil
	}
	return diff.TablesModified[0].CommentChange
}
