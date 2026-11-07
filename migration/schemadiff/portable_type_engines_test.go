package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/migration/schemadiff"
)

// TestComparePortableType_ANameAndTheColumnItBuildsAreOneType holds a column an engine reads
// back under another name than the one declared. A row that plans here plans a statement
// that changes nothing, on every run.
func TestComparePortableType_ANameAndTheColumnItBuildsAreOneType(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		held     string
		declared string
	}{
		{"CockroachDB blob", "cockroachdb", "bytea", "blob"},
		{"CockroachDB bytes", "cockroachdb", "bytea", "bytes"},
		{"CockroachDB an array of blobs", "cockroachdb", "bytea[]", "blob[]"},
		{"CockroachDB json", "cockroachdb", "jsonb", "json"},
		{"Oracle bool the server built", "oracle", "BOOLEAN", "bool"},
		{"Oracle bool Ptah built", "oracle", "NUMBER(1)", "bool"},
		{"SQLite boolean", "sqlite", "boolean", "boolean"},
		{"SQLite another case of boolean", "sqlite", "BOOLEAN", "boolean"},
		{"SQLite bytea", "sqlite", "bytea", "bytea"},
		{"SQLite serial", "sqlite", "serial", "serial"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// TestComparePortableType_ARealDifferenceIsAChange holds the guard the other way, and the
// engines where the same two names are two types.
func TestComparePortableType_ARealDifferenceIsAChange(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		held        string
		declared    string
		wantNarrows bool
	}{
		{"PostgreSQL a bigint under integer", "postgres", "bigint", "integer", true},
		{"PostgreSQL a jsonb under json", "postgres", "jsonb", "json", false},
		{"CockroachDB text under blob", "cockroachdb", "text", "blob", false},
		{"CockroachDB text under json", "cockroachdb", "text", "json", false},
		{"Oracle a number under bool", "oracle", "NUMBER(5)", "bool", true},
		{"SQLite another affinity", "sqlite", "text", "boolean", false},
		{"SQLite another word with another affinity", "sqlite", "boolean", "text", false},
		{"MariaDB text under json", "mariadb", "text", "json", false},
		{"MySQL longtext under json", "mysql", "longtext", "json", false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared),
				heldTemporalColumn(test.held), test.dialect, builtintest.Runtime())
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].ColumnsModified, qt.HasLen, 1)
			column := diff.TablesModified[0].ColumnsModified[0]
			c.Assert(column.Changes["type"], qt.Not(qt.Equals), "")
			c.Assert(column.TypeNarrows, qt.Equals, test.wantNarrows)
		})
	}
}

// TestComparePortableType_ASerialColumnIsNotNull holds a serial column read back NOT NULL
// under a declaration that does not say so. Compared by the word alone, the plan drops a
// NOT NULL the server would not let go of, rated destructive.
func TestComparePortableType_ASerialColumnIsNotNull(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		held     string
		declared string
	}{
		{"YugabyteDB serial", "yugabytedb", "integer", "serial"},
		{"YugabyteDB bigserial", "yugabytedb", "bigint", "bigserial"},
		{"YugabyteDB smallserial", "yugabytedb", "smallint", "smallserial"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			held := heldTemporalColumn(test.held)
			held.Tables[0].Columns[0].IsNullable = "NO"
			held.Tables[0].Columns[0].OwnedSequence = "t_c_seq"
			diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration(test.declared), held, test.dialect, builtintest.Runtime())
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}

// heldMariaDBJSONColumn is a read of t.c on MariaDB, holding the CHECK a JSON column carries.
func heldMariaDBJSONColumn(check *string) *catalog.Database {
	held := heldTemporalColumn("longtext")
	held.Constraints = []catalog.Constraint{{
		Name: "c", TableName: "t", Type: "CHECK", ColumnName: "c", ColumnNames: []string{"c"}, CheckClause: check,
	}}
	return held
}

// TestComparePortableType_MariaDBHoldsJSONAsCheckedText compares a JSON column with what the
// renderer writes for it. A plan on the column it builds rewrites the column and then drops a
// CHECK the rewrite has replaced, which the server refuses.
func TestComparePortableType_MariaDBHoldsJSONAsCheckedText(t *testing.T) {
	valid := "json_valid(`c`)"
	c := qt.New(t)
	runtime := builtintest.Runtime()

	built, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration("json"), heldMariaDBJSONColumn(&valid), "mariadb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(built.TablesModified, qt.HasLen, 0)
	c.Assert(built.ConstraintsAdded, qt.HasLen, 0)
	c.Assert(built.ConstraintsRemoved, qt.HasLen, 0)

	unchecked, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration("json"), heldTemporalColumn("longtext"), "mariadb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(unchecked.TablesModified, qt.HasLen, 0)
	c.Assert(unchecked.ConstraintsAdded, qt.HasLen, 1)

	retyped, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration("json"), heldTemporalColumn("text"), "mariadb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(retyped.TablesModified, qt.HasLen, 1)
	c.Assert(retyped.ConstraintsAdded, qt.HasLen, 0)

	dropped, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration("longtext"), heldMariaDBJSONColumn(&valid), "mariadb", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(dropped.TablesModified, qt.HasLen, 0)
	c.Assert(dropped.ConstraintsRemoved, qt.HasLen, 1)
}

// TestComparePortableType_MySQLHasNoSuchCheck holds the same comparison on the engine with a
// JSON type of its own, where no CHECK is implied.
func TestComparePortableType_MySQLHasNoSuchCheck(t *testing.T) {
	c := qt.New(t)
	diff, err := schemadiff.CompareWithDialect(c.Context(), temporalDeclaration("json"), heldTemporalColumn("json"), "mysql", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesModified, qt.HasLen, 0)
	c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
}

// TestComparePortableType_TwoFilesCompareAsTheyWrite compares a schema file with itself, where
// the current side is a declaration rather than a read.
func TestComparePortableType_TwoFilesCompareAsTheyWrite(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		declared string
	}{
		{"MariaDB json", "mariadb", "json"},
		{"CockroachDB blob", "cockroachdb", "blob"},
		{"CockroachDB json", "cockroachdb", "json"},
		{"SQLite boolean", "sqlite", "boolean"},
		{"Oracle bool", "oracle", "bool"},
		{"YugabyteDB serial", "yugabytedb", "serial"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			declared := func() *schemamodel.Database { return temporalDeclaration(test.declared) }
			current, err := goschematodb.ToDBSchema(c.Context(), declared(), test.dialect, runtime)
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDialect(c.Context(), declared(), current, test.dialect, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
			c.Assert(diff.ConstraintsAdded, qt.HasLen, 0)
			c.Assert(diff.ConstraintsRemoved, qt.HasLen, 0)
		})
	}
}
