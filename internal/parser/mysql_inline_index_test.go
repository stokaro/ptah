package parser_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/internal/parser"
)

// parsedTable parses one CREATE TABLE and returns the node.
func parsedTable(c *qt.C, sql string) *ast.CreateTableNode {
	c.Helper()
	statements, err := parser.NewParser(sql, parser.WithDialect("mysql")).Parse()
	c.Assert(err, qt.IsNil)
	for _, statement := range statements.Statements {
		table, ok := statement.(*ast.CreateTableNode)
		if ok {
			return table
		}
	}
	c.Fatalf("no CREATE TABLE in %q", sql)
	return nil
}

func indexNames(table *ast.CreateTableNode) []string {
	names := make([]string, 0, len(table.Indexes))
	for _, index := range table.Indexes {
		names = append(names, index.Name)
	}
	return names
}

func constraintTypes(table *ast.CreateTableNode) []ast.ConstraintType {
	types := make([]ast.ConstraintType, 0, len(table.Constraints))
	for _, constraint := range table.Constraints {
		types = append(types, constraint.Type)
	}
	return types
}

// TestParse_MySQLInlineIndexIsNotAUniqueConstraint covers stokaro/ptah#2713.
//
// A plain table-level KEY or INDEX is an ordinary non-unique index. Parsed as
// ast.UniqueConstraint it became a uniqueness guarantee the declaration never
// made: Ptah converged to a schema stricter than the DDL it was given, reported
// it in sync, and the resulting constraint rejects duplicate values the author
// allowed.
func TestParse_MySQLInlineIndexIsNotAUniqueConstraint(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "KEY",
			sql:  "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, KEY idx_users_email (email));",
			want: "idx_users_email",
		},
		{
			name: "INDEX",
			sql:  "CREATE TABLE users (id BIGINT NOT NULL PRIMARY KEY, email VARCHAR(255) NOT NULL, INDEX idx_users_email (email));",
			want: "idx_users_email",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, test.sql)

			c.Assert(indexNames(table), qt.DeepEquals, []string{test.want})
			c.Assert(table.Indexes[0].Columns, qt.DeepEquals, []string{"email"})
			c.Assert(table.Indexes[0].Unique, qt.IsFalse)
			// The primary key is the only constraint the declaration makes.
			c.Assert(constraintTypes(table), qt.HasLen, 0)
		})
	}
}

// TestParse_MySQLSpatialIndexKeepsItsMethodAndName covers stokaro/ptah#2711.
//
// SPATIAL INDEX was parsed as ast.UniqueConstraint with the fixed name
// "SPATIAL_INDEX", which lost the access method, replaced whatever the author
// called it, and gave every spatial index in a document one identity.
func TestParse_MySQLSpatialIndexKeepsItsMethodAndName(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want string
	}{
		{
			name: "named",
			sql:  "CREATE TABLE geo (id BIGINT PRIMARY KEY, location POINT, SPATIAL INDEX sx_geo_location (location));",
			want: "sx_geo_location",
		},
		{
			// An unnamed index keeps the empty string so the dialect's own
			// naming applies. The old code invented "SPATIAL_INDEX" here, which
			// collided the moment a table declared two.
			name: "unnamed",
			sql:  "CREATE TABLE geo (id BIGINT PRIMARY KEY, location POINT, SPATIAL INDEX (location));",
			want: "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			table := parsedTable(c, test.sql)

			c.Assert(indexNames(table), qt.DeepEquals, []string{test.want})
			c.Assert(table.Indexes[0].Type, qt.Equals, "SPATIAL")
			c.Assert(table.Indexes[0].Columns, qt.DeepEquals, []string{"location"})
			c.Assert(table.Indexes[0].Unique, qt.IsFalse)
			c.Assert(constraintTypes(table), qt.HasLen, 0)
		})
	}
}

// TestParse_MySQLTwoSpatialIndexesKeepSeparateIdentities is the collision the
// hard-coded name produced. Without it, a fix that preserved only the first
// name would pass the table above.
func TestParse_MySQLTwoSpatialIndexesKeepSeparateIdentities(t *testing.T) {
	c := qt.New(t)

	table := parsedTable(c, "CREATE TABLE geo (id BIGINT PRIMARY KEY, a POINT, b POINT, "+
		"SPATIAL INDEX sx_a (a), SPATIAL INDEX sx_b (b));")

	c.Assert(indexNames(table), qt.DeepEquals, []string{"sx_a", "sx_b"})
}

// TestParse_MySQLUniqueIsStillAConstraint is the control for both.
//
// The fix moves KEY and SPATIAL INDEX out of the constraint list; a fix that
// moved UNIQUE out with them would satisfy every assertion above and delete a
// real guarantee.
func TestParse_MySQLUniqueIsStillAConstraint(t *testing.T) {
	c := qt.New(t)

	table := parsedTable(c, "CREATE TABLE u (id BIGINT PRIMARY KEY, email VARCHAR(255), "+
		"CONSTRAINT uq_u_email UNIQUE (email));")

	c.Assert(constraintTypes(table), qt.DeepEquals, []ast.ConstraintType{ast.UniqueConstraint})
	c.Assert(table.Constraints[0].Name, qt.Equals, "uq_u_email")
	c.Assert(table.Indexes, qt.HasLen, 0)
}

// TestParse_MySQLInlineIndexKeepsPerColumnAttributes covers the half a
// column-name list cannot carry.
//
// parseConstraintColumn already reads MySQL's prefix length and DESC ordering.
// Building the index from Columns alone kept the index and silently flattened
// `KEY k (name(7) DESC)` into `KEY k (name)` -- a different index that applies
// cleanly, so nothing downstream would have reported it.
func TestParse_MySQLInlineIndexKeepsPerColumnAttributes(t *testing.T) {
	c := qt.New(t)

	table := parsedTable(c, "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(255), "+
		"KEY idx_t_name (name(7) DESC));")

	c.Assert(table.Indexes, qt.HasLen, 1)
	c.Assert(table.Indexes[0].Parts, qt.DeepEquals, []ast.IndexPart{
		{Name: "name", Prefix: "7", Desc: true},
	})
}

// TestParse_MySQLInlineIndexWithoutAttributesCarriesPlainParts is its control:
// the parts travel for an ordinary column too, with the attribute fields empty
// rather than invented.
func TestParse_MySQLInlineIndexWithoutAttributesCarriesPlainParts(t *testing.T) {
	c := qt.New(t)

	table := parsedTable(c, "CREATE TABLE t (id BIGINT PRIMARY KEY, name VARCHAR(255), "+
		"KEY idx_t_name (name));")

	c.Assert(table.Indexes, qt.HasLen, 1)
	c.Assert(table.Indexes[0].Parts, qt.DeepEquals, []ast.IndexPart{
		{Name: "name"},
	})
}
