package mssql_test

import (
	"regexp"
	"strings"
	"testing"
	"unicode/utf8"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ast"
	"ptah.run/engine/builtin/internal/dialects/mssql"
)

// TestCreateTable_APrimaryKeyKeepsItsName pins that SQL Server is told the name.
//
// It keeps one. Measured on SQL Server 2025, sys.key_constraints:
//
//	t -> c_pk                      CONSTRAINT c_pk PRIMARY KEY (b)
//	u -> PK__u__3BD0198F3A610101   PRIMARY KEY (b)
//
// so a key written without the name arrives under one the operator did not
// choose and cannot predict -- and it is what a later ALTER TABLE ... DROP
// CONSTRAINT has to name (stokaro/ptah#2180).
func TestCreateTable_APrimaryKeyKeepsItsName(t *testing.T) {
	tests := []struct {
		name           string
		constraintName string
		want           string
	}{
		{
			name:           "a named key",
			constraintName: "c_pk",
			want:           "CONSTRAINT [c_pk] PRIMARY KEY ([b])",
		},
		{
			// The name a later file can write on any database, where the server
			// would make up one with a hash.
			name: "an unnamed key",
			want: "CONSTRAINT [t_pkey] PRIMARY KEY ([b])",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := mssql.New().Render(&ast.CreateTableNode{
				Name:    "t",
				Columns: []*ast.ColumnNode{{Name: "b", Type: "INT"}},
				Constraints: []*ast.ConstraintNode{
					{Type: ast.PrimaryKeyConstraint, Name: test.constraintName, Columns: []string{"b"}},
				},
			})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestCreateTable_AKeyNobodyNamedTakesAName is each place a primary key goes without a
// name. SQL Server would name it with a hash that differs by database, so a later file
// that drops it could name only the one its dev database held. A name that would not fit
// is cut short, since the server answers Msg 103 past 128 characters, and a hash of the
// whole name keeps two long names that start alike apart.
func TestCreateTable_AKeyNobodyNamedTakesAName(t *testing.T) {
	long := strings.Repeat("t", 125)
	tests := []struct {
		name  string
		table string
		node  func(table string) *ast.ColumnNode
		want  string
	}{
		{
			name: "a column's primary key", table: "t",
			node: func(string) *ast.ColumnNode { return &ast.ColumnNode{Name: "b", Type: "INT", Primary: true} },
			want: "[b] INT CONSTRAINT [t_pkey] PRIMARY KEY",
		},
		{
			name: "a table in a schema", table: "app.t",
			node: func(string) *ast.ColumnNode { return &ast.ColumnNode{Name: "b", Type: "INT", Primary: true} },
			want: "[b] INT CONSTRAINT [t_pkey] PRIMARY KEY",
		},
		{
			name: "a name that fits exactly", table: strings.Repeat("t", 123),
			node: func(string) *ast.ColumnNode { return &ast.ColumnNode{Name: "b", Type: "INT", Primary: true} },
			want: "CONSTRAINT [" + strings.Repeat("t", 123) + "_pkey] PRIMARY KEY",
		},
		{
			name: "a name that would not fit", table: long,
			node: func(string) *ast.ColumnNode { return &ast.ColumnNode{Name: "b", Type: "INT", Primary: true} },
			want: "CONSTRAINT [" + strings.Repeat("t", 114) + "_7c217e06_pkey] PRIMARY KEY",
		},
		{
			name: "a name cut inside a character SQL Server counts twice", table: strings.Repeat("t", 113) + strings.Repeat("\U0001F600", 6),
			node: func(string) *ast.ColumnNode { return &ast.ColumnNode{Name: "b", Type: "INT", Primary: true} },
			want: "CONSTRAINT [" + strings.Repeat("t", 113) + "_",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := mssql.New().Render(&ast.CreateTableNode{Name: test.table, Columns: []*ast.ColumnNode{test.node(test.table)}})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestAlterTable_AKeyNobodyNamedTakesAName is the same key added to a table that stands.
func TestAlterTable_AKeyNobodyNamedTakesAName(t *testing.T) {
	tests := []struct {
		name      string
		operation ast.AlterOperation
		want      string
	}{
		{
			name:      "a primary key",
			operation: &ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Columns: []string{"a", "b"}}},
			want:      "ALTER TABLE [t] ADD CONSTRAINT [t_pkey] PRIMARY KEY ([a], [b]);",
		},
		{
			name:      "a unique constraint, which the server names",
			operation: &ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Type: ast.UniqueConstraint, Columns: []string{"a", "b"}}},
			want:      "ALTER TABLE [t] ADD UNIQUE ([a], [b]);",
		},
		{
			name:      "a column with a key",
			operation: &ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "b", Type: "INT", Primary: true}},
			want:      "ALTER TABLE [t] ADD [b] INT CONSTRAINT [t_pkey] PRIMARY KEY;",
		},
		{
			name:      "a key its author named",
			operation: &ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Type: ast.PrimaryKeyConstraint, Name: "c_pk", Columns: []string{"a"}}},
			want:      "ALTER TABLE [t] ADD CONSTRAINT [c_pk] PRIMARY KEY ([a]);",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := mssql.New().Render(&ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{test.operation}})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}

// TestCreateTable_TwoLongNamesThatStartAlike_HappyPath is two tables whose first 114
// characters agree. A constraint's name is unique in a schema, so they need two names.
func TestCreateTable_TwoLongNamesThatStartAlike_HappyPath(t *testing.T) {
	c := qt.New(t)
	render := func(table string) string {
		return must.Must(mssql.New().Render(&ast.CreateTableNode{
			Name:    table,
			Columns: []*ast.ColumnNode{{Name: "b", Type: "INT", Primary: true}},
		}))
	}
	keyName := regexp.MustCompile(`CONSTRAINT \[([^\]]+)\]`)

	first := keyName.FindStringSubmatch(render(strings.Repeat("t", 125) + "a"))
	second := keyName.FindStringSubmatch(render(strings.Repeat("t", 125) + "b"))

	c.Assert(first, qt.HasLen, 2)
	c.Assert(second, qt.HasLen, 2)
	c.Assert(first[1], qt.Not(qt.Equals), second[1])
	c.Assert(utf8.RuneCountInString(first[1]), qt.Equals, 128)
}
