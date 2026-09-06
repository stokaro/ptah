package mssql_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/renderer/internal/dialects/mssql"
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
			// The control. An unnamed key must render exactly what it rendered
			// before, and let the server generate the name.
			name: "an unnamed key",
			want: "PRIMARY KEY ([b])",
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
