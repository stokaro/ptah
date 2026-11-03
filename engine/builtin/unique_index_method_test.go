package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// uniqueMethodSchema is table t with an index t_a on column a, built with
// method and unique as given.
func uniqueMethodSchema(method string, unique bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INT", Primary: true},
			{StructName: "T", Name: "a", Type: "INT"},
		},
		Indexes: []schemamodel.Index{{StructName: "T", Name: "t_a", Fields: []string{"a"}, Type: method, Unique: unique}},
	}
}

// uniqueMethodNode is the index of uniqueMethodSchema as a migration adds it.
func uniqueMethodNode(method string, unique bool) *ast.IndexNode {
	return &ast.IndexNode{Name: "t_a", Table: "t", Columns: []string{"a"}, Type: method, Unique: unique}
}

// TestRender_UniqueIndexMethod_FailurePath refuses a UNIQUE index built with
// an access method its server builds no unique index with, naming the index,
// in a whole schema, rendered or only validated, and in an index a migration
// adds. Rendered, the
// statement fails only at apply: PostgreSQL 18.6 answers `access method
// "hash" does not support unique indexes`, YugabyteDB 2026.1.2.0 `access
// method "ybgin" does not support unique indexes` for gin, and CockroachDB
// 26.3.2 `inverted indexes can't be unique` for gin and gist.
func TestRender_UniqueIndexMethod_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		method  string
		wantErr string
	}{
		{
			name: "postgres hash", dialect: platform.Postgres, method: "hash",
			wantErr: `unique index "t_a" uses access method "hash", which builds no unique index on postgres; ` +
				`declare it without UNIQUE or with another access method`,
		},
		{
			name: "postgres HASH as an annotation writes it", dialect: platform.Postgres, method: "HASH",
			wantErr: `unique index "t_a" uses access method "hash", which builds no unique index on postgres; .*`,
		},
		{
			name: "postgres gin", dialect: platform.Postgres, method: "gin",
			wantErr: `unique index "t_a" uses access method "gin", which builds no unique index on postgres; .*`,
		},
		{
			name: "postgres gist", dialect: platform.Postgres, method: "gist",
			wantErr: `unique index "t_a" uses access method "gist", which builds no unique index on postgres; .*`,
		},
		{
			name: "postgres brin", dialect: platform.Postgres, method: "brin",
			wantErr: `unique index "t_a" uses access method "brin", which builds no unique index on postgres; .*`,
		},
		{
			name: "postgres spgist", dialect: platform.Postgres, method: "spgist",
			wantErr: `unique index "t_a" uses access method "spgist", which builds no unique index on postgres; .*`,
		},
		{
			name: "yugabytedb gin", dialect: platform.YugabyteDB, method: "gin",
			wantErr: `unique index "t_a" uses access method "gin", which builds no unique index on yugabytedb; .*`,
		},
		{
			name: "yugabytedb ybgin", dialect: platform.YugabyteDB, method: "ybgin",
			wantErr: `unique index "t_a" uses access method "ybgin", which builds no unique index on yugabytedb; .*`,
		},
		{
			name: "cockroachdb gin", dialect: platform.CockroachDB, method: "gin",
			wantErr: `unique index "t_a" uses access method "gin", which builds no unique index on cockroachdb; .*`,
		},
		{
			name: "cockroachdb gist", dialect: platform.CockroachDB, method: "gist",
			wantErr: `unique index "t_a" uses access method "gist", which builds no unique index on cockroachdb; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(uniqueMethodSchema(test.method, true), test.dialect)
			validateErr := builtin.ValidateSchema(uniqueMethodSchema(test.method, true), test.dialect)
			sql, nodeErr := builtin.RenderSQL(test.dialect, uniqueMethodNode(test.method, true))

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*`+test.wantErr)
			c.Assert(statements, qt.IsNil)
			c.Assert(validateErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(validateErr, qt.ErrorMatches, `(?s).*`+test.wantErr)
			c.Assert(nodeErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(nodeErr, qt.ErrorMatches, `(?s).*`+test.wantErr)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// TestRender_UniqueIndexMethod_HappyPath is the control: a unique index with
// the default method or one that builds unique indexes, and a plain index on
// any method, render as declared. YugabyteDB builds a unique hash index as
// lsm, measured on 2026.1.2.0, and MySQL declares no method it refuses.
func TestRender_UniqueIndexMethod_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		method  string
		unique  bool
		want    string
	}{
		{name: "postgres default unique", dialect: platform.Postgres, unique: true, want: `CREATE UNIQUE INDEX`},
		{name: "postgres btree unique", dialect: platform.Postgres, method: "btree", unique: true, want: `CREATE UNIQUE INDEX`},
		{name: "postgres hash plain", dialect: platform.Postgres, method: "hash", want: `USING hash ("a")`},
		{name: "postgres gin plain", dialect: platform.Postgres, method: "gin", want: `USING gin ("a")`},
		{name: "yugabytedb hash unique", dialect: platform.YugabyteDB, method: "hash", unique: true, want: `USING hash ("a")`},
		{name: "mysql HASH unique", dialect: platform.MySQL, method: "HASH", unique: true, want: `USING HASH`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(uniqueMethodSchema(test.method, test.unique), test.dialect)
			validateErr := builtin.ValidateSchema(uniqueMethodSchema(test.method, test.unique), test.dialect)
			sql, nodeErr := builtin.RenderSQL(test.dialect, uniqueMethodNode(test.method, test.unique))

			c.Assert(err, qt.IsNil)
			c.Assert(validateErr, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
			c.Assert(nodeErr, qt.IsNil)
			c.Assert(sql, qt.Contains, test.want)
		})
	}
}
