package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/engine/builtin/internal/dialects/ydb"
)

// TestRender_UniqueConstraint_HappyPath pins how a UNIQUE constraint and a
// UNIQUE column render on YDB, which has unique indexes and no UNIQUE
// constraint: as a global unique index named after the constraint, or after
// the table and the column, and as nothing over the key, which already holds
// the rows unique. Each rendering was applied to local-ydb 26.2.1.14 and
// 25.1.4.7 and read back as the unique index it writes.
func TestRender_UniqueConstraint_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		node ast.Node
		want string
	}{
		{
			name: "a named constraint, an unnamed one and a column",
			caps: capability.YDB251(),
			node: &ast.CreateTableNode{
				Name: "app.users",
				Columns: []*ast.ColumnNode{
					ast.NewColumn("id", "BIGINT").SetPrimary(),
					{Name: "email", Type: "TEXT", Nullable: true, Unique: true},
					{Name: "tenant", Type: "BIGINT", Nullable: true},
					{Name: "login", Type: "TEXT", Nullable: true},
				},
				Constraints: []*ast.ConstraintNode{
					{Type: ast.UniqueConstraint, Name: "uq_tenant_login", Columns: []string{"tenant", "login"}, IncludeColumns: []string{"email"}},
					{Type: ast.UniqueConstraint, Columns: []string{"login"}},
				},
			},
			want: "CREATE TABLE `app/users` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    `email` Utf8,\n" +
				"    `tenant` Int64,\n" +
				"    `login` Utf8,\n" +
				"    PRIMARY KEY (`id`),\n" +
				"    INDEX `uq_tenant_login` GLOBAL UNIQUE SYNC ON (`tenant`, `login`) COVER (`email`),\n" +
				"    INDEX `users_login_key` GLOBAL UNIQUE SYNC ON (`login`),\n" +
				"    INDEX `users_email_key` GLOBAL UNIQUE SYNC ON (`email`)\n" +
				");\n",
		},
		{
			name: "a UNIQUE over the key columns in another order folds into the key",
			caps: capability.YDB262(),
			node: &ast.CreateTableNode{
				Name: "t",
				Columns: []*ast.ColumnNode{
					{Name: "a", Type: "BIGINT", Primary: true},
					{Name: "b", Type: "BIGINT", Primary: true},
				},
				Constraints: []*ast.ConstraintNode{{Type: ast.UniqueConstraint, Name: "uq_ba", Columns: []string{"b", "a"}}},
			},
			want: "CREATE TABLE `t` (\n" +
				"    `a` Int64 NOT NULL,\n" +
				"    `b` Int64 NOT NULL,\n" +
				"    PRIMARY KEY (`a`, `b`)\n" +
				");\n",
		},
		{
			name: "a UNIQUE key column folds into the key",
			caps: capability.YDB262(),
			node: &ast.CreateTableNode{
				Name:    "t",
				Columns: []*ast.ColumnNode{{Name: "id", Type: "BIGINT", Primary: true, Unique: true}},
			},
			want: "CREATE TABLE `t` (\n" +
				"    `id` Int64 NOT NULL,\n" +
				"    PRIMARY KEY (`id`)\n" +
				");\n",
		},
		{
			name: "a UNIQUE over part of the key is an index of its own",
			caps: capability.YDB262(),
			node: &ast.CreateTableNode{
				Name: "t",
				Columns: []*ast.ColumnNode{
					{Name: "a", Type: "BIGINT", Primary: true, Unique: true},
					{Name: "b", Type: "BIGINT", Primary: true},
				},
			},
			want: "CREATE TABLE `t` (\n" +
				"    `a` Int64 NOT NULL,\n" +
				"    `b` Int64 NOT NULL,\n" +
				"    PRIMARY KEY (`a`, `b`),\n" +
				"    INDEX `t_a_key` GLOBAL UNIQUE SYNC ON (`a`)\n" +
				");\n",
		},
		{
			name: "a UNIQUE added to a table that exists, where the cluster takes one",
			caps: capability.YDB262().With(capability.UniqueIndexOnExistingTable, true),
			node: alter(&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{
				Type: ast.UniqueConstraint, Name: "uq_a", Columns: []string{"a"},
			}}),
			want: "ALTER TABLE `t` ADD INDEX `uq_a` GLOBAL UNIQUE SYNC ON (`a`);\n",
		},
		{
			name: "a UNIQUE column added to a table that exists, where the cluster takes one",
			caps: capability.YDB262().With(capability.UniqueIndexOnExistingTable, true),
			node: alter(&ast.AddColumnOperation{Column: &ast.ColumnNode{Name: "code", Type: "TEXT", Nullable: true, Unique: true}}),
			want: "ALTER TABLE `t` ADD COLUMN `code` Utf8;\n" +
				"ALTER TABLE `t` ADD INDEX `t_code_key` GLOBAL UNIQUE SYNC ON (`code`);\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(test.caps).Render(test.node)
			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestRender_UniqueConstraint_FailurePath refuses a UNIQUE whose index YDB
// would refuse or could not hold as declared, and holds a UNIQUE added to a
// table that exists to the key any unique index added there needs.
func TestRender_UniqueConstraint_FailurePath(t *testing.T) {
	unique := func(constraint ast.ConstraintNode) *ast.CreateTableNode {
		constraint.Type = ast.UniqueConstraint
		constraint.Name = "uq"
		constraint.Columns = []string{"a"}
		return &ast.CreateTableNode{
			Name:        "t",
			Columns:     []*ast.ColumnNode{ast.NewColumn("id", "BIGINT").SetPrimary(), {Name: "a", Type: "TEXT", Nullable: true}},
			Constraints: []*ast.ConstraintNode{&constraint},
		}
	}
	tests := []struct {
		name    string
		node    ast.Node
		wantErr string
	}{
		{name: "deferrable", node: unique(ast.ConstraintNode{Deferrable: true}),
			wantErr: `UNIQUE constraint "uq" on table "t" is deferrable, .*which requires target capability deferrable_constraints, .*`},
		{name: "not enforced", node: unique(ast.ConstraintNode{NotEnforced: true}),
			wantErr: `UNIQUE constraint "uq" on table "t": it is NOT ENFORCED, and YDB enforces every unique index`},
		{name: "partial", node: unique(ast.ConstraintNode{WhereCondition: "a <> ''"}),
			wantErr: `UNIQUE constraint "uq" on table "t": YDB has no partial index`},
		{name: "NULLS NOT DISTINCT", node: unique(ast.ConstraintNode{NullsDistinct: new(false)}),
			wantErr: `index "uq" treats NULLs as equal; .*which requires target capability unique_nulls_distinct_clause, .*`},
		{name: "a descending column", node: unique(ast.ConstraintNode{ColumnParts: []ast.ConstraintColumn{{Name: "a", Desc: true}}}),
			wantErr: `UNIQUE constraint "uq" on table "t": a YDB index is a list of columns, .*`},
		{name: "two indexes under one name",
			node: func() *ast.CreateTableNode {
				table := unique(ast.ConstraintNode{})
				table.AddIndex(&ast.IndexNode{Name: "uq", Columns: []string{"a"}})
				return table
			}(),
			wantErr: `table "t": two of its indexes are named "uq", and YDB names an index once per table`},
		{name: "added to a table that exists",
			node:    alter(&ast.AddConstraintOperation{Constraint: &ast.ConstraintNode{Type: ast.UniqueConstraint, Name: "uq_a", Columns: []string{"a"}}}),
			wantErr: `unique index "uq_a" is added to table "t", which exists already .*which requires target capability unique_index_on_existing_table, .*`},
		{name: "a UNIQUE column on its own",
			node:    &ast.ColumnNode{Name: "a", Type: "TEXT", Unique: true},
			wantErr: `column "a": its UNIQUE is a unique index of its table on YDB, .*`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydb.NewWithCapabilities(capability.YDB262()).Render(test.node)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(got, qt.Equals, "")
		})
	}
}
