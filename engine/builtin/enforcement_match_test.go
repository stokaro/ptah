package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// notEnforcedCheckSchema is a table with a column CHECK and a table CHECK the
// server keeps and does not check.
func notEnforcedCheckSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "T", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "T", Name: "n", Type: "INTEGER", Nullable: true, Check: "n > 0", CheckNotEnforced: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "T", Table: "t", Name: "t_id_positive", Type: "CHECK", CheckExpression: "id > 0",
			NotEnforced: true,
		}},
	}
}

// foreignKeyClauseSchema is a child table whose column's foreign key carries
// match and notEnforced, beside a table-level foreign key that carries the
// same.
func foreignKeyClauseSchema(match string, notEnforced bool) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parents", PrimaryKey: []string{"id"}},
			{StructName: "Child", Name: "children", PrimaryKey: []string{"id"}},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Child", Name: "id", Type: "INTEGER", Primary: true},
			{
				StructName: "Child", Name: "parent_id", Type: "INTEGER", Nullable: true,
				Foreign: "parents(id)", ForeignKeyName: "children_parent_id_fkey",
				ForeignKeyMatch: match, ForeignKeyNotEnforced: notEnforced,
			},
			{StructName: "Child", Name: "other_id", Type: "INTEGER", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Child", Table: "children", Name: "children_other_id_fkey", Type: "FOREIGN KEY",
			Columns: []string{"other_id"}, ForeignTable: "parents", ForeignColumn: "id",
			Match: match, NotEnforced: notEnforced,
		}},
	}
}

// TestRenderEnforcementAndMatch_HappyPath writes `NOT ENFORCED` and the MATCH
// type where the target keeps them (stokaro/ptah#3853). PostgreSQL 18.6,
// MySQL 8.4.11 and 9.7.2 and CockroachDB v26.3.2 created each statement and
// read the clause back.
func TestRenderEnforcementAndMatch_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		schema  *schemamodel.Database
		want    []string
	}{
		{
			name: "CHECKs on PostgreSQL 18", dialect: "postgres", caps: capability.Postgres18(),
			schema: notEnforcedCheckSchema(),
			want:   []string{`CHECK (n > 0) NOT ENFORCED`, `CONSTRAINT "t_id_positive" CHECK (id > 0) NOT ENFORCED`},
		},
		{
			name: "CHECKs on MySQL", dialect: "mysql", caps: capability.MySQL84(),
			schema: notEnforcedCheckSchema(),
			want:   []string{`CHECK (n > 0) NOT ENFORCED`, "CONSTRAINT `t_id_positive` CHECK (id > 0) NOT ENFORCED"},
		},
		{
			name: "foreign keys on PostgreSQL 18", dialect: "postgres", caps: capability.Postgres18(),
			schema: foreignKeyClauseSchema("FULL", true),
			want: []string{
				`FOREIGN KEY ("parent_id") REFERENCES "parents"("id") MATCH FULL NOT ENFORCED`,
				`FOREIGN KEY ("other_id") REFERENCES "parents"("id") MATCH FULL NOT ENFORCED`,
			},
		},
		{
			name: "MATCH FULL on PostgreSQL 16", dialect: "postgres", caps: capability.Postgres16(),
			schema: foreignKeyClauseSchema("FULL", false),
			want:   []string{`FOREIGN KEY ("parent_id") REFERENCES "parents"("id") MATCH FULL;`},
		},
		{
			name: "MATCH PARTIAL on MySQL", dialect: "mysql", caps: capability.MySQL84(),
			schema: foreignKeyClauseSchema("PARTIAL", false),
			want: []string{
				"FOREIGN KEY (`parent_id`) REFERENCES `parents`(`id`) MATCH PARTIAL",
				"FOREIGN KEY (`other_id`) REFERENCES `parents`(`id`) MATCH PARTIAL",
			},
		},
		{
			name: "MATCH FULL on CockroachDB", dialect: "cockroachdb", caps: capability.ForDialect("cockroachdb"),
			schema: foreignKeyClauseSchema("FULL", false),
			want:   []string{`REFERENCES "parents"("id") MATCH FULL`},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.schema, test.dialect, test.caps)

			c.Assert(err, qt.IsNil)
			for _, want := range test.want {
				c.Assert(strings.Join(statements, "\n"), qt.Contains, want)
			}
		})
	}
}

// TestRenderEnforcementAndMatch_FailurePath refuses a clause the target does
// not keep rather than build a constraint without it. Each server answered as
// its row says: PostgreSQL 17.11 a syntax error, MariaDB 11.8.9 ERROR 1064 to
// NOT ENFORCED and NONE recorded for MATCH FULL, SQLite 3.51 NONE, MySQL 8.4.11
// ERROR 1064 to a foreign key NOT ENFORCED, PostgreSQL 18.6 `MATCH PARTIAL not
// yet implemented`.
func TestRenderEnforcementAndMatch_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		schema  *schemamodel.Database
		wantErr string
	}{
		{
			name: "a CHECK on PostgreSQL 17", dialect: "postgres", caps: capability.Postgres17(),
			schema:  notEnforcedCheckSchema(),
			wantErr: `.*the CHECK of column "n" declares a NOT ENFORCED CHECK, which requires target capability not_enforced_checks, unavailable on this postgres target`,
		},
		{
			name: "a CHECK on MariaDB", dialect: "mariadb", caps: capability.MariaDB1011(),
			schema:  notEnforcedCheckSchema(),
			wantErr: `.*declares a NOT ENFORCED CHECK, which requires target capability not_enforced_checks, unavailable on this mariadb target`,
		},
		{
			name: "a table CHECK alone on PostgreSQL 17", dialect: "postgres", caps: capability.Postgres17(),
			schema: &schemamodel.Database{
				Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}}},
				Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INTEGER", Primary: true}},
				Constraints: []schemamodel.Constraint{{
					StructName: "T", Table: "t", Name: "t_id_positive", Type: "CHECK", CheckExpression: "id > 0",
					NotEnforced: true,
				}},
			},
			wantErr: `.*constraint "t_id_positive" declares a NOT ENFORCED CHECK, which requires target capability not_enforced_checks, unavailable on this postgres target`,
		},
		{
			name: "a foreign key NOT ENFORCED on MySQL", dialect: "mysql", caps: capability.MySQL84(),
			schema:  foreignKeyClauseSchema("", true),
			wantErr: `.*constraint "children_parent_id_fkey" declares a NOT ENFORCED foreign key, which requires target capability not_enforced_foreign_keys, unavailable on this mysql target`,
		},
		{
			name: "MATCH FULL on MariaDB", dialect: "mariadb", caps: capability.MariaDB1011(),
			schema:  foreignKeyClauseSchema("FULL", false),
			wantErr: `.*constraint "children_parent_id_fkey" declares MATCH FULL, which requires target capability foreign_key_match_full, unavailable on this mariadb target`,
		},
		{
			name: "MATCH FULL on SQLite", dialect: "sqlite", caps: capability.SQLite3(),
			schema:  foreignKeyClauseSchema("FULL", false),
			wantErr: `.*declares MATCH FULL, which requires target capability foreign_key_match_full, unavailable on this sqlite target`,
		},
		{
			name: "MATCH PARTIAL on PostgreSQL 18", dialect: "postgres", caps: capability.Postgres18(),
			schema:  foreignKeyClauseSchema("PARTIAL", false),
			wantErr: `.*declares MATCH PARTIAL, which requires target capability foreign_key_match_partial, unavailable on this postgres target`,
		},
		{
			name: "a foreign key NOT ENFORCED on CockroachDB", dialect: "cockroachdb", caps: capability.ForDialect("cockroachdb"),
			schema:  foreignKeyClauseSchema("", true),
			wantErr: `.*declares a NOT ENFORCED foreign key, which requires target capability not_enforced_foreign_keys, unavailable on this cockroachdb target`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.schema, test.dialect, test.caps)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// statementFixtures are the statements a plan hands the renderer: an added
// CHECK the server does not check, an added foreign key with MATCH PARTIAL,
// and a table whose column carries a foreign key with MATCH FULL and a CHECK
// the server does not check.
func statementFixtures() (addCheck, addPartial, table ast.Node) {
	check := &ast.ConstraintNode{Type: ast.CheckConstraint, Name: "t_n_positive", Expression: "n > 0", NotEnforced: true}
	partial := ast.NewForeignKeyConstraint("c_p_fkey", []string{"p"}, &ast.ForeignKeyRef{
		Table: "parents", Column: "id", Match: "PARTIAL",
	})
	return &ast.AlterTableNode{Name: "t", Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: check}}},
		&ast.AlterTableNode{Name: "c", Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: partial}}},
		&ast.CreateTableNode{Name: "c", Columns: []*ast.ColumnNode{
			{Name: "p", Type: "INTEGER", Nullable: true, ForeignKey: &ast.ForeignKeyRef{
				Table: "parents", Column: "id", Name: "c_p_fkey", Match: "FULL", Deferrable: true,
			}},
			{Name: "n", Type: "INTEGER", Nullable: true, Check: "n > 0", CheckNotEnforced: true},
		}}
}

// TestRenderEnforcementAndMatch_Statements_HappyPath writes the clauses on the
// statement path a plan takes, where no declaration was validated first. A
// column's foreign key is written as a table constraint and keeps every clause
// of its reference, its deferral among them.
func TestRenderEnforcementAndMatch_Statements_HappyPath(t *testing.T) {
	addCheck, addPartial, table := statementFixtures()
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		node    ast.Node
		want    []string
	}{
		{
			name: "a CHECK added on PostgreSQL 18", dialect: "postgres", caps: capability.Postgres18(), node: addCheck,
			want: []string{`ALTER TABLE "t" ADD CONSTRAINT "t_n_positive" CHECK (n > 0) NOT ENFORCED;`},
		},
		{
			name: "a foreign key added on MySQL", dialect: "mysql", caps: capability.MySQL84(), node: addPartial,
			want: []string{"FOREIGN KEY (`p`) REFERENCES `parents`(`id`) MATCH PARTIAL"},
		},
		{
			name: "a column's clauses on PostgreSQL 18", dialect: "postgres", caps: capability.Postgres18(), node: table,
			want: []string{
				`FOREIGN KEY ("p") REFERENCES "parents"("id") MATCH FULL DEFERRABLE`,
				`"n" INTEGER CHECK (n > 0) NOT ENFORCED`,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, test.node)

			c.Assert(err, qt.IsNil)
			for _, want := range test.want {
				c.Assert(sql, qt.Contains, want)
			}
		})
	}
}

// TestRenderEnforcementAndMatch_Statements_FailurePath refuses the clauses on
// the statement path where the target does not keep them.
func TestRenderEnforcementAndMatch_Statements_FailurePath(t *testing.T) {
	addCheck, addPartial, table := statementFixtures()
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		node    ast.Node
		wantErr string
	}{
		{
			name: "a CHECK added on PostgreSQL 17", dialect: "postgres", caps: capability.Postgres17(), node: addCheck,
			wantErr: `.*constraint "t_n_positive" declares a NOT ENFORCED CHECK, which requires target capability not_enforced_checks, unavailable on this postgres target`,
		},
		{
			name: "a column's CHECK on PostgreSQL 17", dialect: "postgres", caps: capability.Postgres17(), node: table,
			wantErr: `.*the CHECK of column "n" declares a NOT ENFORCED CHECK, which requires target capability not_enforced_checks, unavailable on this postgres target`,
		},
		{
			name: "a foreign key added on MariaDB", dialect: "mariadb", caps: capability.MariaDB1011(), node: addPartial,
			wantErr: `.*constraint "c_p_fkey" declares MATCH PARTIAL, which requires target capability foreign_key_match_partial, unavailable on this mariadb target`,
		},
		{
			name: "a column's foreign key on MariaDB", dialect: "mariadb", caps: capability.MariaDB1011(), node: table,
			wantErr: `.*constraint "c_p_fkey" declares MATCH FULL, which requires target capability foreign_key_match_full, unavailable on this mariadb target`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := builtin.RenderSQLWithCapabilities(test.dialect, test.caps, test.node)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(sql, qt.Equals, "")
		})
	}
}
