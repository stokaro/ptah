package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// deferrableKeySchema is a table whose primary key and UNIQUE both defer their
// check to the end of the transaction.
func deferrableKeySchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Slot", Name: "slots", PrimaryKey: []string{"id"},
			PrimaryKeyDeferrable: true, PrimaryKeyInitially: "deferred",
		}},
		Fields: []schemamodel.Field{
			{StructName: "Slot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Slot", Name: "position", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Slot", Table: "slots", Name: "slots_position_key", Type: "UNIQUE",
			Columns: []string{"position"}, Deferrable: true,
		}},
	}
}

// TestRenderKeyDeferral_HappyPath writes the deferral of a key where the target
// takes it. PostgreSQL 18.6 and Oracle Free 23 create each statement and read
// the deferral back.
func TestRenderKeyDeferral_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    []string
	}{
		{
			name: "PostgreSQL", dialect: "postgres", caps: capability.Postgres18(),
			want: []string{
				`PRIMARY KEY ("id") DEFERRABLE INITIALLY DEFERRED`,
				`CONSTRAINT "slots_position_key" UNIQUE ("position") DEFERRABLE`,
			},
		},
		{
			name: "PostgreSQL 13", dialect: "postgres", caps: capability.Postgres13(),
			want: []string{`PRIMARY KEY ("id") DEFERRABLE INITIALLY DEFERRED`},
		},
		{
			name: "Oracle", dialect: "oracle", caps: capability.ForDialect("oracle"),
			want: []string{
				`PRIMARY KEY (id) DEFERRABLE INITIALLY DEFERRED`,
				`CONSTRAINT slots_position_key UNIQUE (position) DEFERRABLE`,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(deferrableKeySchema(), tt.dialect, tt.caps)

			c.Assert(err, qt.IsNil)
			for _, want := range tt.want {
				c.Assert(strings.Join(statements, "\n"), qt.Contains, want)
			}
		})
	}
}

// TestRenderKeyDeferral_UnsupportedTarget refuses a deferrable key on a target
// without deferrable keys rather than write it plain. YugabyteDB 2026.1.2 and
// CockroachDB v26.3.2 refuse the statement; the others have no such clause.
func TestRenderKeyDeferral_UnsupportedTarget(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		wantErr string
	}{
		{name: "YugabyteDB", dialect: "yugabytedb", caps: capability.YugabyteDB25(), wantErr: `yugabytedb does not support a DEFERRABLE PRIMARY KEY; an unnamed key of table "slots" declares one`},
		{name: "CockroachDB", dialect: "cockroachdb", caps: capability.ForDialect("cockroachdb"), wantErr: `cockroachdb does not support a DEFERRABLE PRIMARY KEY; .*`},
		{name: "Spanner", dialect: "spanner", caps: capability.ForDialect("spanner"), wantErr: `spanner does not support a DEFERRABLE PRIMARY KEY; .*`},
		{name: "MySQL", dialect: "mysql", caps: capability.ForDialect("mysql"), wantErr: `mysql does not support a DEFERRABLE PRIMARY KEY; .*`},
		{name: "MariaDB", dialect: "mariadb", caps: capability.ForDialect("mariadb"), wantErr: `mariadb does not support a DEFERRABLE PRIMARY KEY; .*`},
		{name: "SQLite", dialect: "sqlite", caps: capability.ForDialect("sqlite"), wantErr: `sqlite does not support a DEFERRABLE PRIMARY KEY; .*`},
		{name: "SQL Server", dialect: "sqlserver", caps: capability.ForDialect("sqlserver"), wantErr: `sqlserver does not support a DEFERRABLE PRIMARY KEY; .*`},
		{name: "ClickHouse", dialect: "clickhouse", caps: capability.ForDialect("clickhouse"), wantErr: `clickhouse does not support a DEFERRABLE PRIMARY KEY; .*`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(deferrableKeySchema(), tt.dialect, tt.caps)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// deferrableUniqueAddition is ALTER TABLE ... ADD of a deferrable UNIQUE, the
// statement a plan writes when a key starts deferring its check.
func deferrableUniqueAddition() *ast.AlterTableNode {
	unique := ast.NewUniqueConstraint("slots_position_key", "position")
	unique.Deferrable, unique.Initially = true, "deferred"
	return &ast.AlterTableNode{
		Name:       "slots",
		Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: unique}},
	}
}

// TestRenderKeyDeferral_AddedConstraint writes the deferral of an added key,
// and refuses it where the target has none.
func TestRenderKeyDeferral_AddedConstraint(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQLWithCapabilities("postgres", capability.Postgres18(), deferrableUniqueAddition())

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Contains, `ALTER TABLE "slots" ADD CONSTRAINT "slots_position_key" UNIQUE ("position") DEFERRABLE INITIALLY DEFERRED;`)
}

// TestRenderKeyDeferral_AddedConstraintOnAnUnsupportedTarget is the refusal
// on the statement path.
func TestRenderKeyDeferral_AddedConstraintOnAnUnsupportedTarget(t *testing.T) {
	c := qt.New(t)

	sql, err := renderer.RenderSQLWithCapabilities("yugabytedb", capability.YugabyteDB25(), deferrableUniqueAddition())

	c.Assert(err, qt.ErrorMatches, `.*yugabytedb does not support a DEFERRABLE UNIQUE; constraint "slots_position_key" declares one`)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(sql, qt.Equals, "")
}

// deferrableExcludeSchema is a table whose EXCLUDE defers its check, and whose
// foreign key to itself defers too.
func deferrableExcludeSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Slot", Name: "slots", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Slot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Slot", Name: "r", Type: "INTEGER"},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Slot", Table: "slots", Name: "slots_r_excl", Type: "EXCLUDE",
			UsingMethod: "btree", ExcludeElements: "r WITH =", Deferrable: true, Initially: "deferred",
		}},
	}
}

// TestRenderKeyDeferral_Exclude writes the deferral of an EXCLUDE, in the
// table and in an added constraint.
func TestRenderKeyDeferral_Exclude(t *testing.T) {
	c := qt.New(t)
	exclude := ast.NewExcludeConstraint("slots_r_excl", "btree", "r WITH =").SetWhereCondition("r > 0")
	exclude.Deferrable = true

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(deferrableExcludeSchema(), "postgres", capability.Postgres18())
	c.Assert(err, qt.IsNil)
	added, err := renderer.RenderSQLWithCapabilities("postgres", capability.Postgres18(), &ast.AlterTableNode{
		Name: "slots", Operations: []ast.AlterOperation{&ast.AddConstraintOperation{Constraint: exclude}},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains,
		`CONSTRAINT "slots_r_excl" EXCLUDE USING btree (r WITH =) DEFERRABLE INITIALLY DEFERRED`)
	c.Assert(added, qt.Contains, `ADD CONSTRAINT "slots_r_excl" EXCLUDE USING btree (r WITH =) WHERE (r > 0) DEFERRABLE;`)
}

// TestRenderKeyDeferral_ForeignKeyIsNotAKey renders a deferrable foreign key
// on YugabyteDB, which defers a foreign key and not a key: the refusal is for
// keys alone.
func TestRenderKeyDeferral_ForeignKeyIsNotAKey(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Slot", Name: "slots", PrimaryKey: []string{"id"}}},
		Fields: []schemamodel.Field{
			{StructName: "Slot", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Slot", Name: "next_id", Type: "INTEGER", Nullable: true},
		},
		Constraints: []schemamodel.Constraint{{
			StructName: "Slot", Table: "slots", Name: "slots_next_fk", Type: "FOREIGN KEY",
			Columns: []string{"next_id"}, ForeignTable: "slots", ForeignColumns: []string{"id"}, Deferrable: true,
		}},
	}

	statements, err := renderer.GetOrderedCreateStatementsWithCapabilities(database, "yugabytedb", capability.YugabyteDB25())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `REFERENCES "slots"("id") DEFERRABLE`)
}
