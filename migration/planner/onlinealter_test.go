package planner_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

// onlineAlterDiff adds a column to an existing table, which every engine
// applies with one ALTER TABLE.
func onlineAlterDiff() *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesModified: []difftypes.TableDiff{{
			TableName:    "users",
			ColumnsAdded: difftypes.ColumnChanges{{StructName: "User", Name: "nickname", Type: "VARCHAR(64)", Nullable: true}},
		}},
		// The declared tables a comparison fills, which the planner asks
		// whether the table this change names is declared at all.
		DeclaredTables: []schemamodel.Table{{StructName: "User", Name: "users"}},
	}
}

// TestOnlineAlter_MySQLAsksTheServerToApplyItInPlace is the property the
// clause exists for: the request reaches the statement, so the server is the
// one that decides whether the change runs online.
func TestOnlineAlter_MySQLAsksTheServerToApplyItInPlace(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "mysql", dialect: "mysql", caps: capability.MySQL84()},
		{name: "mariadb", dialect: "mariadb", caps: capability.MariaDB1011()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQLWithOptions(
				onlineAlterDiff(), test.dialect,
				planner.Options{Capabilities: test.caps, OnlineAlter: true},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, ", ALGORITHM=INPLACE, LOCK=NONE;")
		})
	}
}

// The control: without the request the statement is what it was, so a reader
// comparing two plans sees the clause and nothing else.
func TestOnlineAlter_MySQLWritesNoClauseWhenNothingAsked(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQLWithOptions(
		onlineAlterDiff(), "mysql",
		planner.Options{Capabilities: capability.MySQL84()},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "ALGORITHM=")
	c.Assert(sql, qt.Not(qt.Contains), "LOCK=")
}

// A target without the grammar is planned as if nothing was asked. Writing the
// clause there would be a statement the server refuses for a reason that has
// nothing to do with the change.
func TestOnlineAlter_WritesNoClauseWhereTheGrammarIsAbsent(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
	}{
		{name: "postgres", dialect: "postgres", caps: capability.Postgres18()},
		{name: "sqlite", dialect: "sqlite", caps: capability.SQLite3()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQLWithOptions(
				onlineAlterDiff(), test.dialect,
				planner.Options{Capabilities: test.caps, OnlineAlter: true},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.ToUpper(sql), qt.Not(qt.Contains), "ALGORITHM=")
		})
	}
}

// A PostgreSQL constraint whose cost is a scan of the rows already in the
// table is added without one, and validated by a statement of its own. The
// validation takes a weaker lock, which is the whole reason for the pair.
func TestOnlineAlter_PostgresAddsTheConstraintWithoutItsScan(t *testing.T) {
	tests := []struct {
		name       string
		constraint difftypes.ConstraintAdditionInfo
	}{
		{
			name: "check",
			constraint: difftypes.ConstraintAdditionInfo{
				Name: "ck_amount", TableName: "orders", Type: "CHECK", CheckExpression: "amount > 0",
			},
		},
		{
			name: "foreign key",
			constraint: difftypes.ConstraintAdditionInfo{
				Name: "fk_customer", TableName: "orders", Type: "FOREIGN KEY",
				Columns:       []string{"customer_id"},
				ForeignTable:  "customers",
				ForeignColumn: "id",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQLWithOptions(
				&difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{test.constraint}},
				"postgres",
				planner.Options{Capabilities: capability.Postgres18(), OnlineAlter: true},
			)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, "NOT VALID;")
			c.Assert(sql, qt.Contains, "VALIDATE CONSTRAINT")
		})
	}
}

// The control that keeps the rewrite to the constraints it is about. A unique
// constraint builds an index, which NOT VALID says nothing about, and
// PostgreSQL refuses the clause there.
func TestOnlineAlter_PostgresLeavesAnIndexBackedConstraintAlone(t *testing.T) {
	c := qt.New(t)

	sql, err := planner.GenerateSchemaDiffSQLWithOptions(
		&difftypes.SchemaDiff{ConstraintsAdded: difftypes.ConstraintAdditions{{
			Name: "uq_email", TableName: "users", Type: "UNIQUE", Columns: []string{"email"},
		}}},
		"postgres",
		planner.Options{Capabilities: capability.Postgres18(), OnlineAlter: true},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(sql, qt.Not(qt.Contains), "NOT VALID")
	c.Assert(sql, qt.Not(qt.Contains), "VALIDATE CONSTRAINT")
}

// Every ALTER TABLE the plan writes carries the request, not only the ones an
// early version of this pass happened to route: MySQL reads ALGORITHM and LOCK
// per statement, so one that went without them would run under whatever the
// server chose.
func TestOnlineAlter_MySQLAsksOnEveryStatementShape(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{
			name: "a table comment",
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{{
					TableName:     "users",
					CommentChange: &difftypes.CommentChange{Current: "", Desired: "people"},
				}},
				DeclaredTables: []schemamodel.Table{{StructName: "User", Name: "users"}},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql, err := planner.GenerateSchemaDiffSQLWithOptions(test.diff, "mysql",
				planner.Options{Capabilities: capability.MySQL84(), OnlineAlter: true})

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, ", ALGORITHM=INPLACE, LOCK=NONE;")
		})
	}
}
