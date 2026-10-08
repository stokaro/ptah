package mysql_test

import (
	"context"
	"regexp"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	migrationplanner "ptah.run/migration/planner"
	"ptah.run/migration/schemadiff/difftypes"
)

var sqlCommentLineRE = regexp.MustCompile(`(?m)^\s*--.*\n?`)

// planStatementsSQL renders a plan for dialect with comment lines removed, so a
// test can pin the order of the statements a server receives.
func planStatementsSQL(c *qt.C, diff *difftypes.SchemaDiff, dialect string) string {
	sql, err := migrationplanner.GenerateSchemaDiffSQL(
		context.Background(), must.Must(builtin.New()),
		diff, dialect,
	)
	c.Assert(err, qt.IsNil)
	return strings.TrimSpace(sqlCommentLineRE.ReplaceAllString(legacyRenderedSQL(sql), ""))
}

// mysqlSemanticsIn is the identifier semantics of a connection to database.
func mysqlSemanticsIn(database string) *identifier.Semantics {
	semantics := identifier.ForDialect(platform.MySQL)
	semantics.DefaultSchema = database
	return &semantics
}

func auditTrigger(name, table, body string) schemamodel.Trigger {
	return schemamodel.Trigger{Name: name, Table: table, Timing: "AFTER", Event: "INSERT", Body: body}
}

// TestPlanner_TriggerSwap_StatementOrder pins where trigger changes sit in a
// MySQL-family plan, for the cases ariga/atlas#3534 describes
// (stokaro/ptah#4014).
//
// Each row is a real defect without the ordering it pins, measured on MySQL
// 8.4.11: a trigger created before a column it writes to breaks every write to
// its table, a column dropped before the trigger that reads it does the same,
// and a write that lands between two trigger statements of one table sees no
// trigger or both of them.
func TestPlanner_TriggerSwap_StatementOrder(t *testing.T) {
	newBody := "INSERT INTO secondtable (tbl_time, NewColumn) VALUES (NOW(), NEW.name)"
	oldBody := "INSERT INTO secondtable (tbl_time) VALUES (NOW())"
	addNewColumn := difftypes.TableDiff{
		TableName:    "secondtable",
		ColumnsAdded: difftypes.ColumnChanges{{Name: "NewColumn", Type: "VARCHAR(100)", Nullable: true}},
	}
	// A column addition is planned only on a table the schema declares.
	declaresSecondtable := []schemamodel.Table{{Name: "secondtable"}}
	dropLegacy := difftypes.TableDiff{
		TableName:      "mytable",
		ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy", Type: "VARCHAR(100)"}},
	}

	tests := []struct {
		name    string
		dialect string
		diff    *difftypes.SchemaDiff
		want    string
	}{
		{
			name:    "a renamed trigger is created before the old one is dropped, under one lock, after the column it writes to",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{addNewColumn},
				DeclaredTables: declaresSecondtable,
				TriggersAdded: []difftypes.TriggerRef{{
					TriggerName: "mytable_after_insert_2025_06_12", TableName: "mytable",
					Desired: auditTrigger("mytable_after_insert_2025_06_12", "mytable", newBody),
				}},
				TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "mytable_after_insert_2025_03_20", TableName: "mytable"}},
			},
			want: "ALTER TABLE secondtable ADD COLUMN `NewColumn` VARCHAR(100);\n" +
				"LOCK TABLES mytable WRITE;\n" +
				"CREATE TRIGGER mytable_after_insert_2025_06_12 AFTER INSERT ON mytable FOR EACH ROW " + newBody + ";\n" +
				"DROP TRIGGER IF EXISTS mytable_after_insert_2025_03_20;\n" +
				"UNLOCK TABLES;",
		},
		{
			name:    "MariaDB swaps a renamed trigger under the same lock",
			dialect: platform.MariaDB,
			diff: &difftypes.SchemaDiff{
				TriggersAdded: []difftypes.TriggerRef{{
					TriggerName: "audit_v2", TableName: "mytable",
					Desired: auditTrigger("audit_v2", "mytable", oldBody),
				}},
				TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "audit_v1", TableName: "mytable"}},
			},
			want: "LOCK TABLES mytable WRITE;\n" +
				"CREATE TRIGGER audit_v2 AFTER INSERT ON mytable FOR EACH ROW " + oldBody + ";\n" +
				"DROP TRIGGER IF EXISTS audit_v1;\n" +
				"UNLOCK TABLES;",
		},
		{
			name:    "MySQL replaces a same-name trigger with DROP and CREATE under a lock",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{addNewColumn},
				DeclaredTables: declaresSecondtable,
				TriggersModified: []difftypes.TriggerDiff{{
					TriggerName: "mytable_after_insert", TableName: "mytable",
					Changes: map[string]string{"body": "old -> new"},
					Desired: auditTrigger("mytable_after_insert", "mytable", newBody),
				}},
			},
			want: "ALTER TABLE secondtable ADD COLUMN `NewColumn` VARCHAR(100);\n" +
				"LOCK TABLES mytable WRITE;\n" +
				"DROP TRIGGER IF EXISTS mytable_after_insert;\n" +
				"CREATE TRIGGER mytable_after_insert AFTER INSERT ON mytable FOR EACH ROW " + newBody + ";\n" +
				"UNLOCK TABLES;",
		},
		{
			name:    "MariaDB replaces a same-name trigger in one statement and takes no lock",
			dialect: platform.MariaDB,
			diff: &difftypes.SchemaDiff{
				TriggersModified: []difftypes.TriggerDiff{{
					TriggerName: "mytable_after_insert", TableName: "mytable",
					Changes: map[string]string{"body": "old -> new"},
					Desired: auditTrigger("mytable_after_insert", "mytable", newBody),
				}},
			},
			want: "CREATE OR REPLACE TRIGGER mytable_after_insert AFTER INSERT ON mytable FOR EACH ROW " + newBody + ";",
		},
		{
			name:    "a column the old trigger reads is dropped after the trigger is replaced",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				TablesModified: []difftypes.TableDiff{dropLegacy},
				TriggersModified: []difftypes.TriggerDiff{{
					TriggerName: "mytable_after_insert", TableName: "mytable",
					Changes: map[string]string{"body": "old -> new"},
					Desired: auditTrigger("mytable_after_insert", "mytable", oldBody),
				}},
			},
			want: "LOCK TABLES mytable WRITE;\n" +
				"DROP TRIGGER IF EXISTS mytable_after_insert;\n" +
				"CREATE TRIGGER mytable_after_insert AFTER INSERT ON mytable FOR EACH ROW " + oldBody + ";\n" +
				"UNLOCK TABLES;\n" +
				"ALTER TABLE mytable DROP COLUMN legacy;",
		},
		{
			name:    "a removed trigger is dropped before a column it reads, and one statement takes no lock",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				TablesModified:  []difftypes.TableDiff{dropLegacy},
				TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "mytable_after_insert", TableName: "mytable"}},
			},
			want: "DROP TRIGGER IF EXISTS mytable_after_insert;\n" +
				"ALTER TABLE mytable DROP COLUMN legacy;",
		},
		{
			name:    "two new triggers replace nothing and take no lock",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				TriggersAdded: []difftypes.TriggerRef{
					{TriggerName: "audit_a", TableName: "mytable", Desired: auditTrigger("audit_a", "mytable", oldBody)},
					{TriggerName: "audit_b", TableName: "mytable", Desired: auditTrigger("audit_b", "mytable", oldBody)},
				},
			},
			want: "CREATE TRIGGER audit_a AFTER INSERT ON mytable FOR EACH ROW " + oldBody + ";\n" +
				"CREATE TRIGGER audit_b AFTER INSERT ON mytable FOR EACH ROW " + oldBody + ";",
		},
		{
			name:    "two removed triggers take no lock",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				TriggersRemoved: []difftypes.TriggerRef{
					{TriggerName: "audit_a", TableName: "mytable"},
					{TriggerName: "audit_b", TableName: "mytable"},
				},
			},
			want: "DROP TRIGGER IF EXISTS audit_a;\n" +
				"DROP TRIGGER IF EXISTS audit_b;",
		},
		{
			name:    "a trigger that moves to another table is dropped before its name is created again",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				TriggersAdded: []difftypes.TriggerRef{{
					TriggerName: "audit", TableName: "othertable",
					Desired: auditTrigger("audit", "othertable", oldBody),
				}},
				TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "AUDIT", TableName: "mytable"}},
			},
			want: "DROP TRIGGER IF EXISTS `AUDIT`;\n" +
				"CREATE TRIGGER audit AFTER INSERT ON othertable FOR EACH ROW " + oldBody + ";",
		},
		{
			name:    "once one table needs the lock, every table the block touches is locked",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				TriggersAdded: []difftypes.TriggerRef{
					{TriggerName: "audit_v2", TableName: "mytable", Desired: auditTrigger("audit_v2", "mytable", oldBody)},
					{TriggerName: "audit_other", TableName: "othertable", Desired: auditTrigger("audit_other", "othertable", oldBody)},
				},
				TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "audit_v1", TableName: "mytable"}},
			},
			want: "LOCK TABLES mytable WRITE, othertable WRITE;\n" +
				"CREATE TRIGGER audit_v2 AFTER INSERT ON mytable FOR EACH ROW " + oldBody + ";\n" +
				"CREATE TRIGGER audit_other AFTER INSERT ON othertable FOR EACH ROW " + oldBody + ";\n" +
				"DROP TRIGGER IF EXISTS audit_v1;\n" +
				"UNLOCK TABLES;",
		},
		{
			name:    "a table spelled bare and qualified with the connection's database is locked once",
			dialect: platform.MySQL,
			diff: &difftypes.SchemaDiff{
				IdentifierSemantics: mysqlSemanticsIn("app"),
				TriggersAdded: []difftypes.TriggerRef{{
					TriggerName: "audit_v2", TableName: "mytable",
					Desired: auditTrigger("audit_v2", "mytable", oldBody),
				}},
				TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "audit_v1", TableName: "app.mytable"}},
			},
			want: "LOCK TABLES mytable WRITE;\n" +
				"CREATE TRIGGER audit_v2 AFTER INSERT ON mytable FOR EACH ROW " + oldBody + ";\n" +
				"DROP TRIGGER IF EXISTS audit_v1;\n" +
				"UNLOCK TABLES;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planStatementsSQL(c, test.diff, test.dialect), qt.Equals, test.want)
		})
	}
}

// TestPlanner_TriggerSwap_HeldBackDropKeepsItsTableHeader reads the comments a
// migration file shows: a column drop held back until after the trigger block
// carries its table's header, and a table with nothing else to change has no
// header in front of the block.
func TestPlanner_TriggerSwap_HeldBackDropKeepsItsTableHeader(t *testing.T) {
	c := qt.New(t)
	sql, err := migrationplanner.GenerateSchemaDiffSQL(
		context.Background(), must.Must(builtin.New()),
		&difftypes.SchemaDiff{
			TablesModified: []difftypes.TableDiff{{
				TableName:      "mytable",
				ColumnsRemoved: difftypes.ColumnChanges{{Name: "legacy", Type: "VARCHAR(100)"}},
			}},
			TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "mytable_after_insert", TableName: "mytable"}},
		}, platform.MySQL,
	)
	c.Assert(err, qt.IsNil)
	sql = legacyRenderedSQL(sql)

	c.Assert(strings.Count(sql, "-- Modify table: mytable"), qt.Equals, 1)
	c.Assert(sql, qt.Matches, `(?s).*DROP TRIGGER IF EXISTS mytable_after_insert;\n-- Modify table: mytable\n.*ALTER TABLE mytable DROP COLUMN legacy;.*`)
}

// TestPlanner_TriggerSwap_NoLockOutsideTheMySQLFamily keeps LOCK TABLES out of
// the dialects that share this planner and have no such statement.
func TestPlanner_TriggerSwap_NoLockOutsideTheMySQLFamily(t *testing.T) {
	diff := &difftypes.SchemaDiff{
		TriggersAdded: []difftypes.TriggerRef{{
			TriggerName: "audit_v2", TableName: "mytable",
			Desired: auditTrigger("audit_v2", "mytable", "SELECT 1"),
		}},
		TriggersRemoved: []difftypes.TriggerRef{{TriggerName: "audit_v1", TableName: "mytable"}},
	}
	for _, dialect := range []string{platform.SQLServer, platform.Oracle} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(planStatementsSQL(c, diff, dialect), qt.Not(qt.Contains), "LOCK TABLES")
		})
	}
}
