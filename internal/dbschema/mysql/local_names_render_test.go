package mysql_test

import (
	"context"
	"database/sql/driver"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/planext"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/internal/dbschema/mysql"
	"ptah.run/internal/planstyle"
	"ptah.run/internal/readscope"
	"ptah.run/migration/generator"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// plannedRemoval plans the removal of every view, routine and trigger read
// leaves in database, and the rollback that creates them again, the way
// `ptah migrations generate` plans a migration from a read: the desired
// schema is the read without them.
func plannedRemoval(c *qt.C, read *catalog.Database, dialect string) (up, down []string) {
	c.Helper()
	ctx := context.Background()
	runtime := must.Must(builtin.New())
	desired, err := dbschematogo.ConvertDBSchemaToGoSchema(ctx, read, dialect, runtime)
	c.Assert(err, qt.IsNil)
	desired.Views, desired.Functions, desired.Triggers = nil, nil, nil
	diff, err := schemadiff.CompareWithDialect(ctx, desired, read, dialect, runtime)
	c.Assert(err, qt.IsNil)
	plan, err := generator.PlanBidirectionalSchemaDiff(ctx, generator.BidirectionalSchemaPlanOptions{
		Runtime: runtime, Diff: diff, DesiredSchema: desired, CurrentSchema: read, Dialect: dialect,
		Policy: generator.BidirectionalPlanPolicy{Statements: planstyle.Statements},
	})
	c.Assert(err, qt.IsNil)
	options := planext.Options{Capabilities: capability.ForDialect(dialect), Statements: planstyle.Statements}
	up, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, runtime, plan.Forward.Diff, dialect, options)
	c.Assert(err, qt.IsNil)
	down, err = planner.GenerateSchemaDiffSQLStatementsWithOptions(ctx, runtime, plan.Reverse.Diff, dialect, options)
	c.Assert(err, qt.IsNil)
	return up, down
}

// renderedCatalog is database fx holding a table, a view of it, a function, a
// procedure and a trigger on the table, read through a connection that
// selected fx.
func renderedCatalog() visibleCatalog {
	return visibleCatalog{
		selected:  "fx",
		databases: []string{"fx"},
		tables:    []string{"child"},
		views:     [][]driver.Value{{"v_child", "select `fx`.`child`.`id` AS `id` from `fx`.`child`", "NONE", "DEFINER", "app@%", "app@%"}},
		triggers:  [][]driver.Value{triggerRow("trg_child", "SET NEW.n = 1")},
		routines:  [][]driver.Value{functionRow("f_double", "RETURN 2"), procedureRow("p_noop", "SELECT 1")},
	}
}

// TestGeneratedMigration_NamesNoDatabaseOfTheRead plans, from a read of
// database fx, the migration that removes its view, function, procedure and
// trigger, and the rollback that creates them again. No statement names fx:
// the file is planned against one database and runs on another, as a
// migration generated from a dev database's replay does, and a derived
// rollback planned from a shadow database's read runs on the target
// (stokaro/ptah#6710).
func TestGeneratedMigration_NamesNoDatabaseOfTheRead(t *testing.T) {
	tests := []struct {
		name     string
		version  string
		dialect  string
		wantUp   []string
		wantDown []string
	}{
		{
			name:    "MySQL",
			version: dbtest.MySQLVersion,
			dialect: platform.MySQL,
			wantUp: []string{
				"DROP FUNCTION `f_double`",
				"DROP PROCEDURE `p_noop`",
				"DROP TRIGGER `trg_child`",
				"DROP VIEW `v_child`",
			},
			wantDown: []string{
				"CREATE VIEW `v_child` AS\nselect `child`.`id` AS `id` from `child`",
				"CREATE FUNCTION `f_double`() RETURNS int DETERMINISTIC SQL SECURITY DEFINER RETURN 2",
				"CREATE PROCEDURE `p_noop`() READS SQL DATA SQL SECURITY DEFINER SELECT 1",
				"CREATE TRIGGER `trg_child` BEFORE INSERT ON `child` FOR EACH ROW SET NEW.n = 1",
			},
		},
		{
			name:    "MariaDB",
			version: dbtest.MariaDBVersion,
			dialect: platform.MariaDB,
			wantUp: []string{
				"DROP FUNCTION `f_double`",
				"DROP PROCEDURE `p_noop`",
				"DROP TRIGGER `trg_child`",
				"DROP VIEW `v_child`",
			},
			wantDown: []string{
				"CREATE VIEW `v_child` AS\nselect `child`.`id` AS `id` from `child`",
				"CREATE FUNCTION `f_double`() RETURNS int DETERMINISTIC SQL SECURITY DEFINER RETURN 2",
				"CREATE PROCEDURE `p_noop`() READS SQL DATA SQL SECURITY DEFINER SELECT 1",
				"CREATE TRIGGER `trg_child` BEFORE INSERT ON `child` FOR EACH ROW SET NEW.n = 1",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := dbtest.Open(t, dbtest.MySQLSession(test.version, []string{"GRANT ALL PRIVILEGES ON *.* TO `app`@`%`"},
				renderedCatalog().answer))
			read, err := mysql.NewMySQLReader(db.SQL, "").ReadSchemaContext(readscope.Authored(t.Context()))
			c.Assert(err, qt.IsNil)

			up, down := plannedRemoval(c, read, test.dialect)

			c.Assert(up, qt.DeepEquals, test.wantUp)
			c.Assert(down, qt.DeepEquals, test.wantDown)
		})
	}
}
