//go:build integration

package driftcmd_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/migration/safety"
)

// roleName stands for the role a row's statements grant to. A role belongs to
// the server rather than to a database, so each row creates one of its own.
const roleName = "{role}"

// postgresObjects holds one object of every kind a PostgreSQL drift report
// names beside a table: a sequence, a view, a materialized view, a trigger
// and a privilege granted to a role.
var postgresObjects = []string{
	`CREATE TABLE users (id integer PRIMARY KEY, email text NOT NULL)`,
	`CREATE SEQUENCE order_seq INCREMENT BY 1`,
	`CREATE VIEW user_emails AS SELECT email FROM users`,
	`CREATE MATERIALIZED VIEW user_count AS SELECT count(*) AS n FROM users`,
	`CREATE FUNCTION touch_users() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`,
	`CREATE TRIGGER touch_users BEFORE UPDATE ON users FOR EACH ROW EXECUTE FUNCTION touch_users()`,
	`GRANT SELECT ON users TO ` + roleName,
}

// postgresExtras is one more object of each of those kinds.
var postgresExtras = []string{
	`CREATE SEQUENCE extra_seq`,
	`CREATE VIEW extra_view AS SELECT id FROM users`,
	`CREATE MATERIALIZED VIEW extra_count AS SELECT count(*) AS n FROM users WHERE id > 0`,
	`CREATE TRIGGER extra_touch BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION touch_users()`,
	`GRANT INSERT ON users TO ` + roleName,
}

// postgresChanged is postgresObjects with each object of those kinds changed
// in a way the comparison reads: the sequence's increment, the bodies of both
// views, the trigger's event and the grant's WITH GRANT OPTION.
var postgresChanged = []string{
	`CREATE TABLE users (id integer PRIMARY KEY, email text NOT NULL)`,
	`CREATE SEQUENCE order_seq INCREMENT BY 5`,
	`CREATE VIEW user_emails AS SELECT email FROM users WHERE id > 0`,
	`CREATE MATERIALIZED VIEW user_count AS SELECT count(*) AS n FROM users WHERE id > 0`,
	`CREATE FUNCTION touch_users() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$`,
	`CREATE TRIGGER touch_users BEFORE INSERT ON users FOR EACH ROW EXECUTE FUNCTION touch_users()`,
	`GRANT SELECT ON users TO ` + roleName + ` WITH GRANT OPTION`,
}

// postgresInventory counts postgresObjects on both sides, desired then
// observed for each of the kinds the rows change.
func postgresInventory(desired, observed int) []safety.InventoryEntry {
	return []safety.InventoryEntry{
		{Kind: "columns", Desired: 2, Observed: 2},
		{Kind: "functions", Desired: 1, Observed: 1},
		{Kind: "grants", Desired: desired, Observed: observed},
		{Kind: "materialized_views", Desired: desired, Observed: observed},
		{Kind: "roles", Desired: 1, Observed: 1},
		{Kind: "sequences", Desired: desired, Observed: observed},
		{Kind: "tables", Desired: 1, Observed: 1},
		{Kind: "triggers", Desired: desired, Observed: observed},
		{Kind: "views", Desired: desired, Observed: observed},
	}
}

// mysqlObjects is the MySQL family's share of those kinds: a view, a trigger
// and a privilege granted to a role. Neither engine has a materialized view,
// and MySQL has no sequence.
var mysqlObjects = []string{
	"CREATE TABLE users (id int PRIMARY KEY, email varchar(255) NOT NULL)",
	"CREATE VIEW user_emails AS SELECT email FROM users",
	"CREATE TRIGGER touch_users BEFORE UPDATE ON users FOR EACH ROW SET NEW.email = NEW.email",
	"GRANT SELECT ON users TO " + roleName,
}

var mysqlExtras = []string{
	"CREATE VIEW extra_view AS SELECT id FROM users",
	"CREATE TRIGGER extra_touch BEFORE INSERT ON users FOR EACH ROW SET NEW.email = NEW.email",
	"GRANT INSERT ON users TO " + roleName,
}

var mysqlChanged = []string{
	"CREATE TABLE users (id int PRIMARY KEY, email varchar(255) NOT NULL)",
	"CREATE VIEW user_emails AS SELECT email FROM users WHERE id > 0",
	"CREATE TRIGGER touch_users BEFORE INSERT ON users FOR EACH ROW SET NEW.email = NEW.email",
	"GRANT SELECT ON users TO " + roleName,
}

func mysqlInventory(desired, observed int) []safety.InventoryEntry {
	return []safety.InventoryEntry{
		{Kind: "columns", Desired: 2, Observed: 2},
		{Kind: "grants", Desired: desired, Observed: observed},
		{Kind: "roles", Desired: 1, Observed: 1},
		{Kind: "tables", Desired: 1, Observed: 1},
		{Kind: "triggers", Desired: desired, Observed: observed},
		{Kind: "views", Desired: desired, Observed: observed},
	}
}

// mariadbObjects adds a sequence to mysqlObjects; MariaDB has them.
var (
	mariadbObjects = append(slices.Clone(mysqlObjects), "CREATE SEQUENCE order_seq INCREMENT BY 1")
	mariadbExtras  = append(slices.Clone(mysqlExtras), "CREATE SEQUENCE extra_seq")
	mariadbChanged = append(slices.Clone(mysqlChanged), "CREATE SEQUENCE order_seq INCREMENT BY 5")
)

func mariadbInventory(desired, observed int) []safety.InventoryEntry {
	return []safety.InventoryEntry{
		{Kind: "columns", Desired: 2, Observed: 2},
		{Kind: "grants", Desired: desired, Observed: observed},
		{Kind: "roles", Desired: 1, Observed: 1},
		{Kind: "sequences", Desired: desired, Observed: observed},
		{Kind: "tables", Desired: 1, Observed: 1},
		{Kind: "triggers", Desired: desired, Observed: observed},
		{Kind: "views", Desired: desired, Observed: observed},
	}
}

// declaring is a desired schema: the role, which the schema has to declare for
// the comparison to take back a privilege it holds, and the statements.
func declaring(statements ...[]string) []string {
	return append([]string{"CREATE ROLE " + roleName}, slices.Concat(statements...)...)
}

// TestSchemaDriftNamesEveryKindE2E drives `ptah schema drift --format json`
// against a live server holding one object of every kind its engine has
// beside a table. Each row differs from the declaration in one direction for
// every kind at once, so each finding counts one object, and the inventory
// counts the object on the side that holds it. The database that matches
// reports no finding and counts each kind the same on both sides.
//
// The live server is what decides the readings: how it reports a view's body
// and a trigger's event, the privileges it lists for a role, and the options
// of a sequence nobody gave every option.
func TestSchemaDriftNamesEveryKindE2E(t *testing.T) {
	tests := []struct {
		name          string
		engine        dbtarget.Engine
		desired       []string
		database      []string
		wantFindings  []safety.Finding
		wantInventory []safety.InventoryEntry
	}{
		{
			name:          "PostgreSQL matches",
			engine:        dbtarget.PostgreSQL,
			desired:       declaring(postgresObjects),
			database:      postgresObjects,
			wantInventory: postgresInventory(1, 1),
		},
		{
			name:   "PostgreSQL lacks one of each",
			engine: dbtarget.PostgreSQL,
			desired: declaring(postgresObjects[:len(postgresObjects)-1], postgresExtras,
				[]string{`GRANT SELECT ON users TO ` + roleName + ` WITH GRANT OPTION`}),
			database: postgresObjects,
			wantFindings: []safety.Finding{
				{Category: "grant_options_added", Count: 1, Severity: safety.Safe},
				{Category: "grants_added", Count: 1, Severity: safety.Safe},
				{Category: "materialized_views_added", Count: 1, Severity: safety.Safe},
				{Category: "sequences_added", Count: 1, Severity: safety.Safe},
				{Category: "triggers_added", Count: 1, Severity: safety.Safe},
				{Category: "views_added", Count: 1, Severity: safety.Safe},
			},
			wantInventory: postgresInventory(2, 1),
		},
		{
			name:     "PostgreSQL holds one more of each",
			engine:   dbtarget.PostgreSQL,
			desired:  declaring(postgresObjects),
			database: slices.Concat(postgresObjects, postgresExtras),
			wantFindings: []safety.Finding{
				{Category: "materialized_views_removed", Count: 1, Severity: safety.Destructive},
				{Category: "sequences_removed", Count: 1, Severity: safety.Destructive},
				{Category: "triggers_removed", Count: 1, Severity: safety.Destructive},
				{Category: "views_removed", Count: 1, Severity: safety.Destructive},
				{Category: "grants_removed", Count: 1, Severity: safety.Warning},
			},
			wantInventory: postgresInventory(1, 2),
		},
		{
			name:     "PostgreSQL differs in one of each",
			engine:   dbtarget.PostgreSQL,
			desired:  declaring(postgresObjects),
			database: postgresChanged,
			wantFindings: []safety.Finding{
				{Category: "materialized_views_modified", Count: 1, Severity: safety.Destructive},
				{Category: "grant_options_removed", Count: 1, Severity: safety.Warning},
				{Category: "sequences_modified", Count: 1, Severity: safety.Warning},
				{Category: "triggers_modified", Count: 1, Severity: safety.Warning},
				{Category: "views_modified", Count: 1, Severity: safety.Warning},
			},
			wantInventory: postgresInventory(1, 1),
		},
		{
			name:          "MySQL matches",
			engine:        dbtarget.MySQLAdmin,
			desired:       declaring(mysqlObjects),
			database:      mysqlObjects,
			wantInventory: mysqlInventory(1, 1),
		},
		{
			name:     "MySQL lacks one of each",
			engine:   dbtarget.MySQLAdmin,
			desired:  declaring(mysqlObjects, mysqlExtras),
			database: mysqlObjects,
			wantFindings: []safety.Finding{
				{Category: "grants_added", Count: 1, Severity: safety.Safe},
				{Category: "triggers_added", Count: 1, Severity: safety.Safe},
				{Category: "views_added", Count: 1, Severity: safety.Safe},
			},
			wantInventory: mysqlInventory(2, 1),
		},
		{
			name:     "MySQL holds one more of each",
			engine:   dbtarget.MySQLAdmin,
			desired:  declaring(mysqlObjects),
			database: slices.Concat(mysqlObjects, mysqlExtras),
			wantFindings: []safety.Finding{
				{Category: "triggers_removed", Count: 1, Severity: safety.Destructive},
				{Category: "views_removed", Count: 1, Severity: safety.Destructive},
				{Category: "grants_removed", Count: 1, Severity: safety.Warning},
			},
			wantInventory: mysqlInventory(1, 2),
		},
		{
			name:     "MySQL differs in one of each",
			engine:   dbtarget.MySQLAdmin,
			desired:  declaring(mysqlObjects),
			database: mysqlChanged,
			wantFindings: []safety.Finding{
				{Category: "triggers_modified", Count: 1, Severity: safety.Warning},
				{Category: "views_modified", Count: 1, Severity: safety.Warning},
			},
			wantInventory: mysqlInventory(1, 1),
		},
		{
			name:          "MariaDB matches",
			engine:        dbtarget.MariaDBAdmin,
			desired:       declaring(mariadbObjects),
			database:      mariadbObjects,
			wantInventory: mariadbInventory(1, 1),
		},
		{
			name:     "MariaDB lacks one of each",
			engine:   dbtarget.MariaDBAdmin,
			desired:  declaring(mariadbObjects, mariadbExtras),
			database: mariadbObjects,
			wantFindings: []safety.Finding{
				{Category: "grants_added", Count: 1, Severity: safety.Safe},
				{Category: "sequences_added", Count: 1, Severity: safety.Safe},
				{Category: "triggers_added", Count: 1, Severity: safety.Safe},
				{Category: "views_added", Count: 1, Severity: safety.Safe},
			},
			wantInventory: mariadbInventory(2, 1),
		},
		{
			name:     "MariaDB holds one more of each",
			engine:   dbtarget.MariaDBAdmin,
			desired:  declaring(mariadbObjects),
			database: slices.Concat(mariadbObjects, mariadbExtras),
			wantFindings: []safety.Finding{
				{Category: "sequences_removed", Count: 1, Severity: safety.Destructive},
				{Category: "triggers_removed", Count: 1, Severity: safety.Destructive},
				{Category: "views_removed", Count: 1, Severity: safety.Destructive},
				{Category: "grants_removed", Count: 1, Severity: safety.Warning},
			},
			wantInventory: mariadbInventory(1, 2),
		},
		{
			name:     "MariaDB differs in one of each",
			engine:   dbtarget.MariaDBAdmin,
			desired:  declaring(mariadbObjects),
			database: mariadbChanged,
			wantFindings: []safety.Finding{
				{Category: "sequences_modified", Count: 1, Severity: safety.Warning},
				{Category: "triggers_modified", Count: 1, Severity: safety.Warning},
				{Category: "views_modified", Count: 1, Severity: safety.Warning},
			},
			wantInventory: mariadbInventory(1, 1),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			role := scratchRole(c, test.engine)
			url := scratchDatabase(c, test.engine, withRole(test.database, role))
			schema := writeSchema(c, withRole(test.desired, role))

			report := runDrift(c, "--db-url", url, "--schema-file", schema, "--format", "json")

			c.Assert(report.Error, qt.Equals, "")
			c.Assert(report.Drift, qt.Equals, len(test.wantFindings) > 0)
			c.Assert(report.Findings, qt.DeepEquals, test.wantFindings)
			c.Assert(report.Inventory, qt.DeepEquals, test.wantInventory)
		})
	}
}

// scratchRole creates a role of its own on the engine's server and drops it
// when the test ends. It is registered before the scratch database, so the
// database, and the privileges the role holds in it, are gone first.
func scratchRole(c *qt.C, engine dbtarget.Engine) string {
	c.Helper()
	admin, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, engine))
	c.Assert(err, qt.IsNil)
	// A MySQL account name holds at most 32 characters.
	name := fmt.Sprintf("ptah_drift_%d", time.Now().UnixNano()%1_000_000_000)
	_, err = admin.ExecContext(c.Context(), "CREATE ROLE "+name)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, dropErr := admin.ExecContext(context.Background(), "DROP ROLE "+name)
		c.Check(dropErr, qt.IsNil)
		dbschema.CloseAndWarn(admin)
	})
	return name
}

// withRole names the row's role in its statements.
func withRole(statements []string, role string) []string {
	named := make([]string, 0, len(statements))
	for _, statement := range statements {
		named = append(named, strings.ReplaceAll(statement, roleName, role))
	}
	return named
}
