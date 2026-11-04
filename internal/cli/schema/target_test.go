package schema_test

import (
	"fmt"
	"net"
	"os"
	"path/filepath"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/atlasschema"
	"ptah.run/internal/errorkind"
	"ptah.run/migration/safety"
)

// refusedMySQLServerURL is a MySQL URL that names no database, on a port
// nothing listens on.
func refusedMySQLServerURL(c *qt.C) string {
	c.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	c.Assert(err, qt.IsNil)
	address := listener.Addr().String()
	c.Assert(listener.Close(), qt.IsNil)
	return "mysql://root:secret@" + address + "/"
}

// mysqlServerPlanFile writes a MySQL plan with one statement, which apply
// reads and validates before it opens the target.
func mysqlServerPlanFile(c *qt.C) string {
	c.Helper()
	document, err := atlasschema.MarshalPlanFile(atlasschema.PlanFile{
		FormatVersion:   atlasschema.PlanFormatVersion,
		Name:            "add_orders",
		Dialect:         platform.MySQL,
		FromFingerprint: "sha256:" + fmt.Sprintf("%064x", 1),
		ToFingerprint:   "sha256:" + fmt.Sprintf("%064x", 2),
		Statements: []atlasschema.PlanStatement{{
			SQL: "CREATE TABLE `app`.`orders` (`id` int)", Severity: safety.Safe, Reason: "create table orders",
		}},
	})
	c.Assert(err, qt.IsNil)
	path := filepath.Join(c.TempDir(), "add_orders.plan.json")
	c.Assert(os.WriteFile(path, document, 0o600), qt.IsNil)
	return path
}

// TestSchemaPlanAndApplyDialAMySQLServerURL gives `schema plan`, `schema
// apply` and `schema apply --plan` a MySQL URL that names no database. Each
// accepts it as the whole server and dials it, so the run fails on the port
// nothing listens on rather than on the URL. A command that opens one
// database refuses that URL before it dials, and that refusal carries no
// connection kind.
func TestSchemaPlanAndApplyDialAMySQLServerURL(t *testing.T) {
	tests := []struct {
		name string
		// source names what the run reads: "plan" for a plan file, "schema"
		// for a desired schema.
		source string
		args   []string
	}{
		{name: "schema plan", source: "schema", args: []string{"plan", "--save"}},
		{name: "schema apply", source: "schema", args: []string{"apply", "--dry-run"}},
		{name: "schema apply --plan", source: "plan", args: []string{"apply", "--dry-run"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sources := map[string][]string{
				"plan":   {"--plan", mysqlServerPlanFile(c)},
				"schema": {"--schema-file", writeSchemaSQLFile(c, c.TempDir(), "schema.sql", "CREATE TABLE app.orders (id int);\n")},
			}
			args := slices.Concat(test.args, []string{"--db-url", refusedMySQLServerURL(c)}, sources[test.source])

			_, _, err := runSchemaStreams(args...)

			c.Assert(err, qt.ErrorMatches, `connect to --db-url: .*`)
			c.Assert(errorkind.Classify(err), qt.Equals, errorkind.DatabaseUnreachable, qt.Commentf("%v", err))
		})
	}
}
