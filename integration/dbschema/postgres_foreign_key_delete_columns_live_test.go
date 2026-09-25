//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
)

// These tests read a SQL schema document whose composite foreign key limits
// ON DELETE SET NULL to one column, the PostgreSQL 15 form stokaro/ptah#3562
// found refused. They apply what the document plans, plan again, and read the
// key back from the server.

// newDeleteListSchema creates a schema of its own on the PostgreSQL target and
// drops it when the test ends.
func newDeleteListSchema(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.PostgreSQL))
	c.Assert(err, qt.IsNil)
	schemaName := fmt.Sprintf("ptah_fk_delete_list_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(c.Context(), `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
		dbschema.CloseAndWarn(conn)
	})
	return conn, schemaName
}

const (
	notNullTenant  = "tenant integer NOT NULL"
	nullableTenant = "tenant integer"
)

// deleteListDocument is a parent and a child whose key covers (tenant,
// parent_id). tenant declares the child's tenant column, and action is the
// ON DELETE clause, list included.
func deleteListDocument(tenant, action string) string {
	return `
CREATE TABLE "%[1]s".parents (tenant integer, id integer, PRIMARY KEY (tenant, id));
CREATE TABLE "%[1]s".children (
    id        integer PRIMARY KEY,
    ` + tenant + `,
    parent_id integer,
    CONSTRAINT children_parent_fk FOREIGN KEY (tenant, parent_id)
        REFERENCES "%[1]s".parents (tenant, id) ` + action + `
);
`
}

// planDeleteListDocument loads the document the way every CLI verb does and
// plans it against the live schema.
func planDeleteListDocument(c *qt.C, conn *dbschema.DatabaseConnection, schemaName, document string) []string {
	c.Helper()
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(fmt.Sprintf(document, schemaName)), 0o600), qt.IsNil)
	declared, err := schemafile.LoadSources(
		[]schemafile.Source{{URL: path}},
		schemafile.Options{Dialect: platform.Postgres},
	)
	c.Assert(err, qt.IsNil)
	return planDocumentAgainstLive(c, conn, declared, schemaName)
}

// settleDeleteListDocument applies what the document plans and checks that
// planning it again finds nothing to do.
func settleDeleteListDocument(c *qt.C, conn *dbschema.DatabaseConnection, schemaName, document string) {
	c.Helper()
	statements := planDeleteListDocument(c, conn, schemaName, document)
	c.Assert(statements, qt.Not(qt.HasLen), 0)
	execDeleteListStatements(c, conn, statements)
	c.Assert(planDeleteListDocument(c, conn, schemaName, document), qt.HasLen, 0,
		qt.Commentf("the document plans again after its own statements ran"))
}

func execDeleteListStatements(c *qt.C, conn *dbschema.DatabaseConnection, statements []string) {
	c.Helper()
	for _, statement := range statements {
		_, err := conn.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}
}

// liveDeleteColumns reads back the ON DELETE column list of the child's key.
func liveDeleteColumns(c *qt.C, conn *dbschema.DatabaseConnection, schemaName string) []string {
	c.Helper()
	live, err := dbschema.ReadSchemaWithSchemasContext(c.Context(), conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	var constraints []catalog.Constraint
	for _, constraint := range live.Constraints {
		if constraint.Name == "children_parent_fk" {
			constraints = append(constraints, constraint)
		}
	}
	c.Assert(constraints, qt.HasLen, 1)
	return constraints[0].OnDeleteColumns
}

// The key is created with its list over a NOT NULL tenant, compares clean
// against the document, reads back, and does what the list says: deleting the
// parent clears parent_id and keeps tenant.
func TestPostgresLiveForeignKeyDeleteColumnListConverges(t *testing.T) {
	c := qt.New(t)
	conn, schemaName := newDeleteListSchema(c)

	settleDeleteListDocument(c, conn, schemaName, deleteListDocument(notNullTenant, "ON DELETE SET NULL (parent_id)"))

	c.Assert(liveDeleteColumns(c, conn, schemaName), qt.DeepEquals, []string{"parent_id"})
	execDeleteListStatements(c, conn, []string{
		fmt.Sprintf(`INSERT INTO "%s".parents VALUES (1, 1)`, schemaName),
		fmt.Sprintf(`INSERT INTO "%s".children VALUES (10, 1, 1)`, schemaName),
		fmt.Sprintf(`DELETE FROM "%s".parents`, schemaName),
	})
	var tenant int
	var parentID *int
	err := conn.QueryRowContext(c.Context(),
		fmt.Sprintf(`SELECT tenant, parent_id FROM "%s".children WHERE id = 10`, schemaName),
	).Scan(&tenant, &parentID)
	c.Assert(err, qt.IsNil)
	c.Assert(tenant, qt.Equals, 1)
	c.Assert(parentID, qt.IsNil)
}

// A list that changes which columns the action clears plans a change, in both
// directions, and the change settles. The narrowing row is the control for the
// test above: a clean second plan there is agreement, not a comparator that
// ignores the list.
func TestPostgresLiveForeignKeyDeleteColumnListChangePlans(t *testing.T) {
	tests := []struct {
		name        string
		from, to    string
		wantInPlan  string
		wantColumns []string
	}{
		{
			name:        "narrowed",
			from:        "ON DELETE SET NULL",
			to:          "ON DELETE SET NULL (parent_id)",
			wantInPlan:  `ON DELETE SET NULL ("parent_id")`,
			wantColumns: []string{"parent_id"},
		},
		{
			name:       "widened",
			from:       "ON DELETE SET NULL (parent_id)",
			to:         "ON DELETE SET NULL",
			wantInPlan: "ON DELETE SET NULL",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newDeleteListSchema(c)
			settleDeleteListDocument(c, conn, schemaName, deleteListDocument(nullableTenant, test.from))

			statements := planDeleteListDocument(c, conn, schemaName, deleteListDocument(nullableTenant, test.to))

			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.wantInPlan)
			execDeleteListStatements(c, conn, statements)
			c.Assert(planDeleteListDocument(c, conn, schemaName, deleteListDocument(nullableTenant, test.to)), qt.HasLen, 0)
			c.Assert(liveDeleteColumns(c, conn, schemaName), qt.DeepEquals, test.wantColumns)
		})
	}
}

// A list naming every column of the key says what no list says. The server
// prints such a list back, so a key created with one compares clean against a
// document without it, and the reverse.
func TestPostgresLiveForeignKeyFullDeleteColumnListIsNoList(t *testing.T) {
	tests := []struct {
		name     string
		from, to string
	}{
		{name: "created with every column, declared with none", from: "ON DELETE SET NULL (tenant, parent_id)", to: "ON DELETE SET NULL"},
		{name: "created with none, declared with every column", from: "ON DELETE SET NULL", to: "ON DELETE SET NULL (parent_id, tenant)"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newDeleteListSchema(c)
			settleDeleteListDocument(c, conn, schemaName, deleteListDocument(nullableTenant, test.from))

			statements := planDeleteListDocument(c, conn, schemaName, deleteListDocument(nullableTenant, test.to))

			c.Assert(statements, qt.HasLen, 0)
		})
	}
}
