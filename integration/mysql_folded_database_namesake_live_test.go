//go:build integration

package integration_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
)

// A diff that creates the database Sales and drops the database sales names
// one database on a server started with lower_case_table_names=1: the create
// makes nothing, and the drop removes the database with its rows. The planner
// refuses such a diff unless the connection that produced it recorded that
// the server keeps the two apart (stokaro/ptah#6785). Each test hands the
// planner the diff built by hand, as an embedder may, with the folding the
// connection reads.

// namesakeDiff is the diff that creates the run's declared database and drops
// its held one, carrying the folding conn read.
func (r scopeRun) namesakeDiff(c *qt.C) (*difftypes.SchemaDiff, *dbschema.DatabaseConnection) {
	c.Helper()
	conn, err := dbschema.ConnectToServer(c.Context(), r.scratch.adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })
	return &difftypes.SchemaDiff{
		NameFolding:    conn.Info().NameFolding,
		SchemasAdded:   []schemamodel.Schema{{Name: r.declared}},
		SchemasRemoved: []string{r.held},
	}, conn
}

func TestPlannerMySQLFamilyFoldedDatabaseNamesakesLive_FailurePath(t *testing.T) {
	for _, server := range foldedScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 1)
			diff, conn := run.namesakeDiff(c)

			statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), must.Must(builtin.New()), diff, conn.Info().Dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `(?s).*the server the comparison read compares database names without regard to case.*`)
			c.Assert(statements, qt.IsNil)
			c.Assert(diff.NameFolding, qt.Equals, catalog.NameFoldingFolds)
			c.Assert(run.databasesOf(c), qt.DeepEquals, []string{run.held})
			c.Assert(run.rows(c, run.held), qt.DeepEquals, []string{"t:1", "u:1"})
		})
	}
}

// The control: a server that keeps names apart by case holds two databases
// under the two names, and the planned create and drop run as written.
func TestPlannerMySQLFamilyExactDatabaseNamesakesLive_HappyPath(t *testing.T) {
	for _, server := range exactScopeServers {
		t.Run(server.name, func(t *testing.T) {
			c := qt.New(t)
			run := newScopeRun(c, server.server, 0)
			diff, conn := run.namesakeDiff(c)

			statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), must.Must(builtin.New()), diff, conn.Info().Dialect)
			c.Assert(err, qt.IsNil)
			for _, statement := range statements {
				_, err := run.scratch.admin.ExecContext(c.Context(), statement)
				c.Assert(err, qt.IsNil, qt.Commentf("%s", statement))
			}

			c.Assert(diff.NameFolding, qt.Equals, catalog.NameFoldingExact)
			c.Assert(run.databasesOf(c), qt.DeepEquals, []string{run.declared})
		})
	}
}
