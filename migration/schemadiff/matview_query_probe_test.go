package schemadiff_test

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/dbschema/dbtest"
	"ptah.run/migration/schemadiff"
)

// viewProbeServer answers the view-body probe the way PostgreSQL does: a
// plain view refuses WITH NO DATA, and a body it accepts is printed back as
// spelled, the way pg_get_viewdef prints it. It records each view the probe
// creates.
type viewProbeServer struct {
	spelled string
	mu      sync.Mutex
	created []string
}

func (s *viewProbeServer) exec(query string, _ []driver.NamedValue) (driver.Result, error) {
	if !strings.HasPrefix(query, "CREATE TEMPORARY VIEW") {
		return driver.RowsAffected(0), nil
	}
	s.mu.Lock()
	s.created = append(s.created, query)
	s.mu.Unlock()
	if strings.Contains(strings.ToUpper(query), "WITH NO DATA") {
		return nil, errors.New(`ERROR: syntax error at or near "WITH" (SQLSTATE 42601)`)
	}
	return driver.RowsAffected(0), nil
}

func (s *viewProbeServer) query(string, []driver.NamedValue) (dbtest.QueryResult, error) {
	return dbtest.QueryResult{Columns: []string{"pg_get_viewdef"}, Rows: [][]driver.Value{{s.spelled}}}, nil
}

// viewProbeConnection is a PostgreSQL connection to a viewProbeServer.
type viewProbeConnection struct{ db *sql.DB }

func (viewProbeConnection) Info() catalog.ServerInfo {
	semantics := identifier.ForDialect("postgres")
	semantics.DefaultSchema = "public"
	return catalog.ServerInfo{Dialect: "postgres", Schema: "public", IdentifierSemantics: semantics}
}

func (c viewProbeConnection) ResolveIdentifierSemantics(context.Context, []string) (identifier.Semantics, error) {
	return c.Info().IdentifierSemantics, nil
}

func (c viewProbeConnection) WithRolledBackTransaction(
	ctx context.Context, _ string, run func(context.Context, *sql.Tx) error,
) (bool, error) {
	tx := must.Must(c.db.BeginTx(ctx, nil))
	defer func() { _ = tx.Rollback() }()
	return true, run(ctx, tx)
}

// TestCompareWithDatabase_SpellsAMaterializedViewsQuery pins the probe a
// materialized view declared WITH NO DATA is spelled with: the query, without
// the clause, which a plain view refuses. A function in FROM is printed back
// with a column alias list no fold of the text produces, so only the server's
// spelling matches the stored definition (stokaro/ptah#4057). Asked with the
// clause, the probe is refused, and the view is changed on every plan
// (stokaro/ptah#5006).
func TestCompareWithDatabase_SpellsAMaterializedViewsQuery(t *testing.T) {
	c := qt.New(t)
	stored := "SELECT n FROM generate_series(1, 7) n(n);"
	server := &viewProbeServer{spelled: stored}
	db := dbtest.OpenWithExec(t, server.query, server.exec)
	desired := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{
		Name: "day_numbers", Body: "SELECT n FROM generate_series(1, 7) AS n WITH NO DATA",
	}}}
	current := &catalog.Database{MatViews: []catalog.MaterializedView{{Name: "day_numbers", Body: stored}}}

	diff, err := schemadiff.CompareWithDatabase(c.Context(), viewProbeConnection{db: db.SQL}, desired, current, nil, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(server.created, qt.HasLen, 1)
	c.Assert(server.created[0], qt.Matches, `(?s)CREATE TEMPORARY VIEW \S+ AS\nSELECT n FROM generate_series\(1, 7\) AS n`)
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(diff.Matched.MaterializedViews, qt.Equals, 1)
}
