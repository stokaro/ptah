// Package ydb gives migration/migrator what YDB does differently from what
// its descriptor states: a migration body runs as the queries YDB runs it as,
// a statement timeout is a deadline on each query, and a check assertion is
// proved to read only local tables before it runs.
package ydb

import (
	"context"
	"database/sql"
	"fmt"

	"ptah.run/core/platform"
	ydbschema "ptah.run/internal/dbschema/ydb"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/sqlreach"
	"ptah.run/internal/yqlquery"
	"ptah.run/migration/migrationfile"
)

// Services returns YDB's services.
//
// A check runs in a snapshot read-only transaction, which dbschema's isolated
// query session opens for a read-only request and which refuses a write. YDB
// has no catalog SQL could ask; the migrator describes its tables through the
// connection's schema reader instead, so there is no Catalog here.
func Services() migratorext.Services {
	return migratorext.Services{
		Dialects:       []string{platform.YDB},
		Timeouts:       timeouts{},
		QueryDeadlines: true,
		Units:          units{},
		Checks:         checks{},
		ReadProof:      checks{},
		ReadOnlyChecks: true,
	}
}

// units splits a body as YDB reads it. YDB runs a scheme statement only
// outside a transaction and never in the same query as a data statement, and
// a named expression, an action, a DECLARE or a PRAGMA exists only in the
// query that holds it, so each scheme statement is a query of its own and
// each run of data statements one query (see internal/yqlquery).
type units struct{}

func (units) Split(sqlText string) ([]migratorext.Unit, error) {
	queries, err := yqlquery.Split(sqlText)
	if err != nil {
		return nil, err
	}
	split := make([]migratorext.Unit, 0, len(queries))
	for _, query := range queries {
		split = append(split, migratorext.Unit{
			Text:       query.Text,
			Source:     query.Source,
			Statements: query.Statements,
			Mixed:      query.Mixed,
		})
	}
	return split, nil
}

func (units) IsData(query string) bool {
	kind, ok := yqlquery.KindOf(query)
	return ok && kind == yqlquery.Data
}

// timeouts sends no statement: the statement timeout travels in the context,
// and the executor bounds each query with it.
type timeouts struct{}

func (timeouts) Statements(migrationfile.Timeouts, bool) (setup, restore []string, err error) {
	return nil, nil, nil
}

type checks struct{}

// Validate holds an assertion to one read-only SELECT. Starting with SELECT
// proves less in YQL: an action, a lambda or a subquery carries a statement
// no semicolon precedes.
func (checks) Validate(assertion string) error {
	if err := sqlreach.YQLReadOnly(assertion); err != nil {
		return fmt.Errorf("check assertion must be one read-only SELECT statement: %w", err)
	}
	return nil
}

// ReadProof describes each name an assertion reads before it runs. The text
// cannot say whether a read stays in the database: an external table is named
// exactly as a table is, and measured on 25.1.4.7 and 26.2.1.14 a SELECT over
// one, or over a view that reads one, fetches the table's files from the
// object storage its data source names, inside the read-only transaction a
// check runs in. One that is an external table, or that cannot be followed, is
// refused (see ydbschema.ProveLocalReads).
func (checks) ReadProof(_ context.Context, session *sql.Conn) (func(context.Context, string) error, error) {
	catalog, root, err := ydbschema.SessionReadCatalog(session)
	if err != nil {
		return nil, err
	}
	return func(ctx context.Context, assertion string) error {
		if err := ydbschema.ProveLocalReads(ctx, catalog, root, assertion); err != nil {
			return fmt.Errorf("check assertion cannot be proved read-only: %w", err)
		}
		return nil
	}, nil
}
