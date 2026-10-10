package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/config"
	"ptah.run/internal/pgexclude"
)

// ExcludeExpressionProbe is one declared EXCLUDE constraint whose elements and
// predicate need the target server's own spelling.
type ExcludeExpressionProbe struct {
	// Key identifies the constraint to the caller and is never sent to the
	// server.
	Key string
	// Table is the bare name of the table the constraint is on, as the server
	// stores it. The probe table takes that name, so a declaration naming its
	// own table resolves; see [newProbeRelation]. Empty keeps a numbered name.
	Table string
	// Columns are the columns of the table the constraint is on, from the LIVE
	// read: the elements have to parse against the table they will guard.
	Columns []CheckProbeColumn
	// UsingMethod is the index method, such as gist.
	UsingMethod string
	// Elements is the declared element list, without its parentheses.
	Elements string
	// Where is the declared WHERE clause, empty for a constraint over every
	// row.
	Where string
}

// ResolveExcludeExpressions asks the connected server to normalize each
// declared EXCLUDE constraint's elements and WHERE clause.
//
// The same rewrite as [ResolveCheckExpressions]: PostgreSQL 18.6 stores `WHERE
// (s > 0)` as `WHERE ((s > 0))`, `(lower(t)) WITH =` as `lower(t) WITH =`, and
// casts a literal to the type of the column it is compared with, so an
// exclusion constraint nobody had changed was dropped and added again on every
// run (stokaro/ptah#3767).
//
// The probe is a temporary table carrying the live table's columns and the
// declared constraint, inside a transaction that is rolled back. Its definition
// is read back with pg_get_constraintdef and split by
// [pgexclude.Parse], which is what the reader asks of
// a live constraint and how it splits it, so both sides of the comparison are
// printed and cut by the same code.
//
// A declaration the server refuses is returned with Resolved false. Dialects
// outside the PostgreSQL family, and a connection pinned to a session with a
// transaction open, return nil, for the reason the package documentation
// gives.
func ResolveExcludeExpressions(
	ctx context.Context,
	conn Session,
	probes []ExcludeExpressionProbe,
) (map[string]config.ExcludeExpression, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve exclude expressions: database connection is nil")
	}
	if len(probes) == 0 {
		return nil, nil
	}
	if !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}
	return resolveProbes(ctx, conn, "resolve exclude expressions", probes,
		func(probe ExcludeExpressionProbe) string { return probe.Key },
		resolveOneExcludeExpression)
}

func resolveOneExcludeExpression(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe ExcludeExpressionProbe,
) (config.ExcludeExpression, error) {
	method := strings.TrimSpace(probe.UsingMethod)
	elements := strings.TrimSpace(probe.Elements)
	if method == "" || elements == "" || len(probe.Columns) == 0 {
		return config.ExcludeExpression{}, nil
	}

	relation := newProbeRelation(probe.Table, "ptah_exclude_probe", index)
	constraint := fmt.Sprintf("CONSTRAINT ptah_exclude_probe_x EXCLUDE USING %s (%s)", method, elements)
	if where := strings.TrimSpace(probe.Where); where != "" {
		constraint += fmt.Sprintf(" WHERE (%s)", where)
	}
	statements := relation.statements(fmt.Sprintf(
		"CREATE TEMPORARY TABLE %s (%s, %s)", relation.name, checkProbeColumnList(probe.Columns), constraint))

	const query = `
		SELECT COALESCE(pg_get_constraintdef(c.oid), '')
		FROM pg_constraint c
		WHERE c.conrelid = $1::regclass AND c.contype = 'x'`

	var definition string
	ok, err := runProbe(ctx, tx, "resolve exclude expressions", probe.Key, "ptah_exclude_probe", postgresSavepoints,
		statements, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, query, relation.regclass).Scan(&definition)
		})
	if err != nil || !ok {
		return config.ExcludeExpression{}, err
	}
	parsed, err := pgexclude.Parse(definition)
	if err != nil {
		// The server printed a definition the reader's own parser cannot
		// split. The reader skips such a constraint too, so there is nothing
		// on the other side to compare with.
		return config.ExcludeExpression{}, nil
	}
	return config.ExcludeExpression{Elements: parsed.Elements, Where: parsed.WhereCondition, Resolved: true}, nil
}
