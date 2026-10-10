package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/config"
)

// IndexExpressionProbe is one declared index whose expression or predicate
// needs the target server's own spelling.
type IndexExpressionProbe struct {
	// Key identifies the index to the caller and is never sent to the server.
	Key string
	// Table is the bare name of the table the index is on, as the server stores
	// it. The probe table takes that name, so a declaration naming its own
	// table resolves; see [newProbeRelation]. Empty keeps a numbered name.
	Table string
	// Columns are the columns of the table the index is on, from the LIVE read.
	Columns []CheckProbeColumn
	// Expression is what the index is over, empty for an index over plain
	// columns. Parts is what those plain columns are.
	Expression string
	Parts      []string
	// Predicate is the declared WHERE clause, empty for a full index.
	Predicate string
}

// ResolveIndexExpressions asks the connected server to normalize each declared
// index's expression and predicate.
//
// The third object with the same rewrite. Measured on 17.11, `lower(code)` over
// a varchar column is stored as `lower((code)::text)`, and a partial index's
// `unit >= 0` over numeric as `(unit >= (0)::numeric)`, so an index nobody had
// touched was dropped and rebuilt on every run (stokaro/ptah#2047). A
// connection pinned to a session with a transaction open returns nil, for the
// reason the package documentation gives.
func ResolveIndexExpressions(
	ctx context.Context,
	conn Session,
	probes []IndexExpressionProbe,
) (map[string]config.IndexExpression, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve index expressions: database connection is nil")
	}
	if len(probes) == 0 {
		return nil, nil
	}
	if !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}
	return resolveProbes(ctx, conn, "resolve index expressions", probes,
		func(probe IndexExpressionProbe) string { return probe.Key },
		resolveOneIndexExpression)
}

func resolveOneIndexExpression(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe IndexExpressionProbe,
) (config.IndexExpression, error) {
	expression := strings.TrimSpace(probe.Expression)
	predicate := strings.TrimSpace(probe.Predicate)
	if (expression == "" && predicate == "") || len(probe.Columns) == 0 {
		return config.IndexExpression{}, nil
	}

	relation := newProbeRelation(probe.Table, "ptah_index_probe", index)
	over := expression
	if over == "" {
		over = strings.Join(quoteProbeParts(probe.Parts), ", ")
	}
	if strings.TrimSpace(over) == "" {
		return config.IndexExpression{}, nil
	}
	create := fmt.Sprintf("CREATE INDEX ptah_index_probe_idx ON %s ((%s))", relation.name, over)
	if expression == "" {
		create = fmt.Sprintf("CREATE INDEX ptah_index_probe_idx ON %s (%s)", relation.name, over)
	}
	if predicate != "" {
		create += fmt.Sprintf(" WHERE (%s)", predicate)
	}
	statements := relation.statements(
		fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", relation.name, checkProbeColumnList(probe.Columns)),
		create,
	)

	// The expression is read with `pg_get_indexdef` per key, and the predicate
	// with `pg_get_expr`, because that is what the reader asks of a live index.
	// The two print the same tree differently: measured on 17.11,
	// `pg_get_expr(indexprs, …)` answers `lower((code)::text)` and
	// `pg_get_indexdef(oid, 1, true)` answers `lower(code::text)`, and a probe
	// that used the first would put a paren between two spellings of one index.
	const query = `
		SELECT COALESCE(pg_get_indexdef(i.indexrelid, 1, true), ''),
		       COALESCE(pg_get_expr(i.indpred, i.indrelid), '')
		FROM pg_index i
		WHERE i.indrelid = $1::regclass`

	var answer config.IndexExpression
	ok, err := runProbe(ctx, tx, "resolve index expressions", probe.Key, "ptah_index_probe", postgresSavepoints,
		statements, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, query, relation.regclass).
				Scan(&answer.Expression, &answer.Predicate)
		})
	if err != nil || !ok {
		return config.IndexExpression{}, err
	}
	if expression == "" {
		// The probe indexed plain columns, so the first key is a column name
		// rather than an expression. Reporting it as one would offer a
		// replacement for a declaration that has nothing to replace.
		answer.Expression = ""
	}
	answer.Resolved = true
	return answer, nil
}

// quoteProbeParts quotes each plain column an index is over.
func quoteProbeParts(parts []string) []string {
	quoted := make([]string, 0, len(parts))
	for _, part := range parts {
		quoted = append(quoted, quoteCheckProbeIdentifier(part))
	}
	return quoted
}
