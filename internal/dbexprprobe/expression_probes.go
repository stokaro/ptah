package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/dbschema"
)

// PolicyExpressionProbe is one declared RLS policy whose clauses need the
// target server's own spelling before they can be compared.
type PolicyExpressionProbe struct {
	// Key identifies the policy to the caller and is never sent to the server.
	Key string
	// Columns are the columns of the table the policy is on, from the LIVE
	// read: the clauses have to parse against the table they will guard.
	Columns []CheckProbeColumn
	// Using and WithCheck are the declared clauses, either of which may be
	// empty.
	Using     string
	WithCheck string
}

// IndexExpressionProbe is one declared index whose expression or predicate
// needs the target server's own spelling.
type IndexExpressionProbe struct {
	// Key identifies the index to the caller and is never sent to the server.
	Key string
	// Columns are the columns of the table the index is on, from the LIVE read.
	Columns []CheckProbeColumn
	// Expression is what the index is over, empty for an index over plain
	// columns. Parts is what those plain columns are.
	Expression string
	Parts      []string
	// Predicate is the declared WHERE clause, empty for a full index.
	Predicate string
}

// ResolvePolicyExpressions asks the connected server to normalize each declared
// policy's USING and WITH CHECK.
//
// Same rewrite as [ResolveCheckExpressions] and the same reason it cannot be
// folded textually: the cast PostgreSQL inserts depends on the type of the
// column the clause names. Measured on 17.11, `owner = 'x'` is stored as
// `((owner)::text = 'x'::text)` over a varchar column and unchanged over text,
// so a policy nobody had touched was dropped and recreated on every run -- a
// security control taken away and put back for no reason (stokaro/ptah#2049).
//
// The probe is a temporary table with row-level security enabled and the
// declared policy on it, inside a transaction that is rolled back. A
// connection pinned to a session returns nil, for the reason the package
// documentation gives.
func ResolvePolicyExpressions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	probes []PolicyExpressionProbe,
) (map[string]config.PolicyExpression, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve policy expressions: database connection is nil")
	}
	if len(probes) == 0 {
		return nil, nil
	}
	if !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}
	return resolveProbes(ctx, conn, "resolve policy expressions", probes,
		func(probe PolicyExpressionProbe) string { return probe.Key },
		resolveOnePolicyExpression)
}

func resolveOnePolicyExpression(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe PolicyExpressionProbe,
) (config.PolicyExpression, error) {
	using := strings.TrimSpace(probe.Using)
	withCheck := strings.TrimSpace(probe.WithCheck)
	if (using == "" && withCheck == "") || len(probe.Columns) == 0 {
		return config.PolicyExpression{}, nil
	}

	table := fmt.Sprintf("ptah_policy_probe_%d", index)
	statements := []string{
		fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", table, checkProbeColumnList(probe.Columns)),
		fmt.Sprintf("ALTER TABLE %s ENABLE ROW LEVEL SECURITY", table),
		fmt.Sprintf("CREATE POLICY %s_pol ON %s%s", table, table, policyClauses(using, withCheck)),
	}

	const query = `
		SELECT COALESCE(pg_get_expr(p.polqual, p.polrelid), ''),
		       COALESCE(pg_get_expr(p.polwithcheck, p.polrelid), '')
		FROM pg_policy p
		WHERE p.polrelid = $1::regclass`

	var answer config.PolicyExpression
	ok, err := runProbe(ctx, tx, "resolve policy expressions", probe.Key, "ptah_policy_probe", postgresSavepoints,
		statements, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, query, "pg_temp."+table).
				Scan(&answer.Using, &answer.WithCheck)
		})
	if err != nil || !ok {
		return config.PolicyExpression{}, err
	}
	answer.Resolved = true
	return answer, nil
}

// policyClauses renders the two optional clauses of a CREATE POLICY.
func policyClauses(using, withCheck string) string {
	var clauses strings.Builder
	if using != "" {
		fmt.Fprintf(&clauses, " USING (%s)", using)
	}
	if withCheck != "" {
		fmt.Fprintf(&clauses, " WITH CHECK (%s)", withCheck)
	}
	return clauses.String()
}

// ResolveIndexExpressions asks the connected server to normalize each declared
// index's expression and predicate.
//
// The third object with the same rewrite. Measured on 17.11, `lower(code)` over
// a varchar column is stored as `lower((code)::text)`, and a partial index's
// `unit >= 0` over numeric as `(unit >= (0)::numeric)`, so an index nobody had
// touched was dropped and rebuilt on every run (stokaro/ptah#2047). A
// connection pinned to a session returns nil, for the reason the package
// documentation gives.
func ResolveIndexExpressions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
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

	table := fmt.Sprintf("ptah_index_probe_%d", index)
	over := expression
	if over == "" {
		over = strings.Join(quoteProbeParts(probe.Parts), ", ")
	}
	if strings.TrimSpace(over) == "" {
		return config.IndexExpression{}, nil
	}
	create := fmt.Sprintf("CREATE INDEX %s_idx ON %s ((%s))", table, table, over)
	if expression == "" {
		create = fmt.Sprintf("CREATE INDEX %s_idx ON %s (%s)", table, table, over)
	}
	if predicate != "" {
		create += fmt.Sprintf(" WHERE (%s)", predicate)
	}
	statements := []string{
		fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s)", table, checkProbeColumnList(probe.Columns)),
		create,
	}

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
			return tx.QueryRowContext(ctx, query, "pg_temp."+table).
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
