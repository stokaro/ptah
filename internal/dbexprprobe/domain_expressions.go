package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/dbschema"
)

// DomainExpressionProbe is one declared domain whose CHECK and DEFAULT need the
// target server's own spelling before they can be compared with what the
// catalog holds.
type DomainExpressionProbe struct {
	// Key identifies the domain to the caller. It is returned unchanged as the
	// map key and is never sent to the server.
	Key string
	// BaseType is the domain's underlying type. The server needs it to parse
	// the expressions: `VALUE <> ''` normalizes to `VALUE <> ''::text` over
	// text and does not parse at all over integer.
	BaseType string
	// Check is the declared CHECK expression, without the CHECK keyword and
	// without its enclosing parentheses.
	Check string
	// Default is the declared DEFAULT expression or literal.
	Default string
}

// ResolveDomainExpressions asks the connected server to normalize each declared
// domain's CHECK and DEFAULT, so a comparison holds the same spelling on both
// sides.
//
// PostgreSQL stores a parsed CHECK, not its text, and prints it back from the
// parse tree. Every read-back therefore differs from the declaration that
// produced it: parentheses appear, literals gain casts, `IN` becomes
// `= ANY (ARRAY[...])` and `BETWEEN` becomes a pair of comparisons. Ptah
// declined to compare CHECK and DEFAULT for exactly that reason, which made a
// changed constraint a silent no-op: `schema apply` reported a synced schema
// over a database still enforcing the old rule (stokaro/ptah#1717).
//
// The declaration is put through the same rewrite the catalog form already
// went through: a domain over the declared base type is created in the session's
// temporary schema, its stored expressions are read back, and the transaction
// is rolled back. Nothing is created, nothing is dropped, and the physical
// session is discarded afterwards, so no temporary object outlives the call.
//
// A probe the server refuses -- an expression that does not parse, a base type
// this connection cannot see -- is returned with Resolved false rather than
// omitted, because an absent key and an unresolvable one are different facts
// and only one of them is a domain the caller may compare.
//
// Dialects other than the PostgreSQL family return nil: no other engine Ptah
// targets has CREATE DOMAIN. A connection pinned to a session also returns
// nil, for the reason the package documentation gives: the rollback the probe
// needs would discard the session owner's work, so nothing is asked and the
// domain stays uncompared.
func ResolveDomainExpressions(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	probes []DomainExpressionProbe,
) (map[string]config.DomainExpression, error) {
	if conn == nil {
		return nil, fmt.Errorf("resolve domain expressions: database connection is nil")
	}
	if len(probes) == 0 {
		return nil, nil
	}
	if !isPostgresFamily(conn.Info().Dialect) {
		return nil, nil
	}
	return resolveProbes(ctx, conn, "resolve domain expressions", probes,
		func(probe DomainExpressionProbe) string { return probe.Key },
		resolveOneDomainExpression)
}

// resolveOneDomainExpression creates one probe domain and reads its stored
// expressions back.
//
// Each probe runs inside its own savepoint. A declaration the server refuses
// aborts the transaction, and without the savepoint the first unparseable
// expression would take every later probe with it -- reporting a whole schema
// as uncomparable because one domain was.
func resolveOneDomainExpression(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe DomainExpressionProbe,
) (config.DomainExpression, error) {
	if probe.BaseType == "" {
		return config.DomainExpression{}, nil
	}
	if probe.Check == "" && probe.Default == "" {
		return config.DomainExpression{Resolved: true}, nil
	}

	name := fmt.Sprintf("pg_temp.ptah_domain_probe_%d", index)
	statement := fmt.Sprintf("CREATE DOMAIN %s AS %s", name, probe.BaseType)
	if probe.Default != "" {
		statement += " DEFAULT " + probe.Default
	}
	if probe.Check != "" {
		statement += " CHECK (" + probe.Check + ")"
	}

	const savepoint = "ptah_domain_probe"
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return config.DomainExpression{}, fmt.Errorf("resolve domain expressions: savepoint: %w", err)
	}
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			return config.DomainExpression{}, fmt.Errorf(
				"resolve domain expressions: roll back to savepoint after %q: %w",
				strings.TrimSpace(probe.Key),
				rollbackErr,
			)
		}
		// The declaration is the caller's, and refusing it here would fail a
		// comparison over a domain the server will refuse later anyway, with a
		// worse message. Unresolved is the honest answer.
		return config.DomainExpression{}, nil
	}

	const query = `
		SELECT
			COALESCE(t.typdefault, ''),
			COALESCE((
				SELECT string_agg(pg_get_expr(c.conbin, c.conrelid), ' AND ')
				FROM pg_constraint c
				WHERE c.contypid = t.oid AND c.contype = 'c'
			), '')
		FROM pg_type t
		WHERE t.oid = $1::regtype`

	var expression config.DomainExpression
	if err := tx.QueryRowContext(ctx, query, name).Scan(&expression.Default, &expression.Check); err != nil {
		return config.DomainExpression{}, fmt.Errorf(
			"resolve domain expressions: read back %q: %w",
			strings.TrimSpace(probe.Key),
			err,
		)
	}
	expression.Resolved = true

	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
		return config.DomainExpression{}, fmt.Errorf("resolve domain expressions: release probe: %w", err)
	}
	return expression, nil
}
