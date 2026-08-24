package dbschema

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform"
)

// CheckProbeColumn is one column the probe table needs so a declared CHECK can
// be parsed against it.
type CheckProbeColumn struct {
	// Name is the column's name, quoted by the resolver.
	Name string
	// Type is the column's type as the catalog spells it, which is what makes
	// the rewrite faithful: `price >= 0` normalizes to `(0)::numeric` over
	// numeric and to `(0)` over integer.
	Type string
}

// CheckExpressionProbe is one declared CHECK whose expression needs the target
// server's own spelling before it can be compared with what the catalog holds.
type CheckExpressionProbe struct {
	// Key identifies the constraint to the caller. It is returned unchanged as
	// the map key and is never sent to the server.
	Key string
	// Columns are the columns of the table the constraint belongs to, taken
	// from the LIVE read rather than from the declaration: the expression has
	// to parse against the table it will be added to.
	Columns []CheckProbeColumn
	// Expression is the declared CHECK, without the CHECK keyword and without
	// its enclosing parentheses.
	Expression string
}

// ResolveCheckExpressions asks the connected server to normalize each declared
// table CHECK, so a comparison holds the same spelling on both sides.
//
// PostgreSQL stores a parsed CHECK, not its text, and prints it back from the
// parse tree. Every read-back therefore differs from the declaration that
// produced it: parentheses appear, literals gain casts, `IN` becomes
// `= ANY (ARRAY[...])` and `BETWEEN` becomes a pair of comparisons. A textual
// normalizer folds some of those and cannot fold the rest, so a schema Ptah had
// just applied planned `DROP CONSTRAINT` and `ADD CONSTRAINT` for a check
// nobody had changed -- on every run, and reported as destructive
// (stokaro/ptah#2044).
//
// The declaration is put through the same rewrite the catalog form already went
// through: a TEMPORARY table carrying the live table's columns and the declared
// constraint is created, its stored expression is read back, and the
// transaction is rolled back.
//
// The probe table is temporary and the target table is never touched. An
// `ALTER TABLE ... ADD CONSTRAINT ... NOT VALID` on the real table would parse
// the expression against the right columns with no rendering at all, and it
// would take an ACCESS EXCLUSIVE lock on a user's table to answer a question
// asked by `schema diff`. A comparison does not get to lock the thing it is
// comparing.
//
// A probe the server refuses -- an expression that does not parse, a column the
// live table does not have -- is returned with Resolved false rather than
// omitted, because an absent key and an unresolvable one are different facts
// and only one of them is a constraint the caller may compare.
//
// Dialects other than the PostgreSQL family return nil: this rewrite is
// PostgreSQL's, and the engines that store the text they were given need no
// normalization at all.
func (dc *DatabaseConnection) ResolveCheckExpressions(
	ctx context.Context,
	probes []CheckExpressionProbe,
) (result map[string]config.CheckExpression, resultErr error) {
	if dc == nil || dc.db == nil {
		return nil, fmt.Errorf("resolve check expressions: database connection is nil")
	}
	if len(probes) == 0 {
		return nil, nil
	}
	one, supported := checkProbeFor(dc.Info().Dialect)
	if !supported {
		return nil, nil
	}
	return resolveProbes(ctx, dc, "resolve check expressions", probes,
		func(probe CheckExpressionProbe) string { return probe.Key },
		one)
}

// checkProbeFor picks the probe an engine needs, or reports that this engine
// stores the text it was given and needs none.
//
// Two engines rewrite a CHECK before storing it, and each prints its rewrite
// its own way. Measured for `price >= 0` on a numeric column:
//
//	PostgreSQL 17.11      (price >= (0)::numeric)
//	SQL Server 2025       ([price]>=(0))
//
// MySQL 8.4.11 and MariaDB 11.4.12 converged on every CHECK shape in the sweep
// that found this, so they are deliberately absent rather than unmeasured.
func checkProbeFor(dialect string) (probeFunc[CheckExpressionProbe, config.CheckExpression], bool) {
	if isPostgresFamily(dialect) {
		return resolveOnePostgresCheckExpression, true
	}
	if platform.NormalizeDialect(dialect) == platform.SQLServer {
		return resolveOneSQLServerCheckExpression, true
	}
	return nil, false
}

// resolveOnePostgresCheckExpression creates one probe table and reads its
// stored expression back.
//
// Each probe runs inside its own savepoint, for the reason
// [resolveOneDomainExpression] gives: a declaration the server refuses aborts
// the transaction, and without the savepoint the first unparseable expression
// would take every later probe with it.
func resolveOnePostgresCheckExpression(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe CheckExpressionProbe,
) (config.CheckExpression, error) {
	expression := strings.TrimSpace(probe.Expression)
	if expression == "" || len(probe.Columns) == 0 {
		return config.CheckExpression{}, nil
	}

	name := fmt.Sprintf("ptah_check_probe_%d", index)
	statement := fmt.Sprintf("CREATE TEMPORARY TABLE %s (%s, CONSTRAINT %s_ck CHECK (%s))",
		name, checkProbeColumnList(probe.Columns), name, expression)

	const savepoint = "ptah_check_probe"
	if _, err := tx.ExecContext(ctx, "SAVEPOINT "+savepoint); err != nil {
		return config.CheckExpression{}, fmt.Errorf("resolve check expressions: savepoint: %w", err)
	}
	if _, err := tx.ExecContext(ctx, statement); err != nil {
		if _, rollbackErr := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); rollbackErr != nil {
			return config.CheckExpression{}, fmt.Errorf(
				"resolve check expressions: roll back to savepoint after %q: %w",
				strings.TrimSpace(probe.Key), rollbackErr)
		}
		// The declaration is the caller's, and refusing it here would fail a
		// comparison over a constraint the server will refuse later anyway,
		// with a worse message. Unresolved is the honest answer.
		return config.CheckExpression{}, nil
	}

	// pg_get_expr is what the reader asks of a live constraint, so both sides
	// of the comparison are printed by the same function.
	const query = `
		SELECT COALESCE(pg_get_expr(c.conbin, c.conrelid), '')
		FROM pg_constraint c
		WHERE c.conrelid = $1::regclass AND c.contype = 'c'`

	var stored string
	if err := tx.QueryRowContext(ctx, query, "pg_temp."+name).Scan(&stored); err != nil {
		return config.CheckExpression{}, fmt.Errorf(
			"resolve check expressions: read back %q: %w", strings.TrimSpace(probe.Key), err)
	}

	if _, err := tx.ExecContext(ctx, "ROLLBACK TO SAVEPOINT "+savepoint); err != nil {
		return config.CheckExpression{}, fmt.Errorf("resolve check expressions: release probe: %w", err)
	}
	return config.CheckExpression{Expression: strings.TrimSpace(stored), Resolved: true}, nil
}

// checkProbeColumnList renders the probe table's columns.
//
// The names are quoted and the types are not: a type is a type expression --
// `numeric(10,2)`, `character varying(80)`, `text[]` -- and quoting one would
// make it an identifier the server has never heard of.
func checkProbeColumnList(columns []CheckProbeColumn) string {
	rendered := make([]string, 0, len(columns))
	for _, column := range columns {
		rendered = append(rendered, fmt.Sprintf(`%s %s`, quoteCheckProbeIdentifier(column.Name), column.Type))
	}
	return strings.Join(rendered, ", ")
}

// quoteCheckProbeIdentifier quotes a column name for the probe table, doubling
// any quote inside it.
func quoteCheckProbeIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// resolveOneSQLServerCheckExpression is [resolveOnePostgresCheckExpression] for
// the other engine that rewrites a CHECK.
//
// SQL Server prints its own rewrite: measured on 2025 (RTM-CU8), a declared
// `price >= 0` on a decimal column is stored as `([price]>=(0))` -- brackets
// around the identifier, parentheses around the literal, and the spaces gone.
// A comparison of the two texts planned a DROP and an ADD on every run, at
// severity destructive (stokaro/ptah#2054).
//
// The probe differs from the PostgreSQL one in two mechanical ways and in
// nothing else: a temporary table is `#name` rather than `pg_temp.name`, and
// its constraints live in `tempdb.sys.check_constraints`, keyed by the object
// id of `tempdb..#name`.
func resolveOneSQLServerCheckExpression(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe CheckExpressionProbe,
) (config.CheckExpression, error) {
	expression := strings.TrimSpace(probe.Expression)
	if expression == "" || len(probe.Columns) == 0 {
		return config.CheckExpression{}, nil
	}

	name := fmt.Sprintf("#ptah_check_probe_%d", index)
	statements := []string{
		fmt.Sprintf("CREATE TABLE %s (%s, CONSTRAINT ck_ptah_check_probe_%d CHECK (%s))",
			name, sqlServerProbeColumnList(probe.Columns), index, expression),
	}

	const query = `
		SELECT COALESCE(MAX(c.definition), '')
		FROM tempdb.sys.check_constraints c
		WHERE c.parent_object_id = OBJECT_ID(@p1)`

	var stored string
	ok, err := runProbe(ctx, tx, "resolve check expressions", probe.Key, "ptah_check_probe", sqlServerSavepoints,
		statements, func(ctx context.Context, tx *sql.Tx) error {
			return tx.QueryRowContext(ctx, query, "tempdb.."+name).Scan(&stored)
		})
	if err != nil || !ok {
		return config.CheckExpression{}, err
	}
	return config.CheckExpression{Expression: strings.TrimSpace(stored), Resolved: true}, nil
}

// sqlServerProbeColumnList renders the probe table's columns with SQL Server's
// own quoting.
func sqlServerProbeColumnList(columns []CheckProbeColumn) string {
	rendered := make([]string, 0, len(columns))
	for _, column := range columns {
		rendered = append(rendered, fmt.Sprintf("%s %s",
			quoteSQLServerProbeIdentifier(column.Name), column.Type))
	}
	return strings.Join(rendered, ", ")
}

// quoteSQLServerProbeIdentifier brackets a column name, doubling any bracket
// inside it.
func quoteSQLServerProbeIdentifier(name string) string {
	return "[" + strings.ReplaceAll(name, "]", "]]") + "]"
}
