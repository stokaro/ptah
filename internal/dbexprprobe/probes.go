package dbexprprobe

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
)

// resolveProbes runs every probe through one rolled-back transaction and
// collects the answers, keyed the way the caller keys them.
//
// It is a package function rather than a method because a method cannot carry
// type parameters, and the two types that vary between resolvers are exactly
// the probe and the answer. What does not vary -- the session, the transaction,
// the rollback, and returning nil for a pinned session with a transaction open
// -- lives in [dbschema.DatabaseConnection.WithRolledBackTransaction] beneath
// it.
func resolveProbes[Probe any, Answer any](
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	label string,
	probes []Probe,
	key func(Probe) string,
	one func(ctx context.Context, tx *sql.Tx, index int, probe Probe) (Answer, error),
) (map[string]Answer, error) {
	resolved := make(map[string]Answer, len(probes))
	ran, err := conn.WithRolledBackTransaction(ctx, label, func(ctx context.Context, tx *sql.Tx) error {
		for i, probe := range probes {
			answer, err := one(ctx, tx, i, probe)
			if err != nil {
				return err
			}
			resolved[key(probe)] = answer
		}
		return nil
	})
	if err != nil || !ran {
		return nil, err
	}
	return resolved, nil
}

// probeRelation is the temporary table one probe creates: how the probe's
// statements name it, the text its ::regclass lookup takes, and the statements
// that run before it is created.
type probeRelation struct {
	name     string
	regclass string
	setup    []string
}

// statements returns the relation's setup followed by the probe's own
// statements, in a slice of its own.
func (r probeRelation) statements(probe ...string) []string {
	return slices.Concat(r.setup, probe)
}

// searchPathWithTempLast puts pg_temp at the end of the transaction's search
// path. set_config's third argument makes it local to the transaction, and the
// savepoint each probe rolls back to undoes it with the rest of the probe.
const searchPathWithTempLast = `SELECT set_config('search_path', CASE
	WHEN current_setting('search_path') = '' THEN 'pg_temp'
	ELSE current_setting('search_path') || ', pg_temp' END, true)`

// newProbeRelation names the probe table after the table the declaration is
// on, inside pg_temp, when that table is known.
//
// A declaration may name its own table: `CHECK (clients.n > 0)`, an index
// predicate `WHERE clients.parent_id IS NOT NULL`, a policy whose subquery
// compares with `clients.id`. The server resolves that name against the table
// the object is on and stores it unqualified. Under a numbered probe name
// PostgreSQL 18.6 refuses the same declaration with `missing FROM-clause entry
// for table "clients"`, and the comparison falls back to text, which plans the
// object again on every run (stokaro/ptah#3654).
//
// Named after the real table, the probe table would shadow it: pg_temp is
// searched first, so a subquery reading `public.clients` prints it qualified in
// the probe and unqualified in the catalog. pg_temp goes last in the search path
// for the probe, so every name but the object's own qualifier resolves as it
// does for the real object. Measured on 18.6, the policy, CHECK and predicate
// above print identically on the probe table and on the real one.
//
// Without a table name the probe keeps a numbered one.
func newProbeRelation(table, prefix string, index int) probeRelation {
	if strings.TrimSpace(table) == "" {
		name := fmt.Sprintf("%s_%d", prefix, index)
		return probeRelation{name: name, regclass: "pg_temp." + name}
	}
	name := "pg_temp." + quoteCheckProbeIdentifier(table)
	return probeRelation{name: name, regclass: name, setup: []string{searchPathWithTempLast}}
}

// runProbe creates one probe's objects inside a savepoint, reads the answer
// back, and releases the savepoint.
//
// The savepoint is what keeps one refused declaration from taking the rest with
// it: a statement the server rejects aborts the transaction, and without it the
// first unparseable expression would report a whole schema as uncomparable.
//
// It answers false, with no error, for a declaration the server refused. That
// is the honest result: refusing here would fail a comparison over an object
// the server will refuse later anyway, with a worse message.
func runProbe(
	ctx context.Context,
	tx *sql.Tx,
	label, key, savepoint string,
	marks savepointSyntax,
	statements []string,
	read func(ctx context.Context, tx *sql.Tx) error,
) (bool, error) {
	if _, err := tx.ExecContext(ctx, marks.save(savepoint)); err != nil {
		return false, fmt.Errorf("%s: savepoint: %w", label, err)
	}
	for _, statement := range statements {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			if _, rollbackErr := tx.ExecContext(ctx, marks.rollback(savepoint)); rollbackErr != nil {
				return false, fmt.Errorf("%s: roll back to savepoint after %q: %w", label, key, rollbackErr)
			}
			return false, nil
		}
	}
	if err := read(ctx, tx); err != nil {
		return false, fmt.Errorf("%s: read back %q: %w", label, key, err)
	}
	if _, err := tx.ExecContext(ctx, marks.rollback(savepoint)); err != nil {
		return false, fmt.Errorf("%s: release probe: %w", label, err)
	}
	return true, nil
}

// probeFunc is one engine's way of putting a declaration through the server and
// reading back what it stored. It is a named type so a resolver can pick one by
// dialect without spelling the signature out twice.
type probeFunc[Probe any, Answer any] func(
	ctx context.Context,
	tx *sql.Tx,
	index int,
	probe Probe,
) (Answer, error)

// savepointSyntax is how one engine spells a savepoint and the rollback to it.
//
// The two engines that need a probe spell both differently, and SQL Server
// answers `Could not find stored procedure 'SAVEPOINT'` for the other's
// spelling -- which is a runtime error inside a comparison, not a compile-time
// one, so the difference belongs in a value rather than in a comment.
type savepointSyntax struct {
	save     func(name string) string
	rollback func(name string) string
}

// postgresSavepoints is the SQL-standard spelling, which the PostgreSQL family
// takes.
var postgresSavepoints = savepointSyntax{
	save:     func(name string) string { return "SAVEPOINT " + name },
	rollback: func(name string) string { return "ROLLBACK TO SAVEPOINT " + name },
}

// sqlServerSavepoints is T-SQL's, where a savepoint is a named transaction mark
// and the rollback names it directly.
var sqlServerSavepoints = savepointSyntax{
	save:     func(name string) string { return "SAVE TRANSACTION " + name },
	rollback: func(name string) string { return "ROLLBACK TRANSACTION " + name },
}

// isPostgresFamily reports whether the dialect renders CREATE DOMAIN at all.
func isPostgresFamily(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return true
	default:
		return false
	}
}
