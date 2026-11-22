package dbexprprobe

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/internal/ownqualifier"
	"ptah.run/internal/probetext"
	"ptah.run/internal/probetx"
)

// resolveProbes runs every probe through one rolled-back transaction and
// collects the answers, keyed the way the caller keys them.
//
// It is a package function rather than a method because a method cannot carry
// type parameters, and the two types that vary between resolvers are exactly
// the probe and the answer. What does not vary -- the session, the transaction,
// the rollback, and returning nil for a pinned session with a transaction open
// -- lives in [Session.WithRolledBackTransaction] beneath
// it.
//
// one checks the text it writes into its statements with
// [ptah.run/internal/probetext] before it runs any of them, and answers the
// check's error for text that could reach past its place, or an error
// wrapping errNotPermitted where its temporary table was refused for lack of
// privilege ([warnedRefusal]). That probe gets the zero answer, as a
// declaration the server refused does, and a warning naming it; the probes
// after it run. After each probe the transaction is named again
// ([ptah.run/internal/probetx]), and one that is not the transaction the probes
// began in stops them all with an error.
func resolveProbes[Probe any, Answer any](
	ctx context.Context,
	conn schemaext.ProbeSession,
	dialect, label string,
	probes []Probe,
	describe func(Probe) (key, object string),
	one probeFunc[Probe, Answer],
) (map[string]Answer, error) {
	resolved := make(map[string]Answer, len(probes))
	ran, err := conn.WithRolledBackTransaction(ctx, label, func(ctx context.Context, tx *sql.Tx) error {
		for i, probe := range probes {
			key, object := describe(probe)
			answer, err := one(ctx, tx, dialect, i, probe)
			warned := errors.Is(err, probetext.ErrUncontained) || errors.Is(err, errNotPermitted)
			if err != nil && !warned {
				return err
			}
			// A probe refused for its text may have run a statement first, as
			// a routine's fallback does after its main statement is refused,
			// so the transaction is named again on this path too.
			if err := probetx.Check(ctx); err != nil {
				return fmt.Errorf("%s: %s: %w", label, object, err)
			}
			if warned {
				probetext.Warn(ctx, key, object, err)
				var unresolved Answer
				resolved[key] = unresolved
				continue
			}
			resolved[key] = answer
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

// ownSchemaStripped is expression as the probe table can read it. The probe
// table sits in pg_temp, so a column written as `public.clients.n` names the
// real table, and PostgreSQL refuses it there as an invalid reference to a
// FROM-clause entry; the probe would answer nothing, and the comparison would
// plan the object again on every run. The server stores that reference as `n`,
// as it stores `clients.n`, so the probe sends `clients.n`. A probe whose table
// or schema is unknown sends the expression as written.
func ownSchemaStripped(expression, schema, table string) string {
	if schema == "" || table == "" {
		return expression
	}
	return ownqualifier.Strip(expression, ownqualifier.StoredName(schema), ownqualifier.StoredName(table))
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
	refusal, err := runRefusableProbe(ctx, tx, label, key, savepoint, marks, statements, read)
	if err == nil && errors.Is(refusal, probetext.ErrUncontained) {
		// The server read more than one statement in the declared text, and
		// the caller's warning names the declaration.
		return false, refusal
	}
	return err == nil && refusal == nil, err
}

// runRefusableProbe is [runProbe] for a caller that reports a refusal. refusal
// is the server's answer to the statement it refused, and nil when the probe
// ran; err is a failure of the transaction itself.
func runRefusableProbe(
	ctx context.Context,
	tx *sql.Tx,
	label, key, savepoint string,
	marks savepointSyntax,
	statements []string,
	read func(ctx context.Context, tx *sql.Tx) error,
) (refusal, err error) {
	if _, err := tx.ExecContext(ctx, marks.save(savepoint)); err != nil {
		return nil, fmt.Errorf("%s: savepoint: %w", label, err)
	}
	for _, statement := range statements {
		if refused := marks.exec(ctx, tx, statement); refused != nil {
			if _, rollbackErr := tx.ExecContext(ctx, marks.rollback(savepoint)); rollbackErr != nil {
				return nil, fmt.Errorf("%s: roll back to savepoint after %q: %w", label, key, rollbackErr)
			}
			return refused, nil
		}
	}
	if err := read(ctx, tx); err != nil {
		return nil, fmt.Errorf("%s: read back %q: %w", label, key, err)
	}
	if _, err := tx.ExecContext(ctx, marks.rollback(savepoint)); err != nil {
		return nil, fmt.Errorf("%s: release probe: %w", label, err)
	}
	return nil, nil
}

// uncontainedOnly is refused when the server refused a probe statement because
// it read more than one statement in the declared text, which the caller
// reports as a warning, and nil for any other refusal, which leaves the
// declaration unresolved without one.
func uncontainedOnly(refused error) error {
	if errors.Is(refused, probetext.ErrUncontained) {
		return refused
	}
	return nil
}

// insufficientPrivilege is the SQLSTATE a probe statement is refused with
// when the role lacks a privilege it needs. A role without TEMPORARY on the
// database is the common case: PostgreSQL 19 answers `permission denied to
// create temporary tables in database`, and CockroachDB v26.4.1 `does not
// have TEMPORARY privilege on database`. It is not the only one: PostgreSQL 19
// answers `permission denied for schema private` for a CHECK that calls
// `private.positive(amount)` from a schema the role may not use.
const insufficientPrivilege = "42501"

// errNotPermitted is what [warnedRefusal] wraps a refusal for lack of
// privilege in. It reads as the reason in the warning [probetext.Warn] raises,
// before the server's answer, which names the privilege.
var errNotPermitted = errors.New("the role lacks a privilege the probe statement needs")

// notPermitted is a probe statement's refusal for lack of privilege: it wraps
// errNotPermitted and the server's answer.
type notPermitted struct{ server error }

func (e notPermitted) Error() string   { return errNotPermitted.Error() + ": " + e.server.Error() }
func (e notPermitted) Unwrap() []error { return []error{errNotPermitted, e.server} }

// warnedRefusal is [uncontainedOnly] for a probe that also warns when the
// server refused one of its statements for lack of privilege. Without the
// warning such a declaration is compared as written on every run with nothing
// to say why, and a CHECK the server spells otherwise is planned again each
// time. Any other refusal is nil, which leaves the declaration unresolved
// without a warning.
func warnedRefusal(refused error) error {
	if errors.Is(refused, probetext.ErrUncontained) {
		return refused
	}
	var state interface{ SQLState() string }
	if errors.As(refused, &state) && state.SQLState() == insufficientPrivilege {
		return notPermitted{server: refused}
	}
	return nil
}

// probeFunc is one engine's way of putting a declaration through the server and
// reading back what it stored. It is a named type so a resolver can pick one by
// dialect without spelling the signature out twice.
//
// dialect is the session's, which the probe checks the text it writes into
// its statements under; see [ptah.run/internal/probetext].
type probeFunc[Probe any, Answer any] func(
	ctx context.Context,
	tx *sql.Tx,
	dialect string,
	index int,
	probe Probe,
) (Answer, error)

// savepointSyntax is how one engine spells a savepoint and the rollback to it,
// and how it runs a probe statement.
//
// The two engines that need a probe spell both differently, and SQL Server
// answers `Could not find stored procedure 'SAVEPOINT'` for the other's
// spelling -- which is a runtime error inside a comparison, not a compile-time
// one, so the difference belongs in a value rather than in a comment.
type savepointSyntax struct {
	save     func(name string) string
	rollback func(name string) string
	// exec runs one probe statement, which may carry declared text.
	exec func(ctx context.Context, tx *sql.Tx, statement string) error
}

// postgresSavepoints is the SQL-standard spelling, which the PostgreSQL family
// takes. A statement runs through [probetext.Exec], so the server reads it as
// one command.
var postgresSavepoints = savepointSyntax{
	save:     func(name string) string { return "SAVEPOINT " + name },
	rollback: func(name string) string { return "ROLLBACK TO SAVEPOINT " + name },
	exec:     probetext.Exec,
}

// sqlServerSavepoints is T-SQL's, where a savepoint is a named transaction mark
// and the rollback names it directly. A batch may hold several statements
// whichever way the driver sends it, so a statement runs as it is, and the
// text check and the transaction check are what keep declared text in place.
var sqlServerSavepoints = savepointSyntax{
	save:     func(name string) string { return "SAVE TRANSACTION " + name },
	rollback: func(name string) string { return "ROLLBACK TRANSACTION " + name },
	exec: func(ctx context.Context, tx *sql.Tx, statement string) error {
		_, err := tx.ExecContext(ctx, statement)
		return err
	},
}

// isPostgresFamily reports whether the server answers the probes through
// PostgreSQL's catalog functions, as its descriptor states: the server
// rewrites the expressions these probes ask about.
func isPostgresFamily(dialect string) bool {
	return expressionProbes(dialect) == "pg-catalog"
}

// expressionProbes is the catalog dialect's descriptor says its server is
// asked how it stores a declared expression.
func expressionProbes(dialect string) string {
	described, _ := platform.Lookup(dialect)
	return described.Comparison.ExpressionProbes
}
