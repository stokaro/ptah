package capabilityprobe

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
)

// Attempt is one statement and the server's answer to it.
//
// Both halves are kept. A row that says only "the server refused" cannot be
// argued with; a row that quotes the statement and the server's own error text
// can be re-executed by hand from the report.
type Attempt struct {
	Statement string
	Accepted  bool
	ServerErr string
}

// String renders the attempt as one line of evidence.
func (a Attempt) String() string {
	if a.Accepted {
		return "ACCEPTED  " + collapse(a.Statement)
	}
	return "REFUSED   " + collapse(a.Statement) + "  -> " + collapse(a.ServerErr)
}

func collapse(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

// session is a pinned physical connection inside a throwaway namespace.
//
// It is pinned because the namespace is session state: a pooled connection
// would run half the statements in the probe schema and half in the caller's.
// It is a namespace rather than a transaction because PostgreSQL refuses
// CREATE INDEX CONCURRENTLY inside an explicit transaction block, so
// BEGIN/ROLLBACK isolation would report two true capabilities as false.
type session struct {
	conn      *dbschema.DatabaseConnection
	dialect   string
	namespace string
	// roles are cluster-scoped objects the probe created; dropping the
	// namespace does not remove them.
	roles []string
	// broken records the transport failure that ended the run, if any.
	broken error
	// inExplicitTransaction suspends the liveness check.
	//
	// One decider deliberately provokes a refusal inside an explicit
	// transaction block, and PostgreSQL then answers every statement —
	// including SELECT 1 — with "current transaction is aborted". Reading that
	// as a dead session would abort the run at exactly the statement the run
	// exists to make. The check resumes after the ROLLBACK, where a truly dead
	// session still fails.
	inExplicitTransaction bool
}

// exec runs one statement and reports what the server did.
//
// A refusal is an observation, not an error: the whole point of the probe is
// that some statements must be refused. A DROPPED CONNECTION is a different
// thing entirely, and reading it as a refusal would report every remaining
// capability as absent. So after any failure the session is asked whether it
// is still alive, and a dead session poisons the run instead of answering it.
func (s *session) exec(ctx context.Context, statement string) Attempt {
	if s.broken != nil {
		return Attempt{Statement: statement, ServerErr: "session already broken: " + s.broken.Error()}
	}
	_, err := s.conn.ExecContext(ctx, statement)
	if err == nil {
		return Attempt{Statement: statement, Accepted: true}
	}
	if s.inExplicitTransaction {
		return Attempt{Statement: statement, ServerErr: err.Error()}
	}
	if alive := s.alive(ctx); alive != nil {
		s.broken = fmt.Errorf("session died executing %q: %w (statement error: %w)", statement, alive, err)
		return Attempt{Statement: statement, ServerErr: s.broken.Error()}
	}
	return Attempt{Statement: statement, ServerErr: err.Error()}
}

// query runs a single-value query and reports the value alongside the attempt.
func (s *session) query(ctx context.Context, statement string) (int64, Attempt) {
	if s.broken != nil {
		return 0, Attempt{Statement: statement, ServerErr: "session already broken: " + s.broken.Error()}
	}
	var value int64
	err := s.conn.QueryRowContext(ctx, statement).Scan(&value)
	if err == nil {
		return value, Attempt{Statement: statement, Accepted: true}
	}
	if alive := s.alive(ctx); alive != nil {
		s.broken = fmt.Errorf("session died executing %q: %w (statement error: %w)", statement, alive, err)
		return 0, Attempt{Statement: statement, ServerErr: s.broken.Error()}
	}
	return 0, Attempt{Statement: statement, ServerErr: err.Error()}
}

// alive returns nil while the session can still answer.
func (s *session) alive(ctx context.Context) error {
	var one int
	if err := s.conn.QueryRowContext(ctx, "SELECT 1").Scan(&one); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("session answered SELECT 1 with %d", one)
	}
	return nil
}

// runAll executes statements in order and stops at the first refusal.
func (s *session) runAll(ctx context.Context, statements []string) ([]Attempt, bool) {
	attempts := make([]Attempt, 0, len(statements))
	for _, statement := range statements {
		attempt := s.exec(ctx, statement)
		attempts = append(attempts, attempt)
		if !attempt.Accepted {
			return attempts, false
		}
	}
	return attempts, true
}

// tryInTransaction runs one statement inside an explicit transaction block and
// always rolls back.
//
// The block is the measurement, not the isolation: a real concurrent index
// build cannot live inside a transaction and a parser that merely swallows the
// CONCURRENTLY keyword has no reason to refuse it, so the refusal is the
// evidence. Liveness checking is suspended for the duration because an aborted
// PostgreSQL transaction answers SELECT 1 with an error too, and resumes on the
// far side of the ROLLBACK.
func (s *session) tryInTransaction(ctx context.Context, statement string) ([]Attempt, bool) {
	begin := s.exec(ctx, "BEGIN")
	if !begin.Accepted {
		return []Attempt{begin}, false
	}
	s.inExplicitTransaction = true
	inside := s.exec(ctx, statement)
	s.inExplicitTransaction = false
	rollback := s.exec(ctx, "ROLLBACK")
	return []Attempt{begin, inside, rollback}, true
}

// nonsenseControl is the statement the server MUST refuse.
//
// Every "the server accepted it" row is only worth reading if the server is
// capable of saying no on this session. A connection that answers OK to
// everything — a proxy that swallows DDL, a dry-run wrapper, a driver that
// defers errors — would otherwise fill the matrix with agreements nobody
// earned. The capability presets already use this exact control in their own
// doc comments; here it gates the whole run.
const nonsenseControl = "CREATE NONSENSE ptah_capability_probe_control"

// namespaceSQL returns the statements that create and enter the throwaway
// namespace, and the statement that removes it.
func namespaceSQL(dialect, namespace string) (enter []string, leave string) {
	if platform.IsPostgresFamily(dialect) {
		return []string{
				"CREATE SCHEMA " + namespace,
				"SET search_path TO " + namespace,
			},
			"DROP SCHEMA " + namespace + " CASCADE"
	}
	return []string{
			"CREATE DATABASE " + namespace,
			"USE " + namespace,
		},
		"DROP DATABASE " + namespace
}

// newNamespace returns a fresh identifier no other run will collide with. The
// machine this is developed on routinely has forty containers and several
// agents pointed at the same server, and a fixed name turns another run's
// leftovers into this run's capability findings.
func newNamespace() (string, error) {
	raw := make([]byte, 8)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate probe namespace: %w", err)
	}
	return "ptah_capprobe_" + hex.EncodeToString(raw), nil
}

// dropRoles removes the cluster-scoped roles the probe created. Roles outlive
// the schema, so a run that forgot them would leave the server dirtier every
// time it ran.
func (s *session) dropRoles(ctx context.Context) []Attempt {
	attempts := make([]Attempt, 0, 2*len(s.roles))
	for _, role := range s.roles {
		attempts = append(attempts,
			s.exec(ctx, "DROP OWNED BY "+role),
			s.exec(ctx, "DROP ROLE IF EXISTS "+role),
		)
	}
	return attempts
}
