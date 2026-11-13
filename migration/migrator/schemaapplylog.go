package migrator

import (
	"context"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"time"

	"ptah.run/core/sqlutil"
	"ptah.run/dbschema"
	"ptah.run/internal/revisiontable"
	"ptah.run/internal/servercode"
)

// The schema apply log is to `ptah schema apply` what the operation log is to
// `ptah migrations up`: a record in the target database of each apply that
// reached it, written beside the work and never read to decide anything. It is
// a log and not an audit trail, for the reason the operation log is not one:
// whoever can apply a schema can edit its record of having done so.
//
// An apply writes two entries under one run_id: started, just before the first
// statement leaves, and the outcome once the apply returned. An apply that
// stopped before it sent a statement -- refused, failed before dispatch,
// canceled at the prompt, a dry run -- reached nothing and writes nothing. A
// started entry with no outcome is the shape of an apply that stopped
// mid-flight.

// SchemaApplyLogState is what one entry of the schema apply log records.
type SchemaApplyLogState string

const (
	// SchemaApplyLogStarted is written just before the first statement of the
	// plan leaves for the database.
	SchemaApplyLogStarted SchemaApplyLogState = "started"
	// SchemaApplyLogApplied is an apply that ran every statement.
	SchemaApplyLogApplied SchemaApplyLogState = "applied"
	// SchemaApplyLogFailed is an apply that stopped after a transaction
	// confirmed its rollback.
	SchemaApplyLogFailed SchemaApplyLogState = "failed"
	// SchemaApplyLogUnknown is an apply that stopped where nothing confirms
	// what the database kept.
	SchemaApplyLogUnknown SchemaApplyLogState = "unknown"
)

// SchemaApplyLogOptions are what an apply records about itself.
type SchemaApplyLogOptions struct {
	// Actor is the name the entries record, as [Migrator.WithActor] takes it:
	// empty records the user the process runs as, under another
	// [ActorSource].
	Actor string
	// ActorOrigin is the caller's word for where a provided Actor came from,
	// as [Migrator.WithActorOrigin] takes it.
	ActorOrigin string
	// RunReference is the caller's identifier for the apply, stored on both
	// entries, as [Migrator.WithRunReference] takes it.
	RunReference string
	// Logger is where a failed write is reported for a person. Nil uses
	// slog.Default().
	Logger *slog.Logger
}

// SchemaApplyLogPlan identifies the plan an apply runs.
type SchemaApplyLogPlan struct {
	// Name and Digest are the plan file's name and the digest of its bytes,
	// both empty for a plan computed from a desired schema.
	Name   string
	Digest string
	// Statements is how many units the plan sends.
	Statements int
}

// SchemaApplyLogOutcome is how an apply that started ended.
type SchemaApplyLogOutcome struct {
	// State is applied, failed or unknown.
	State SchemaApplyLogState
	// Error is the failure written for a person, empty for applied.
	Error string
	// Position is the 1-based position of the unit the apply stopped at, and
	// zero when no unit is to blame, as after a failed commit.
	Position int
	// Statement is the text of that unit, where it has one.
	Statement string
}

// SchemaApplyLogReport is what one apply's writes to the log came to.
type SchemaApplyLogReport struct {
	// Written counts the entries the database accepted.
	Written int `json:"written"`
	// Lost names the entries the database did not accept, in the order they
	// were tried.
	Lost []SchemaApplyLogLoss `json:"lost,omitempty"`
	// RunReference is the caller's reference the written entries carry. It is
	// empty when the apply was given none or no entry was written.
	RunReference string `json:"run_reference,omitempty"`
}

// SchemaApplyLogLoss is one entry the log should hold and does not. It says
// nothing about the apply, whose outcome the rest of the report gives.
type SchemaApplyLogLoss struct {
	// State is what the entry would have recorded.
	State SchemaApplyLogState `json:"state"`
	// SQLState and ErrorNumber are the server's codes, as in
	// [MigrationLogLoss].
	SQLState    string `json:"sqlstate,omitempty"`
	ErrorNumber int    `json:"error_number,omitempty"`
	// Message is the failure written for a person. It can quote what the
	// database said, including part of the value it refused.
	Message string `json:"message"`
}

// SchemaApplyLog writes one apply's entries to the schema apply log, the
// table [revisiontable.ApplyLog] names, in the schema an unqualified name
// resolves to on the connection (dbo on SQL Server).
//
// A failed write never fails the apply. It is reported by the logger and in
// [SchemaApplyLog.Report], and the value the database refused is not shortened
// to fit: a record that names somebody else is worse than none. A nil
// SchemaApplyLog writes nothing and reports nothing. Its methods are safe for
// concurrent use.
type SchemaApplyLog struct {
	// migrator names the table, checks who owns it, and writes on the
	// connection the log was opened with.
	migrator     *Migrator
	logger       *slog.Logger
	runID        string
	actor        string
	actorSource  ActorSource
	actorOrigin  string
	runReference string

	mu      sync.Mutex
	seq     int64
	plan    SchemaApplyLogPlan
	started bool
	ready   bool
	report  SchemaApplyLogReport
}

// NewSchemaApplyLog returns the log of one apply over conn, which is where the
// outcome entry is written. It returns nil for a connection whose engine keeps
// no such log: a target served by an executable provider, whose protocol has
// no store for one. Nothing is written until [SchemaApplyLog.Start].
//
// The options are recorded as given. [ValidateRunReference] and
// [ValidateActorOrigin] say which values the log stores unchanged; a value
// the database refuses loses the entry.
func NewSchemaApplyLog(conn *dbschema.DatabaseConnection, options SchemaApplyLogOptions) *SchemaApplyLog {
	if conn == nil {
		return nil
	}
	// A provider-backed connection is the one with a migration log store of
	// its own, or with a refusal to name one. Either way it has no SQL path
	// for this table to take.
	if store, err := conn.MigrationLogStore(); store != nil || err != nil {
		return nil
	}
	logger := options.Logger
	if logger == nil {
		logger = slog.Default()
	}
	actor, source := resolveActor(options.Actor)
	origin := ""
	if source == ActorProvided {
		origin = options.ActorOrigin
	}
	return &SchemaApplyLog{
		migrator:     NewMigrator(conn, nil),
		logger:       logger,
		runID:        newMigrationLogRunID(),
		actor:        actor,
		actorSource:  source,
		actorOrigin:  origin,
		runReference: options.RunReference,
	}
}

// Start writes the started entry over session, the connection the plan is
// about to execute on, before any of its statements is sent. Writing it there
// keeps it ahead of the plan on an engine whose pool holds one connection,
// which the apply's own session occupies. It creates the table the first time.
func (l *SchemaApplyLog) Start(ctx context.Context, session *dbschema.DatabaseConnection, plan SchemaApplyLogPlan) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.started = true
	l.plan = plan
	if session == nil {
		session = l.migrator.conn
	}
	l.write(ctx, session, SchemaApplyLogOutcome{State: SchemaApplyLogStarted})
}

// Finish writes the outcome entry over the connection the log was opened
// with, under a context of its own: an apply that failed because its context
// ended still has its outcome to record. An apply that never started writes
// none.
func (l *SchemaApplyLog) Finish(ctx context.Context, outcome SchemaApplyLogOutcome) {
	if l == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.started {
		return
	}
	durable, cancel := durableRevisionWriteContext(ctx)
	defer cancel()
	l.write(durable, l.migrator.conn, outcome)
}

// Report returns what the writes have come to so far. It is nil for a nil
// log, and for an apply that has not started: one that stopped before its plan
// began executing tried to write no entry and has nothing to report, as a dry
// run has nothing.
func (l *SchemaApplyLog) Report() *SchemaApplyLogReport {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.started {
		return nil
	}
	report := l.report
	report.Lost = slices.Clone(l.report.Lost)
	return &report
}

// write appends the entry that records entry.State over conn. The caller
// holds l.mu.
func (l *SchemaApplyLog) write(ctx context.Context, conn *dbschema.DatabaseConnection, entry SchemaApplyLogOutcome) {
	scoped := *l.migrator
	scoped.conn = conn
	err := l.ensureTable(ctx, &scoped)
	if err == nil {
		l.seq++
		err = l.insert(ctx, &scoped, l.seq, entry)
	}
	if err != nil {
		l.logger.Warn("Could not append to the schema apply log",
			"table", revisiontable.ApplyLog, "state", string(entry.State), "error", err)
		loss := SchemaApplyLogLoss{State: entry.State, Message: err.Error()}
		loss.SQLState, loss.ErrorNumber = servercode.Read(err)
		l.report.Lost = append(l.report.Lost, loss)
		return
	}
	l.report.Written++
	if l.runReference != "" {
		l.report.RunReference = l.runReference
	}
}

// ensureTable creates the table once per apply, refusing one somebody else
// owns before and after the create, as the operation log does: an adopted
// table hands its owner a hook on every apply. See
// [Migrator.refuseForeignMetadataTable].
func (l *SchemaApplyLog) ensureTable(ctx context.Context, m *Migrator) error {
	if l.ready {
		return nil
	}
	if err := m.refuseForeignMetadataTable(ctx, revisiontable.ApplyLog); err != nil {
		return err
	}
	ddl, err := m.createSchemaApplyLogTableSQL()
	if err != nil {
		return err
	}
	if _, err := m.conn.ExecContext(ctx, ddl); err != nil {
		return fmt.Errorf("create the schema apply log table: %w", err)
	}
	if err := m.refuseForeignMetadataTable(ctx, revisiontable.ApplyLog); err != nil {
		return err
	}
	l.ready = true
	return nil
}

// insert writes one row.
func (l *SchemaApplyLog) insert(ctx context.Context, m *Migrator, seq int64, entry SchemaApplyLogOutcome) error {
	// Strict on the MySQL family for the reason the operation log is: a
	// non-strict session answers success after cutting a value short.
	insert := m.revisionTable().LogInsertPrefix
	if insert == "" {
		insert = "INSERT"
	}
	query := sqlutil.Rebind(m.connectionDialect(), fmt.Sprintf(
		`%s INTO %s (run_id, seq, run_reference, state, actor, actor_source, actor_origin, plan_name, plan_digest, statement_count, error_position, logged_at, error, error_stmt)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, insert, m.qualifiedMetadataTable(revisiontable.ApplyLog)))
	var position any
	if entry.Position > 0 {
		position = entry.Position
	} else if m.revisionTable().TypedNulls {
		position = (*int64)(nil)
	}
	_, err := m.conn.ExecContext(ctx, query,
		l.runID, seq, m.nullableText(l.runReference), string(entry.State),
		m.nullableText(l.actor), string(l.actorSource), m.nullableText(l.actorOrigin),
		m.nullableText(l.plan.Name), m.nullableText(l.plan.Digest), l.plan.Statements, position,
		time.Now().UTC(), m.nullableText(entry.Error), m.nullableText(entry.Statement))
	return err
}

// createSchemaApplyLogTableSQL is the DDL for the schema apply log in the
// spelling the engine's revision table states. An engine that describes its
// own tables and not this one keeps no schema apply log, and every entry is
// lost with that reason rather than written in another engine's spelling.
func (m *Migrator) createSchemaApplyLogTableSQL() (string, error) {
	statement := m.revisionTable().CreateApplyLog
	if statement == "" {
		return "", fmt.Errorf("the %s revision table descriptor states no schema apply log", m.connectionDialect())
	}
	table := m.qualifiedMetadataTable(revisiontable.ApplyLog)
	object := m.quoteIdentifier(revisiontable.ApplyLog)
	if schema := m.metadataTableSchemaName(); schema != "" {
		object = m.quoteIdentifier(schema) + "." + object
	}
	return expandTableStatement(statement, table, sqlStringLiteral(object),
		revisionEngineClauseFor(m.connectionDialect(), m.migrationsEngine)), nil
}
