package migrator_test

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/migrationsplit"
	"ptah.run/internal/statementtimeout"
	"ptah.run/migration/migrationfile"
	"ptah.run/migration/migrator"
)

// unitsOwner is a provider session whose provider splits migration bodies by
// a toy grammar: every statement is a unit of its own, an INSERT changes data
// only and every other statement runs outside a transaction, and COMMIT is
// refused. It records each call the migrator makes, keeps the statements it
// applied, and holds a unit transaction's statement and history writes until
// the transaction commits.
type unitsOwner struct {
	*metadataSession
	dialect string
	// splits is units.migration-split; ddl is transactional DDL.
	splits, ddl bool
	// failing is a statement the engine refuses, applying nothing.
	failing string
	// abortCommits and abortExecutions abort that many unit transactions
	// for a conflict; loseCommit loses the reply of that commit, counted
	// from one, after it lands; failReads fails that many history reads
	// after a lost reply.
	abortCommits, abortExecutions, loseCommit, failReads int
	calls                                                []string
	applied                                              []string
	commits                                              int
	readsToFail                                          int
	// answers holds each answer, as the provider host does, so a call names
	// a body asked about for the first time.
	answers map[string]migrationsplit.Split
	// timeouts are the statement timeouts the executions carried.
	timeouts []time.Duration
}

// timeoutOf is the statement timeout an execution carries to its provider.
func timeoutOf(ctx context.Context) time.Duration {
	timeout, _ := statementtimeout.From(ctx)
	return timeout
}

func (u *unitsOwner) Open(context.Context, dbschema.NativeRequest) (dbschema.NativeSession, error) {
	return u, nil
}
func (u *unitsOwner) OpenSession(context.Context) (dbschema.NativeSession, error) { return u, nil }
func (u *unitsOwner) Writer() catalog.SchemaWriter                                { return u }
func (u *unitsOwner) Info() catalog.ServerInfo {
	info := u.metadataSession.Info()
	info.Dialect, info.IdentifierSemantics = u.dialect, identifier.ForDialect(u.dialect)
	info.Capabilities = capability.Of(map[capability.Capability]bool{capability.TransactionalDDL: u.ddl, capability.MigrationStatementTimeout: true})
	return info
}

// BoundsStatements says the provider stops a unit at the timeout it carries.
func (*unitsOwner) BoundsStatements() bool { return true }
func (u *unitsOwner) ExecuteSQL(ctx context.Context, text string, _ ...any) error {
	if u.IsDryRun() {
		return nil
	}
	_, deadline := ctx.Deadline()
	u.calls = append(u.calls, fmt.Sprintf("outside %s deadline=%t", text, deadline))
	u.timeouts = append(u.timeouts, timeoutOf(ctx))
	return u.apply(text)
}
func (u *unitsOwner) apply(text string) error {
	if text == u.failing {
		return errors.New("the engine refused the statement")
	}
	u.applied = append(u.applied, text)
	return nil
}
func (u *unitsOwner) HistoryStore() (dbschema.HistoryStore, error) { return unitsHistory{u}, nil }

func (u *unitsOwner) BeginTransaction(context.Context) (catalog.SchemaTransaction, error) {
	if !u.ddl {
		return nil, ptaherr.ErrUnsupportedFeature
	}
	u.calls = append(u.calls, "begin file")
	return &unitsTransaction{owner: u, history: slices.Clone(u.history)}, nil
}
func (u *unitsOwner) SplitsMigrations() bool { return u.splits }
func (u *unitsOwner) SplitMigration(_ context.Context, body string, assertions []string) (migrationsplit.Split, error) {
	if !u.splits {
		return migrationsplit.Split{}, ptaherr.ErrUnsupportedFeature
	}
	key := body + "\x00" + strings.Join(assertions, "\x00")
	if answer, asked := u.answers[key]; asked {
		return answer, nil
	}
	u.calls = append(u.calls, "split "+body)
	split := toySplit(body, assertions)
	if u.answers == nil {
		u.answers = make(map[string]migrationsplit.Split)
	}
	u.answers[key] = split
	return split, nil
}

// toySplit makes each statement of body a unit, skipping comment lines.
func toySplit(body string, assertions []string) migrationsplit.Split {
	var split migrationsplit.Split
	offset := 0
	for part := range strings.SplitAfterSeq(body, ";") {
		trimmed := strings.TrimLeft(part, " \n")
		for strings.HasPrefix(trimmed, "--") {
			_, trimmed, _ = strings.Cut(trimmed, "\n")
			trimmed = strings.TrimLeft(trimmed, " \n")
		}
		start, statement := offset+len(part)-len(trimmed), strings.TrimSpace(trimmed)
		offset += len(part)
		text := strings.TrimSuffix(statement, ";")
		switch {
		case text == "":
		case text == "COMMIT":
			return migrationsplit.Split{Refusal: &migrationsplit.Refusal{Start: start, End: start + len(statement), Reason: "it ends a transaction"},
				Verdicts: make([]migrationsplit.Verdict, len(assertions))}
		case strings.HasPrefix(text, "INSERT"):
			split.Units = append(split.Units, migrationsplit.Unit{SQL: text, Source: statement, Placement: migrationsplit.WithCheckpoint})
		default:
			split.Units = append(split.Units, migrationsplit.Unit{SQL: text, Source: statement, Placement: migrationsplit.Outside})
		}
	}
	for _, assertion := range assertions {
		split.Verdicts = append(split.Verdicts, migrationsplit.Verdict{ReadOnly: !strings.Contains(assertion, "external"), Reason: "it reads an external table"})
	}
	return split
}

// OperatorStatements runs a check on an empty database of its own.
func (*unitsOwner) OperatorStatements(bool) (*sql.DB, error) { return sql.Open("sqlite", ":memory:") }
func (u *unitsOwner) BeginUnitTransaction(context.Context) (catalog.SchemaTransaction, error) {
	u.calls = append(u.calls, "begin unit")
	return &unitsTransaction{owner: u, unit: true, history: slices.Clone(u.history)}, nil
}

// unitsHistory is the session's history outside a transaction, whose reads
// fail after a lost commit reply when the owner says so.
type unitsHistory struct{ owner *unitsOwner }

func (h unitsHistory) Prepare(ctx context.Context, table dbschema.HistoryTable) error {
	return metadataHistory{h.owner.metadataSession}.Prepare(ctx, table)
}
func (h unitsHistory) Read(ctx context.Context, table dbschema.HistoryTable) (dbschema.HistorySnapshot, error) {
	if h.owner.readsToFail > 0 {
		h.owner.readsToFail--
		return dbschema.HistorySnapshot{}, errors.New("the history read was lost")
	}
	return metadataHistory{h.owner.metadataSession}.Read(ctx, table)
}
func (h unitsHistory) CompareAndSwap(ctx context.Context, table dbschema.HistoryTable, expected, replacement *dbschema.HistoryRecord) error {
	if replacement != nil && replacement.Error != "" {
		h.owner.calls = append(h.owner.calls, fmt.Sprintf("mark %d", replacement.Applied))
	}
	return metadataHistory{h.owner.metadataSession}.CompareAndSwap(ctx, table, expected, replacement)
}

// unitsTransaction holds one transaction's statements and history writes.
type unitsTransaction struct {
	owner      *unitsOwner
	unit       bool
	statements []string
	history    []dbschema.HistoryRecord
}

func (*unitsTransaction) IsDryRun() bool { return false }
func (t *unitsTransaction) ExecuteSQL(ctx context.Context, text string, _ ...any) error {
	t.owner.calls = append(t.owner.calls, "inside "+text)
	t.owner.timeouts = append(t.owner.timeouts, timeoutOf(ctx))
	if t.unit && t.owner.abortExecutions > 0 {
		t.owner.abortExecutions--
		return fmt.Errorf("%w: execution", migrationsplit.ErrTransactionAborted)
	}
	if text == t.owner.failing {
		return errors.New("the engine refused the statement")
	}
	t.statements = append(t.statements, text)
	return nil
}
func (t *unitsTransaction) HistoryStore() (dbschema.HistoryStore, error) {
	return unitsTransactionHistory{t}, nil
}
func (t *unitsTransaction) Commit() error {
	if t.unit && t.owner.abortCommits > 0 {
		t.owner.abortCommits--
		t.owner.calls = append(t.owner.calls, "abort")
		return fmt.Errorf("%w: commit", migrationsplit.ErrTransactionAborted)
	}
	t.owner.calls = append(t.owner.calls, "commit")
	t.owner.applied = append(t.owner.applied, t.statements...)
	t.owner.history = t.history
	t.owner.commits++
	if t.unit && t.owner.commits == t.owner.loseCommit {
		t.owner.readsToFail = t.owner.failReads
		return fmt.Errorf("the commit reply was lost: %w", ptaherr.ErrOutcomeUnknown)
	}
	return nil
}
func (t *unitsTransaction) Rollback() error {
	t.owner.calls = append(t.owner.calls, "rollback")
	return nil
}

type unitsTransactionHistory struct{ tx *unitsTransaction }

func (unitsTransactionHistory) Prepare(context.Context, dbschema.HistoryTable) error {
	return ptaherr.ErrUnsupportedFeature
}
func (h unitsTransactionHistory) Read(context.Context, dbschema.HistoryTable) (dbschema.HistorySnapshot, error) {
	return dbschema.HistorySnapshot{Exists: true, Records: slices.Clone(h.tx.history)}, nil
}
func (h unitsTransactionHistory) CompareAndSwap(_ context.Context, _ dbschema.HistoryTable, expected, replacement *dbschema.HistoryRecord) error {
	index := slices.IndexFunc(h.tx.history, func(record dbschema.HistoryRecord) bool { return expected != nil && record.Version == expected.Version })
	if expected != nil && (index < 0 || !reflect.DeepEqual(h.tx.history[index], *expected)) {
		return errors.New("history record changed")
	}
	h.tx.history = slices.DeleteFunc(h.tx.history, func(record dbschema.HistoryRecord) bool { return expected != nil && record.Version == expected.Version })
	h.tx.history = append(h.tx.history, *replacement)
	h.tx.owner.calls = append(h.tx.owner.calls, fmt.Sprintf("checkpoint %d", replacement.Applied))
	return nil
}

func newUnitsOwner() *unitsOwner {
	return &unitsOwner{metadataSession: &metadataSession{}, dialect: "sqlite", splits: true}
}

// unitsRunner is a migrator over owner that runs one migration of up.
func unitsRunner(c *qt.C, owner *unitsOwner, up string) *migrator.Migrator {
	c.Helper()
	ctx, err := dbschema.WithNativeConnections(c.Context(), map[string]dbschema.NativeRegistration{"units": {Target: owner.dialect, Connector: owner}})
	c.Assert(err, qt.IsNil)
	conn, err := dbschema.ConnectToDatabase(ctx, "units://database")
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { c.Check(conn.Close(), qt.IsNil) })
	runner, err := migrator.NewFSMigrator(conn, fstest.MapFS{
		"0000000001_units.up.sql":   &fstest.MapFile{Data: []byte(up)},
		"0000000001_units.down.sql": &fstest.MapFile{Data: []byte("DROP TABLE a;")},
	})
	c.Assert(err, qt.IsNil)
	return runner.WithMigrationsTable("main", "_revisions").WithoutMigrationLock()
}

// unitsRow is what a test reads off the only revision.
func unitsRow(owner *unitsOwner) []any {
	return []any{owner.history[0].State, owner.history[0].Applied, owner.history[0].Total}
}

const unitsBody = "CREATE TABLE a (id INTEGER); INSERT INTO a VALUES (1); ALTER TABLE a ADD COLUMN b TEXT;"

var statementTimeout = migrationfile.Timeouts{StatementTimeout: 5 * time.Second, HasStatementTimeout: true}

// A unit that runs outside a transaction is marked in flight, run and
// checkpointed; a unit that changes data only runs in a unit transaction with
// its checkpoint and commits once. `file` runs as `none` on an engine without
// transactional DDL, and the host puts no deadline of its own on a unit.
func TestProviderUnits_PlaceEachUnit(t *testing.T) {
	c := qt.New(t)
	owner := newUnitsOwner()

	c.Assert(unitsRunner(c, owner, unitsBody).WithDefaultTimeouts(statementTimeout).MigrateUp(c.Context()), qt.IsNil)

	c.Assert(owner.timeouts, qt.DeepEquals, []time.Duration{5 * time.Second, 5 * time.Second, 5 * time.Second})
	// The first mark is the pending row the run opens with.
	c.Assert(owner.calls, qt.DeepEquals, []string{
		"split " + unitsBody, "mark 0", "mark 0", "outside CREATE TABLE a (id INTEGER) deadline=false",
		"begin unit", "inside INSERT INTO a VALUES (1)", "checkpoint 2", "commit",
		"mark 2", "outside ALTER TABLE a ADD COLUMN b TEXT deadline=false",
	})
	c.Assert(unitsRow(owner), qt.DeepEquals, []any{"applied", 3, 3})
}

// An engine that aborted a unit's transaction for a conflict applied nothing,
// so the unit runs again, up to the host's bound; any other failure is not
// run again.
func TestProviderUnits_RunAnAbortedUnitAgain(t *testing.T) {
	tests := []struct {
		name                         string
		abortCommits, abortExecution int
		wantBegins                   int
	}{
		{name: "an aborted commit", abortCommits: 1, wantBegins: 2},
		{name: "an aborted execution", abortExecution: 2, wantBegins: 3},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := newUnitsOwner()
			owner.abortCommits, owner.abortExecutions = test.abortCommits, test.abortExecution

			c.Assert(unitsRunner(c, owner, unitsBody).MigrateUp(c.Context()), qt.IsNil)

			c.Assert(countCalls(owner.calls, "begin unit"), qt.Equals, test.wantBegins)
			c.Assert(owner.applied, qt.DeepEquals, []string{"CREATE TABLE a (id INTEGER)", "INSERT INTO a VALUES (1)", "ALTER TABLE a ADD COLUMN b TEXT"})
			c.Assert(unitsRow(owner), qt.DeepEquals, []any{"applied", 3, 3})
		})
	}
}

func countCalls(calls []string, call string) int {
	return len(slices.DeleteFunc(slices.Clone(calls), func(made string) bool { return made != call }))
}

// A unit that does not commit leaves the units before it recorded and nothing
// of its own applied.
func TestProviderUnits_StopRunningAUnitAgain(t *testing.T) {
	tests := []struct {
		name         string
		abortCommits int
		failing      string
		wantBegins   int
		wantErr      string
	}{
		{name: "an abort on every attempt", abortCommits: 9, wantBegins: 5, wantErr: `(?s).*aborted the transaction for a conflict.*`},
		{name: "a refusal", failing: "INSERT INTO a VALUES (1)", wantBegins: 1, wantErr: `(?s).*the engine refused the statement.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := newUnitsOwner()
			owner.abortCommits, owner.failing = test.abortCommits, test.failing

			err := unitsRunner(c, owner, unitsBody).MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(countCalls(owner.calls, "begin unit"), qt.Equals, test.wantBegins)
			c.Assert(owner.applied, qt.DeepEquals, []string{"CREATE TABLE a (id INTEGER)"})
			c.Assert(unitsRow(owner), qt.DeepEquals, []any{"failed", 1, 3})
		})
	}
}

// A commit whose reply was lost is settled by reading the checkpoint back.
func TestProviderUnits_SettleALostCommitReply_HappyPath(t *testing.T) {
	c := qt.New(t)
	owner := newUnitsOwner()
	owner.loseCommit = 1

	c.Assert(unitsRunner(c, owner, unitsBody).MigrateUp(c.Context()), qt.IsNil)

	c.Assert(unitsRow(owner), qt.DeepEquals, []any{"applied", 3, 3})
	c.Assert(countCalls(owner.calls, "begin unit"), qt.Equals, 1)
}

// A failure the read-back cannot settle keeps the progress the row records,
// which the lost commit raised, so a resume does not run the committed unit
// again.
func TestProviderUnits_SettleALostCommitReply_FailurePath(t *testing.T) {
	c := qt.New(t)
	owner := newUnitsOwner()
	owner.loseCommit, owner.failReads = 1, 1

	err := unitsRunner(c, owner, unitsBody).MigrateUp(c.Context())

	c.Assert(err, qt.ErrorMatches, `(?s).*the commit reply was lost.*read back whether the query committed: .*the history read was lost.*`)
	c.Assert(unitsRow(owner), qt.DeepEquals, []any{"failed", 2, 3})
	c.Assert(countCalls(owner.calls, "begin unit"), qt.Equals, 1)
}

// A body the provider refuses is refused before anything runs, with the text
// the refusal names quoted.
func TestProviderUnits_RefuseWhatTheProviderRefuses(t *testing.T) {
	c := qt.New(t)
	owner := newUnitsOwner()

	err := unitsRunner(c, owner, "INSERT INTO a VALUES (1); COMMIT;").MigrateUp(c.Context())

	c.Assert(err, qt.ErrorMatches, `migration 1 cannot run up on sqlite: the provider refuses "COMMIT;": it ends a transaction`)
	c.Assert(owner.calls, qt.DeepEquals, []string{"split INSERT INTO a VALUES (1); COMMIT;"})
	c.Assert(owner.history, qt.HasLen, 0)
}

// The placement decides which transaction mode a body can run in.
func TestProviderUnits_RefuseAModeTheUnitsCannotRunIn(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		ddl     bool
		splits  bool
		mode    migrator.MigrationTxMode
		body    string
		wantErr string
	}{
		{
			name: "file on transactional DDL with a unit outside", ddl: true, splits: true, mode: migrator.MigrationTxModeFile, body: unitsBody,
			wantErr: `migration 1 cannot run up on sqlite with tx-mode file: its unit 1 runs outside any transaction, so one transaction ` +
				`cannot hold the body; run it with tx-mode none, which commits each unit on its own`,
		},
		{
			name: "all on transactional DDL with a unit outside", ddl: true, splits: true, mode: migrator.MigrationTxModeAll, body: unitsBody,
			wantErr: `migration 1 cannot run up on sqlite with tx-mode all: its unit 1 runs outside any transaction, .*`,
		},
		{
			name: "all without transactional DDL", splits: true, mode: migrator.MigrationTxModeAll, body: unitsBody,
			wantErr: `tx-mode all is not supported for dialect "sqlite": .*`,
		},
		{
			name: "none without a provider split", mode: migrator.MigrationTxModeNone, body: unitsBody,
			wantErr: `migration 1 cannot run up on sqlite: the provider does not split a migration body into the units its engine runs ` +
				`\(units.migration-split\), so a body of 3 statements cannot run outside a transaction; upgrade the provider`,
		},
		{
			// The built-in YDB engine would run these as one query; a
			// provider's session counts them by the generic grammar.
			name: "none without a provider split, named like a built-in engine with query units", dialect: "ydb", mode: migrator.MigrationTxModeNone,
			body:    "INSERT INTO a VALUES (1); INSERT INTO a VALUES (2);",
			wantErr: `migration 1 cannot run up on ydb: the provider does not split .*, so a body of 2 statements cannot run outside a transaction; .*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := newUnitsOwner()
			owner.ddl, owner.splits = test.ddl, test.splits
			owner.dialect = cmp.Or(test.dialect, owner.dialect)

			err := unitsRunner(c, owner, test.body).WithTransactionMode(test.mode).MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(owner.applied, qt.HasLen, 0)
		})
	}
}

// The controls: a mode the units can run in runs.
func TestProviderUnits_RunAModeTheUnitsCanRunIn(t *testing.T) {
	tests := []struct {
		name      string
		ddl       bool
		splits    bool
		mode      migrator.MigrationTxMode
		body      string
		wantCalls []string
	}{
		{
			name: "file on transactional DDL with data units only", ddl: true, splits: true, mode: migrator.MigrationTxModeFile,
			body: "INSERT INTO a VALUES (1); INSERT INTO a VALUES (2);", wantCalls: []string{"begin file", "inside INSERT INTO a VALUES (1)", "inside INSERT INTO a VALUES (2)"},
		},
		{
			name: "none without a provider split, one statement", mode: migrator.MigrationTxModeNone,
			body: "CREATE TABLE a (id INTEGER);", wantCalls: []string{"outside CREATE TABLE a (id INTEGER) deadline=false"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			owner := newUnitsOwner()
			owner.ddl, owner.splits = test.ddl, test.splits

			c.Assert(unitsRunner(c, owner, test.body).WithTransactionMode(test.mode).MigrateUp(c.Context()), qt.IsNil)
			c.Assert(slices.DeleteFunc(slices.Clone(owner.calls), func(call string) bool {
				return strings.HasPrefix(call, "split") || call == "commit" || strings.HasPrefix(call, "mark") || strings.HasPrefix(call, "checkpoint")
			}), qt.DeepEquals, test.wantCalls)
		})
	}
}

// A dry run asks for the split and runs nothing.
func TestProviderUnits_DryRunRunsNothing(t *testing.T) {
	c := qt.New(t)
	owner := newUnitsOwner()
	owner.SetDryRun(true)

	c.Assert(unitsRunner(c, owner, unitsBody).MigrateUp(c.Context()), qt.IsNil)

	c.Assert(owner.calls, qt.DeepEquals, []string{"split " + unitsBody})
	c.Assert(owner.applied, qt.HasLen, 0)
}

// A check assertion takes the provider's verdict in place of the host's text
// rules, which accept this one, in a real run and in a dry run that defers it.
func TestProviderUnits_CheckTakesTheVerdict(t *testing.T) {
	for _, dry := range []bool{false, true} {
		t.Run(fmt.Sprintf("dry run %t", dry), func(t *testing.T) {
			c := qt.New(t)
			owner := newUnitsOwner()
			owner.SetDryRun(dry)
			body := "-- +ptah check name=feed assert=\"SELECT COUNT(*) = 0 FROM external_feed\"\n" + unitsBody

			err := unitsRunner(c, owner, body).MigrateUp(c.Context())

			c.Assert(err, qt.ErrorMatches, `(?s).*pre-migration check feed for migration 1 could not run: `+
				`check assertion must be one read-only SELECT statement: it reads an external table.*`)
			c.Assert(owner.applied, qt.HasLen, 0)
		})
	}
}

// A provider session takes no unit split, query deadline or check rule from a
// built-in engine of the same name: the units are the provider's, and the
// statement timeout reaches the provider with the unit alone.
func TestProviderUnits_TakeNothingFromABuiltInEngineOfTheSameName(t *testing.T) {
	c := qt.New(t)
	owner := newUnitsOwner()
	owner.dialect = "ydb"

	c.Assert(unitsRunner(c, owner, unitsBody).WithDefaultTimeouts(statementTimeout).MigrateUp(c.Context()), qt.IsNil)

	c.Assert(owner.calls, qt.Contains, "outside CREATE TABLE a (id INTEGER) deadline=false")
	c.Assert(owner.timeouts, qt.DeepEquals, []time.Duration{5 * time.Second, 5 * time.Second, 5 * time.Second})
	c.Assert(owner.calls, qt.Contains, "inside INSERT INTO a VALUES (1)")
	c.Assert(unitsRow(owner), qt.DeepEquals, []any{"applied", 3, 3})
}

// unitsRepair fails a run of broken at its second unit, then repairs the
// revision it left from the second unit of fixed. The owner keeps the history
// across both runners.
func unitsRepair(c *qt.C, owner *unitsOwner, broken, fixed string) error {
	c.Helper()
	owner.failing = "CREATE TABLE a (id INTEGER)"
	c.Assert(unitsRunner(c, owner, broken).MigrateUp(c.Context()), qt.IsNotNil)
	owner.failing, owner.calls = "", nil
	return unitsRunner(c, owner, fixed).RepairMigration(c.Context(), migrator.RepairMigrationOptions{Version: 1, ResumeFrom: 2})
}

// A repair that resumes a body runs a unit that changes data only the way a
// run does, in a unit transaction with its checkpoint and no mark before it.
func TestProviderUnits_RepairRunsADataUnitWithItsCheckpoint(t *testing.T) {
	c := qt.New(t)
	owner := newUnitsOwner()

	err := unitsRepair(c, owner, "CREATE TABLE b (id INTEGER); CREATE TABLE a (id INTEGER);", "CREATE TABLE b (id INTEGER); INSERT INTO b VALUES (1);")

	c.Assert(err, qt.IsNil)
	c.Assert(owner.calls, qt.DeepEquals, []string{
		"split CREATE TABLE b (id INTEGER); INSERT INTO b VALUES (1);", "split DROP TABLE a;",
		"begin unit", "inside INSERT INTO b VALUES (1)", "checkpoint 2", "commit",
	})
	c.Assert(unitsRow(owner), qt.DeepEquals, []any{"applied", 2, 2})
}

// A repair refuses what the provider refuses, with the provider's refusal,
// before any resumed unit runs.
func TestProviderUnits_RepairRefusesWhatTheProviderRefuses(t *testing.T) {
	c := qt.New(t)
	owner := newUnitsOwner()

	err := unitsRepair(c, owner, "CREATE TABLE b (id INTEGER); CREATE TABLE a (id INTEGER);", "CREATE TABLE b (id INTEGER); COMMIT; CREATE TABLE c (id INTEGER);")

	c.Assert(err, qt.ErrorMatches, `migration 1 cannot run up on sqlite: the provider refuses "COMMIT;": it ends a transaction`)
	c.Assert(owner.calls, qt.DeepEquals, []string{"split CREATE TABLE b (id INTEGER); COMMIT; CREATE TABLE c (id INTEGER);", "split DROP TABLE a;"})
	c.Assert(unitsRow(owner), qt.DeepEquals, []any{"failed", 1, 2})
}
