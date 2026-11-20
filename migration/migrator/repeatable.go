package migrator

import (
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"io/fs"
	"slices"
	"strings"
	"time"

	"ptah.run/migration/migrationfile"
)

// A repeatable migration is SQL that runs again whenever its content changes,
// rather than once at a version: a view, a routine, or a script written to be
// run more than once. A directory read in the Ptah format holds one as
// R__<identity>.sql (see [migrationfile.RepeatableFilePrefix]).
//
// An up run executes the new and changed ones after every selected versioned
// migration has succeeded, in byte order of their identities, under the same
// migration lock. The record of what ran is a table of its own beside the
// revision table, one row per identity, so a repeatable migration never takes
// a version and never weakens the checksum rule of a versioned one.

// Repeatable migration states a [RepeatableRecord] reports.
const (
	// RepeatableStatePending is a repeatable migration the database has no
	// record of. The next up run executes it.
	RepeatableStatePending = "pending"
	// RepeatableStateChanged is a repeatable migration whose file no longer
	// hashes to the digest recorded as applied. The next up run executes it.
	RepeatableStateChanged = "changed"
	// RepeatableStateApplied is a repeatable migration whose file hashes to the
	// digest recorded as applied. An up run leaves it alone.
	RepeatableStateApplied = "applied"
	// RepeatableStateRunning is an attempt that recorded its start and no
	// outcome: the run stopped, lost its connection, or a commit answered with
	// an error. Whether its statements took effect is not known.
	RepeatableStateRunning = "running"
	// RepeatableStateFailed is an attempt whose body failed. Inside a
	// transaction it took no effect; without one, some of its statements may
	// have.
	RepeatableStateFailed = "failed"
	// RepeatableStateOrphaned is a record whose identity no file of the
	// directory carries any more, because the file was deleted or renamed,
	// and whose last attempt applied. An up run leaves it alone, and a
	// rollback is refused while it exists. A record without a file whose last
	// attempt did not finish keeps the state [RepeatableStateFailed] or
	// [RepeatableStateRunning] instead.
	RepeatableStateOrphaned = "orphaned"
)

// Reasons a [RepeatableWork] gives for running a repeatable migration.
const (
	// RepeatableReasonNew runs a repeatable migration the database has no
	// record of.
	RepeatableReasonNew = "new"
	// RepeatableReasonChanged runs a repeatable migration whose content
	// changed since it last applied.
	RepeatableReasonChanged = "changed"
	// RepeatableReasonRetry runs again a repeatable migration whose last
	// attempt failed or has no outcome, which only a run that asked to retry
	// dirty work does.
	RepeatableReasonRetry = "retry"
)

// RepeatableWork is one repeatable migration an up run selected, in the order
// the run executes them.
type RepeatableWork struct {
	// Identity names the repeatable migration, as its file name spells it.
	Identity string `json:"identity"`
	// Path is the file's slash-separated path in the migration directory.
	Path string `json:"path"`
	// Checksum is the h1 digest of the content the run executes, the same
	// value ptah.sum records for the file.
	Checksum string `json:"checksum"`
	// AppliedChecksum is the digest recorded as applied before the run, empty
	// when no content of this identity has applied yet. The JSON form always
	// carries it, as "" for no record: an approval binds the record by copying
	// this value, and an absent field there makes no claim about the record.
	AppliedChecksum string `json:"applied_checksum"`
	// Reason is one of the RepeatableReason constants.
	Reason string `json:"reason"`
}

// RepeatableRecord is one repeatable migration of the directory, or one
// record of the database that no file of the directory carries, with what the
// database records for it.
type RepeatableRecord struct {
	// Identity names the repeatable migration, as its file name spells it.
	Identity string `json:"identity"`
	// Path is the file's slash-separated path in the migration directory,
	// empty for an orphaned record.
	Path string `json:"path"`
	// Checksum is the h1 digest of the file as it is now, empty for an
	// orphaned record.
	Checksum string `json:"checksum"`
	// Orphaned reports a record whose identity no file of the directory
	// carries: the file was deleted or renamed after it ran.
	Orphaned bool `json:"orphaned,omitempty"`
	// AppliedChecksum is the digest of the content that last applied, empty
	// when none has.
	AppliedChecksum string `json:"applied_checksum,omitempty"`
	// State is one of the RepeatableState constants.
	State string `json:"state"`
	// AttemptChecksum is the digest of the content the last attempt executed,
	// and AttemptOrder its 1-based position among the repeatable migrations of
	// that run. Both are absent when the database has no record.
	AttemptChecksum string `json:"attempt_checksum,omitempty"`
	AttemptOrder    int    `json:"attempt_order,omitempty"`
	// NoTransaction reports that the last attempt ran outside a transaction,
	// so a failed or interrupted one may have left part of its work behind.
	NoTransaction bool `json:"no_transaction,omitempty"`
	// AttemptedAt is when the last attempt started and AppliedAt when content
	// last applied, in UTC. Each is absent when the database records none.
	AttemptedAt *time.Time `json:"attempted_at,omitempty"`
	AppliedAt   *time.Time `json:"applied_at,omitempty"`
	// Error is the failure the last attempt recorded, empty unless State is
	// [RepeatableStateFailed].
	Error string `json:"error,omitempty"`
}

// dirty reports whether the record's last attempt has no successful outcome.
// A state this build does not know counts as dirty, so it blocks a run rather
// than being read as finished.
func (r RepeatableRecord) dirty() bool {
	switch r.State {
	case RepeatableStatePending, RepeatableStateChanged, RepeatableStateApplied, RepeatableStateOrphaned:
		return false
	default:
		return true
	}
}

// WithRepeatableMigrations makes the provider load the repeatable migrations
// of a directory read in the Ptah format, so a migrator over it runs, reports
// and guards them. Without it, an R__<identity>.sql file is not loaded, and a
// migrator over the directory behaves as if the file were absent.
//
// A directory read in the Atlas format holds no repeatable migration under
// this option: Atlas reads R__<name>.sql as a one-time version, and that
// reading is kept unless [WithAtlasRepeatableMigrations] is given. An up run
// with repeatable work is refused under [WithOwnTransactions], because a
// repeatable migration runs in a transaction Ptah opens.
func WithRepeatableMigrations() FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.loadsRepeatables = true
	}
}

// WithAtlasRepeatableMigrations is [WithRepeatableMigrations] for a directory
// read in the Atlas format: a top-level file named R__<identity>.sql is a
// repeatable migration, run, reported, guarded and recorded as one in a Ptah
// directory is, instead of the one-time version R the Atlas format reads it as.
// The other files of the directory keep their Atlas reading, and atlas.sum
// still covers every file, so the integrity file is the one the Atlas format
// writes for the same directory.
//
// A revision table that records version R was written under the Atlas
// reading, and the migrator refuses it with an [AtlasRepeatableVersionError]
// before anything runs, because the file would run again as a repeatable
// migration the record has no row for. The provider refuses a directory not
// read in the Atlas format; select it with [WithMigrationDirFormat].
func WithAtlasRepeatableMigrations() FSProviderOption {
	return func(p *FSMigrationProvider) {
		p.loadsRepeatables = true
		p.atlasRepeatables = true
	}
}

// repeatableMigration is one repeatable file as a provider loaded it.
type repeatableMigration struct {
	identity string
	path     string
	checksum string
	// body is the content checksum covers, which the record keeps once it
	// applies.
	body []byte
	// migration carries the body the way a versioned up file carries it, so
	// the statements run through the same splitter, directive reader,
	// interceptor and timeouts.
	migration *Migration
}

// repeatableSource is implemented by a provider that holds repeatable
// migrations.
type repeatableSource interface {
	// loadsRepeatableMigrations reports whether the provider reads repeatable
	// migrations at all, which is what lets a record without a file be told
	// apart from a record of a file the provider did not look for.
	loadsRepeatableMigrations() bool
	repeatableMigrations() []*repeatableMigration
}

// recordedRepeatableSource is implemented by a provider that can read the
// body a record keeps as it reads a file.
type recordedRepeatableSource interface {
	repeatableFromRecord(identity, path string, body []byte) (*repeatableMigration, error)
}

// loadsRepeatableMigrations reports whether [WithRepeatableMigrations] was
// given.
func (p *FSMigrationProvider) loadsRepeatableMigrations() bool {
	return p.loadsRepeatables
}

// repeatableMigrations returns the repeatable migrations the provider loaded,
// in execution order.
func (p *FSMigrationProvider) repeatableMigrations() []*repeatableMigration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return slices.Clone(p.repeatables)
}

// loadRepeatables reads the directory's repeatable files.
func (p *FSMigrationProvider) loadRepeatables() error {
	files, err := p.discoverRepeatableFiles()
	if err != nil {
		return err
	}
	p.repeatables = make([]*repeatableMigration, 0, len(files))
	for _, file := range files {
		data, err := fs.ReadFile(p.fsys, file.Path)
		if err != nil {
			return fmt.Errorf("failed to read repeatable migration %s: %w", file.Path, err)
		}
		repeatable, err := p.newRepeatable(file.Identity, file.Path, data)
		if err != nil {
			return fmt.Errorf("failed to load repeatable migration %s: %w", file.Path, err)
		}
		p.repeatables = append(p.repeatables, repeatable)
	}
	return nil
}

// repeatableFromRecord builds the repeatable migration a record's body holds,
// read the way the provider reads a file, so a replay runs it as the run that
// applied it did. path is the file it came from, or empty when the directory
// has none.
func (p *FSMigrationProvider) repeatableFromRecord(identity, path string, body []byte) (*repeatableMigration, error) {
	name := path
	if name == "" {
		name = "the recorded body of " + identity
	}
	repeatable, err := p.newRepeatable(identity, name, body)
	if err != nil {
		return nil, fmt.Errorf("failed to load the recorded body of repeatable migration %s: %w", identity, err)
	}
	repeatable.path = path
	return repeatable, nil
}

// newRepeatable reads data as the body of the repeatable migration identity.
// name is what a directive error names. The SQL is the text
// [migrationfile.SQLText] reads out of data, while the digest and the kept
// body are data itself, byte for byte.
func (p *FSMigrationProvider) newRepeatable(identity, name string, data []byte) (*repeatableMigration, error) {
	up, err := migrationFuncFromSQLStringWithMetadata(name, migrationfile.SQLText(string(data)), p.hooks)
	if err != nil {
		return nil, err
	}
	migration := &Migration{
		Description:     "Repeatable " + identity,
		Up:              NoopMigrationFunc,
		Down:            NoopMigrationFunc,
		ownTransactions: p.ownTransactions,
	}
	setMigrationUp(migration, up)
	return &repeatableMigration{
		identity:  identity,
		path:      name,
		checksum:  repeatableChecksum(data),
		body:      data,
		migration: migration,
	}, nil
}

// repeatableChecksum is the digest a repeatable migration is recorded under:
// the h1 hash of the file's bytes, which is also the entry ptah.sum carries
// for the file.
func repeatableChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return "h1:" + base64.StdEncoding.EncodeToString(sum[:])
}

// providerRepeatables returns the repeatable migrations of the migrator's
// provider, and none when the provider holds no such thing.
func (m *Migrator) providerRepeatables() []*repeatableMigration {
	source, ok := m.migrationProvider.(repeatableSource)
	if !ok {
		return nil
	}
	return source.repeatableMigrations()
}

// RepeatableMigrationsUnsupportedError reports a directory holding repeatable
// migrations that this migrator cannot run, and is returned before anything
// changes.
type RepeatableMigrationsUnsupportedError struct {
	// Dialect is the connected dialect, empty when the reason is not the
	// engine.
	Dialect string
	// Reason says what stands in the way.
	Reason string
	// Err is the error Reason was read from, nil when nothing returned one.
	Err error
}

// Error names the dialect, when the engine is what stands in the way, and the
// reason.
func (e *RepeatableMigrationsUnsupportedError) Error() string {
	if e.Dialect != "" {
		return "this directory holds repeatable migrations, which this run cannot execute on " + e.Dialect + ": " + e.Reason
	}
	return "this directory holds repeatable migrations, which this run cannot execute: " + e.Reason
}

// Unwrap returns the error Reason was read from.
func (e *RepeatableMigrationsUnsupportedError) Unwrap() error { return e.Err }

// DirtyRepeatableError reports repeatable migrations whose last attempt
// failed or has no outcome. An up run refuses before it changes anything while
// one exists, as it does over a dirty revision; [MigrateUpOptions.AllowDirty]
// runs them again instead. A dirty record that no file of the directory
// carries is refused under AllowDirty too, because there is nothing to run
// again.
type DirtyRepeatableError struct {
	// Records are the dirty repeatable migrations, in identity order.
	Records []RepeatableRecord
	// Table is the quoted name of the table that records them.
	Table string
}

// Error names each dirty repeatable migration and what its record says.
func (e *DirtyRepeatableError) Error() string {
	parts := make([]string, 0, len(e.Records))
	for _, record := range e.Records {
		parts = append(parts, describeDirtyRepeatable(record, e.Table))
	}
	return "repeatable migrations did not finish: " + strings.Join(parts, "; ")
}

// describeDirtyRepeatable is one dirty record as a sentence. table is where
// the row of an orphaned record is deleted.
func describeDirtyRepeatable(record RepeatableRecord, table string) string {
	var b strings.Builder
	fmt.Fprintf(&b, "%s (%s) ", record.Identity, record.AttemptChecksum)
	switch record.State {
	case RepeatableStateFailed:
		fmt.Fprintf(&b, "failed: %s", record.Error)
	case RepeatableStateRunning:
		b.WriteString("recorded no outcome, so whether it took effect is not known")
	default:
		fmt.Fprintf(&b, "is in state %q", record.State)
	}
	if record.NoTransaction {
		b.WriteString(", and it ran without a transaction, so part of it may have taken effect")
	}
	if record.Orphaned {
		fmt.Fprintf(&b, "; no file of the directory carries it, so it cannot run again: check what it left behind, "+
			"then remove its row from %s with 'ptah migrations repair --repeatable %s'", table, record.Identity)
	}
	return b.String()
}

// RepeatableBoundError reports an up run bounded by a target version or an
// amount that would stop below the directory's latest version while
// repeatable work is pending. A repeatable migration is written against the
// latest schema, so it runs only in a run that reaches it; the run is refused
// before it changes anything rather than left to apply the versioned part
// alone.
type RepeatableBoundError struct {
	// TargetVersion and Amount are the bound the run was given, zero where
	// unset.
	TargetVersion int64
	Amount        uint64
	// Work is the repeatable work the run would have had to leave behind.
	Work []RepeatableWork
	// versionLabel names TargetVersion as its file name spells it, for an Atlas
	// file whose version is not a number; empty otherwise.
	versionLabel string
}

// Error names the bound and the repeatable work it would leave behind.
func (e *RepeatableBoundError) Error() string {
	bound := fmt.Sprintf("target version %d", versionText{number: e.TargetVersion, label: e.versionLabel})
	if e.TargetVersion == 0 {
		bound = fmt.Sprintf("a limit of %d", e.Amount)
	}
	return fmt.Sprintf(
		"cannot run up to %s: the run would stop below the latest version while repeatable migrations %s are "+
			"pending, and a repeatable migration runs only in a run that reaches the latest version",
		bound, strings.Join(repeatableWorkIdentities(e.Work), ", "))
}

// RepeatableStatementTooLongError refuses, before anything runs, a repeatable
// migration whose body the record cannot store. On an engine whose driver
// writes bound values into the statement text, the statement that records the
// body carries it escaped, and the server parses no statement longer than the
// setting allows.
type RepeatableStatementTooLongError struct {
	// Path is the repeatable migration's file.
	Path string
	// Length is the least length in bytes of the statement that records the
	// body.
	Length int
	// Limit is the longest statement, in bytes, the server parses.
	Limit int64
	// Setting names the server setting that holds Limit.
	Setting string
}

// Error names the file, the statement's length, the limit and the setting.
func (e *RepeatableStatementTooLongError) Error() string {
	return fmt.Sprintf(
		"repeatable migration %s cannot be recorded: the statement that records its body is %d bytes, and the "+
			"server's %s is %d; raise %s for the migration user, or split the file",
		e.Path, e.Length, e.Setting, e.Limit, e.Setting)
}

// RepeatableRollbackError reports a rollback of a database that records
// repeatable migrations, whether or not the directory still holds their files.
// A repeatable migration has no rollback, and its definition was written
// against the latest schema, so the rollback is refused before it changes
// anything.
type RepeatableRollbackError struct {
	// Identities are every identity the record holds, in identity order.
	Identities []string
	// Table is the quoted name of the table that records them.
	Table string
}

// Error names the repeatable migrations and the record that has to go first.
func (e *RepeatableRollbackError) Error() string {
	return fmt.Sprintf(
		"cannot roll back: this database records repeatable migrations %s, which have no rollback and were written "+
			"against the latest schema; drop what they created and remove their rows from %s with "+
			"'ptah migrations repair --repeatable <identity>' first, or roll forward with a new migration instead",
		strings.Join(e.Identities, ", "), e.Table)
}

// RepeatableMigrationError reports a repeatable migration whose body or
// bookkeeping failed.
type RepeatableMigrationError struct {
	// Identity names the repeatable migration.
	Identity string
	// Err is the failure.
	Err error
	// OutcomeUnknown reports that nothing says whether the body took effect,
	// such as after a commit that answered with an error. Its record stays in
	// state [RepeatableStateRunning].
	OutcomeUnknown bool
	// NoTransaction reports that the body ran outside a transaction.
	NoTransaction bool
}

// Error names the repeatable migration and what is known of its outcome.
func (e *RepeatableMigrationError) Error() string {
	switch {
	case e.OutcomeUnknown:
		return fmt.Sprintf("repeatable migration %s: whether it took effect is not known: %v", e.Identity, e.Err)
	case e.NoTransaction:
		return fmt.Sprintf("repeatable migration %s failed outside a transaction, so part of it may have taken effect: %v",
			e.Identity, e.Err)
	default:
		return fmt.Sprintf("repeatable migration %s failed and took no effect: %v", e.Identity, e.Err)
	}
}

// Unwrap returns the failure.
func (e *RepeatableMigrationError) Unwrap() error { return e.Err }

// repeatableWorkIdentities lists the identities of work, in order.
func repeatableWorkIdentities(work []RepeatableWork) []string {
	identities := make([]string, 0, len(work))
	for _, item := range work {
		identities = append(identities, item.Identity)
	}
	return identities
}
