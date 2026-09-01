package embedpg

import (
	"cmp"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/internal/embedcatchup"
	"go.5x5.cz/ptah/internal/embeddigest"
	"go.5x5.cz/ptah/internal/embedgen"
	"go.5x5.cz/ptah/internal/embedstore"
)

// Outbox is the PostgreSQL transactional outbox for one source table.
//
// The mechanism is a trigger rather than an application write, and that is the
// whole guarantee: the event row and the source change are the same
// transaction, so a change that committed has an event and a change that rolled
// back has none. An application that writes both does not have that property --
// it has two writes and a hope (stokaro/ptah#2068).
type Outbox struct {
	db   *sql.DB
	spec embedgen.Spec
}

// NewOutbox returns an outbox for a specification.
func NewOutbox(db *sql.DB, spec embedgen.Spec) (*Outbox, error) {
	if err := validateSource(spec); err != nil {
		return nil, err
	}
	return &Outbox{db: db, spec: spec}, nil
}

// TableName is what this outbox's table is called.
//
// Named for the source table it watches rather than for the generation,
// because two generations over one table share the changes: the events say
// what happened to a row, and what a row MEANS is the generation's business.
//
// The name carries the sanitized table for a reader and a digest of the
// QUALIFIED name for correctness. Both halves are needed. Without the digest,
// `public.docs` and `archive.docs` produced one outbox table, one capture
// function and one pair of trigger names -- and since Install issues CREATE OR
// REPLACE FUNCTION, preparing the second rewrote the first's capture function
// to read the second specification's columns, after which every insert and
// delete on the first source table failed with `record "new" has no field ...`
// (stokaro/ptah#2629). Without the sanitized table nobody can tell at a glance
// which outbox they are looking at.
//
// The digest covers the schema and the table separately rather than a joined
// string, so a schema called `a_b` with table `c` and a schema `a` with table
// `b_c` cannot fold onto one name -- the same reason a key's components are
// length-prefixed everywhere else in the lifecycle.
func (o *Outbox) TableName() string {
	identity := embeddigest.Short(
		embeddigest.Of(o.spec.Source.Schema, o.spec.Source.Table))
	return embedstore.TablePrefix + "outbox_" +
		sanitizeIdentifier(o.spec.Source.Table) + "_" + identity
}

// FunctionName is what this outbox's trigger function is called.
func (o *Outbox) FunctionName() string {
	return o.TableName() + "_capture"
}

// TriggerNames are what the triggers on the source table are called.
//
// Two of them, not one. An insert or a delete is always a change worth
// recording; an update is only worth recording when it touched a column the
// generation actually reads, and PostgreSQL's WHEN clause is what expresses
// that -- but a WHEN clause referring to OLD cannot sit on a trigger that also
// fires for INSERT.
func (o *Outbox) TriggerNames() []string {
	return []string{o.TableName() + "_write", o.TableName() + "_update"}
}

// Install creates the outbox table, its capture function and its trigger.
//
// Idempotent, because installing is what happens at the start of a run and a
// run can be restarted. It does NOT record a boundary: that is Horizon's job
// and the order matters -- installing after recording a boundary leaves changes
// in the gap between them captured by nothing.
func (o *Outbox) Install(ctx context.Context) error {
	for _, statement := range o.installStatements() {
		if _, err := o.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("install outbox for %s: %w", o.spec.Source.Table, err)
		}
	}
	return nil
}

// installStatements renders what Install runs.
func (o *Outbox) installStatements() []string {
	statements := []string{o.createTable(), o.createFunction()}
	statements = append(statements, o.dropTriggers()...)
	return append(statements, o.createWriteTrigger(), o.createUpdateTrigger())
}

// createTable renders the outbox table.
//
// The transaction identity is a column of its own beside the sequence, and it
// is the one reads are bounded by. A sequence is allocated when the row is
// inserted and a transaction becomes visible when it commits, so two events
// routinely commit out of sequence order -- a reader advancing a sequence cursor
// past a committed event steps over an earlier one still in flight.
func (o *Outbox) createTable() string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
		sequence BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
		xact BIGINT NOT NULL,
		row_key TEXT NOT NULL,
		operation TEXT NOT NULL,
		source_version TEXT,
		at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp()
	)`, quoteIdentifier(o.TableName()))
	// `at` is for a person reading the trail and is never the ordering key.
	// clock_timestamp() is taken at the write like the sequence is, so the two
	// agree on any tidy machine -- and it has microsecond resolution, so two
	// writes can share a value, and it follows the system clock, so it can go
	// backwards. A sequence does neither.
}

// createFunction renders the capture function.
//
// It writes a key and a version and nothing else. Capturing the row would make
// the outbox a second copy of the corpus, with its own retention and its own
// set of people who can read it -- and it would be tempting, because rereading
// the source costs a query and the row is right there in NEW.
func (o *Outbox) createFunction() string {
	return fmt.Sprintf(`CREATE OR REPLACE FUNCTION %s() RETURNS trigger AS $ptah$
		DECLARE
			captured_key jsonb;
			captured_version text;
		BEGIN
			IF (TG_OP = 'DELETE') THEN
				captured_key := to_jsonb(ARRAY[%s]);
				captured_version := %s;
			ELSE
				captured_key := to_jsonb(ARRAY[%s]);
				captured_version := %s;
			END IF;
			INSERT INTO %s (xact, row_key, operation, source_version)
			VALUES (pg_current_xact_id()::text::bigint, captured_key::text, lower(TG_OP), captured_version);
			RETURN NULL;
		END;
	$ptah$ LANGUAGE plpgsql`,
		quoteIdentifier(o.FunctionName()),
		o.keyExpression("OLD"), o.versionExpression("OLD"),
		o.keyExpression("NEW"), o.versionExpression("NEW"),
		quoteIdentifier(o.TableName()))
}

// keyExpression renders the key components as text, from OLD or NEW.
func (o *Outbox) keyExpression(record string) string {
	components := make([]string, 0, len(o.spec.Source.KeyFields))
	for _, field := range o.spec.Source.KeyFields {
		components = append(components, fmt.Sprintf("%s.%s::text", record, quoteIdentifier(field)))
	}
	return strings.Join(components, ", ")
}

// versionExpression renders the source version, or NULL under a strategy that
// establishes none.
func (o *Outbox) versionExpression(record string) string {
	field := strings.TrimSpace(o.spec.Source.VersionField)
	if field == "" {
		return "NULL"
	}
	return fmt.Sprintf("%s.%s::text", record, quoteIdentifier(field))
}

// dropTriggers removes a previous installation.
//
// CREATE TRIGGER has no IF NOT EXISTS, so reinstalling means dropping first.
// The drop is IF EXISTS because the first install has nothing to drop.
func (o *Outbox) dropTriggers() []string {
	statements := make([]string, 0, len(o.TriggerNames()))
	for _, name := range o.TriggerNames() {
		statements = append(statements, fmt.Sprintf("DROP TRIGGER IF EXISTS %s ON %s",
			quoteIdentifier(name), o.qualifiedSourceTable()))
	}
	return statements
}

// createWriteTrigger records every insert and every delete.
//
// AFTER, so a change the statement itself rolls back leaves no event, and FOR
// EACH ROW, because the events are about rows.
func (o *Outbox) createWriteTrigger() string {
	return fmt.Sprintf(
		"CREATE TRIGGER %s AFTER INSERT OR DELETE ON %s FOR EACH ROW EXECUTE FUNCTION %s()",
		quoteIdentifier(o.TriggerNames()[0]), o.qualifiedSourceTable(),
		quoteIdentifier(o.FunctionName()))
}

// createUpdateTrigger records an update only when it touched something the
// generation reads.
//
// This is not an optimization. A generation's vector column lives ON the source
// table, so Ptah's own writes are updates to it -- and an outbox that recorded
// them would hand catch-up an event for every vector it just wrote, which it
// would then reread, re-embed, and write again. Measured against a live server
// before this clause existed: the catch-up loop did not terminate.
//
// The columns compared are the ones that decide a vector: the key, the input
// fields, and the version. An application update to an unrelated column
// produces no event for the same reason -- the vector it would recompute is the
// vector already there.
func (o *Outbox) createUpdateTrigger() string {
	return fmt.Sprintf(
		"CREATE TRIGGER %s AFTER UPDATE ON %s FOR EACH ROW WHEN ((%s) IS DISTINCT FROM (%s)) "+
			"EXECUTE FUNCTION %s()",
		quoteIdentifier(o.TriggerNames()[1]), o.qualifiedSourceTable(),
		o.watchedColumns("OLD"), o.watchedColumns("NEW"),
		quoteIdentifier(o.FunctionName()))
}

// watchedColumns renders the columns whose change decides a vector.
func (o *Outbox) watchedColumns(record string) string {
	fields := append([]string(nil), o.spec.Source.KeyFields...)
	fields = append(fields, o.spec.Source.InputFields...)
	fields = append(fields, o.versionColumns()...)
	rendered := make([]string, 0, len(fields))
	for _, field := range fields {
		rendered = append(rendered, fmt.Sprintf("%s.%s", record, quoteIdentifier(field)))
	}
	return strings.Join(rendered, ", ")
}

// versionColumns names the source version column, and is empty under a strategy
// that establishes none.
func (o *Outbox) versionColumns() []string {
	if field := strings.TrimSpace(o.spec.Source.VersionField); field != "" {
		return []string{field}
	}
	return nil
}

// Installed reports whether the trigger is actually on the source table.
//
// Asked of the catalog rather than remembered, because "we installed it" is a
// statement about a past run and the question is about now: a table restored
// from a dump, or recreated by a migration, has no trigger and no memory of
// having had one.
func (o *Outbox) Installed(ctx context.Context) (bool, error) {
	// Both triggers, and counted rather than existence-checked: half an
	// installation captures half the changes, and the half it misses is
	// whichever one somebody dropped.
	// Scoped to the relation rather than to a bare relname: a same-named table
	// in another schema, carrying its own outbox, answered this question for a
	// source that had no trigger at all -- so a run reported itself as
	// capturing changes while every write went unrecorded (stokaro/ptah#2629).
	const query = `SELECT COUNT(*) FROM pg_trigger
		WHERE pg_trigger.tgname = ANY($1) AND pg_trigger.tgrelid = to_regclass($2)
			AND NOT pg_trigger.tgisinternal`
	names := o.TriggerNames()
	source := o.qualifiedSourceTable()
	var found int
	if err := o.db.QueryRowContext(ctx, query,
		triggerNameArray(names), source).Scan(&found); err != nil {
		return false, fmt.Errorf("read trigger state for %s: %w", source, err)
	}
	return found == len(names), nil
}

// triggerNameArray renders the trigger names as a PostgreSQL text array
// literal, so one query can ask about both.
func triggerNameArray(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, `"`+strings.ReplaceAll(name, `"`, `\"`)+`"`)
	}
	return "{" + strings.Join(quoted, ",") + "}"
}

// Horizon is the boundary below which every transaction has concluded.
//
// It is the current snapshot's xmin: every transaction identity below it has
// either committed and become visible, or aborted and left nothing. No row can
// appear below it afterwards, which is what makes advancing a cursor past it
// safe -- and is exactly what a sequence cannot promise.
func (o *Outbox) Horizon(ctx context.Context) (uint64, error) {
	var horizon uint64
	if err := o.db.QueryRowContext(ctx,
		`SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint`).Scan(&horizon); err != nil {
		return 0, fmt.Errorf("read the transaction horizon: %w", err)
	}
	return horizon, nil
}

// pendingPredicate selects the events a cursor still owes, below the horizon.
//
// The upper bound is not optional and is not the caller's to choose. An event
// written by a transaction that has not concluded is an event whose fate is
// unknown, and reading it would process a change that never happened.
//
// The lower bound is the pair rather than the transaction alone. Both halves of
// the pair are needed and neither is enough:
//
//   - the transaction, because a transaction still in flight has a sequence
//     already allocated. Resuming by sequence alone would step over an event
//     whose neighbours committed first, and it would never come back for it.
//   - the sequence, because a page is a LIMIT. Resuming by transaction alone
//     has to skip to the highest transaction the page held, and the events of
//     that transaction the page did not reach are then below the cursor.
//
// The second is stokaro/ptah#2628, measured at the default page size against an
// ordinary bulk update.
const pendingPredicate = `xact < $3 AND (xact > $1 OR (xact = $1 AND sequence >= $2))`

// Since reads the events a cursor still owes, up to the current horizon.
//
// Two orders appear here and they are not interchangeable. The SELECT orders by
// (xact, sequence) so that the page is a PREFIX of a total order, which is what
// lets the caller resume after the last event read without skipping one. The
// slice is then returned in SEQUENCE order, because that is the order the
// events actually happened in and the order this package's consumers document:
// a transaction can take its identity from an earlier write to some other table
// and reach this source afterwards, by which time a transaction with a later
// identity has already written and committed here, so transaction order would
// put those two the wrong way round.
//
// Sorting the page rather than the query is what keeps both properties. The
// sort is over one page, and Collapse sorts by sequence again on its own, so no
// consumer depends on this one having happened.
func (o *Outbox) Since(
	ctx context.Context, from embedcatchup.Cursor, limit int,
) ([]embedcatchup.Event, uint64, error) {
	if limit <= 0 {
		return nil, 0, fmt.Errorf("a catch-up limit of %d would read the whole outbox into memory", limit)
	}
	horizon, err := o.Horizon(ctx)
	if err != nil {
		return nil, 0, err
	}
	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation name, so
	// the table has to be interpolated. What stops that being an injection is
	// quoteIdentifier, which doubles an embedded quote and is measured against
	// a live server by a fixture whose column name holds one.
	query := fmt.Sprintf(
		`SELECT sequence, xact, row_key, operation, COALESCE(source_version, ''), at
		 FROM %s WHERE %s ORDER BY xact, sequence LIMIT $4`,
		quoteIdentifier(o.TableName()), pendingPredicate)
	rows, err := o.db.QueryContext(ctx, query, from.Transaction, from.Sequence, horizon, limit)
	if err != nil {
		return nil, 0, fmt.Errorf("read outbox for %s: %w", o.spec.Source.Table, err)
	}
	defer rows.Close()

	events, err := scanEvents(rows)
	if err != nil {
		return nil, 0, err
	}
	if err := rows.Err(); err != nil {
		return nil, 0, fmt.Errorf("read outbox for %s: %w", o.spec.Source.Table, err)
	}
	slices.SortStableFunc(events, func(left, right embedcatchup.Event) int {
		return cmp.Compare(left.Sequence, right.Sequence)
	})
	return events, horizon, nil
}

// Unprocessed counts the events between a cursor and the horizon.
func (o *Outbox) Unprocessed(ctx context.Context, from embedcatchup.Cursor) (int, error) {
	horizon, err := o.Horizon(ctx)
	if err != nil {
		return 0, err
	}
	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation name, so
	// the table has to be interpolated. What stops that being an injection is
	// quoteIdentifier, which doubles an embedded quote and is measured against
	// a live server by a fixture whose column name holds one.
	query := fmt.Sprintf(`SELECT COUNT(*) FROM %s WHERE %s`,
		quoteIdentifier(o.TableName()), pendingPredicate)
	var count int
	if err := o.db.QueryRowContext(ctx, query,
		from.Transaction, from.Sequence, horizon).Scan(&count); err != nil {
		return 0, fmt.Errorf("count outbox for %s: %w", o.spec.Source.Table, err)
	}
	return count, nil
}

// Prune removes events the run has processed.
//
// Bounded and policy-controlled, which the epic asks for: an outbox nobody
// prunes is a table that grows for as long as the application writes, and one
// pruned by time rather than by what was processed drops a tombstone a paused
// run still owes.
func (o *Outbox) Prune(ctx context.Context, before uint64) (int64, error) {
	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation name, so
	// the table has to be interpolated. What stops that being an injection is
	// quoteIdentifier, which doubles an embedded quote and is measured against
	// a live server by a fixture whose column name holds one.
	query := fmt.Sprintf(`DELETE FROM %s WHERE xact < $1`, quoteIdentifier(o.TableName()))
	result, err := o.db.ExecContext(ctx, query, before)
	if err != nil {
		return 0, fmt.Errorf("prune outbox for %s: %w", o.spec.Source.Table, err)
	}
	removed, err := result.RowsAffected()
	if err != nil {
		return 0, fmt.Errorf("prune outbox for %s: %w", o.spec.Source.Table, err)
	}
	return removed, nil
}

// qualifiedSourceTable renders the source table with its schema when it has one.
func (o *Outbox) qualifiedSourceTable() string {
	return qualify(o.spec.Source.Schema, o.spec.Source.Table)
}

// scanEvents reads the result set.
func scanEvents(rows *sql.Rows) ([]embedcatchup.Event, error) {
	var events []embedcatchup.Event
	for rows.Next() {
		var event embedcatchup.Event
		var encodedKey, operation string
		if err := rows.Scan(&event.Sequence, &event.Transaction, &encodedKey,
			&operation, &event.Version, &event.At); err != nil {
			return nil, fmt.Errorf("read outbox event: %w", err)
		}
		if err := json.Unmarshal([]byte(encodedKey), &event.Key); err != nil {
			return nil, fmt.Errorf("decode outbox key %q: %w", encodedKey, err)
		}
		event.Operation = embedcatchup.Operation(operation)
		event.At = event.At.UTC()
		events = append(events, event)
	}
	return events, nil
}

// sanitizeIdentifier folds a source table name into something a generated
// object name can hold.
//
// Non-alphanumerics become underscores, which can collide -- two different
// table names can fold onto one. What separates them is the digest TableName
// appends, which is taken over the ORIGINAL schema and table rather than over
// this folded form.
func sanitizeIdentifier(name string) string {
	var b strings.Builder
	for _, symbol := range strings.ToLower(name) {
		switch {
		case symbol >= 'a' && symbol <= 'z', symbol >= '0' && symbol <= '9', symbol == '_':
			b.WriteRune(symbol)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

// Uninstall removes the outbox: both triggers, the capture function, and the
// event table.
//
// It is the other half of [Outbox.Install], and until stokaro/ptah#2649 there
// was no other half. `retire` dropped a generation's index and columns and left
// the outbox alone, so both triggers went on firing on the operator's table for
// every write, the capture function stayed, and the event table grew with
// nothing that would ever read or trim it. The page that says when the cost
// ends -- "retire removes the generation and its bookkeeping" -- described a
// verb that did not.
//
// The order is triggers, then function, then table: a trigger outlives neither
// safely, and dropping the function while a trigger still names it leaves a
// write erroring on the operator's table rather than on ours.
//
// Every statement is IF EXISTS, so uninstalling twice is a no-op and a partial
// installation is removed rather than refused.
func (o *Outbox) Uninstall(ctx context.Context) error {
	statements := append([]string(nil), o.dropTriggers()...)
	statements = append(statements,
		fmt.Sprintf("DROP FUNCTION IF EXISTS %s()", quoteIdentifier(o.FunctionName())),
		fmt.Sprintf("DROP TABLE IF EXISTS %s", quoteIdentifier(o.TableName())))
	for _, statement := range statements {
		if _, err := o.db.ExecContext(ctx, statement); err != nil {
			return fmt.Errorf("uninstall outbox for %s: %w", o.spec.Source.Table, err)
		}
	}
	return nil
}
