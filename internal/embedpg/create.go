package embedpg

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/internal/embeddigest"
	"go.5x5.cz/ptah/internal/embedgen"
)

// EnsureTarget creates the generation's vector column and the four metadata
// columns beside it.
//
// It exists because nothing did. `Spec.TargetObjects` has always derived what a
// generation needs, and every caller of it read the answer -- to verify a
// column, to retire one -- while the only ALTER TABLE in the tree was the DROP
// in RetireColumns. Ptah dropped the column it never created, `prepare` reported
// success without it, and the lifecycle ran only against databases whose
// operator, or whose test fixture, had written the DDL by hand
// (stokaro/ptah#2390).
//
// Idempotent, because a worker starting is the normal time to call this and
// several of them start at once -- the same reasoning EnsureSchema carries.
func EnsureTarget(ctx context.Context, db targetDatabase, spec embedgen.Spec) error {
	objects, err := spec.TargetObjects()
	if err != nil {
		return err
	}
	if err := requireVectorExtension(ctx, db); err != nil {
		return err
	}
	if objects.OwnsTable {
		if err := ensureOwnedTable(ctx, db, spec, objects); err != nil {
			return err
		}
	}

	table := qualify(spec.Target.Schema, spec.Target.Table)
	additions := []string{fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s %s",
		quoteIdentifier(spec.Target.Column), objects.Column.Type)}
	for _, suffix := range MetadataSuffixes() {
		additions = append(additions, fmt.Sprintf("ADD COLUMN IF NOT EXISTS %s TEXT",
			quoteIdentifier(spec.Target.Column+suffix)))
	}
	statement := fmt.Sprintf("ALTER TABLE %s %s", table, strings.Join(additions, ", "))
	if _, err := db.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("create the target columns: %w", err)
	}

	// The comment names the generation, the model and the metric. It is the one
	// place a person reading the table in psql learns what the column holds --
	// a `vector(384)` called `embedding` says nothing about which corpus it is.
	comment := fmt.Sprintf("COMMENT ON COLUMN %s.%s IS %s",
		table, quoteIdentifier(spec.Target.Column), quoteLiteral(objects.Column.Comment))
	if _, err := db.ExecContext(ctx, comment); err != nil {
		return fmt.Errorf("comment the target column: %w", err)
	}
	return refuseAnotherGenerationsColumn(ctx, db, spec)
}

// refuseAnotherGenerationsColumn says so before any embedding is paid for.
//
// The write path refuses a row belonging to another generation, which is where
// the guarantee lives -- but it refuses one row at a time, in the middle of a
// backfill, after the provider has already been called for that batch. A
// generation whose column is somebody else's is a specification to edit, and
// the moment to learn that is before the run starts (stokaro/ptah#2391).
//
// Two generations over one table need two columns. That is Decision 6, and it
// is what makes a cutover a pointer move rather than a data migration: the
// previous generation's vectors are still there to go back to.
func refuseAnotherGenerationsColumn(
	ctx context.Context, db indexDatabase, spec embedgen.Spec,
) error {
	// #nosec G201 -- identifiers from the specification, through quoteIdentifier.
	query := fmt.Sprintf(
		"SELECT DISTINCT %s FROM %s WHERE %s IS NOT NULL AND %s <> $1 LIMIT 1",
		quoteIdentifier(spec.Target.Column+GenerationSuffix), qualify(spec.Target.Schema, spec.Target.Table),
		quoteIdentifier(spec.Target.Column+GenerationSuffix),
		quoteIdentifier(spec.Target.Column+GenerationSuffix))

	var occupant string
	switch err := db.QueryRowContext(ctx, query, spec.Identity().Digest).Scan(&occupant); {
	case errors.Is(err, sql.ErrNoRows):
		return nil
	case err != nil:
		return fmt.Errorf("read the target generation: %w", err)
	}
	return fmt.Errorf(
		"column %q on %s holds generation %s, and this run is generation %s: a generation "+
			"writes its own column so the previous one is still there to go back to. "+
			"Give this one its own target.column in the specification",
		spec.Target.Column, spec.Target.Table,
		embeddigest.Short(occupant), spec.Identity().Short())
}

// targetDatabase is the SQL surface target preparation needs.
//
// It is indexDatabase plus the ability to open a transaction, and the two
// cannot be one interface: EnsureIndex issues CREATE INDEX CONCURRENTLY, which
// PostgreSQL refuses inside a transaction block, while creating a relation of
// Ptah's own and marking it as Ptah's have to land together or not at all.
// Both *sql.DB and *sql.Conn satisfy this.
type targetDatabase interface {
	indexDatabase
	BeginTx(context.Context, *sql.TxOptions) (*sql.Tx, error)
}

// ensureOwnedTable creates the relation a LayoutOwnTable generation stores its
// vectors in, keyed by the source key and referencing it.
//
// Everything here is in one transaction because the relation and the comment
// declaring it Ptah's are one fact. Written as two statements, a process that
// died between them would leave a relation Ptah created and can no longer
// prove it created: the next prepare would refuse it as somebody else's, and
// retirement would refuse to drop it, so an operator would have to remove by
// hand a table nothing else in the system admits to owning.
//
// The key columns are not rendered from a type mapping of our own. CREATE
// TABLE ... AS SELECT takes them from the source relation, so a domain, an
// enumerated type, a collation and a citext key all arrive as whatever the
// application actually declared -- which is also the condition the foreign key
// below needs in order to be creatable at all.
func ensureOwnedTable(
	ctx context.Context, db targetDatabase, spec embedgen.Spec, objects embedgen.TargetObjects,
) error {
	table := qualify(spec.Target.Schema, spec.Target.Table)
	source := qualify(spec.Source.Schema, spec.Source.Table)
	keys := make([]string, 0, len(spec.Source.KeyFields))
	for _, key := range spec.Source.KeyFields {
		keys = append(keys, quoteIdentifier(key))
	}
	keyList := strings.Join(keys, ", ")

	transaction, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("create the target table: %w", err)
	}
	defer func() { _ = transaction.Rollback() }()

	if err := refuseAnotherOwnersTable(ctx, transaction, table, objects.TableComment); err != nil {
		return err
	}
	// #nosec G201 -- relation and column names from the specification, through
	// qualify and quoteIdentifier.
	create := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s AS SELECT %s FROM %s WITH NO DATA",
		table, keyList, source)
	if _, err := transaction.ExecContext(ctx, create); err != nil {
		return fmt.Errorf("create the target table %s: %w", table, err)
	}
	comment := fmt.Sprintf("COMMENT ON TABLE %s IS %s", table, quoteLiteral(objects.TableComment))
	if _, err := transaction.ExecContext(ctx, comment); err != nil {
		return fmt.Errorf("mark the target table %s as Ptah's: %w", table, err)
	}
	if err := ensureOwnedTableKey(ctx, transaction, table, keyList); err != nil {
		return err
	}
	if err := ensureOwnedTableReference(
		ctx, transaction, table, source, keyList, objects.ForeignKeyName); err != nil {
		return err
	}
	if err := transaction.Commit(); err != nil {
		return fmt.Errorf("create the target table %s: %w", table, err)
	}
	return nil
}

// refuseAnotherOwnersTable refuses to adopt a relation Ptah did not create for
// this generation.
//
// A relation already there carrying no comment of ours is the application's,
// and writing our marker onto it would both destroy whatever the application's
// own comment said and authorize retirement to DROP it -- with every row the
// application keeps in it. One carrying another generation's marker is that
// generation's storage, and Decision 6 is that a generation never writes over
// another's.
//
// A relation that does not exist is not a refusal: creating it is the point.
func refuseAnotherOwnersTable(
	ctx context.Context, transaction *sql.Tx, table, want string,
) error {
	const query = `SELECT to_regclass($1) IS NOT NULL,
		COALESCE(obj_description(to_regclass($1), 'pg_class'), '')`
	var exists bool
	var found string
	if err := transaction.QueryRowContext(ctx, query, table).Scan(&exists, &found); err != nil {
		return fmt.Errorf("read the target table %s: %w", table, err)
	}
	switch {
	case !exists, found == want:
		return nil
	case found == "":
		return fmt.Errorf(
			"relation %s already exists and Ptah did not create it: under layout %q Ptah "+
				"creates the target relation and drops it when the generation is retired, "+
				"so it will not adopt one it cannot prove is its own. Name a relation that "+
				"does not exist, or use the default layout to put the columns on this one",
			table, string(embedgen.LayoutOwnTable))
	default:
		return fmt.Errorf(
			"relation %s belongs to another generation: it is commented %q and this run "+
				"wants %q. Two generations each store their own vectors, which is what makes "+
				"a cutover a pointer move rather than a data migration: give this one its own "+
				"target.table in the specification",
			table, found, want)
	}
}

// ensureOwnedTableKey gives the relation its primary key.
//
// Guarded by asking whether the relation has one rather than by IF NOT EXISTS,
// which ALTER TABLE ... ADD PRIMARY KEY does not take: a second run would
// answer `multiple primary keys for table are not allowed` and a prepare that
// is documented idempotent would fail on its retry.
//
// The key also makes the columns NOT NULL, which CREATE TABLE ... AS does not
// carry over from the source.
func ensureOwnedTableKey(ctx context.Context, transaction *sql.Tx, table, keyList string) error {
	const query = `SELECT EXISTS (
		SELECT 1 FROM pg_constraint WHERE conrelid = to_regclass($1) AND contype = 'p')`
	var present bool
	if err := transaction.QueryRowContext(ctx, query, table).Scan(&present); err != nil {
		return fmt.Errorf("read the target table's key on %s: %w", table, err)
	}
	if present {
		return nil
	}
	// #nosec G201 -- relation and column names from the specification, through
	// qualify and quoteIdentifier.
	statement := fmt.Sprintf("ALTER TABLE %s ADD PRIMARY KEY (%s)", table, keyList)
	if _, err := transaction.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf("key the target table %s: %w", table, err)
	}
	return nil
}

// ensureOwnedTableReference ties the relation to the source rows its keys name.
//
// ON DELETE CASCADE, so the application deleting a source row takes that row's
// vectors with it rather than leaving a vector addressed by a key nothing has.
// The alternative, refusing the delete, would be Ptah standing in the way of
// the application's own writes.
//
// It does not make the coverage check that reports rows outside the
// generation's source scope unreachable, which is the reason to say what
// CASCADE covers and what it does not: it removes the rows whose SOURCE ROW is
// gone, and leaves the rows whose source row is still there and no longer
// passes the specification's filter. The second is the case that check exists
// for.
//
// Guarded by asking whether the relation already references the source, rather
// than by the constraint's name: PostgreSQL truncates an identifier at
// NAMEDATALEN, so a long table and column would be created under a name the
// lookup then failed to find, and the ADD would answer `already exists` on
// every retry.
func ensureOwnedTableReference(
	ctx context.Context, transaction *sql.Tx, table, source, keyList, name string,
) error {
	const query = `SELECT EXISTS (
		SELECT 1 FROM pg_constraint
		WHERE conrelid = to_regclass($1) AND contype = 'f' AND confrelid = to_regclass($2))`
	var present bool
	if err := transaction.QueryRowContext(ctx, query, table, source).Scan(&present); err != nil {
		return fmt.Errorf("read the target table's reference on %s: %w", table, err)
	}
	if present {
		return nil
	}
	// #nosec G201 -- relation, column and constraint names from the
	// specification, through qualify, quoteIdentifier and embedgen.ForeignKeyName.
	statement := fmt.Sprintf(
		"ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s (%s) ON DELETE CASCADE",
		table, quoteIdentifier(name), keyList, source, keyList)
	if _, err := transaction.ExecContext(ctx, statement); err != nil {
		return fmt.Errorf(
			"reference %s from the target table %s: %w. The key fields have to be unique on "+
				"the source, because a vector is addressed by them",
			source, table, err)
	}
	return nil
}

// requireVectorExtension refuses before the ALTER rather than after it.
//
// Ptah does not install the extension. CREATE EXTENSION is a database-wide,
// privileged act, and a migration tool taking it on behalf of an operator who
// did not ask is the kind of surprise this repository refuses elsewhere. The
// refusal carries the statement to run, so being told is one copy away from
// being fixed.
func requireVectorExtension(ctx context.Context, db indexDatabase) error {
	installed, err := extensionInstalled(ctx, db, "vector")
	if err != nil {
		return err
	}
	if installed {
		return nil
	}
	return fmt.Errorf(
		"the target database has no pgvector: a generation stores vectors and there is " +
			"nowhere to put them. Install it with `CREATE EXTENSION vector`, which needs " +
			"a privilege Ptah does not assume")
}

// quoteLiteral renders a string the way PostgreSQL reads one.
func quoteLiteral(text string) string {
	return "'" + strings.ReplaceAll(text, "'", "''") + "'"
}
