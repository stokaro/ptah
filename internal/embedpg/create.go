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
func EnsureTarget(ctx context.Context, db indexDatabase, spec embedgen.Spec) error {
	objects, err := spec.TargetObjects()
	if err != nil {
		return err
	}
	if err := requireVectorExtension(ctx, db); err != nil {
		return err
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
