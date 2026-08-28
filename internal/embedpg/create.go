package embedpg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

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
func EnsureTarget(ctx context.Context, db *sql.DB, spec embedgen.Spec) error {
	objects, err := spec.TargetObjects()
	if err != nil {
		return err
	}
	if err := requireVectorExtension(ctx, db); err != nil {
		return err
	}

	table := qualifiedTargetTable(spec)
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
	return nil
}

// requireVectorExtension refuses before the ALTER rather than after it.
//
// Ptah does not install the extension. CREATE EXTENSION is a database-wide,
// privileged act, and a migration tool taking it on behalf of an operator who
// did not ask is the kind of surprise this repository refuses elsewhere. The
// refusal carries the statement to run, so being told is one copy away from
// being fixed.
func requireVectorExtension(ctx context.Context, db *sql.DB) error {
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

// qualifiedTargetTable renders the target table with its schema when it has one.
func qualifiedTargetTable(spec embedgen.Spec) string {
	if schema := strings.TrimSpace(spec.Target.Schema); schema != "" {
		return quoteIdentifier(schema) + "." + quoteIdentifier(spec.Target.Table)
	}
	return quoteIdentifier(spec.Target.Table)
}

// quoteLiteral renders a string the way PostgreSQL reads one.
func quoteLiteral(text string) string {
	return "'" + strings.ReplaceAll(text, "'", "''") + "'"
}
