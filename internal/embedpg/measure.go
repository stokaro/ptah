package embedpg

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/internal/embedgen"
)

// ColumnExists reports whether a column is there.
//
// Asked of the catalog rather than inferred from a failed query, because a
// query that failed for another reason -- a permission, a typo in the schema --
// would answer "no" and the plan would propose creating a column that is
// already there.
func ColumnExists(ctx context.Context, db *sql.DB, table, column string) (bool, error) {
	const query = `SELECT EXISTS (
		SELECT 1 FROM information_schema.columns
		WHERE table_name = $1 AND column_name = $2)`
	var exists bool
	if err := db.QueryRowContext(ctx, query, table, column).Scan(&exists); err != nil {
		return false, fmt.Errorf("read whether %s.%s exists: %w", table, column, err)
	}
	return exists, nil
}

// CountRows counts the source rows in scope.
//
// It returns a negative number rather than an error when the table is not there
// yet, because "the source does not exist" is a fact the plan reports as
// unknown rather than a reason to refuse to plan.
func CountRows(ctx context.Context, db *sql.DB, spec embedgen.Spec) (int64, error) {
	exists, err := tableExists(ctx, db, spec.Source.Table)
	if err != nil {
		return -1, err
	}
	if !exists {
		return -1, nil
	}
	source, err := NewSource(db, spec)
	if err != nil {
		return -1, err
	}
	// #nosec G201 -- PostgreSQL takes no bind parameter for a relation name.
	// The table comes from the specification and goes through quoteIdentifier;
	// the filter is the operator's own SQL, which is what the field is for.
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", source.qualifiedTable())
	if filter := strings.TrimSpace(spec.Source.Filter); filter != "" {
		query += " WHERE (" + filter + ")"
	}
	var count int64
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return -1, fmt.Errorf("count %s: %w", spec.Source.Table, err)
	}
	return count, nil
}

// tableExists reports whether a table is there.
func tableExists(ctx context.Context, db *sql.DB, table string) (bool, error) {
	const query = `SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_name = $1)`
	var exists bool
	if err := db.QueryRowContext(ctx, query, table).Scan(&exists); err != nil {
		return false, fmt.Errorf("read whether %s exists: %w", table, err)
	}
	return exists, nil
}

// VectorCapabilities reports what the server can do with vectors.
//
// A key is absent from the answer only when it could not be established at all.
// Present-and-false is "asked, and no", which the plan reports differently: one
// refuses a database that would have worked, the other promises one that will
// not.
func VectorCapabilities(ctx context.Context, db *sql.DB) (map[string]bool, error) {
	installed, err := extensionInstalled(ctx, db, "vector")
	if err != nil {
		return nil, err
	}
	capabilities := map[string]bool{"vector_type": installed}
	if !installed {
		// Without the extension there is no index method to ask about, and
		// answering "no index" would name the wrong problem.
		return capabilities, nil
	}
	methods, err := indexMethods(ctx, db)
	if err != nil {
		return nil, err
	}
	capabilities["vector_index"] = methods > 0
	return capabilities, nil
}

// extensionInstalled reports whether an extension is installed.
func extensionInstalled(ctx context.Context, db *sql.DB, name string) (bool, error) {
	const query = `SELECT EXISTS (SELECT 1 FROM pg_extension WHERE extname = $1)`
	var installed bool
	if err := db.QueryRowContext(ctx, query, name).Scan(&installed); err != nil {
		return false, fmt.Errorf("read whether the %s extension is installed: %w", name, err)
	}
	return installed, nil
}

// indexMethods counts the vector index methods the server offers.
func indexMethods(ctx context.Context, db *sql.DB) (int, error) {
	const query = `SELECT COUNT(*) FROM pg_am WHERE amname IN ('hnsw', 'ivfflat')`
	var count int
	if err := db.QueryRowContext(ctx, query).Scan(&count); err != nil {
		return 0, fmt.Errorf("read the available index methods: %w", err)
	}
	return count, nil
}
