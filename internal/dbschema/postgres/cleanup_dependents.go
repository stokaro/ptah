package postgres

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/internal/dbreset"
)

// dependentRelationsQuery asks which relations are defined in terms of the
// selected ones. It reaches views and materialized views, which are the
// dependents a scoped cleanup meets: both store their definition in pg_rewrite,
// and pg_depend links that rule back to every relation the definition reads.
//
// Placeholders are built by the caller rather than passed as an array, because
// the two PostgreSQL-family drivers in use disagree about how a Go slice binds
// to ANY($1) and a scoped cleanup should not depend on that.
const dependentRelationsQuery = `
	SELECT DISTINCT source.relname, dependent.relname, dependent.relkind
	FROM pg_depend d
	JOIN pg_rewrite r ON r.oid = d.objid
	JOIN pg_class dependent ON dependent.oid = r.ev_class
	JOIN pg_class source ON source.oid = d.refobjid
	JOIN pg_namespace n ON n.oid = source.relnamespace
	WHERE n.nspname = current_schema()
	  AND dependent.oid <> source.oid
	  AND source.relname IN (%s)`

// CleanupDependents lists, through q, the relations of the session's current
// schema that are defined in terms of the relations named, each with the
// relation it reads, in the order the catalog returns them. A scoped cleanup
// reads them to name, before it drops anything, the dependents its selection
// leaves behind. It answers nil for no names, and an error when the catalog
// cannot be read.
//
// The writer offers it on every dialect it serves, without a descriptor
// gate: internal/schemaclean asks for it only for a dialect whose cleanup is
// schema-scoped (descriptor.Cleanup.SchemaScoped), so a gate here would repeat
// that check and decide nothing.
func (w *PostgreSQLWriter) CleanupDependents(ctx context.Context, q dbreset.Querier, names []string) ([]dbreset.Dependent, error) {
	if len(names) == 0 {
		return nil, nil
	}
	placeholders := make([]string, 0, len(names))
	args := make([]any, 0, len(names))
	for i, name := range names {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, name)
	}
	rows, err := q.QueryContext(ctx, fmt.Sprintf(dependentRelationsQuery, strings.Join(placeholders, ", ")), args...)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var dependents []dbreset.Dependent
	for rows.Next() {
		var source, dependent, kind string
		if err := rows.Scan(&source, &dependent, &kind); err != nil {
			return nil, err
		}
		dependents = append(dependents, dbreset.Dependent{Source: source, Name: dependent, Kind: relationKindName(kind)})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return dependents, nil
}

// relationKindName spells a pg_class.relkind for a diagnostic.
func relationKindName(kind string) string {
	switch kind {
	case "v":
		return "view"
	case "m":
		return "materialized view"
	case "r", "p":
		return "table"
	default:
		return "object"
	}
}
