package postgres

import (
	"context"
	"fmt"
	"slices"
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

// externalRelationDependentsQuery asks which views and materialized views read
// a relation of the schemas named, wherever the dependent lives, through the
// pg_rewrite rule that stores its definition. The caller keeps the pairs whose
// relation the cleanup drops and whose dependent it does not.
const externalRelationDependentsQuery = `
	SELECT DISTINCT source_ns.nspname, source.relname, dependent_ns.nspname, dependent.relname, dependent.relkind
	FROM pg_depend d
	JOIN pg_rewrite r ON r.oid = d.objid
	JOIN pg_class dependent ON dependent.oid = r.ev_class
	JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent.relnamespace
	JOIN pg_class source ON source.oid = d.refobjid
	JOIN pg_namespace source_ns ON source_ns.oid = source.relnamespace
	WHERE dependent.oid <> source.oid
	  AND source_ns.nspname IN (%s)
	ORDER BY 1, 2, 3, 4`

// cleanupRelationKinds are the object kinds of a cleanup that a view can read.
var cleanupRelationKinds = []string{"table", "view", "materialized view", "foreign table", "sequence"}

// refuseExternalCleanupDependents refuses the cleanup of objects, before it
// drops any, when a view or materialized view outside objects reads a relation
// in them. It runs where the server commits each drop as it runs, so a refusal
// that came at that relation's drop would keep every drop before it: a
// dependent the cleanup leaves behind is reported here, while nothing is
// dropped yet, as the rollback reports it on a server that holds the drops in
// one transaction. Other dependencies, such as a routine body in another
// schema, the catalog does not report; the server refuses those at the drop.
func refuseExternalCleanupDependents(ctx context.Context, q cleanupConn, objects []postgresCleanupObject) error {
	type relation struct{ schema, name string }
	dropped := make(map[relation]string)
	var schemas []string
	for _, object := range objects {
		if object.Qualifier != "" || !slices.Contains(cleanupRelationKinds, object.Kind) {
			continue
		}
		dropped[relation{object.Schema, object.Name}] = object.Kind
		if !slices.Contains(schemas, object.Schema) {
			schemas = append(schemas, object.Schema)
		}
	}
	if len(schemas) == 0 {
		return nil
	}
	placeholders := make([]string, 0, len(schemas))
	args := make([]any, 0, len(schemas))
	for i, schema := range schemas {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, schema)
	}
	rows, err := q.QueryContext(ctx, fmt.Sprintf(externalRelationDependentsQuery, strings.Join(placeholders, ", ")), args...)
	if err != nil {
		return fmt.Errorf("failed to read the views that depend on the cleanup's relations: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var blocked []string
	for rows.Next() {
		var source, dependent relation
		var kind string
		if err := rows.Scan(&source.schema, &source.name, &dependent.schema, &dependent.name, &kind); err != nil {
			return fmt.Errorf("failed to read the views that depend on the cleanup's relations: %w", err)
		}
		sourceKind, drops := dropped[source]
		if _, alsoDropped := dropped[dependent]; !drops || alsoDropped {
			continue
		}
		blocked = append(blocked, fmt.Sprintf("%s %q.%q reads %s %q.%q",
			relationKindName(kind), dependent.schema, dependent.name, sourceKind, source.schema, source.name))
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("failed to read the views that depend on the cleanup's relations: %w", err)
	}
	if len(blocked) == 0 {
		return nil
	}
	return fmt.Errorf(
		"a view the cleanup does not drop reads a relation it drops: %s; the server commits each drop as it "+
			"runs, so the cleanup refuses before it drops anything",
		strings.Join(blocked, "; "),
	)
}
