package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/pgdefaultacl"
	"ptah.run/internal/sqlident"
)

// CleanupCatalog returns what PostgreSQL's catalog answers a cleanup plan:
// the runtime objects the schema reader does not describe, the depth of each
// view and the relation locks of a scoped plan. It is nil on a dialect whose
// descriptor does not state that the cleanup was measured against its
// catalog (descriptor.Cleanup.CatalogInventory): CockroachDB, YugabyteDB and
// Spanner answer the PostgreSQL wire protocol through the same writer, and
// the inventory was measured on PostgreSQL alone.
func (w *PostgreSQLWriter) CleanupCatalog() dbreset.CleanupCatalog {
	if described, _ := platform.Lookup(w.dialect); !described.Cleanup.CatalogInventory {
		return nil
	}
	return postgresCleanupCatalog{}
}

// postgresCleanupCatalog reads PostgreSQL's catalog for a cleanup plan.
type postgresCleanupCatalog struct{}

// Objects lists the sequences, foreign tables, procedures, aggregates,
// window functions and collations of schema, which the writer's cleanup
// drops and the schema reader does not describe, and the default privileges
// the cleanup revokes. The empty schema is public.
func (postgresCleanupCatalog) Objects(ctx context.Context, q dbreset.Querier, schema string) ([]dbreset.CleanupObject, error) {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		schema = "public"
	}
	rows, err := q.QueryContext(ctx, `
		WITH sequence_objects AS (
			SELECT
				c.oid,
				n.nspname,
				c.relname,
				owner_tbl.relname AS owner_table,
				CASE
					WHEN dep.refobjid IS NULL THEN false
					WHEN dep.deptype = 'i' THEN true
					WHEN owner_col_type.typtype = 'd' THEN false
					ELSE EXISTS (
						SELECT 1
						FROM pg_attrdef ad
						JOIN pg_depend dd ON dd.classid = 'pg_attrdef'::regclass
							AND dd.objid = ad.oid
							AND dd.refclassid = 'pg_class'::regclass
							AND dd.refobjid = c.oid
							AND dd.deptype = 'n'
						WHERE ad.adrelid = dep.refobjid AND ad.adnum = dep.refobjsubid
					)
				END AS is_implicit
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			LEFT JOIN pg_depend dep ON dep.objid = c.oid
				AND dep.classid = 'pg_class'::regclass
				AND dep.refclassid = 'pg_class'::regclass
				AND dep.deptype IN ('a', 'i')
				AND dep.refobjsubid > 0
			LEFT JOIN pg_class owner_tbl ON owner_tbl.oid = dep.refobjid
			LEFT JOIN pg_attribute owner_col ON owner_col.attrelid = dep.refobjid
				AND owner_col.attnum = dep.refobjsubid
			LEFT JOIN pg_type owner_col_type ON owner_col_type.oid = owner_col.atttypid
			WHERE n.nspname = $1 AND c.relkind = 'S'
		),
		runtime_objects AS (
			SELECT
				'sequence'::text AS object_kind,
				nspname AS object_schema,
				relname AS object_name,
				relname AS selector_name,
				is_implicit,
				COALESCE(owner_table, '') AS owner_table,
				format('DROP SEQUENCE IF EXISTS %I.%I RESTRICT', nspname, relname) AS drop_statement
			FROM sequence_objects

			UNION ALL

			SELECT
				'foreign_table',
				n.nspname,
				c.relname,
				c.relname,
				false,
				'',
				format('DROP FOREIGN TABLE IF EXISTS %I.%I RESTRICT', n.nspname, c.relname)
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relkind = 'f'

			UNION ALL

			SELECT
				CASE p.prokind
					WHEN 'p' THEN 'procedure'
					WHEN 'a' THEN 'aggregate'
					ELSE 'function'
				END,
				n.nspname,
				format('%s(%s)', p.proname, pg_get_function_identity_arguments(p.oid)),
				p.proname,
				false,
				'',
				format(
					'DROP %s IF EXISTS %I.%I(%s) RESTRICT',
					CASE p.prokind
						WHEN 'p' THEN 'PROCEDURE'
						WHEN 'a' THEN 'AGGREGATE'
						ELSE 'FUNCTION'
					END,
					n.nspname,
					p.proname,
					pg_get_function_identity_arguments(p.oid)
				)
			FROM pg_proc p
			JOIN pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = $1
			  AND p.prokind IN ('p', 'a', 'w')
			  AND NOT EXISTS (
				SELECT 1
				FROM pg_depend d
				WHERE d.classid = 'pg_proc'::regclass
				  AND d.objid = p.oid
				  AND d.deptype = 'i'
			  )

			UNION ALL

			SELECT
				'collation',
				n.nspname,
				c.collname,
				c.collname,
				false,
				'',
				format('DROP COLLATION IF EXISTS %I.%I RESTRICT', n.nspname, c.collname)
			FROM pg_collation c
			JOIN pg_namespace n ON n.oid = c.collnamespace
			WHERE n.nspname = $1
		)
		SELECT object_kind, object_schema, object_name, selector_name, is_implicit, owner_table, drop_statement
		FROM runtime_objects
		ORDER BY object_kind, object_name`, schema)
	if err != nil {
		return nil, fmt.Errorf("inspect PostgreSQL runtime objects: %w", err)
	}
	defer rows.Close()

	objects := make([]dbreset.CleanupObject, 0)
	for rows.Next() {
		var kind string
		var schema string
		var name string
		var selectorName string
		var implicit bool
		var table sql.NullString
		var command string
		if err := rows.Scan(&kind, &schema, &name, &selectorName, &implicit, &table, &command); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL runtime object: %w", err)
		}
		objectKind, ok := cleanupObjectKind(kind)
		if !ok {
			return nil, fmt.Errorf("inspect PostgreSQL runtime objects: unsupported object kind %q", kind)
		}
		objects = append(objects, dbreset.CleanupObject{
			Kind:         objectKind,
			Schema:       schema,
			Table:        table.String,
			Name:         name,
			SelectorName: selectorName,
			Implicit:     implicit,
			Statement:    command,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL runtime objects: %w", err)
	}
	revokes, err := cleanupDefaultPrivileges(ctx, q, schema)
	if err != nil {
		return nil, err
	}
	return append(objects, revokes...), nil
}

// cleanupObjectKinds are the kinds the runtime query reports, each the plan's
// own name for it.
var cleanupObjectKinds = []string{"aggregate", "collation", "default_privilege", "foreign_table", "function", "procedure", "sequence"}

// cleanupObjectKind folds a kind the runtime query reports onto the plan's
// name for it, and reports false for one the plan has no name for.
func cleanupObjectKind(kind string) (string, bool) {
	kind = strings.ToLower(strings.TrimSpace(kind))
	return kind, slices.Contains(cleanupObjectKinds, kind)
}

// cleanupDefaultPrivileges names each default privilege the cleanup revokes,
// one object per grantee, with the statement the writer's cleanup runs for
// it.
//
// Both read through [pgdefaultacl.ReadRevokes], so the plan prints the revoke
// DropAllTables executes and a narrowed plan executes the same statement. A
// read of its own spelled the revoke differently and exploded the ACL with
// aclexplode, which answers nothing on CockroachDB v25.4 (stokaro/ptah#3832).
func cleanupDefaultPrivileges(ctx context.Context, q dbreset.Querier, schema string) ([]dbreset.CleanupObject, error) {
	revokes, err := pgdefaultacl.ReadRevokes(ctx, q, []string{schema})
	if err != nil {
		return nil, fmt.Errorf("inspect PostgreSQL default privileges: %w", err)
	}
	objects := make([]dbreset.CleanupObject, 0, len(revokes))
	for _, revoke := range revokes {
		objects = append(objects, dbreset.CleanupObject{
			Kind:         "default_privilege",
			Schema:       revoke.Schema,
			Name:         revoke.Name(),
			SelectorName: revoke.GrantorName() + ":" + revoke.Class + ":" + revoke.GranteeName(),
			Statement:    revoke.Statement,
		})
	}
	return objects, nil
}

// ViewDepths answers, for each view and materialized view of schema, the
// longest chain of views of the schema it reads through, through pg_rewrite
// and pg_depend, and an empty list, not nil, for a schema with no view: the
// engine orders views. The empty schema is public.
func (postgresCleanupCatalog) ViewDepths(ctx context.Context, q dbreset.Querier, schema string) ([]dbreset.ViewDepth, error) {
	schema = strings.TrimSpace(schema)
	if schema == "" {
		schema = "public"
	}
	rows, err := q.QueryContext(ctx, `
		WITH RECURSIVE
		managed_views AS (
			SELECT c.oid, c.relnamespace, c.relname, c.relkind
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = $1 AND c.relkind IN ('v', 'm')
		),
		view_dependencies AS (
			SELECT
				dependent.oid AS dependent_oid,
				referenced.oid AS referenced_oid
			FROM pg_rewrite rewrite
			JOIN managed_views dependent ON dependent.oid = rewrite.ev_class
			JOIN pg_depend dependency
				ON dependency.classid = 'pg_rewrite'::regclass
			   AND dependency.objid = rewrite.oid
			   AND dependency.refclassid = 'pg_class'::regclass
			JOIN managed_views referenced ON referenced.oid = dependency.refobjid
			WHERE dependent.oid <> referenced.oid
		),
		view_depths AS (
			SELECT view.oid, 0 AS depth
			FROM managed_views view
			WHERE NOT EXISTS (
				SELECT 1
				FROM view_dependencies dependency
				WHERE dependency.dependent_oid = view.oid
			)

			UNION ALL

			SELECT dependency.dependent_oid, depth.depth + 1
			FROM view_depths depth
			JOIN view_dependencies dependency
				ON dependency.referenced_oid = depth.oid
		)
		SELECT
			view.relname,
			view.relkind,
			COALESCE(MAX(depth.depth), 0)
		FROM managed_views view
		LEFT JOIN view_depths depth ON depth.oid = view.oid
		GROUP BY view.oid, view.relname, view.relkind
		ORDER BY view.relname
	`, schema)
	if err != nil {
		return nil, fmt.Errorf("inspect PostgreSQL view dependencies before cleanup: %w", err)
	}
	defer rows.Close()

	depths := make([]dbreset.ViewDepth, 0)
	for rows.Next() {
		var name string
		var relkind string
		var depth int
		if err := rows.Scan(&name, &relkind, &depth); err != nil {
			return nil, fmt.Errorf("scan PostgreSQL view dependency: %w", err)
		}
		kind := "view"
		if relkind == "m" {
			kind = "materialized_view"
		}
		depths = append(depths, dbreset.ViewDepth{Kind: kind, Name: name, Depth: depth})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate PostgreSQL view dependencies: %w", err)
	}
	return depths, nil
}

// LockRelations locks every relation a scoped plan drops in ACCESS EXCLUSIVE
// mode, in one statement and in name order, so a concurrent session cannot
// add a dependent between the check and the drops.
func (postgresCleanupCatalog) LockRelations(ctx context.Context, executor dbreset.Executor, relations []dbreset.Relation) error {
	qualified := make([]string, 0, len(relations))
	for _, relation := range relations {
		name := sqlident.Qualified(platform.Postgres, relation.Schema, relation.Name)
		if !slices.Contains(qualified, name) {
			qualified = append(qualified, name)
		}
	}
	if len(qualified) == 0 {
		return nil
	}
	slices.Sort(qualified)
	statement := "LOCK TABLE " + strings.Join(qualified, ", ") + " IN ACCESS EXCLUSIVE MODE"
	if err := executor.ExecuteSQL(ctx, statement); err != nil {
		return fmt.Errorf("lock planned schema cleanup relations: %w", err)
	}
	return nil
}
