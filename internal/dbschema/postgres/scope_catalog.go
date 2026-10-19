package postgres

import (
	"context"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/pgsystemschema"
)

// ScopeCatalog returns what the PostgreSQL family's catalog answers about the
// schemas and base tables a connection reaches, read through pg_namespace and
// pg_class. It is offered on every dialect the writer serves: PostgreSQL,
// CockroachDB, YugabyteDB and Spanner each answer these reads, with the
// differences the predicates below name. Which schemas belong to the realm is
// the writer's dialect's, as its cleanup reads them.
func (w *PostgreSQLWriter) ScopeCatalog() dbreset.ScopeCatalog {
	return postgresScopeCatalog{dialect: w.dialect}
}

// postgresScopeCatalog reads the PostgreSQL family's catalog for the schemas
// and base tables of a connection's scope.
type postgresScopeCatalog struct {
	// dialect is the member of the family whose system schemas a realm read
	// leaves out.
	dialect string
}

// Tables lists the base tables of schema, or of the session's
// current_schema() when schema is empty, in name order.
//
// relkind 'r' and 'p' are ordinary and partitioned tables. Views, sequences,
// materialized views and indexes are excluded on purpose: measured on
// PostgreSQL 17, a database holding only a view or only a sequence applies at
// exit 0. [postgresUserTable] says which tables of those the pinned binary
// does not count.
func (postgresScopeCatalog) Tables(
	ctx context.Context, q dbreset.Querier, caps capability.Capabilities, schema string,
) ([]dbreset.ScopeTable, error) {
	query := `
			SELECT n.nspname, c.relname
			FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = COALESCE(NULLIF($1, ''), current_schema())
			  AND c.relkind IN ('r', 'p')
			  AND ` + postgresUserTable(caps) + `
			ORDER BY c.relname`
	return dbreset.QueryScopeTables(ctx, q, query, schema)
}

// Realm lists every schema of the database with the base tables it holds, in
// schema and then table order.
//
// The LEFT JOIN is the point: a schema with no table comes back too, because
// at realm scope an empty schema is enough to refuse. The relkind filter rides
// on the join rather than the WHERE clause for the same reason: moving it down
// would drop exactly the empty schemas the check exists to see.
//
// Which schemas belong to the realm is
// [pgsystemschema.PostgresDescribedSchemasPredicate], which
// [postgresScopeCatalog.RealmSchemas] reads too, so the check and `schema
// inspect` cannot disagree about it. It
// leaves out a schema an extension created, and everything in it: measured,
// the pinned binary applies against a schema an extension owns even while
// that schema holds a table the user created. What this read adds is the
// tables, which the check needs and inspection does not.
func (s postgresScopeCatalog) Realm(
	ctx context.Context, q dbreset.Querier, caps capability.Capabilities,
) ([]dbreset.ScopeTable, error) {
	query := `
		SELECT n.nspname, COALESCE(c.relname, '')
		FROM pg_namespace n
		LEFT JOIN pg_class c
		  ON c.relnamespace = n.oid
		 AND c.relkind IN ('r', 'p')
		 AND ` + postgresUserTable(caps) + `
		WHERE ` + pgsystemschema.PostgresDescribedSchemasPredicate(s.dialect, caps) + `
		ORDER BY n.nspname, c.relname`
	return dbreset.QueryScopeTables(ctx, q, query)
}

// RealmSchemas lists every schema of the database that a realm-scoped read
// describes, through the predicate [postgresScopeCatalog.Realm] reads.
func (s postgresScopeCatalog) RealmSchemas(
	ctx context.Context, q dbreset.Querier, caps capability.Capabilities,
) ([]string, error) {
	query := `
		SELECT n.nspname
		FROM pg_namespace n
		WHERE ` + pgsystemschema.PostgresDescribedSchemasPredicate(s.dialect, caps)
	return dbreset.QueryNames(ctx, q, query)
}

// postgresUserTable returns the predicate over a `pg_class c` that keeps the
// tables the pinned binary counts, which is fewer than every table in the
// catalog. Measured against v1.3.0 on PostgreSQL 18 with its inspector's own
// table query as the reference:
//
//   - A table an extension owns is not counted. TimescaleDB keeps its catalog
//     in tables of its own and PostGIS puts spatial_ref_sys in `public`, and
//     the binary applies against either database. A table the user adds to an
//     extension with ALTER EXTENSION ... ADD TABLE reads the same way, which
//     is what makes the rule testable on a server with no such extension.
//   - A partition is not counted. Its parent is, where the parent lives: with
//     the parent in `extra` and the partition in `public`, a URL that pins
//     `search_path=public` applies at exit 0.
//
// Both change which table a schema-scope refusal names as well as whether it
// refuses, because the refusal names the first counted table by name.
//
// The partition arm is `IS NOT TRUE` rather than `NOT`: CockroachDB answers
// NULL for relispartition on every table, measured on 26.2, and `NOT NULL`
// would leave out every table there, so a dev database holding one would read
// as clean and be reset.
//
// The extension arm reads pg_depend and is gated on
// [capability.CatalogDependencies] for the reason
// [pgsystemschema.PostgresDescribedSchemasPredicate] gives.
func postgresUserTable(caps capability.Capabilities) string {
	predicate := `c.relispartition IS NOT TRUE`
	if !caps.Has(capability.CatalogDependencies) {
		return predicate
	}
	return predicate + `
			  AND NOT EXISTS (
			        SELECT 1
			        FROM pg_depend d
			        WHERE d.classid = 'pg_class'::regclass
			          AND d.objid = c.oid
			          AND d.deptype = 'e')`
}
