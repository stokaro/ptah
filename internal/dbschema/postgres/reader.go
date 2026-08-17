package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/reservedrole"
	"go.5x5.cz/ptah/internal/rolescope"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

// Reader reads schema from PostgreSQL databases
type Reader struct {
	db      sqlrunner.Runner
	schema  string
	schemas []string
	scoped  bool
	caps    capability.Capabilities

	// relationSize records whether pg_catalog.pg_relation_size exists on the
	// connected server, and relationSizeProbed whether that has been asked yet.
	// The pair is a cache, not configuration: see supportsRelationSize.
	relationSize       bool
	relationSizeProbed bool
}

// NewPostgreSQLReader creates a new PostgreSQL schema reader
func NewPostgreSQLReader(db sqlrunner.Runner, schema string) *Reader {
	return NewPostgreSQLReaderWithCapabilities(db, schema, capability.Postgres16())
}

// NewPostgreSQLReaderWithCapabilities creates a PostgreSQL-family schema reader
// whose PostgreSQL-specific catalog reads are gated by target capabilities.
func NewPostgreSQLReaderWithCapabilities(
	db sqlrunner.Runner,
	schema string,
	caps capability.Capabilities,
) *Reader {
	if schema == "" {
		schema = "public"
	}
	return &Reader{
		db:      db,
		schema:  schema,
		schemas: []string{schema},
		caps:    caps,
	}
}

// SetSchemas restricts schema introspection to the provided allow-list.
func (r *Reader) SetSchemas(schemas []string) {
	r.schemas = normalizeSchemas(schemas, r.schema)
	r.scoped = len(schemas) > 0
}

func (r *Reader) schemasToRead() []string {
	return normalizeSchemas(r.schemas, r.schema)
}

func normalizeSchemas(schemas []string, fallback string) []string {
	seen := make(map[string]struct{}, len(schemas)+1)
	out := make([]string, 0, len(schemas)+1)
	for _, schema := range schemas {
		schema = strings.TrimSpace(schema)
		if schema == "" {
			continue
		}
		if _, ok := seen[schema]; ok {
			continue
		}
		seen[schema] = struct{}{}
		out = append(out, schema)
	}
	if len(out) > 0 {
		return out
	}
	if fallback == "" {
		fallback = "public"
	}
	return []string{fallback}
}

func (r *Reader) outputSchema(schemaName string) string {
	if r.scoped && schemaName != r.schema {
		return schemaName
	}
	return ""
}

// ReadSchema reads the complete database schema
func (r *Reader) ReadSchema() (*types.DBSchema, error) {
	schema := &types.DBSchema{}

	schemas, err := r.readSchemas()
	if err != nil {
		return nil, fmt.Errorf("failed to read schemas: %w", err)
	}
	schema.Schemas = schemas

	// Read tables
	tables, err := r.readTables()
	if err != nil {
		return nil, fmt.Errorf("failed to read tables: %w", err)
	}
	schema.Tables = tables

	// Read enums
	enums, err := r.readEnums()
	if err != nil {
		return nil, fmt.Errorf("failed to read enums: %w", err)
	}
	schema.Enums = enums

	// Read PostgreSQL user-defined types (domains, composites, ranges)
	if err := r.readUserTypesInto(schema); err != nil {
		return nil, err
	}

	// Read indexes
	indexes, err := r.readIndexes()
	if err != nil {
		return nil, fmt.Errorf("failed to read indexes: %w", err)
	}
	schema.Indexes = indexes

	// Read constraints
	constraints, err := r.readConstraints()
	if err != nil {
		return nil, fmt.Errorf("failed to read constraints: %w", err)
	}
	schema.Constraints = constraints

	if err := r.readCapabilityGatedObjects(schema); err != nil {
		return nil, err
	}

	if r.caps.Has(capability.Sequences) {
		// Read standalone sequences (PostgreSQL-specific)
		sequences, err := r.readSequences()
		if err != nil {
			return nil, fmt.Errorf("failed to read sequences: %w", err)
		}
		schema.Sequences = sequences
	}

	if r.caps.Has(capability.RowLevelSecurity) {
		// Read RLS policies (PostgreSQL-specific)
		rlsPolicies, err := r.readRLSPolicies()
		if err != nil {
			return nil, fmt.Errorf("failed to read RLS policies: %w", err)
		}
		schema.RLSPolicies = rlsPolicies
	}

	if r.caps.Has(capability.RoleManagement) {
		// Read roles and grants (PostgreSQL-specific)
		if err := r.readRolesInto(schema); err != nil {
			return nil, err
		}

		grants, err := r.readGrants(standaloneSequenceSet(schema.Sequences))
		if err != nil {
			return nil, fmt.Errorf("failed to read grants: %w", err)
		}
		schema.Grants = grants
	}

	// Enhance tables with constraint information
	r.enhanceTablesWithConstraints(schema.Tables, schema.Constraints)

	// Enhance tables with primary key information from indexes
	r.enhanceTablesWithIndexes(schema.Tables, schema.Indexes)

	return schema, nil
}

// readSchemas reports the schemas this read covered.
//
// It asks schemasToRead(), the same list readTables, readIndexes and every
// other read below iterate, so the schema list and the objects underneath it
// are one decision rather than two. It used to answer nothing at all unless an
// allow-list had been passed, which meant an unscoped read described tables in
// `public` while denying it had read any schema -- and whatever consumed that
// silence had to guess. stokaro/ptah#1276.
//
// Which schemas an unscoped read covers is a separate question, and this
// change does not move it: it is still the connected schema. What moves is that
// the read says so. `schema inspect` resolves its own wider scope from the URL
// and hands the names in explicitly (stokaro/ptah#1264).
func (r *Reader) readSchemas() ([]types.DBSchemaInfo, error) {
	names := r.schemasToRead()
	schemas := make([]types.DBSchemaInfo, 0, len(names))
	for _, schemaName := range names {
		schema, err := r.readSchemaInfo(schemaName)
		if err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				continue
			}
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

func (r *Reader) readSchemaInfo(schemaName string) (types.DBSchemaInfo, error) {
	schemasQuery := `
		SELECT
			n.nspname,
			COALESCE(obj_description(n.oid, 'pg_namespace'), '') AS schema_comment
		FROM pg_namespace n
		WHERE n.nspname = $1`
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		// A catalog that refuses obj_description refuses the whole statement,
		// so asking for the comment costs the schema. Reading without it
		// returns a schema with no comment, which is what a catalog that
		// cannot store one has (stokaro/ptah#942).
		schemasQuery = `
		SELECT
			n.nspname,
			'' AS schema_comment
		FROM pg_namespace n
		WHERE n.nspname = $1`
	}

	var schema types.DBSchemaInfo
	err := r.db.QueryRow(schemasQuery, schemaName).Scan(&schema.Name, &schema.Comment)
	if err != nil {
		return types.DBSchemaInfo{}, fmt.Errorf("failed to query schema %s: %w", schemaName, err)
	}
	return schema, nil
}

// readTables reads all tables and their columns
func (r *Reader) readTables() ([]types.DBTable, error) {
	var tables []types.DBTable
	for _, schemaName := range r.schemasToRead() {
		schemaTables, err := r.readTablesForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		tables = append(tables, schemaTables...)
	}
	return tables, nil
}

// rowStatsUnknownStatisticsOnly is the part of the tri-state a server with the
// statistics views can answer: the statistics carry no usable row count.
const rowStatsUnknownStatisticsOnly = `NOT COALESCE(c.reltuples >= 0 OR COALESCE(st.n_live_tup, 0) > 0, false)`

// rowStatsUnknownReltuplesOnly is the same question on a catalog with no
// statistics views, where pg_class.reltuples is the only estimate there is.
const rowStatsUnknownReltuplesOnly = `NOT COALESCE(c.reltuples >= 0, false)`

// rowStatsUnknownProjection builds the row_stats_unknown projection for the
// server at hand.
//
// Statistics alone are not enough to call a row count unknown. reltuples = -1
// is the state of ANY never-analyzed relation, which includes every table that
// has never had a row inserted, so the statistics-only test marks a freshly
// created empty table as one whose row count nobody knows -- and a caller
// resolving the unknown toward a concurrent build then emits CREATE INDEX
// CONCURRENTLY, in its own non-transactional migration, for a table with
// nothing in it.
//
// pg_relation_size separates the two, because it reads the main fork's size
// from the file system rather than from statistics, so no analyze, counter
// reset, crash-recovery restart or restore moves it. Measured on PostgreSQL
// 17.10: a never-analyzed table holding 5000 rows reports 188416 and a
// never-analyzed empty one reports 0, while both report reltuples = -1 and
// n_live_tup = 0. It is the table's own main fork on purpose -- pg_table_size
// adds the TOAST relation and its index, and an empty table with one text
// column already carries an 8192-byte TOAST index.
//
// The function is asked for rather than assumed. CockroachDB does not
// implement it: on CCL v26.2.5, pg_relation_size('t'::regclass) is
// "unknown function: pg_relation_size()" (SQLSTATE 42883), and because that is
// a planning error it takes the whole table read down with it rather than one
// column -- which is what the integration suite reported. pg_catalog.pg_proc
// answers the question there (`f`) and on PostgreSQL 17.10 (`t`), so the
// projection degrades to the statistics-only test on a server without the
// function instead of failing the read.
func (r *Reader) rowStatsUnknownProjection() (string, error) {
	// A catalog with no statistics views has no st to read, and asking for one
	// is a missing-FROM-clause error rather than a null (stokaro/ptah#942).
	statistics := rowStatsUnknownStatisticsOnly
	if !r.caps.Has(capability.CatalogRowStatistics) {
		statistics = rowStatsUnknownReltuplesOnly
	}
	hasRelationSize, err := r.supportsRelationSize()
	if err != nil {
		return "", err
	}
	if !hasRelationSize {
		return statistics, nil
	}
	return `(` + statistics + `)
		           AND COALESCE(pg_relation_size(c.oid), 0) > 0`, nil
}

// supportsRelationSize reports whether pg_catalog.pg_relation_size exists on
// the connected server, asking once per reader.
func (r *Reader) supportsRelationSize() (bool, error) {
	if r.relationSizeProbed {
		return r.relationSize, nil
	}
	const probe = `
		SELECT EXISTS (
			SELECT 1
			FROM pg_catalog.pg_proc p
			JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace
			WHERE n.nspname = 'pg_catalog' AND p.proname = 'pg_relation_size'
		)`
	var supported bool
	if err := r.db.QueryRow(probe).Scan(&supported); err != nil {
		return false, fmt.Errorf("failed to probe pg_relation_size availability: %w", err)
	}
	r.relationSize = supported
	r.relationSizeProbed = true
	return supported, nil
}

func (r *Reader) readTablesForSchema(schemaName string) ([]types.DBTable, error) {
	columnsByTable, err := r.readColumnsForSchema(schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to read columns for schema %s: %w", schemaName, err)
	}

	// Read tables, excluding system tables like schema_migrations.
	//
	// row_stats_unknown is the tri-state EstimatedRows cannot carry on its own.
	// PostgreSQL 14 and later store pg_class.reltuples = -1 for a relation that
	// has never been vacuumed or analyzed, and the cumulative statistics view
	// reports n_live_tup = 0 both for an empty table and for one whose counters
	// were reset -- after a crash-recovery restart, a pg_stat_reset(), or a
	// restored dump. GREATEST floors all of that to 0, which reads as "empty"
	// and is how a populated table silently earned a blocking index build.
	//
	// The storage conjunct rowStatsUnknownProjection adds when the server has
	// pg_relation_size is what keeps that tri-state from swallowing every
	// ordinary empty table -- see that method.
	//
	// relkind = 'p' is the declaratively partitioned parent.
	// information_schema.tables reports it as an ordinary BASE TABLE, so the
	// catalog is the only place the distinction survives, and PostgreSQL
	// rejects both CREATE INDEX CONCURRENTLY and DROP INDEX CONCURRENTLY on
	// such a relation.
	rowStatsUnknown, err := r.rowStatsUnknownProjection()
	if err != nil {
		return nil, err
	}
	tablesQuery := `
		SELECT table_schema, table_name, table_type,
		       ` + r.tableCommentExpr() + `,
		       ` + r.estimatedRowsExpr() + `,
		       ` + rowStatsUnknown + ` AS row_stats_unknown,
		       COALESCE(c.relkind = 'p', false) AS partitioned,
		       COALESCE(c.relrowsecurity, false) AS rls_enabled,
		       ` + r.rowTTLOptionsExpr() + `
			FROM information_schema.tables t
			LEFT JOIN pg_namespace n ON n.nspname = t.table_schema
			LEFT JOIN pg_class c ON c.relname = t.table_name AND c.relnamespace = n.oid
			` + r.rowStatisticsJoin() + `
			WHERE t.table_schema = $1
			AND t.table_type = 'BASE TABLE'
			AND t.table_name NOT IN ('schema_migrations')
			ORDER BY table_schema, table_name`

	rows, err := r.db.Query(tablesQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []types.DBTable
	for rows.Next() {
		var table types.DBTable
		var rowTTLOptions string
		err := rows.Scan(
			&table.Schema,
			&table.Name,
			&table.Type,
			&table.Comment,
			&table.EstimatedRows,
			&table.RowStatsUnknown,
			&table.Partitioned,
			&table.RLSEnabled,
			&rowTTLOptions,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		table.Schema = r.outputSchema(table.Schema)
		table.Columns = columnsByTable[table.Name]
		if table.RowTTL, err = readRowTTL(rowTTLOptions); err != nil {
			return nil, fmt.Errorf("failed to read row-level TTL for table %s: %w", table.Name, err)
		}

		tables = append(tables, table)
	}

	return tables, nil
}

// generatedExpressionExpr renders the projection carrying a generated column's
// expression, or a constant where pg_catalog's helpers do not resolve. See
// [Reader.formattedTypeExpr] for why a CASE is not enough.
func (r *Reader) generatedExpressionExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "'' AS generated_expression"
	}
	return `COALESCE(CASE WHEN a.attgenerated <> '' THEN pg_get_expr(ad.adbin, ad.adrelid) ELSE '' END, '') AS generated_expression`
}

// ownedSequenceExpr renders the projection naming the sequence a serial column
// owns, or a constant on a target without sequences.
//
// The gate is [capability.Sequences] rather than the catalog-function key: a
// target with no sequences has no answer to give, and pg_get_serial_sequence is
// only the way the answer would have been fetched. Spanner refuses the
// projection twice over -- `Postgres function format(text, text, text) is not
// supported` comes first, before pg_get_serial_sequence is even reached
// (stokaro/ptah#942).
func (r *Reader) ownedSequenceExpr() string {
	if !r.caps.Has(capability.Sequences) {
		return "'' AS owned_sequence_name"
	}
	return `COALESCE(
				pg_get_serial_sequence(
					format('%I.%I', col.table_schema, col.table_name),
					col.column_name
				),
				''
			) AS owned_sequence_name`
}

// tableCommentExpr renders the projection carrying a table's stored comment,
// or a constant where pg_catalog's helpers do not resolve.
func (r *Reader) tableCommentExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "'' as table_comment"
	}
	return "COALESCE(obj_description(c.oid), '') as table_comment"
}

// estimatedRowsExpr renders the row-count estimate, falling back to pg_class
// alone where the statistics views are absent.
//
// reltuples survives on a catalog with no pg_stat_all_tables -- measured on
// Spanner, which answers `0.0` for it and `relation "pg_stat_all_tables" does
// not exist` for the view. Keeping the pg_class half rather than dropping the
// estimate entirely is what lets the tri-state below still mean something
// (stokaro/ptah#942).
func (r *Reader) estimatedRowsExpr() string {
	if !r.caps.Has(capability.CatalogRowStatistics) {
		return "COALESCE(GREATEST(c.reltuples::bigint, 0), 0) AS estimated_rows"
	}
	return "COALESCE(GREATEST(c.reltuples::bigint, st.n_live_tup, 0), 0) AS estimated_rows"
}

// rowStatisticsJoin joins the statistics view the estimate reads, or nothing.
func (r *Reader) rowStatisticsJoin() string {
	if !r.caps.Has(capability.CatalogRowStatistics) {
		return ""
	}
	return "LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid"
}

// domainBaseTypeExpr renders a domain's base type as the server spells it, or
// a constant where pg_catalog's helpers do not resolve.
//
// A catalog without format_type has no domains either -- domains are a
// PostgreSQL type-system feature -- so the query returns no rows and the
// constant is never read. It exists because the function name is resolved
// before the empty result is (stokaro/ptah#942).
func (r *Reader) domainBaseTypeExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "'' AS base_type"
	}
	return "format_type(t.typbasetype, t.typtypmod) AS base_type"
}

// The projections below all read a pg_catalog helper, and all answer with a
// constant where those helpers do not resolve. Each is separate rather than one
// switch because the SQL differs; the decision they share is one capability.
// See [capability.PostgresCatalogFunctions].

func (r *Reader) domainCheckExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "'' AS check_expr"
	}
	return `COALESCE((
				SELECT string_agg(pg_get_expr(c.conbin, c.conrelid), ' AND ')
				FROM pg_constraint c
				WHERE c.contypid = t.oid AND c.contype = 'c'
			), '') AS check_expr`
}

func (r *Reader) indexCommentExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "'' as index_comment"
	}
	return "COALESCE(obj_description(i.oid, 'pg_class'), '') as index_comment"
}

func (r *Reader) indexPredicateExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "'' as predicate"
	}
	return "COALESCE(pg_get_expr(ix.indpred, ix.indrelid), '') as predicate"
}

func (r *Reader) constraintCheckExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "''"
	}
	return `COALESCE(max(CASE
					WHEN pc.contype = 'c' THEN pg_get_expr(pc.conbin, pc.conrelid)
				END), '')`
}

func (r *Reader) constraintDefinitionExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "''"
	}
	return "COALESCE(max(pg_get_constraintdef(pc.oid)), '')"
}

// formattedTypeExpr renders the projection that spells a column's type the way
// the server does, or a constant where pg_catalog's helpers do not resolve.
//
// The expression it replaces reads format_type inside a CASE that only an array
// or a domain column takes. That is not enough on a catalog without the
// function: the name is resolved before any row is, so a branch no row would
// take still refuses the statement. Spanner has neither arrays nor domains, so
// the constant loses nothing there (stokaro/ptah#942).
func (r *Reader) formattedTypeExpr() string {
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return "'' AS formatted_type"
	}
	return `CASE WHEN data_type = 'ARRAY' OR col.domain_name IS NOT NULL
				THEN format_type(a.atttypid, a.atttypmod)
				ELSE ''
			END AS formatted_type`
}

// readColumnsForSchema reads all columns in a schema in one catalog query and
// groups them by table name.
func (r *Reader) readColumnsForSchema(schemaName string) (map[string][]types.DBColumn, error) {
	columnsQuery := `
		SELECT
			col.table_name,
			column_name,
			data_type,
			udt_name,
			-- information_schema erases an array's element type: data_type is
			-- the bare category "ARRAY" and character_maximum_length is null,
			-- so varchar(100)[] cannot be reconstructed from either. Only
			-- format_type carries it. Read for array and domain columns alone:
			-- preferring it everywhere would change the type string for every
			-- column and reach the SERIAL detection and the sized-type
			-- branches downstream (stokaro/ptah#1138).
			--
			-- It erases a domain the same way. data_type for a column of
			-- domain positive_int is its base type, "integer", so the column
			-- was rebuilt without the domain's CHECK and nothing said so;
			-- domain_name is how information_schema records that the
			-- declared type was a domain, and format_type spells it the way
			-- the server does, schema-qualifying it when the search path
			-- needs that. Measured against the pinned binary v1.3.0, which
			-- reports "type":"positive_int" here, so this is also the
			-- compatible answer. See #1242.
			` + r.formattedTypeExpr() + `,
			-- The same format_type answer means two different things, and only
			-- this column separates them: for an array it is a TYPE, and for a
			-- domain it is the IDENTIFIER its author picked. A comparator that
			-- normalizes the two the same way lets a name decide whether a
			-- column changed -- a domain named "waypoint" contains "int" and one
			-- named "context" contains "text". A domain over an array is
			-- reported with data_type 'ARRAY' just like a plain array column, so
			-- the distinction cannot be recovered downstream (stokaro/ptah#1138).
			--
			-- The Atlas-compatible JSON inspect surface reads the same fact for
			-- the same reason: measured against the pinned binary v1.3.0, an
			-- array column prints "type":"ARRAY" while a domain column prints
			-- "type":"positive", schema-qualified to "doms.positive" when the
			-- domain is off the search path. Carrying the fact separately is
			-- what lets each consumer pick, instead of every consumer
			-- re-deriving it from a coincidence of which other field is empty
			-- (stokaro/ptah#1242).
			COALESCE(col.domain_name, '') AS domain_name,
			-- And the schema that holds it, because the name is only half of a
			-- domain's identity. public.status and other.status are two types;
			-- a comparator given "status" for both reports no change for a
			-- column that must be converted, and the DROP DOMAIN ... CASCADE
			-- the plan keeps then takes the column. Read raw rather than
			-- through outputSchema: the domain may live outside the schemas
			-- being read, and blanking it there is exactly the erasure this
			-- projection exists to prevent (stokaro/ptah#1138).
			COALESCE(col.domain_schema, '') AS domain_schema,
			is_nullable,
			column_default,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			ordinal_position,
			COALESCE(a.attgenerated, '') AS generated_kind,
			` + r.generatedExpressionExpr() + `,
			COALESCE(a.attidentity, '') AS identity_kind,
			` + r.ownedSequenceExpr() + `
		FROM information_schema.columns col
		JOIN information_schema.tables tbl ON tbl.table_schema = col.table_schema
			AND tbl.table_name = col.table_name
			AND tbl.table_type = 'BASE TABLE'
		JOIN pg_namespace n ON n.nspname = col.table_schema
		JOIN pg_class cls ON cls.relname = col.table_name AND cls.relnamespace = n.oid
		LEFT JOIN pg_attribute a ON a.attrelid = cls.oid
			AND a.attname = col.column_name
			AND NOT a.attisdropped
		LEFT JOIN pg_attrdef ad ON ad.adrelid = a.attrelid AND ad.adnum = a.attnum
		WHERE col.table_schema = $1
		AND col.table_name NOT IN ('schema_migrations')
		ORDER BY col.table_name, col.ordinal_position`

	rows, err := r.db.Query(columnsQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	columnsByTable := make(map[string][]types.DBColumn)
	for rows.Next() {
		var col types.DBColumn
		var generatedKind string
		var generatedExpression string
		var identityKind string
		var ownedSequenceName string
		var tableName string
		err := rows.Scan(
			&tableName,
			&col.Name,
			&col.DataType,
			&col.UDTName,
			&col.FormattedType,
			&col.DomainName,
			&col.DomainSchema,
			&col.IsNullable,
			&col.ColumnDefault,
			&col.CharacterMaxLength,
			&col.NumericPrecision,
			&col.NumericScale,
			&col.OrdinalPosition,
			&generatedKind,
			&generatedExpression,
			&identityKind,
			&ownedSequenceName,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}
		if generatedExpression != "" {
			col.GeneratedExpression = &generatedExpression
			col.GeneratedKind = postgresGeneratedKind(generatedKind)
		}
		col.IdentityGeneration = postgresIdentityGeneration(identityKind)

		if col.ColumnDefault != nil {
			defaultVal := *col.ColumnDefault
			col.IsAutoIncrement = ownedSequenceName != "" &&
				strings.Contains(strings.ToLower(defaultVal), "nextval(")
		}

		columnsByTable[tableName] = append(columnsByTable[tableName], col)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columnsByTable, nil
}

func postgresGeneratedKind(code string) string {
	switch code {
	case "s":
		return "STORED"
	default:
		return ""
	}
}

// postgresIdentityGeneration maps pg_attribute.attidentity to the canonical
// identity-generation spelling used across Ptah: "a" (GENERATED ALWAYS AS
// IDENTITY) becomes "ALWAYS", "d" (GENERATED BY DEFAULT AS IDENTITY) becomes
// "BY_DEFAULT", and anything else (including plain and SERIAL columns) is empty.
func postgresIdentityGeneration(code string) string {
	switch code {
	case "a":
		return "ALWAYS"
	case "d":
		return "BY_DEFAULT"
	default:
		return ""
	}
}

// readEnums reads all enum types
func (r *Reader) readEnums() ([]types.DBEnum, error) {
	var enums []types.DBEnum
	for _, schemaName := range r.schemasToRead() {
		schemaEnums, err := r.readEnumsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		enums = append(enums, schemaEnums...)
	}
	return enums, nil
}

func (r *Reader) readEnumsForSchema(schemaName string) ([]types.DBEnum, error) {
	enumsQuery := `
		SELECT
			t.typname AS enum_name,
			e.enumlabel AS enum_value,
			e.enumsortorder
		FROM pg_type t
		JOIN pg_enum e ON t.oid = e.enumtypid
		JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
		WHERE n.nspname = $1
		ORDER BY t.typname, e.enumsortorder`

	rows, err := r.db.Query(enumsQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query enums: %w", err)
	}
	defer rows.Close()

	enumMap := make(map[string][]string)
	for rows.Next() {
		var enumName, enumValue string
		var sortOrder int
		err := rows.Scan(&enumName, &enumValue, &sortOrder)
		if err != nil {
			return nil, fmt.Errorf("failed to scan enum: %w", err)
		}

		enumMap[enumName] = append(enumMap[enumName], enumValue)
	}

	var enums []types.DBEnum
	for name, values := range enumMap {
		enums = append(enums, types.DBEnum{
			Name: name,
			// Same convention as tables, views and domains: blank for the
			// connection's own schema, named otherwise. Filters rebuild the
			// qualified spelling from the connection's default, which is what
			// makes `--exclude app.color` reach this enum (stokaro/ptah#933).
			Schema: r.outputSchema(schemaName),
			Values: values,
		})
	}

	return enums, nil
}

// readUserTypesInto reads PostgreSQL domains, composite types, and range types
// and assigns them onto schema. Split out of ReadSchema to keep that method's
// cyclomatic complexity manageable.
func (r *Reader) readUserTypesInto(schema *types.DBSchema) error {
	// The whole read is skipped rather than gated projection by projection: it
	// joins pg_depend, and a missing relation is not something a constant can
	// stand in for the way a missing function is. A target without the type
	// system has nothing here to find either way (stokaro/ptah#942).
	if !r.caps.Has(capability.CatalogDependencies) {
		return nil
	}
	domains, err := r.readDomains()
	if err != nil {
		return fmt.Errorf("failed to read domains: %w", err)
	}
	schema.Domains = domains

	composites, err := r.readComposites()
	if err != nil {
		return fmt.Errorf("failed to read composite types: %w", err)
	}
	schema.Composites = composites

	ranges, err := r.readRanges()
	if err != nil {
		return fmt.Errorf("failed to read range types: %w", err)
	}
	schema.Ranges = ranges
	return nil
}

// extensionOwnedTypeExclusion is the correlated NOT EXISTS that keeps a type an
// extension owns out of a description. It is written once and shared by the
// domain, composite and range reads because the three ask the same question of
// the same catalog and three copies drift.
//
// The predicate correlates on `t`, which every one of those queries spells for
// its pg_type row.
//
// The reasoning is the one [Reader.readFunctionsForSchema] already carries for
// functions -- an extension's members "cannot be dropped independently and
// should be managed by the extension" -- and it was never applied to its types.
// A description that declares both `extension "lo"` and `domain "lo"` cannot be
// replayed, because CREATE EXTENSION makes the domain and the second
// declaration collides with it. Measured on PostgreSQL 17.10 (stokaro/ptah#1294):
//
//	CREATE EXTENSION lo;
//	CREATE TABLE docs (id integer PRIMARY KEY, payload lo);
//
//	Error: materialize schema on dev database: ... ERROR: type "lo" already
//	exists (SQLSTATE 42710)  SQL: CREATE DOMAIN "lo" AS oid;
//
// classid is part of the predicate rather than left to objid alone: a pg_depend
// row names its object by (classid, objid), and OIDs are drawn from one counter
// shared by every catalog, so objid on its own can collide with a row of another
// class.
//
// Ownership is asked of pg_depend rather than of the type's NAME. A user type
// named close to an extension's -- `lo_own` beside `lo` -- is a user type, and a
// name filter would have to spell `NOT LIKE` with an ESCAPE clause to avoid
// treating its underscore as a wildcard, a mistake this reader has already had
// to correct elsewhere (stokaro/ptah#1291). The catalog edge has no such
// failure mode.
const extensionOwnedTypeExclusion = `
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend extdep
			WHERE extdep.classid = 'pg_type'::regclass
			  AND extdep.objid = t.oid
			  AND extdep.deptype = 'e'
		)`

// readDomains reads PostgreSQL domain types (typtype='d').
func (r *Reader) readDomains() ([]types.DBDomain, error) {
	var domains []types.DBDomain
	for _, schemaName := range r.schemasToRead() {
		schemaDomains, err := r.readDomainsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		domains = append(domains, schemaDomains...)
	}
	return domains, nil
}

func (r *Reader) readDomainsForSchema(schemaName string) ([]types.DBDomain, error) {
	query := `
		SELECT
			n.nspname AS schema_name,
			t.typname AS domain_name,
			` + r.domainBaseTypeExpr() + `,
			t.typnotnull AS not_null,
			COALESCE(t.typdefault, '') AS default_value,
			` + r.domainCheckExpr() + `
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		WHERE t.typtype = 'd' AND n.nspname = $1` +
		extensionOwnedTypeExclusion + `
		ORDER BY t.typname`

	rows, err := r.db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query domains for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var domains []types.DBDomain
	for rows.Next() {
		var domain types.DBDomain
		var rawSchema string
		if err := rows.Scan(&rawSchema, &domain.Name, &domain.BaseType, &domain.NotNull, &domain.Default, &domain.Check); err != nil {
			return nil, fmt.Errorf("failed to scan domain for schema %s: %w", schemaName, err)
		}
		domain.Schema = r.outputSchema(rawSchema)
		domains = append(domains, domain)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read domains for schema %s: %w", schemaName, err)
	}
	return domains, nil
}

// readComposites reads PostgreSQL composite types (typtype='c'), excluding the
// implicit row types of tables (relkind other than 'c').
func (r *Reader) readComposites() ([]types.DBComposite, error) {
	var composites []types.DBComposite
	for _, schemaName := range r.schemasToRead() {
		schemaComposites, err := r.readCompositesForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		composites = append(composites, schemaComposites...)
	}
	return composites, nil
}

func (r *Reader) readCompositesForSchema(schemaName string) ([]types.DBComposite, error) {
	const query = `
		SELECT
			n.nspname AS schema_name,
			t.typname AS type_name,
			a.attname AS field_name,
			format_type(a.atttypid, a.atttypmod) AS field_type,
			a.attnum
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_class c ON c.oid = t.typrelid AND c.relkind = 'c'
		JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
		WHERE t.typtype = 'c' AND n.nspname = $1` +
		extensionOwnedTypeExclusion + `
		ORDER BY t.typname, a.attnum`

	rows, err := r.db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query composite types for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	type key struct{ schema, name string }
	order := make([]key, 0)
	byName := make(map[key]*types.DBComposite)
	for rows.Next() {
		var rawSchema, typeName, fieldName, fieldType string
		var attNum int
		if err := rows.Scan(&rawSchema, &typeName, &fieldName, &fieldType, &attNum); err != nil {
			return nil, fmt.Errorf("failed to scan composite type for schema %s: %w", schemaName, err)
		}
		k := key{r.outputSchema(rawSchema), typeName}
		composite, ok := byName[k]
		if !ok {
			composite = &types.DBComposite{Name: typeName, Schema: k.schema}
			byName[k] = composite
			order = append(order, k)
		}
		composite.Fields = append(composite.Fields, types.DBCompositeField{Name: fieldName, Type: fieldType})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read composite types for schema %s: %w", schemaName, err)
	}

	composites := make([]types.DBComposite, 0, len(order))
	for _, k := range order {
		composites = append(composites, *byName[k])
	}
	return composites, nil
}

// readRanges reads PostgreSQL range types (typtype='r').
func (r *Reader) readRanges() ([]types.DBRange, error) {
	var ranges []types.DBRange
	for _, schemaName := range r.schemasToRead() {
		schemaRanges, err := r.readRangesForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		ranges = append(ranges, schemaRanges...)
	}
	return ranges, nil
}

func (r *Reader) readRangesForSchema(schemaName string) ([]types.DBRange, error) {
	// The four attributes after the subtype are what makes a change to an
	// existing range type visible at all; without them the comparator had only
	// names to compare and called a changed range converged (stokaro/ptah#931
	// item 2). rngcanonical and rngsubdiff are regproc and hold 0 when the range
	// has no such function, which renders as "-", so they are nulled first.
	const query = `
		SELECT
			n.nspname AS schema_name,
			t.typname AS range_name,
			format_type(rng.rngsubtype, NULL) AS subtype,
			COALESCE(opc.opcname, '') AS subtype_opclass,
			COALESCE(coll.collname, '') AS collation_name,
			COALESCE(NULLIF(rng.rngcanonical, 0)::regproc::text, '') AS canonical,
			COALESCE(NULLIF(rng.rngsubdiff, 0)::regproc::text, '') AS subtype_diff
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_range rng ON rng.rngtypid = t.oid
		LEFT JOIN pg_opclass opc ON opc.oid = rng.rngsubopc
		LEFT JOIN pg_collation coll ON coll.oid = rng.rngcollation
		WHERE t.typtype = 'r' AND n.nspname = $1` +
		extensionOwnedTypeExclusion + `
		ORDER BY t.typname`

	rows, err := r.db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query range types for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var ranges []types.DBRange
	for rows.Next() {
		var rangeType types.DBRange
		var rawSchema string
		if err := rows.Scan(
			&rawSchema,
			&rangeType.Name,
			&rangeType.Subtype,
			&rangeType.SubtypeOpClass,
			&rangeType.Collation,
			&rangeType.Canonical,
			&rangeType.SubtypeDiff,
		); err != nil {
			return nil, fmt.Errorf("failed to scan range type for schema %s: %w", schemaName, err)
		}
		rangeType.Schema = r.outputSchema(rawSchema)
		ranges = append(ranges, rangeType)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read range types for schema %s: %w", schemaName, err)
	}
	return ranges, nil
}

// readIndexes reads all indexes
func (r *Reader) readIndexes() ([]types.DBIndex, error) {
	var indexes []types.DBIndex
	for _, schemaName := range r.schemasToRead() {
		schemaIndexes, err := r.readIndexesForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		indexes = append(indexes, schemaIndexes...)
	}
	return indexes, nil
}

// requiredExtensionsProjection builds the correlated sub-select that names the
// extensions an index resolves to through the catalog rather than through its
// own text. indclassExpr is the index's pg_index.indclass vector and
// accessMethodExpr its pg_class.relam.
//
// PostgreSQL prints an operator class in a CREATE INDEX or an EXCLUDE clause
// only when that class is not the default for the key's type on the access
// method, so the dependency this answers is one no identifier in the rendered
// document need carry. Measured on PostgreSQL 17.10 with btree_gin installed,
//
//	CREATE INDEX t_gin ON t USING gin (n int4_ops);   -- n is integer
//
// comes back from the catalog as `CREATE INDEX t_gin ON public.t USING gin (n)`,
// and the same DDL replayed where btree_gin is absent fails with `data type
// integer has no default operator class for access method "gin"` (42704).
// pg_index.indclass holds the class each key resolved to, so the reader can
// answer "which extension owns it" exactly (stokaro/ptah#1286).
//
// Matching the ACCESS METHOD's name would answer it wrongly. `gin` is a pg_am
// row belonging to no extension, so treating "this index says gin" as evidence
// would pin btree_gin to every GIN index in the database -- and tsvector, jsonb
// and array columns all have core GIN operator classes, so most of them do not
// need it. The access method is read here as an OID resolved against pg_depend,
// which is a different question: it pins nothing for `gin`, and pins the owner
// for an access method an extension does supply, such as bloom's `bloom`.
//
// Both arms are unconditional on opcdefault, so a printed class is recorded
// here as well as an unprinted one. A printed class is also matchable by name
// through [goschema.Extension.Provides] wherever the renderer's reference scan
// reads the attribute it lands in, and that is the answer preferred when both
// are available, because a name can be looked up in the document; see
// [go.5x5.cz/ptah/internal/atlashclrender] omitRefusedExtension. This projection
// does not depend on either: Provides excludes names pg_catalog also supplies,
// so a class shadowed by a core one would drop out of it.
func requiredExtensionsProjection(indclassExpr, accessMethodExpr string) string {
	return `COALESCE((
				SELECT json_agg(DISTINCT e.extname)::text
				  FROM pg_depend dep
				  JOIN pg_extension e ON e.oid = dep.refobjid
				 WHERE dep.deptype = 'e'
				   AND ((dep.classid = 'pg_opclass'::regclass
				         AND dep.objid = ANY (` + indclassExpr + `::oid[]))
				     OR (dep.classid = 'pg_am'::regclass
				         AND dep.objid = ` + accessMethodExpr + `))
			), '[]')`
}

func (r *Reader) readIndexesForSchema(schemaName string) ([]types.DBIndex, error) {
	// A server without pg_catalog's introspection helpers has no
	// pg_get_indexdef either, and the query below is pg_index/pg_class/pg_am/
	// pg_get_indexdef throughout, so there is nothing here to degrade: the read
	// asks the SQL-standard catalog instead. The capability is the one the
	// probe already measures on every run, so the branch cannot drift from what
	// the server actually answers (stokaro/ptah#942).
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		return r.readInformationSchemaIndexes(schemaName)
	}
	indexesQuery := `
		SELECT
			n.nspname as schemaname,
			t.relname as tablename,
			i.relname as indexname,
			pg_get_indexdef(i.oid) as indexdef,
			COALESCE((
				SELECT json_agg(pg_get_indexdef(i.oid, keys.ordinality::integer, true) ORDER BY keys.ordinality)::text
				FROM unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ordinality)
				WHERE keys.ordinality <= ix.indnkeyatts
			), '[]') as index_columns,
			-- pg_index.indkey holds 0 for a key that is an expression rather
			-- than a column. Without it the key texts above are ambiguous:
			-- lower(name) and a column literally named "lower(name)" arrive
			-- identically, and treating the former as an identifier renders
			-- CREATE INDEX ... ("lower(name)"), which PostgreSQL rejects with
			-- ERROR: column "lower(name)" does not exist. See #1242.
			COALESCE((
				SELECT json_agg(keys.attnum ORDER BY keys.ordinality)::text
				FROM unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ordinality)
				WHERE keys.ordinality <= ix.indnkeyatts
			), '[]') as index_key_attnums,
			-- pg_index.indclass names the operator class each key was built
			-- with. Only a non-default class has to be carried: dropping
			-- text_pattern_ops leaves an index PostgreSQL will not use for the
			-- LIKE prefix scans it was created for, and nothing reports it.
			-- opcdefault separates the two, so a default class reports the
			-- empty string rather than a name the emitted DDL does not need.
			--
			-- opcdefault does not settle it alone, because an operator class can
			-- take PARAMETERS and the index attribute's pg_attribute.attoptions
			-- is where PostgreSQL keeps them. Measured on PostgreSQL 17.10,
			--
			--   CREATE INDEX i ON t USING gist (tsv tsvector_ops (siglen = 64))
			--
			-- stores opcname tsvector_ops with opcdefault true and attoptions
			-- {siglen=64}: reading opcdefault alone reports the empty string and
			-- rebuilds the index with the 124-byte default signature, which is a
			-- different index that psql accepts at exit 0. A parameterised class
			-- is therefore named even when it is the type's default, because the
			-- name is the only place its parameters can hang. See #1242.
			--
			-- The three catalog values are carried out separately and combined by
			-- postgresOperatorClassSpelling rather than by a CASE here. Which of
			-- them wins is the part that is easy to get wrong -- testing default
			-- before parameters silently drops the parameters of a default class
			-- -- and a rule written in Go is one a table-driven test can hold
			-- against every combination of the three.
			COALESCE((
				SELECT json_agg(
					json_build_object(
						'name', op.opcname::text,
						'is_default', COALESCE(op.opcdefault, false),
						'params', COALESCE(array_to_string(keyatt.attoptions, ', '), '')
					)
					ORDER BY keys.ordinality
				)::text
				FROM unnest(ix.indclass) WITH ORDINALITY AS keys(opcoid, ordinality)
				LEFT JOIN pg_opclass op ON op.oid = keys.opcoid
				LEFT JOIN pg_attribute keyatt
					ON keyatt.attrelid = ix.indexrelid
					AND keyatt.attnum = keys.ordinality::smallint
				WHERE keys.ordinality <= ix.indnkeyatts
			), '[]') as index_key_opclasses,
			-- pg_index.indoption is a per-key bitmask: bit 0 is DESC, bit 1 is
			-- NULLS FIRST. Measured on PostgreSQL 17.10: (a DESC) reports 3,
			-- (c DESC NULLS LAST) reports 1, (b NULLS FIRST) reports 2, and a
			-- plain ascending key reports 0.
			COALESCE((
				SELECT json_agg(keys.optionbits ORDER BY keys.ordinality)::text
				FROM unnest(ix.indoption) WITH ORDINALITY AS keys(optionbits, ordinality)
				WHERE keys.ordinality <= ix.indnkeyatts
			), '[]') as index_key_options,
			-- INCLUDE payload columns are the keys past indnkeyatts. They are
			-- absent from indclass and indoption, which cover key columns
			-- only, so they are read separately rather than filtered out of
			-- the key list afterwards.
			COALESCE((
				SELECT json_agg(pg_get_indexdef(i.oid, keys.ordinality::integer, true) ORDER BY keys.ordinality)::text
				FROM unnest(ix.indkey) WITH ORDINALITY AS keys(attnum, ordinality)
				WHERE keys.ordinality > ix.indnkeyatts
			), '[]') as index_include_columns,
			-- The access method. Losing it is not always the quiet
			-- degradation to btree it looks like: a gist index on a point
			-- column does not replay at all without it, because point has no
			-- default btree operator class. See #1242.
			am.amname as index_method,
			-- The index relation's storage parameters, the WITH (...) clause.
			-- pg_class.reloptions holds them as "name=value" strings. Measured on
			-- PostgreSQL 17.10, CREATE INDEX ... USING brin (ts)
			-- WITH (pages_per_range = 32) stores {pages_per_range=32}, and an
			-- index rebuilt without it summarizes 128 pages per range instead of
			-- 32 -- a different index that psql accepts at exit 0. Only the
			-- parameters the whole model can carry back out again are kept; see
			-- postgresIndexStorageParams. See #1242.
			COALESCE(array_to_json(i.reloptions)::text, '[]') as index_storage_params,
			-- The extensions this index resolves to, from the resolved operator
			-- class OIDs and the access method OID rather than from either
			-- one's name -- including the class the DDL does print, which is
			-- the non-default one. See requiredExtensionsProjection.
			` + requiredExtensionsProjection("ix.indclass", "i.relam") + ` as index_required_extensions,
			-- The index's own object comment. It hangs off the INDEX relation,
			-- so obj_description takes indexrelid and 'pg_class'; the table's
			-- comment is a different object and is read elsewhere. Measured on
			-- PostgreSQL 17.10, the pinned Atlas community binary v1.3.0 reads
			-- this one and emits both COMMENT ON INDEX "i" IS 'keep me' and
			-- comment = "keep me" inside the index block, where Ptah emitted
			-- neither: the comment was dropped between the catalog and the
			-- model, psql accepted the replay at exit 0, and the index simply
			-- had no comment. See #1242.
			` + r.indexCommentExpr() + `,
			` + r.indexPredicateExpr() + `,
			ix.indisprimary,
			ix.indisunique,
			-- Whether this index is a partition's copy of an index on its
			-- partitioned parent. PostgreSQL records the attachment in
			-- pg_inherits over index relations, the same catalog that records
			-- a partition's attachment to its parent table, and an index row
			-- there exists only for that reason.
			--
			-- The distinction matters because the copy is not separately
			-- droppable: PostgreSQL 17.10 answers
			-- DROP INDEX "events_2026_tenant_idx" with the message
			-- "cannot drop index events_2026_tenant_idx because index
			-- idx_events_tenant requires it" (SQLSTATE 2BP01). Reading relkind
			-- instead would answer a different question: the parent is 'I' and
			-- the copy is 'i', so a relkind test marks the object that IS
			-- addressable and leaves the one that is not.
			EXISTS (
				SELECT 1 FROM pg_inherits inh WHERE inh.inhrelid = i.oid
			) as partition_attached
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
		JOIN pg_am am ON am.oid = i.relam
		WHERE n.nspname = $1
		AND t.relname NOT IN ('schema_migrations')
		ORDER BY t.relname, i.relname`

	rows, err := r.db.Query(indexesQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query indexes: %w", err)
	}
	defer rows.Close()

	var indexes []types.DBIndex
	for rows.Next() {
		var row postgresIndexRow
		err := rows.Scan(
			&row.schemaName, &row.tableName, &row.indexName, &row.indexDef,
			&row.keyTexts, &row.keyAttnums, &row.keyOpclasses, &row.keyOptions,
			&row.includeColumns, &row.method, &row.storageParams, &row.requiredExtensions,
			&row.comment,
			&row.predicate, &row.isPrimary, &row.isUnique, &row.partitionAttached,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}

		index, err := buildPostgresIndex(row)
		if err != nil {
			return nil, err
		}
		index.Schema = r.outputSchema(row.schemaName)

		indexes = append(indexes, index)
	}

	return indexes, nil
}

// postgresIndexRow is one row of the index introspection query. Each field
// names the catalog value it carries, so the mapping below can be read without
// counting scan positions.
type postgresIndexRow struct {
	schemaName string
	tableName  string
	indexName  string
	indexDef   string
	// keyTexts is the JSON array of per-key texts from pg_get_indexdef.
	keyTexts string
	// keyAttnums is the JSON array of pg_index.indkey attribute numbers.
	keyAttnums string
	// keyOpclasses is the JSON array of per-key operator class names, empty
	// where the key uses its type's default class.
	keyOpclasses string
	// keyOptions is the JSON array of pg_index.indoption bitmasks.
	keyOptions string
	// includeColumns is the JSON array of INCLUDE payload column texts.
	includeColumns string
	// method is pg_am.amname.
	method string
	// storageParams is the JSON array of pg_class.reloptions entries, each
	// spelled "name=value".
	storageParams string
	// requiredExtensions is the JSON array of extension names the index's
	// resolved operator classes and access method belong to.
	requiredExtensions string
	// comment is obj_description of the index relation.
	comment   string
	predicate string
	isPrimary bool
	isUnique  bool
	// partitionAttached is the pg_inherits attachment of this index relation
	// to an index on the partitioned parent.
	partitionAttached bool
}

// buildPostgresIndex maps one introspection row onto the dialect-neutral index
// model. It does not set Schema, which needs the reader's output-schema policy.
func buildPostgresIndex(row postgresIndexRow) (types.DBIndex, error) {
	index := types.DBIndex{
		Name:              row.indexName,
		TableName:         row.tableName,
		Definition:        row.indexDef,
		Condition:         row.predicate,
		Comment:           row.comment,
		IsUnique:          row.isUnique,
		IsPrimary:         row.isPrimary,
		Method:            row.method,
		NullsDistinct:     postgresNullsDistinctFromDefinition(row.indexDef),
		PartitionAttached: row.partitionAttached,
	}

	columns, err := parsePostgresIndexColumns(row.keyTexts, row.indexDef)
	if err != nil {
		return types.DBIndex{}, fmt.Errorf("failed to parse index columns for %s: %w", row.indexName, err)
	}
	index.Columns = columns

	index.IncludeColumns, err = decodePostgresNameList(row.includeColumns)
	if err != nil {
		return types.DBIndex{}, fmt.Errorf("failed to parse index include columns for %s: %w", row.indexName, err)
	}

	index.RequiresExtensions, err = decodePostgresNameList(row.requiredExtensions)
	if err != nil {
		return types.DBIndex{}, fmt.Errorf("failed to parse index required extensions for %s: %w", row.indexName, err)
	}
	slices.Sort(index.RequiresExtensions)

	index.Parts, err = parsePostgresIndexParts(index.Columns, row.keyAttnums)
	if err != nil {
		return types.DBIndex{}, fmt.Errorf("failed to parse index key parts for %s: %w", row.indexName, err)
	}

	index.Parts, err = applyPostgresIndexOpclasses(index.Parts, row.keyOpclasses)
	if err != nil {
		return types.DBIndex{}, fmt.Errorf("failed to parse index operator classes for %s: %w", row.indexName, err)
	}

	index.Parts, err = applyPostgresIndexOptions(index.Parts, row.keyOptions)
	if err != nil {
		return types.DBIndex{}, fmt.Errorf("failed to parse index key options for %s: %w", row.indexName, err)
	}

	index.StorageParams, err = postgresIndexStorageParams(row.storageParams)
	if err != nil {
		return types.DBIndex{}, fmt.Errorf("failed to parse index storage parameters for %s: %w", row.indexName, err)
	}

	return index, nil
}

// postgresRoundTrippableIndexStorageParams names the PostgreSQL index storage
// parameters this reader records.
//
// It is deliberately not every parameter pg_class.reloptions can hold. A
// parameter recorded here has to survive every surface the model passes
// through, because [go.5x5.cz/ptah/migration/schemadiff] treats a difference in
// the recorded set as a reason to rebuild the index. `pages_per_range` does
// survive: the Atlas-compatible HCL reader accepts `page_per_range` and
// `pages_per_range`, the HCL writer emits one, the SQL parser reads one out of
// a WITH clause, and the PostgreSQL renderer writes one.
//
// Measured on PostgreSQL 17.10, `fillfactor`, `deduplicate_items`, `buffering`,
// `fastupdate`, `gin_pending_list_limit` and `autosummarize` have no slot on
// that HCL surface, and the pinned Atlas community binary v1.3.0 drops all of
// them too -- `CREATE INDEX i ON t (name) WITH (fillfactor = 70)` comes back
// from both as `CREATE INDEX "i" ON "t" ("name")`. Recording one of those would
// not make it survive an inspect-and-diff round trip; it would make every such
// index differ from its own inspected document forever. docs/conformance.md
// records the omission.
var postgresRoundTrippableIndexStorageParams = []string{"pages_per_range"}

// postgresIndexStorageParams decodes pg_class.reloptions, which PostgreSQL
// reports as an array of "name=value" strings, into the storage parameters the
// model carries. An entry with no "=" is skipped rather than recorded with an
// empty value, because a valueless reloption is not something CREATE INDEX can
// be handed back.
func postgresIndexStorageParams(value string) (map[string]string, error) {
	entries, err := decodePostgresNameList(value)
	if err != nil {
		return nil, err
	}
	params := map[string]string{}
	for _, entry := range entries {
		name, paramValue, found := strings.Cut(entry, "=")
		if !found {
			continue
		}
		if !slices.Contains(postgresRoundTrippableIndexStorageParams, name) {
			continue
		}
		params[name] = paramValue
	}
	if len(params) == 0 {
		return nil, nil
	}
	return params, nil
}

// decodePostgresNameList decodes a JSON array of names fetched alongside an
// index row -- the INCLUDE payload columns, the extensions the index resolves
// to -- and reports nil for an absent or empty one. Neither list is expected to
// be present on most indexes, so empty is not an error.
func decodePostgresNameList(value string) ([]string, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "[]" {
		return nil, nil
	}
	var names []string
	if err := json.Unmarshal([]byte(trimmed), &names); err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, nil
	}
	return names, nil
}

// postgresKeyOperatorClass is the operator class of one index key as the
// catalog reports it, before it is reduced to the spelling emitted DDL needs.
type postgresKeyOperatorClass struct {
	// Name is pg_opclass.opcname.
	Name string `json:"name"`
	// IsDefault is pg_opclass.opcdefault: this class is what the key's type
	// resolves to on this access method when CREATE INDEX names none.
	IsDefault bool `json:"is_default"`
	// Params is the class's parameters as PostgreSQL keeps them, in the index
	// attribute's pg_attribute.attoptions -- `siglen=64`, and empty for the
	// overwhelmingly common class that takes none.
	Params string `json:"params"`
}

// postgresOperatorClassSpelling reduces one key's operator class to the text
// the emitted CREATE INDEX has to carry, or the empty string when the index is
// reproduced exactly by naming no class at all.
//
// The parameter test comes first, and that order is the whole rule. A class can
// be both the key type's default and parameterised: measured on PostgreSQL
// 17.10, CREATE INDEX i ON t USING gist (tsv tsvector_ops (siglen = 64)) stores
// opcname tsvector_ops with opcdefault true and attoptions {siglen=64}. Testing
// IsDefault first reports the empty string for it and rebuilds the index with
// the 124-byte default signature -- a different index that psql accepts at exit
// 0 and that nothing reports. The class name is the only place its parameters
// can hang, so a parameterised class is named even when it is the default. See
// #1242.
func postgresOperatorClassSpelling(class postgresKeyOperatorClass) string {
	if class.Params != "" {
		return class.Name + "(" + class.Params + ")"
	}
	if class.IsDefault {
		return ""
	}
	return class.Name
}

// applyPostgresIndexOpclasses attaches the operator class of each key to its
// part.
//
// A list that does not line up with the parts is dropped rather than applied
// off by one: an operator class on the wrong key builds a different index than
// the one being read, which is worse than not carrying it at all.
func applyPostgresIndexOpclasses(parts []types.DBIndexPart, opclassesJSON string) ([]types.DBIndexPart, error) {
	opclasses, err := decodePostgresKeyList[postgresKeyOperatorClass](opclassesJSON, len(parts))
	if err != nil || opclasses == nil {
		return parts, err
	}
	for position := range parts {
		parts[position].Operator = postgresOperatorClassSpelling(opclasses[position])
	}
	return parts, nil
}

// PostgreSQL's per-key index option bits, from pg_index.indoption.
const (
	postgresIndexOptionDesc       = 1
	postgresIndexOptionNullsFirst = 2
)

// applyPostgresIndexOptions attaches the sort direction and NULLS ordering of
// each key to its part.
//
// Only an ordering that contradicts its direction's default is recorded:
// PostgreSQL gives NULLS LAST to ASC and NULLS FIRST to DESC, so recording the
// default would make the renderer emit a clause the pinned binary does not.
func applyPostgresIndexOptions(parts []types.DBIndexPart, optionsJSON string) ([]types.DBIndexPart, error) {
	options, err := decodePostgresKeyList[int](optionsJSON, len(parts))
	if err != nil || options == nil {
		return parts, err
	}
	for position := range parts {
		parts[position].Desc = options[position]&postgresIndexOptionDesc != 0
		parts[position].NullsOrder = postgresNullsOrder(options[position])
	}
	return parts, nil
}

// postgresNullsOrder reads the NULLS ordering out of one pg_index.indoption
// bitmask, or returns the empty string when the ordering is the default for
// the key's direction and does not have to be spelled out.
//
// The two default combinations are the two where the DESC bit and the
// NULLS FIRST bit agree: bitmask 0 is ASC NULLS LAST and bitmask 3 is
// DESC NULLS FIRST.
func postgresNullsOrder(optionBits int) string {
	descending := optionBits&postgresIndexOptionDesc != 0
	nullsFirst := optionBits&postgresIndexOptionNullsFirst != 0
	switch {
	case descending == nullsFirst:
		return ""
	case nullsFirst:
		return types.NullsOrderFirst
	default:
		return types.NullsOrderLast
	}
}

// decodePostgresKeyList decodes a per-key JSON array fetched alongside the key
// texts, returning nil when it is absent or does not have one entry per key.
func decodePostgresKeyList[T any](value string, keyCount int) ([]T, error) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" || trimmed == "[]" {
		return nil, nil
	}
	var decoded []T
	if err := json.Unmarshal([]byte(trimmed), &decoded); err != nil {
		return nil, err
	}
	if len(decoded) != keyCount {
		return nil, nil
	}
	return decoded, nil
}

func parsePostgresIndexColumns(value, indexDef string) ([]string, error) {
	if strings.TrimSpace(value) == "" || strings.TrimSpace(value) == "[]" {
		return extractPostgresIndexColumns(indexDef), nil
	}
	var columns []string
	if err := json.Unmarshal([]byte(value), &columns); err != nil {
		return nil, err
	}
	return columns, nil
}

// parsePostgresIndexParts labels each index key as a column reference or as an
// expression, using the pg_index.indkey attribute numbers fetched alongside the
// key texts. An attnum of 0 marks an expression key.
//
// It returns nil when the attnum list is missing or does not line up with the
// key texts, which leaves DBIndex.Parts empty and keeps the legacy
// columns-only representation rather than guessing.
func parsePostgresIndexParts(columns []string, attnumsJSON string) ([]types.DBIndexPart, error) {
	trimmed := strings.TrimSpace(attnumsJSON)
	if trimmed == "" || trimmed == "[]" {
		return nil, nil
	}
	var attnums []int
	if err := json.Unmarshal([]byte(trimmed), &attnums); err != nil {
		return nil, err
	}
	if len(attnums) != len(columns) {
		return nil, nil
	}

	parts := make([]types.DBIndexPart, len(columns))
	for position, key := range columns {
		// Attribute number 0 is PostgreSQL's marker for "this key is an
		// expression"; every real column has a positive attnum.
		if attnums[position] == 0 {
			parts[position] = types.DBIndexPart{Expr: key}
			continue
		}
		parts[position] = types.DBIndexPart{Name: key}
	}
	return parts, nil
}

func extractPostgresIndexColumns(indexDef string) []string {
	start := strings.Index(indexDef, "(")
	if start == -1 {
		return nil
	}
	depth := 0
	for i := start; i < len(indexDef); i++ {
		switch indexDef[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return splitPostgresIndexColumns(indexDef[start+1 : i])
			}
		}
	}
	return nil
}

func splitPostgresIndexColumns(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	columns := strings.Split(value, ",")
	for i, col := range columns {
		columns[i] = strings.TrimSpace(col)
	}
	return columns
}

// readConstraints reads all constraints
func (r *Reader) readConstraints() ([]types.DBConstraint, error) {
	// A server without pg_catalog cannot answer either half below: the basic
	// read is anchored in pg_constraint and aggregates with FILTER, and the
	// second reads pg_constraint directly. The SQL-standard catalog answers the
	// same question with different joins (stokaro/ptah#942).
	if !r.caps.Has(capability.PostgresCatalogFunctions) {
		var constraints []types.DBConstraint
		for _, schemaName := range r.schemasToRead() {
			schemaConstraints, err := r.readInformationSchemaConstraints(schemaName)
			if err != nil {
				return nil, err
			}
			constraints = append(constraints, schemaConstraints...)
		}
		return constraints, nil
	}

	// First, read basic constraint information from information_schema
	basicConstraints, err := r.readBasicConstraints()
	if err != nil {
		return nil, err
	}

	// Then, read PostgreSQL-specific constraints (like EXCLUDE) from pg_constraint
	pgConstraints, err := r.readPostgreSQLConstraints()
	if err != nil {
		return nil, err
	}

	// Combine both sets of constraints
	basicConstraints = append(basicConstraints, pgConstraints...)

	return basicConstraints, nil
}

// readBasicConstraints reads basic constraint information from information_schema
func (r *Reader) readBasicConstraints() ([]types.DBConstraint, error) {
	var constraints []types.DBConstraint
	for _, schemaName := range r.schemasToRead() {
		schemaConstraints, err := r.readBasicConstraintsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		constraints = append(constraints, schemaConstraints...)
	}
	return constraints, nil
}

func (r *Reader) readBasicConstraintsForSchema(schemaName string) ([]types.DBConstraint, error) {
	// PostgreSQL scopes constraint names to their owning tables. Joining the
	// information_schema FK views by schema and name therefore cross-products
	// same-named constraints on different tables. Anchor the row in
	// pg_constraint by owning relation and pair conkey/confkey by ordinality so
	// every local column, referenced column, and action comes from one object.
	// Unnest the arrays separately because CockroachDB returns no key rows for
	// the multi-array form when confkey is NULL, which hides PRIMARY KEY and
	// UNIQUE columns.
	constraintsQuery := `
			SELECT
				tc.table_schema,
				tc.table_name,
				tc.constraint_name,
				tc.constraint_type,
				COALESCE(string_agg(local_column.attname, ',' ORDER BY local_key_columns.ordinality)
					FILTER (WHERE local_column.attname IS NOT NULL), ''),
				COALESCE(max(foreign_schema.nspname), ''),
				COALESCE(max(foreign_table.relname), ''),
				COALESCE(string_agg(foreign_column.attname, ',' ORDER BY local_key_columns.ordinality)
					FILTER (WHERE foreign_column.attname IS NOT NULL), ''),
				COALESCE(max(CASE pc.confdeltype
					WHEN 'a' THEN 'NO ACTION'
					WHEN 'r' THEN 'RESTRICT'
					WHEN 'c' THEN 'CASCADE'
					WHEN 'n' THEN 'SET NULL'
					WHEN 'd' THEN 'SET DEFAULT'
				END), ''),
				COALESCE(max(CASE pc.confupdtype
					WHEN 'a' THEN 'NO ACTION'
					WHEN 'r' THEN 'RESTRICT'
					WHEN 'c' THEN 'CASCADE'
					WHEN 'n' THEN 'SET NULL'
					WHEN 'd' THEN 'SET DEFAULT'
				END), ''),
				` + r.constraintCheckExpr() + `,
				` + r.constraintDefinitionExpr() + `
		FROM information_schema.table_constraints AS tc
		JOIN pg_namespace AS constraint_schema
			ON constraint_schema.nspname = tc.table_schema
		JOIN pg_class AS constraint_table
			ON constraint_table.relnamespace = constraint_schema.oid
			AND constraint_table.relname = tc.table_name
		JOIN pg_constraint AS pc
			ON pc.connamespace = constraint_schema.oid
			AND pc.conrelid = constraint_table.oid
			AND pc.conname = tc.constraint_name
		LEFT JOIN LATERAL unnest(pc.conkey)
			WITH ORDINALITY AS local_key_columns(local_attnum, ordinality)
			ON true
		LEFT JOIN LATERAL unnest(pc.confkey)
			WITH ORDINALITY AS foreign_key_columns(foreign_attnum, ordinality)
			ON foreign_key_columns.ordinality = local_key_columns.ordinality
		LEFT JOIN pg_attribute AS local_column
			ON local_column.attrelid = pc.conrelid
			AND local_column.attnum = local_key_columns.local_attnum
		LEFT JOIN pg_class AS foreign_table
			ON foreign_table.oid = pc.confrelid
		LEFT JOIN pg_namespace AS foreign_schema
			ON foreign_schema.oid = foreign_table.relnamespace
		LEFT JOIN pg_attribute AS foreign_column
			ON foreign_column.attrelid = pc.confrelid
			AND foreign_column.attnum = foreign_key_columns.foreign_attnum
		WHERE tc.table_schema = $1
		AND tc.table_name NOT IN ('schema_migrations')
		GROUP BY
			tc.table_schema,
			tc.table_name,
			tc.constraint_name,
			tc.constraint_type
		ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name`

	rows, err := r.db.Query(constraintsQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query constraints: %w", err)
	}
	defer rows.Close()

	var constraints []types.DBConstraint
	for rows.Next() {
		var constraint types.DBConstraint
		var columnNames, foreignSchema, foreignTable, foreignColumns, deleteRule, updateRule, checkClause, constraintDefinition string

		err := rows.Scan(
			&constraint.Schema,
			&constraint.TableName,
			&constraint.Name,
			&constraint.Type,
			&columnNames,
			&foreignSchema,
			&foreignTable,
			&foreignColumns,
			&deleteRule,
			&updateRule,
			&checkClause,
			&constraintDefinition,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan constraint: %w", err)
		}

		// Set optional fields
		if columnNames != "" {
			constraint.ColumnNames = strings.Split(columnNames, ",")
			constraint.ColumnName = constraint.ColumnNames[0]
		}
		if foreignTable != "" {
			constraint.ForeignTable = &foreignTable
		}
		constraint.Schema = r.outputSchema(constraint.Schema)
		constraint.ForeignSchema = r.outputSchema(foreignSchema)
		if foreignColumns != "" {
			constraint.ForeignColumns = strings.Split(foreignColumns, ",")
			constraint.ForeignColumn = &constraint.ForeignColumns[0]
		}
		if deleteRule != "" {
			constraint.DeleteRule = &deleteRule
		}
		if updateRule != "" {
			constraint.UpdateRule = &updateRule
		}
		if checkClause != "" {
			constraint.CheckClause = &checkClause
		}
		constraint.NullsDistinct = postgresNullsDistinctFromDefinition(constraintDefinition)
		constraint.IncludeColumns = postgresIncludeColumnsFromDefinition(constraintDefinition)

		constraints = append(constraints, constraint)
	}

	return constraints, nil
}

func postgresNullsDistinctFromDefinition(definition string) *bool {
	upper := strings.ToUpper(definition)
	if strings.Contains(upper, "NULLS NOT DISTINCT") {
		return new(false)
	}
	if strings.Contains(upper, "NULLS DISTINCT") {
		return new(true)
	}
	return nil
}

func postgresIncludeColumnsFromDefinition(definition string) []string {
	upper := strings.ToUpper(definition)
	index := strings.Index(upper, "INCLUDE")
	if index < 0 {
		return nil
	}
	remaining := definition[index+len("INCLUDE"):]
	remaining = strings.TrimSpace(remaining)
	if !strings.HasPrefix(remaining, "(") {
		return nil
	}
	depth := 0
	for i := range remaining {
		switch remaining[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return unquotePostgresIdentifiers(splitPostgresIndexColumns(remaining[1:i]))
			}
		}
	}
	return nil
}

func unquotePostgresIdentifiers(identifiers []string) []string {
	for i, identifier := range identifiers {
		identifiers[i] = unquotePostgresIdentifier(identifier)
	}
	return identifiers
}

func unquotePostgresIdentifier(identifier string) string {
	trimmed := strings.TrimSpace(identifier)
	if len(trimmed) < 2 || trimmed[0] != '"' || trimmed[len(trimmed)-1] != '"' {
		return trimmed
	}
	return strings.ReplaceAll(trimmed[1:len(trimmed)-1], `""`, `"`)
}

// readPostgreSQLConstraints reads PostgreSQL-specific constraints from pg_constraint
func (r *Reader) readPostgreSQLConstraints() ([]types.DBConstraint, error) {
	var constraints []types.DBConstraint
	for _, schemaName := range r.schemasToRead() {
		schemaConstraints, err := r.readPostgreSQLConstraintsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		constraints = append(constraints, schemaConstraints...)
	}
	return constraints, nil
}

func (r *Reader) readPostgreSQLConstraintsForSchema(schemaName string) ([]types.DBConstraint, error) {
	// Query PostgreSQL system catalogs for PostgreSQL-specific constraints
	pgQuery := `
			SELECT
				n.nspname AS schema_name,
				c.conname AS constraint_name,
				cl.relname AS table_name,
				c.contype AS constraint_type,
			pg_get_constraintdef(c.oid) AS constraint_definition,
			-- The extensions the backing index resolves to. An EXCLUDE element
			-- prints its operator, and its operator class only when that class
			-- is not the default, so EXCLUDE USING gist (room WITH =, ...) over
			-- an integer column needs btree_gist and says nothing of it, while
			-- (txt gist_trgm_ops WITH =) needs pg_trgm and does print the
			-- class. See requiredExtensionsProjection.
			` + requiredExtensionsProjection("ix.indclass", "ic.relam") + ` AS required_extensions
		FROM pg_constraint c
		JOIN pg_class cl ON c.conrelid = cl.oid
		JOIN pg_namespace n ON cl.relnamespace = n.oid
		LEFT JOIN pg_index ix ON ix.indexrelid = c.conindid
		LEFT JOIN pg_class ic ON ic.oid = c.conindid
		WHERE c.contype IN ('x')  -- 'x' = exclusion constraint (add more types as needed)
		AND n.nspname = $1
		AND cl.relname NOT IN ('schema_migrations')
		ORDER BY cl.relname, c.conname`

	rows, err := r.db.Query(pgQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query PostgreSQL constraints: %w", err)
	}
	defer rows.Close()

	var constraints []types.DBConstraint
	for rows.Next() {
		var schemaName, constraintName, tableName, constraintType, definition string
		var requiredExtensions string
		err := rows.Scan(&schemaName, &constraintName, &tableName, &constraintType, &definition, &requiredExtensions)
		if err != nil {
			return nil, fmt.Errorf("failed to scan PostgreSQL constraint: %w", err)
		}

		required, err := decodePostgresNameList(requiredExtensions)
		if err != nil {
			return nil, fmt.Errorf("failed to parse required extensions for constraint %s: %w", constraintName, err)
		}
		slices.Sort(required)

		// Convert PostgreSQL constraint type to standard type
		var stdType string
		switch constraintType {
		case "x":
			stdType = "EXCLUDE"
		default:
			continue // Skip unknown types
		}

		constraint := types.DBConstraint{
			Name:               constraintName,
			TableName:          tableName,
			Schema:             r.outputSchema(schemaName),
			Type:               stdType,
			RequiresExtensions: required,
		}

		// Parse constraint definition for EXCLUDE constraints
		if stdType == "EXCLUDE" {
			parsed, err := r.ParseExcludeConstraintDefinition(definition)
			if err != nil {
				// Log the error but continue processing other constraints
				continue
			}

			if parsed.UsingMethod != "" {
				constraint.UsingMethod = &parsed.UsingMethod
			}
			if parsed.Elements != "" {
				constraint.ExcludeElements = &parsed.Elements
			}
			if parsed.WhereCondition != "" {
				constraint.WhereCondition = &parsed.WhereCondition
			}
		}

		constraints = append(constraints, constraint)
	}

	return constraints, nil
}

// ExcludeConstraintDefinition represents the parsed components of an EXCLUDE constraint
type ExcludeConstraintDefinition struct {
	UsingMethod    string
	Elements       string
	WhereCondition string
}

// ParseExcludeConstraintDefinition parses an EXCLUDE constraint definition from pg_get_constraintdef
// Example input: "EXCLUDE USING gist (room_id WITH =, during WITH &&) WHERE (is_active = true)"
func (r *Reader) ParseExcludeConstraintDefinition(definition string) (*ExcludeConstraintDefinition, error) {
	// Remove leading/trailing whitespace
	definition = strings.TrimSpace(definition)

	// Check if it starts with "EXCLUDE USING"
	if !strings.HasPrefix(strings.ToUpper(definition), "EXCLUDE USING") {
		return nil, fmt.Errorf("invalid EXCLUDE constraint definition: %s", definition)
	}

	// Remove "EXCLUDE USING " prefix
	remaining := strings.TrimSpace(definition[13:]) // len("EXCLUDE USING") = 13

	// Find the using method (first word)
	parts := strings.Fields(remaining)
	if len(parts) == 0 {
		return nil, fmt.Errorf("missing using method in EXCLUDE constraint: %s", definition)
	}
	usingMethod := parts[0]

	// Find the opening parenthesis for elements
	openParenIdx := strings.Index(remaining, "(")
	if openParenIdx == -1 {
		return nil, fmt.Errorf("missing opening parenthesis in EXCLUDE constraint: %s", definition)
	}

	// Find the matching closing parenthesis for elements
	parenCount := 0
	elementsEndIdx := -1
	for i := openParenIdx; i < len(remaining); i++ {
		if remaining[i] == '(' {
			parenCount++
		} else if remaining[i] == ')' {
			parenCount--
			if parenCount == 0 {
				elementsEndIdx = i
				break
			}
		}
	}

	if elementsEndIdx == -1 {
		return nil, fmt.Errorf("missing closing parenthesis in EXCLUDE constraint: %s", definition)
	}

	// Extract elements (content between parentheses)
	elements := strings.TrimSpace(remaining[openParenIdx+1 : elementsEndIdx])

	// Check for WHERE clause
	whereCondition := ""
	afterElements := strings.TrimSpace(remaining[elementsEndIdx+1:])
	if strings.HasPrefix(strings.ToUpper(afterElements), "WHERE") {
		whereClause := strings.TrimSpace(afterElements[5:]) // len("WHERE") = 5
		// Remove outer parentheses if present
		if strings.HasPrefix(whereClause, "(") && strings.HasSuffix(whereClause, ")") {
			whereCondition = strings.TrimSpace(whereClause[1 : len(whereClause)-1])
		} else {
			whereCondition = whereClause
		}
	}

	return &ExcludeConstraintDefinition{
		UsingMethod:    usingMethod,
		Elements:       elements,
		WhereCondition: whereCondition,
	}, nil
}

func (r *Reader) readExtensions() ([]types.DBExtension, error) {
	// Use a simpler query that only relies on pg_extension and pg_namespace
	// These are core system catalogs that are consistent across PostgreSQL versions
	extensionsQuery := `
		SELECT
			e.extname AS extension_name,
			e.extversion AS installed_version,
			n.nspname AS schema_name,
			e.extrelocatable AS relocatable,
			obj_description(e.oid, 'pg_extension') AS comment
		FROM pg_extension e
		JOIN pg_namespace n ON n.oid = e.extnamespace
		ORDER BY e.extname`

	rows, err := r.db.Query(extensionsQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query extensions: %w", err)
	}
	defer rows.Close()

	var extensions []types.DBExtension
	for rows.Next() {
		var ext types.DBExtension
		var comment sql.NullString

		err := rows.Scan(
			&ext.Name,
			&ext.Version,
			&ext.Schema,
			&ext.Relocatable,
			&comment,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan extension: %w", err)
		}

		// Set optional fields
		if comment.Valid {
			ext.Comment = &comment.String
		}

		// Set installed version (same as version for installed extensions)
		ext.InstalledVersion = &ext.Version

		extensions = append(extensions, ext)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate extensions: %w", err)
	}

	provides, err := r.readExtensionMembers()
	if err != nil {
		return nil, err
	}
	for i := range extensions {
		extensions[i].Provides = provides[extensions[i].Name]
	}

	return extensions, nil
}

// readExtensionMembers reads, per extension, the catalog names that extension
// supplies, keyed by extension name.
//
// This exists because an extension is almost never referenced by its own name.
// A document that depends on `isn` says `isbn`; one that depends on `pgcrypto`
// says `gen_salt`. Asking the catalog is what makes "does anything still need
// this extension" answerable without a hand-maintained table of well-known
// extensions, which would be both unbounded and wrong the moment someone
// installs an extension nobody listed.
//
// pg_depend with deptype 'e' is the extension-membership edge, and the member's
// name lives in a different catalog per object class, hence the union. The five
// classes covered are the ones whose names can appear as an identifier in a
// rendered schema document: types (column types, domains), functions (defaults,
// checks, generated and index expressions, view and trigger bodies), relations
// (extension-supplied tables, views and sequences), and operator classes and
// families (index USING clauses).
//
// Measured on PostgreSQL 17.10: no pg_namespace row is ever an extension member
// for an extension installed into an existing schema, so `public` does not enter
// this set and naming the schema does not pin every extension in the database.
//
// Every arm excludes members whose name also resolves in pg_catalog. Contrib
// extensions overwhelmingly supply OVERLOADS of core functions, so their raw
// member lists contain ordinary words: measured on PostgreSQL 17.10, `citext`
// supplies fifteen names pg_catalog also supplies, among them `max`, `min`,
// `strpos`, `replace`, `split_part` and `translate`, and `pgcrypto` supplies
// `gen_random_uuid`, which core has had since 13. A name pg_catalog also
// supplies is no evidence of a dependency, because pg_catalog is always on the
// search path -- the document resolves it with the extension dropped. Counting
// such a name as evidence pins the extension to schemas that have no
// relationship to it (stokaro/ptah#1280).
//
// The exclusion cannot cost a genuine dependency. Reaching an extension's
// overload rather than the core one requires arguments of that extension's own
// type, and naming that type is what keeps the extension alive through the
// pg_type arm.
//
// The function arm drops one more class of name, and only where dropping it is
// provably free: a name that is a SQL keyword, when the SAME extension also
// contributes a type this member list reports and that type appears in the
// function's own signature. The keyword list comes from the server through
// pg_get_keywords().
//
// The reason to drop such a name at all is that the scan consuming this list
// splits SQL text into identifier-shaped words with no notion of position.
// Measured on PostgreSQL 18.4, `hstore` supplies three functions named `delete`
// and pg_catalog supplies none, so the exclusion above keeps all three, and
// `DELETE FROM audit` in a plpgsql body then reads exactly like a call to
// `delete(h, 'k')` and pins hstore to a database that does not use it
// (stokaro/ptah#1281).
//
// The list has to come from the server because no hand-written one is right.
// `delete`, `each` and `index` are UNRESERVED words: PostgreSQL 18.4 reports 78
// reserved words against 330 unreserved ones, so filtering the reserved list
// catches none of the three, and pinning the unreserved list in Go would be 330
// words that move every release.
//
// The type condition is what makes the drop safe, and it is enforced here
// rather than asserted about the extensions that happen to be installed. An
// earlier form of this filter dropped every keyword-named function member and
// justified it with a survey: over the contrib extensions this build ships, the
// keyword-named function members are hstore's `delete` and `each`, ltree's
// `index` and cube's `cube`, and every one of them takes or returns a type the
// same extension supplies, so a genuine call spells that type on a column, in a
// signature or in a cast and the pg_type arm keeps the extension. That is a
// property of contrib, not of the query. An extension supplying `merge(text,
// text)` and no type at all has its only evidence in that name, and dropping it
// leaves a document whose CHECK calls a function nothing declares -- measured
// on PostgreSQL 18.4 against a fixture extension of exactly that shape, where
// the pinned Atlas community binary v1.3.0 answered `create "names" table: pq:
// function merge(text, text) does not exist`. Requiring the answering type
// makes the redundancy a precondition instead of a coincidence; on the 46
// extensions available here it excludes the same four names as the survey did.
//
// The answering type must itself survive the pg_type arm, or the redundancy is
// asserted one level down instead of two: an extension supplying a type named
// like a pg_catalog type has that name filtered out above, so it can no longer
// answer for anything.
//
// Only this arm is filtered. A type, a relation, an operator class and an
// operator family are each named by nothing but themselves, so excluding a
// keyword-shaped one would throw away the only evidence there is. `cube` shows
// the residue that leaves: its type and its constructor share a name, the type
// arm keeps supplying it, and a view saying `GROUP BY CUBE(x)` still pins the
// extension. That is the cheaper of the two errors -- a block kept where it
// could have been dropped costs this document its compatibility, a block
// dropped where a column needs it costs a document nobody can read.
func (r *Reader) readExtensionMembers() (map[string][]string, error) {
	const membersQuery = `
		SELECT e.extname AS extension_name, t.typname AS member_name
		  FROM pg_depend d
		  JOIN pg_extension e ON e.oid = d.refobjid
		  JOIN pg_type t ON t.oid = d.objid
		 WHERE d.deptype = 'e' AND d.classid = 'pg_type'::regclass
		   AND NOT EXISTS (
		       SELECT 1 FROM pg_type core
		         JOIN pg_namespace corens ON corens.oid = core.typnamespace
		        WHERE corens.nspname = 'pg_catalog' AND core.typname = t.typname)
		UNION
		SELECT e.extname, p.proname
		  FROM pg_depend d
		  JOIN pg_extension e ON e.oid = d.refobjid
		  JOIN pg_proc p ON p.oid = d.objid
		 WHERE d.deptype = 'e' AND d.classid = 'pg_proc'::regclass
		   AND NOT EXISTS (
		       SELECT 1 FROM pg_proc core
		         JOIN pg_namespace corens ON corens.oid = core.pronamespace
		        WHERE corens.nspname = 'pg_catalog' AND core.proname = p.proname)
		   AND NOT (
		       EXISTS (
		           SELECT 1 FROM pg_get_keywords() k WHERE k.word = p.proname)
		       AND EXISTS (
		           SELECT 1
		             FROM pg_depend typedep
		             JOIN pg_type answering ON answering.oid = typedep.objid
		            WHERE typedep.deptype = 'e'
		              AND typedep.classid = 'pg_type'::regclass
		              AND typedep.refobjid = e.oid
		              -- Argument position ONLY. The redundancy this gate relies on
		              -- is that a genuine call spells the answering type, and only
		              -- an argument obliges the caller to: a return type or an OUT
		              -- parameter is named by the function, not by the call site.
		              -- Measured: an extension supplying merge(text, text)
		              -- RETURNS kwbox had its member dropped while the document
		              -- kept a CHECK calling merge, which then failed to
		              -- materialize (stokaro/ptah#1281).
		              AND answering.oid = ANY (p.proargtypes)
		              AND NOT EXISTS (
		                  SELECT 1 FROM pg_type core
		                    JOIN pg_namespace corens ON corens.oid = core.typnamespace
		                   WHERE corens.nspname = 'pg_catalog'
		                     AND core.typname = answering.typname)))
		UNION
		SELECT e.extname, c.relname
		  FROM pg_depend d
		  JOIN pg_extension e ON e.oid = d.refobjid
		  JOIN pg_class c ON c.oid = d.objid
		 WHERE d.deptype = 'e' AND d.classid = 'pg_class'::regclass
		   AND NOT EXISTS (
		       SELECT 1 FROM pg_class core
		         JOIN pg_namespace corens ON corens.oid = core.relnamespace
		        WHERE corens.nspname = 'pg_catalog' AND core.relname = c.relname)
		UNION
		SELECT e.extname, opc.opcname
		  FROM pg_depend d
		  JOIN pg_extension e ON e.oid = d.refobjid
		  JOIN pg_opclass opc ON opc.oid = d.objid
		 WHERE d.deptype = 'e' AND d.classid = 'pg_opclass'::regclass
		   AND NOT EXISTS (
		       SELECT 1 FROM pg_opclass core
		         JOIN pg_namespace corens ON corens.oid = core.opcnamespace
		        WHERE corens.nspname = 'pg_catalog' AND core.opcname = opc.opcname)
		UNION
		SELECT e.extname, opf.opfname
		  FROM pg_depend d
		  JOIN pg_extension e ON e.oid = d.refobjid
		  JOIN pg_opfamily opf ON opf.oid = d.objid
		 WHERE d.deptype = 'e' AND d.classid = 'pg_opfamily'::regclass
		   AND NOT EXISTS (
		       SELECT 1 FROM pg_opfamily core
		         JOIN pg_namespace corens ON corens.oid = core.opfnamespace
		        WHERE corens.nspname = 'pg_catalog' AND core.opfname = opf.opfname)
		 ORDER BY 1, 2`

	rows, err := r.db.Query(membersQuery)
	if err != nil {
		return nil, fmt.Errorf("failed to query extension members: %w", err)
	}
	defer rows.Close()

	members := map[string][]string{}
	for rows.Next() {
		var extensionName, memberName string
		if err := rows.Scan(&extensionName, &memberName); err != nil {
			return nil, fmt.Errorf("failed to scan extension member: %w", err)
		}
		members[extensionName] = append(members[extensionName], memberName)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate extension members: %w", err)
	}

	return members, nil
}

// readSequences reads standalone PostgreSQL sequences.
//
// It deliberately excludes implicit sequences that back a SERIAL / BIGSERIAL /
// identity column. PostgreSQL makes a SERIAL-generated sequence and a manually
// created sequence attached with OWNED BY catalog-identical at the ownership
// level (both carry an auto pg_depend edge to a column), and pg_get_serial_sequence
// keys on that same ownership edge — so neither deptype nor pg_get_serial_sequence
// alone can separate them. The distinguishing signal is the owning column's
// DEFAULT: a sequence is implicit exactly when it is an identity sequence
// (INTERNAL dependency) or is owned by a column whose DEFAULT draws from that
// very sequence (the SERIAL shape). This means:
//   - a standalone sequence merely consumed via DEFAULT nextval(...) but not
//     owned by any column is surfaced (the common shared-sequence case);
//   - a sequence with a lifecycle-only OWNED BY (owner column does not draw its
//     default from it) is surfaced, with OwnedBy populated;
//   - only genuine SERIAL/identity backing sequences are excluded.
func (r *Reader) readSequences() ([]types.DBSequence, error) {
	var sequences []types.DBSequence
	for _, schemaName := range r.schemasToRead() {
		schemaSequences, err := r.readSequencesForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		sequences = append(sequences, schemaSequences...)
	}
	return sequences, nil
}

func (r *Reader) readSequencesForSchema(schemaName string) ([]types.DBSequence, error) {
	const query = `
		SELECT
			n.nspname AS schema_name,
			c.relname AS sequence_name,
			format_type(s.seqtypid, NULL) AS data_type,
			s.seqstart AS start_value,
			s.seqincrement AS increment_by,
			s.seqmin AS min_value,
			s.seqmax AS max_value,
			s.seqcache AS cache_size,
			s.seqcycle AS is_cycled,
			owner_ns.nspname AS owned_schema,
			owner_tbl.relname AS owned_table,
			owner_col.attname AS owned_column,
			obj_description(c.oid, 'pg_class') AS comment,
			CASE
				WHEN dep.refobjid IS NULL THEN false
				WHEN dep.deptype = 'i' THEN true
				-- A sequence a DOMAIN column draws from is not implicit, however
				-- the catalog edges look. "Implicit" here means "writing the
				-- owning column back recreates this sequence on its own", and
				-- only the SERIAL shorthand and an identity column do that.
				-- Neither is available to a domain column: SERIAL always builds
				-- an integer column, so the column is written back as its domain
				-- with an ordinary nextval default, and the sequence that
				-- default names has to be created too. Calling it implicit
				-- omitted the CREATE SEQUENCE and the emitted DDL failed on
				-- replay with ERROR: relation "s" does not exist. See
				-- stokaro/ptah#1242, and #657 for the rest of this rule.
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
		FROM pg_sequence s
		JOIN pg_class c ON c.oid = s.seqrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN pg_depend dep ON dep.objid = c.oid
			AND dep.classid = 'pg_class'::regclass
			AND dep.refclassid = 'pg_class'::regclass
			AND dep.deptype IN ('a', 'i')
			AND dep.refobjsubid > 0
		LEFT JOIN pg_class owner_tbl ON owner_tbl.oid = dep.refobjid
		LEFT JOIN pg_namespace owner_ns ON owner_ns.oid = owner_tbl.relnamespace
		LEFT JOIN pg_attribute owner_col ON owner_col.attrelid = dep.refobjid AND owner_col.attnum = dep.refobjsubid
		LEFT JOIN pg_type owner_col_type ON owner_col_type.oid = owner_col.atttypid
		WHERE n.nspname = $1
		ORDER BY n.nspname, c.relname`

	rows, err := r.db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query sequences for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var sequences []types.DBSequence
	for rows.Next() {
		var (
			seq         types.DBSequence
			rawSchema   string
			start       int64
			increment   int64
			minValue    int64
			maxValue    int64
			cache       int64
			ownedSchema sql.NullString
			ownedTable  sql.NullString
			ownedColumn sql.NullString
			comment     sql.NullString
			isImplicit  bool
		)
		if err := rows.Scan(
			&rawSchema,
			&seq.Name,
			&seq.DataType,
			&start,
			&increment,
			&minValue,
			&maxValue,
			&cache,
			&seq.Cycle,
			&ownedSchema,
			&ownedTable,
			&ownedColumn,
			&comment,
			&isImplicit,
		); err != nil {
			return nil, fmt.Errorf("failed to scan sequence for schema %s: %w", schemaName, err)
		}
		if isImplicit {
			// Sequence is a SERIAL/identity backing sequence; it is managed by
			// its owning column, not as a standalone object.
			continue
		}
		seq.Schema = r.outputSchema(rawSchema)
		seq.Start = &start
		seq.Increment = &increment
		seq.MinValue = &minValue
		seq.MaxValue = &maxValue
		seq.Cache = &cache
		if ownedTable.Valid && ownedColumn.Valid {
			// Qualify the owner with its schema only when it differs from the
			// sequence's own schema, so same-schema owners round-trip against
			// the common unqualified `owned_by="table.column"` annotation.
			owner := ownedTable.String + "." + ownedColumn.String
			if ownedSchema.Valid && ownedSchema.String != rawSchema {
				owner = ownedSchema.String + "." + owner
			}
			seq.OwnedBy = owner
		}
		if comment.Valid {
			seq.Comment = comment.String
		}
		sequences = append(sequences, seq)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read sequences for schema %s: %w", schemaName, err)
	}
	return sequences, nil
}

// readSequenceGrantsForSchema reads GRANTs on standalone sequences. standalone
// holds the qualified names (schema.name, as introspected) of sequences that
// readSequences classified as standalone, so grants on implicit serial/identity
// sequences are not surfaced as spurious diffs.
func (r *Reader) readSequenceGrantsForSchema(schemaName string, standalone map[string]bool) ([]types.DBGrant, error) {
	const query = `
		SELECT
			COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
			acl.privilege_type,
			n.nspname AS schema_name,
			c.relname AS object_name,
			acl.is_grantable AS with_option,
			grantor.rolname AS grantor
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(c.relacl) acl
		LEFT JOIN pg_roles grantee ON grantee.oid = acl.grantee
		JOIN pg_roles grantor ON grantor.oid = acl.grantor
		WHERE c.relkind = 'S'
		AND n.nspname = $1
		AND COALESCE(grantee.rolname, 'PUBLIC') NOT LIKE 'pg\_%' ESCAPE '\'
		AND COALESCE(grantee.rolname, 'PUBLIC') != 'postgres'
		ORDER BY n.nspname, c.relname, COALESCE(grantee.rolname, 'PUBLIC'), acl.privilege_type`

	rows, err := r.db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query sequence grants for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var grants []types.DBGrant
	for rows.Next() {
		grant := types.DBGrant{ObjectType: "SEQUENCE"}
		var rawSchema string
		if err := rows.Scan(&grant.Role, &grant.Privilege, &rawSchema, &grant.ObjectName, &grant.WithOption, &grant.GrantedBy); err != nil {
			return nil, fmt.Errorf("failed to scan sequence grant for schema %s: %w", schemaName, err)
		}
		if !standalone[types.QualifyTableName(r.outputSchema(rawSchema), grant.ObjectName)] {
			continue
		}
		grant.Schema = r.outputSchema(rawSchema)
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read sequence grants for schema %s: %w", schemaName, err)
	}
	return grants, nil
}

// enhanceTablesWithConstraints adds constraint information to table columns
func (r *Reader) enhanceTablesWithConstraints(tables []types.DBTable, constraints []types.DBConstraint) {
	// Create maps for quick lookup
	primaryKeys := make(map[string]map[string]bool)
	uniqueKeys := make(map[string]map[string]bool)

	for _, constraint := range constraints {
		tableName := constraint.QualifiedTableName()
		if constraint.Type == "PRIMARY KEY" {
			if primaryKeys[tableName] == nil {
				primaryKeys[tableName] = make(map[string]bool)
			}
			primaryKeys[tableName][constraint.ColumnName] = true
		}
		if constraint.Type == "UNIQUE" {
			columns := constraint.ColumnNamesOrDefault()
			if len(columns) != 1 {
				continue
			}
			if uniqueKeys[tableName] == nil {
				uniqueKeys[tableName] = make(map[string]bool)
			}
			uniqueKeys[tableName][columns[0]] = true
		}
	}

	// Update table columns with constraint information
	for i := range tables {
		for j := range tables[i].Columns {
			col := &tables[i].Columns[j]           // #nosec G602 -- index bounded by `range tables[i].Columns`
			tableName := tables[i].QualifiedName() // #nosec G602 -- index bounded by `range tables`

			if primaryKeys[tableName] != nil && primaryKeys[tableName][col.Name] {
				col.IsPrimaryKey = true
			}
			if uniqueKeys[tableName] != nil && uniqueKeys[tableName][col.Name] {
				col.IsUnique = true
			}
		}
	}
}

// enhanceTablesWithIndexes marks primary-key columns from the actual
// primary-key indexes.
//
// This is authoritative: a real SERIAL primary key has a primary-key index that
// PostgreSQL creates, so it is detected here. It deliberately does NOT infer a
// primary key from a column merely being auto-increment: a column that draws
// its default from a standalone sequence via nextval(...) is auto-increment but
// is not a primary key, and inferring one produced a phantom primary-key diff
// on an otherwise clean round-trip (issue #675).
func (r *Reader) enhanceTablesWithIndexes(tables []types.DBTable, indexes []types.DBIndex) {
	primaryKeyColumns := make(map[string]map[string]bool)
	for _, index := range indexes {
		if !index.IsPrimary {
			continue
		}
		// Key on the schema-qualified table name (matching
		// enhanceTablesWithConstraints) so same-named tables in different schemas
		// do not merge their primary-key columns.
		tableName := index.QualifiedTableName()
		if primaryKeyColumns[tableName] == nil {
			primaryKeyColumns[tableName] = make(map[string]bool)
		}
		for _, column := range index.Columns {
			primaryKeyColumns[tableName][column] = true
		}
	}
	for i := range tables {
		tableName := tables[i].QualifiedName()
		for j := range tables[i].Columns {
			col := &tables[i].Columns[j]
			if primaryKeyColumns[tableName][col.Name] {
				col.IsPrimaryKey = true
			}
		}
	}
}

// readFunctions reads all PostgreSQL custom functions from the database.
//
// This function automatically excludes ALL extension-owned functions to prevent
// migration generation from attempting to drop functions that are managed by
// PostgreSQL extensions. This is a generic solution that works for any extension
// (btree_gin, pg_trgm, uuid-ossp, postgis, hstore, etc.) without requiring
// manual configuration of specific extension names.
//
// The exclusion is implemented using PostgreSQL system catalogs:
// - pg_depend: tracks dependencies between database objects
// - pg_extension: contains information about installed extensions
// - Functions with dependency type 'e' (extension) are automatically filtered out
//
// This approach is more robust than maintaining a manual list of problematic
// extensions because it automatically handles any extension that creates functions.
func (r *Reader) readFunctions() ([]types.DBFunction, error) {
	var functions []types.DBFunction
	for _, schemaName := range r.schemasToRead() {
		schemaFunctions, err := r.readFunctionsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		functions = append(functions, schemaFunctions...)
	}
	return functions, nil
}

func (r *Reader) readViews() ([]types.DBView, error) {
	var views []types.DBView
	for _, schemaName := range r.schemasToRead() {
		schemaViews, err := r.readViewsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		views = append(views, schemaViews...)
	}
	return views, nil
}

func (r *Reader) readViewsForSchema(schemaName string) ([]types.DBView, error) {
	viewsQuery := `
		SELECT
			n.nspname AS schema_name,
			c.relname AS view_name,
			pg_get_viewdef(c.oid, true) AS view_definition,
			COALESCE(v.check_option, 'NONE') AS check_option,
			COALESCE(obj_description(c.oid, 'pg_class'), '') AS comment
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		LEFT JOIN information_schema.views v
			ON v.table_schema = n.nspname AND v.table_name = c.relname
		WHERE n.nspname = $1
		AND c.relkind = 'v'
		AND c.relname NOT IN ('schema_migrations')
		ORDER BY c.relname`

	rows, err := r.db.Query(viewsQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query views: %w", err)
	}
	defer rows.Close()

	var views []types.DBView
	for rows.Next() {
		var view types.DBView
		err := rows.Scan(&view.Schema, &view.Name, &view.Body, &view.CheckOption, &view.Comment)
		if err != nil {
			return nil, fmt.Errorf("failed to scan view: %w", err)
		}
		view.Schema = r.outputSchema(view.Schema)
		views = append(views, view)
	}
	return views, nil
}

func (r *Reader) readMaterializedViews() ([]types.DBMatView, error) {
	var views []types.DBMatView
	for _, schemaName := range r.schemasToRead() {
		schemaViews, err := r.readMaterializedViewsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		views = append(views, schemaViews...)
	}
	return views, nil
}

func (r *Reader) readMaterializedViewsForSchema(schemaName string) ([]types.DBMatView, error) {
	viewsQuery := `
		SELECT
			n.nspname AS schema_name,
			c.relname AS view_name,
			pg_get_viewdef(c.oid, true) AS view_definition,
			COALESCE(obj_description(c.oid, 'pg_class'), '') AS comment
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		AND c.relkind = 'm'
		ORDER BY c.relname`

	rows, err := r.db.Query(viewsQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query materialized views: %w", err)
	}
	defer rows.Close()

	var views []types.DBMatView
	for rows.Next() {
		var view types.DBMatView
		err := rows.Scan(&view.Schema, &view.Name, &view.Body, &view.Comment)
		if err != nil {
			return nil, fmt.Errorf("failed to scan materialized view: %w", err)
		}
		view.Schema = r.outputSchema(view.Schema)
		view.RefreshStrategy = "manual"
		views = append(views, view)
	}
	return views, nil
}

func (r *Reader) readTriggers() ([]types.DBTrigger, error) {
	var triggers []types.DBTrigger
	for _, schemaName := range r.schemasToRead() {
		schemaTriggers, err := r.readTriggersForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers, schemaTriggers...)
	}
	return triggers, nil
}

func (r *Reader) readTriggersForSchema(schemaName string) ([]types.DBTrigger, error) {
	triggersQuery := `
		SELECT
			n.nspname AS schema_name,
			tbl.relname AS table_name,
			trg.tgname AS trigger_name,
			CASE
				WHEN (trg.tgtype & 2) <> 0 THEN 'BEFORE'
				WHEN (trg.tgtype & 64) <> 0 THEN 'INSTEAD OF'
				ELSE 'AFTER'
			END AS timing,
			concat_ws(' OR ',
				CASE WHEN (trg.tgtype & 4) <> 0 THEN 'INSERT' END,
				CASE WHEN (trg.tgtype & 8) <> 0 THEN 'DELETE' END,
				CASE WHEN (trg.tgtype & 16) <> 0 THEN 'UPDATE' END,
				CASE WHEN (trg.tgtype & 32) <> 0 THEN 'TRUNCATE' END
			) AS event,
			CASE WHEN (trg.tgtype & 1) <> 0 THEN 'ROW' ELSE 'STATEMENT' END AS for_each,
			p.prosrc AS body,
			COALESCE(obj_description(trg.oid, 'pg_trigger'), '') AS comment
		FROM pg_trigger trg
		JOIN pg_class tbl ON tbl.oid = trg.tgrelid
		JOIN pg_namespace n ON n.oid = tbl.relnamespace
		JOIN pg_proc p ON p.oid = trg.tgfoid
		WHERE n.nspname = $1
		AND NOT trg.tgisinternal
		ORDER BY tbl.relname, trg.tgname`

	rows, err := r.db.Query(triggersQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query triggers: %w", err)
	}
	defer rows.Close()

	var triggers []types.DBTrigger
	for rows.Next() {
		var trigger types.DBTrigger
		err := rows.Scan(
			&trigger.Schema,
			&trigger.Table,
			&trigger.Name,
			&trigger.Timing,
			&trigger.Event,
			&trigger.ForEach,
			&trigger.Body,
			&trigger.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan trigger: %w", err)
		}
		trigger.Schema = r.outputSchema(trigger.Schema)
		triggers = append(triggers, trigger)
	}
	return triggers, nil
}

func (r *Reader) readFunctionsForSchema(schemaName string) ([]types.DBFunction, error) {
	functionsQuery := `
		SELECT
			p.proname AS function_name,
			pg_get_function_arguments(p.oid) AS parameters,
			pg_get_function_identity_arguments(p.oid) AS identity_arguments,
			pg_get_function_result(p.oid) AS returns,
			l.lanname AS language,
			CASE p.prosecdef WHEN true THEN 'DEFINER' ELSE 'INVOKER' END AS security,
			CASE p.provolatile
				WHEN 'i' THEN 'IMMUTABLE'
				WHEN 's' THEN 'STABLE'
				WHEN 'v' THEN 'VOLATILE'
			END AS volatility,
			p.prosrc AS body,
			COALESCE(obj_description(p.oid, 'pg_proc'), '') AS comment
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		JOIN pg_language l ON l.oid = p.prolang
		WHERE n.nspname = $1
		AND p.prokind = 'f'  -- Only functions, not procedures
		AND l.lanname != 'internal'  -- Exclude internal functions
		-- Escaped for the same reason as the role filters above: a bare _
		-- is a LIKE wildcard, so the unescaped form also excluded ordinary
		-- functions such as ptahxtriggery (stokaro/ptah#1291).
		AND p.proname NOT LIKE 'ptah\_trigger\_%' ESCAPE '\'
		-- Exclude extension-owned functions to prevent migration issues
		-- Extension functions cannot be dropped independently and should be managed by the extension
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY p.proname, pg_get_function_identity_arguments(p.oid)`

	rows, err := r.db.Query(functionsQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query functions: %w", err)
	}
	defer rows.Close()

	var functions []types.DBFunction
	for rows.Next() {
		var fn types.DBFunction
		var identityArguments string
		err := rows.Scan(
			&fn.Name,
			&fn.Parameters,
			&identityArguments,
			&fn.Returns,
			&fn.Language,
			&fn.Security,
			&fn.Volatility,
			&fn.Body,
			&fn.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan function: %w", err)
		}

		// Same convention as tables, views and domains: blank for the
		// connection's own schema, named otherwise, so `--exclude app.fn_app`
		// has a qualified candidate to match (stokaro/ptah#933).
		fn.Schema = r.outputSchema(schemaName)
		fn.IdentityArguments = &identityArguments
		functions = append(functions, fn)
	}

	return functions, nil
}

// readRLSPolicies reads all PostgreSQL RLS policies from the database
func (r *Reader) readRLSPolicies() ([]types.DBRLSPolicy, error) {
	var policies []types.DBRLSPolicy
	for _, schemaName := range r.schemasToRead() {
		schemaPolicies, err := r.readRLSPoliciesForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		policies = append(policies, schemaPolicies...)
	}
	return policies, nil
}

func (r *Reader) readRLSPoliciesForSchema(schemaName string) ([]types.DBRLSPolicy, error) {
	rlsPoliciesQuery := `
		SELECT
			n.nspname AS schema_name,
			pol.polname AS policy_name,
			c.relname AS table_name,
			CASE pol.polcmd
				WHEN 'r' THEN 'SELECT'
				WHEN 'a' THEN 'INSERT'
				WHEN 'w' THEN 'UPDATE'
				WHEN 'd' THEN 'DELETE'
				WHEN '*' THEN 'ALL'
			END AS policy_for,
			CASE
				WHEN array_length(pol.polroles, 1) = 1 AND 0 = ANY(pol.polroles) THEN 'PUBLIC'
				ELSE array_to_string(ARRAY(
					SELECT rolname FROM pg_roles WHERE oid = ANY(pol.polroles)
				), ',')
			END AS to_roles,
			COALESCE(pg_get_expr(pol.polqual, pol.polrelid), '') AS using_expression,
			COALESCE(pg_get_expr(pol.polwithcheck, pol.polrelid), '') AS with_check_expression,
			COALESCE(obj_description(pol.oid, 'pg_policy'), '') AS comment
		FROM pg_policy pol
		JOIN pg_class c ON c.oid = pol.polrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = $1
		ORDER BY c.relname, pol.polname`

	rows, err := r.db.Query(rlsPoliciesQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query RLS policies: %w", err)
	}
	defer rows.Close()

	var policies []types.DBRLSPolicy
	for rows.Next() {
		var policy types.DBRLSPolicy
		var schemaName string
		err := rows.Scan(
			&schemaName,
			&policy.Name,
			&policy.Table,
			&policy.PolicyFor,
			&policy.ToRoles,
			&policy.UsingExpression,
			&policy.WithCheckExpression,
			&policy.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan RLS policy: %w", err)
		}
		policy.Table = types.QualifyTableName(r.outputSchema(schemaName), policy.Table)

		policies = append(policies, policy)
	}

	return policies, nil
}

// rolesInScopeClauses returns the branches of the role-scoping union, one per
// reason a role counts as used by the inspected schemas. Each branch reads the
// catalog column that carries the reason, so the reason a role is reported is
// always a fact about the inspected scope rather than about the server.
//
// The `scope` CTE the branches join against is defined by readRoles.
func (r *Reader) rolesInScopeClauses() []string {
	clauses := []string{
		// Holds a privilege on a relation in scope -- table, view,
		// materialized view or sequence (pg_class.relacl). An owner appears
		// here as soon as the relation carries any explicit privilege, which
		// is also exactly when readTableGrantsForSchema reports the owner's
		// own grants.
		`SELECT acl.grantee AS roleoid FROM pg_class c
			JOIN scope s ON s.oid = c.relnamespace
			CROSS JOIN LATERAL aclexplode(c.relacl) acl`,
		// Granted a privilege on a relation in scope (pg_class.relacl).
		`SELECT acl.grantor FROM pg_class c
			JOIN scope s ON s.oid = c.relnamespace
			CROSS JOIN LATERAL aclexplode(c.relacl) acl`,
		// Holds a privilege on a schema in scope (pg_namespace.nspacl).
		`SELECT acl.grantee FROM scope s
			CROSS JOIN LATERAL aclexplode(s.nspacl) acl`,
		// Granted a privilege on a schema in scope (pg_namespace.nspacl).
		`SELECT acl.grantor FROM scope s
			CROSS JOIN LATERAL aclexplode(s.nspacl) acl`,
	}
	if r.caps.Has(capability.RowLevelSecurity) {
		// Named by a row-level security policy on a table in scope
		// (pg_policy.polroles), read in the same shape readRLSPolicies uses.
		clauses = append(clauses, `SELECT policyrole FROM pg_policy pol
			JOIN pg_class c ON c.oid = pol.polrelid
			JOIN scope s ON s.oid = c.relnamespace
			CROSS JOIN LATERAL unnest(pol.polroles) AS policyrole`)
	}
	return clauses
}

// readRoles reads the PostgreSQL roles the inspected scope actually uses.
//
// Roles are cluster-wide: pg_roles lists every role on the server, including
// roles that exist only for databases this reader was never pointed at.
// Reporting all of them describes objects the inspected schema does not
// contain, cannot be replayed (CREATE ROLE is not idempotent, and creating a
// fresh database does not clear cluster roles), and on a shared instance
// discloses every other tenant's role names. See stokaro/ptah#1267.
//
// "Uses" is defined here, deliberately, as any of:
//
//   - holding a privilege on a relation in a schema in scope, or on one of
//     those schemas -- or having granted one;
//   - being named by a row-level security policy on a table in scope.
//
// Equivalently: a role is described exactly when some other statement in the
// same description can name it. Nothing else is reported, and a role that
// merely exists in the cluster certainly is not.
//
// Ownership is deliberately NOT a reason, and this was measured rather than
// assumed. Ptah describes no ownership -- it emits no OWNER TO and no CREATE
// SCHEMA ... AUTHORIZATION -- so an owner is a role the description creates
// and then never refers to. Because the connecting superuser owns every object
// it creates, treating ownership as a reason made every inspect describe the
// connecting role, and a diff between a populated database and an empty dev
// database in the same cluster then planned
// `CREATE ROLE "..." WITH LOGIN SUPERUSER ...` and failed to apply it at
// SQLSTATE 42710. An owner that a description does refer to still appears,
// through the privilege clauses: granting anything on a relation makes its
// ACL explicit, and an explicit ACL always carries the owner's own privileges.
//
// A role that belongs to the inspected schemas is still reported in full, on
// the native and the compatibility surface alike. What the scoping does cost
// is naming a role the inspected schemas do not use, which a description could
// do before -- enough to copy one cluster's roles into another. That capability
// is not discarded: readRolesInto restores the full read under
// [rolescope.DescribeAllEnvVar], and what the default leaves out is reported
// rather than dropped in silence.
//
// The privilege clauses read the grantor as well as the grantee because
// readGrants reports both. That makes this set a superset of every role name
// the rest of the description can mention, so the description never references
// a role it does not also define.
func (r *Reader) readRoles() ([]types.DBRole, error) {
	return r.queryRoles(rolesUsedByScope)
}

// readRolesInto performs both role reads and decides which of them the
// description carries.
//
// By default the description is the scoped read and the complement is carried
// separately, for the comparator alone. Under
// [rolescope.DescribeAllEnvVar] the description is the union -- every role Ptah
// manages on the server, which is what a read produced before
// stokaro/ptah#1267 -- and the complement is then empty because nothing is
// left out.
//
// Both reads happen either way, so the set the comparator takes existence from
// is byte-for-byte the same set under both settings. That is the property that
// makes the variable safe: turning it on changes what is described and can
// never make Ptah plan a CREATE ROLE for a role that is already there.
//
// The union is re-sorted by name rather than concatenated, so the fuller
// description is ordered exactly as the single unscoped query ordered it.
func (r *Reader) readRolesInto(schema *types.DBSchema) error {
	// Resolved FIRST, before either query, so a malformed value is refused on
	// every role read and not only on the runs that would have widened the
	// description. A server whose scoped and unscoped reads happen to agree
	// leaves nothing out, and resolving below would let
	// PTAH_POSTGRES_INSPECT_ALL_ROLES=maybe pass in silence there --
	// the healthy half of a pipeline, and the only runs a CI environment file
	// is read on until the schema grows the role that makes the two differ.
	// See stokaro/ptah#1334.
	describeAll, err := rolescope.DescribeAll()
	if err != nil {
		return err
	}

	described, err := r.readRoles()
	if err != nil {
		return fmt.Errorf("failed to read roles: %w", err)
	}

	// The roles the description leaves out still exist on the server, and a
	// comparator that is not told so plans CREATE ROLE for them. Read the
	// complement so "not described" and "not present" stay different answers.
	// See stokaro/ptah#1267 and stokaro/ptah#1276.
	outOfScope, err := r.readRolesOutOfScope()
	if err != nil {
		return fmt.Errorf("failed to read roles outside the inspected scope: %w", err)
	}

	if describeAll {
		everyManagedRole := slices.Concat(described, outOfScope)
		slices.SortFunc(everyManagedRole, func(a, b types.DBRole) int {
			return strings.Compare(a.Name, b.Name)
		})
		schema.Roles = everyManagedRole
		schema.RolesOutOfScope = nil
		return nil
	}

	schema.Roles = described
	schema.RolesOutOfScope = outOfScope
	return nil
}

// readRolesOutOfScope reads the roles the server has that readRoles left out:
// the exact complement, over the same catalog and the same reserved-name
// exclusions.
//
// Scoping the description (stokaro/ptah#1267) is right for describing a schema
// and says nothing about what the server has. The comparator asks a different
// question -- does this role exist -- and answering it from the description
// alone reads "out of scope" as "absent" and plans CREATE ROLE for a role that
// is already there, which the server refuses at SQLSTATE 42710. That is
// requirement 2 of stokaro/ptah#1276: "not described" and "not present" have
// to be distinguishable in what the comparator consumes.
//
// The two reads partition the roles this reader reports at all, so their union
// is every such role regardless of which scoping rule readRoles applies. A
// change to the scoping rule moves roles between the two lists and can never
// make a reported role look absent.
//
// The partition is over managed roles, not over pg_roles. queryRoles ends both
// statements with the exclusion [reservedrole.ExcludeSQL] renders, so the
// reserved roles and the bootstrap superuser are in neither list -- Ptah
// manages neither, in either direction. A desired schema that names one is
// refused before anything is compared or planned, through
// [reservedrole.ValidateDeclared] over the same definition of "reserved"
// (stokaro/ptah#1312), rather than being compared against nothing here. Do not
// restate this as "every role the server has".
//
// The escape is load-bearing for both reads at once: an unescaped underscore
// is a single-character wildcard, so it would drop pgbouncer, pgadmin and
// pgpool from the description and from this complement alike, leaving them in
// neither list and back in the CREATE ROLE failure this method prevents
// (stokaro/ptah#1291).
//
// The complement is spelled NOT EXISTS rather than NOT IN because a NOT IN
// over a subquery yielding a single NULL matches nothing at all, which would
// silently empty this list and restore the very defect it exists to prevent.
func (r *Reader) readRolesOutOfScope() ([]types.DBRole, error) {
	return r.queryRoles(rolesNotUsedByScope)
}

// Membership predicates for the role query, applied against the `used` set the
// scope branches build. They are complements of one another, and nothing else
// in the two queries differs.
const (
	rolesUsedByScope    = `EXISTS (SELECT 1 FROM used u WHERE u.roleoid = r.oid)`
	rolesNotUsedByScope = `NOT EXISTS (SELECT 1 FROM used u WHERE u.roleoid = r.oid)`
)

// queryRoles reads pg_roles restricted by one of the two membership
// predicates above.
func (r *Reader) queryRoles(membership string) ([]types.DBRole, error) {
	schemas := r.schemasToRead()
	placeholders := make([]string, 0, len(schemas))
	args := make([]any, 0, len(schemas))
	for i, schemaName := range schemas {
		placeholders = append(placeholders, fmt.Sprintf("$%d", i+1))
		args = append(args, schemaName)
	}

	rolesQuery := `
		WITH scope AS (
			SELECT n.oid, n.nspacl
			FROM pg_namespace n
			WHERE n.nspname IN (` + strings.Join(placeholders, ", ") + `)
		),
		used AS (
			` + strings.Join(r.rolesInScopeClauses(), `
			UNION
			`) + `
		)
		SELECT
			r.rolname AS role_name,
			r.rolcanlogin AS login,
			r.rolsuper AS superuser,
			r.rolcreatedb AS create_db,
			r.rolcreaterole AS create_role,
			r.rolinherit AS inherit,
			r.rolreplication AS replication,
			COALESCE(a.rolpassword IS NOT NULL AND a.rolpassword != '', false) AS has_password,
			COALESCE(shobj_description(r.oid, 'pg_authid'), '') AS comment
		FROM pg_roles r
		LEFT JOIN pg_authid a ON r.oid = a.oid
		-- Reserved system roles and the bootstrap superuser, excluded through
		-- the same definition the desired-schema refusal tests against, so the
		-- two cannot drift into disagreeing about what "reserved" means
		-- (stokaro/ptah#1312). The rendered fragment escapes the underscore,
		-- because LIKE reads a bare _ as a single-character wildcard: an
		-- unescaped 'pg_%' matches pgbouncer, pgadmin and pgpool, which are
		-- ordinary user roles (stokaro/ptah#1291).
		WHERE ` + membership + `
		AND ` + reservedrole.ExcludeSQL("r.rolname") + `
		ORDER BY r.rolname`

	rows, err := r.db.Query(rolesQuery, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query roles: %w", err)
	}
	defer rows.Close()

	var roles []types.DBRole
	for rows.Next() {
		var role types.DBRole
		err := rows.Scan(
			&role.Name,
			&role.Login,
			&role.Superuser,
			&role.CreateDB,
			&role.CreateRole,
			&role.Inherit,
			&role.Replication,
			&role.HasPassword,
			&role.Comment,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan role: %w", err)
		}

		roles = append(roles, role)
	}

	return roles, nil
}

func (r *Reader) readGrants(standaloneSequences map[string]bool) ([]types.DBGrant, error) {
	var grants []types.DBGrant
	for _, schemaName := range r.schemasToRead() {
		tableGrants, err := r.readTableGrantsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		grants = append(grants, tableGrants...)

		schemaGrants, err := r.readSchemaGrantsForSchema(schemaName)
		if err != nil {
			return nil, err
		}
		grants = append(grants, schemaGrants...)

		if r.caps.Has(capability.Sequences) {
			sequenceGrants, err := r.readSequenceGrantsForSchema(schemaName, standaloneSequences)
			if err != nil {
				return nil, err
			}
			grants = append(grants, sequenceGrants...)
		}
	}
	return grants, nil
}

// standaloneSequenceSet returns a lookup keyed by each sequence's qualified
// name, used to keep sequence-grant introspection scoped to standalone
// sequences (excluding implicit serial/identity sequences).
func standaloneSequenceSet(sequences []types.DBSequence) map[string]bool {
	set := make(map[string]bool, len(sequences))
	for _, sequence := range sequences {
		set[sequence.QualifiedName()] = true
	}
	return set
}

func (r *Reader) readTableGrantsForSchema(schemaName string) ([]types.DBGrant, error) {
	const query = `
		SELECT
			grantee,
			privilege_type,
			table_schema,
			table_name,
			is_grantable = 'YES' AS with_option,
			grantor
		FROM information_schema.role_table_grants g
		WHERE table_schema = $1
		AND grantee NOT LIKE 'pg\_%' ESCAPE '\'
		AND grantee != 'postgres'
		-- Only relations on which some privilege has actually been granted.
		-- information_schema reports an owner's built-in privileges as grants
		-- even for a table whose pg_class.relacl is null, meaning nobody has
		-- ever run GRANT on it. Describing those as GRANT statements describes
		-- something no one wrote; CREATE TABLE re-establishes them for the new
		-- owner on replay anyway. It also made the description name a role it
		-- did not define once role reporting was scoped (stokaro/ptah#1267),
		-- and the pinned Atlas community binary refuses such a document.
		AND EXISTS (
			SELECT 1 FROM pg_class c
			JOIN pg_namespace n ON n.oid = c.relnamespace
			WHERE n.nspname = g.table_schema
			AND c.relname = g.table_name
			AND c.relacl IS NOT NULL
		)
		ORDER BY table_schema, table_name, grantee, privilege_type`

	rows, err := r.db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query table grants for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var grants []types.DBGrant
	for rows.Next() {
		grant := types.DBGrant{ObjectType: "TABLE"}
		if err := rows.Scan(&grant.Role, &grant.Privilege, &grant.Schema, &grant.ObjectName, &grant.WithOption, &grant.GrantedBy); err != nil {
			return nil, fmt.Errorf("failed to scan table grant for schema %s: %w", schemaName, err)
		}
		grant.Schema = r.outputSchema(grant.Schema)
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read table grants for schema %s: %w", schemaName, err)
	}
	return grants, nil
}

func (r *Reader) readSchemaGrantsForSchema(schemaName string) ([]types.DBGrant, error) {
	const query = `
		SELECT
			COALESCE(grantee.rolname, 'PUBLIC') AS grantee,
			acl.privilege_type,
			n.nspname AS schema_name,
			acl.is_grantable AS with_option,
			grantor.rolname AS grantor
		FROM pg_namespace n
		CROSS JOIN LATERAL aclexplode(n.nspacl) acl
		LEFT JOIN pg_roles grantee ON grantee.oid = acl.grantee
		JOIN pg_roles grantor ON grantor.oid = acl.grantor
		WHERE n.nspname = $1
		AND COALESCE(grantee.rolname, 'PUBLIC') NOT LIKE 'pg\_%' ESCAPE '\'
		AND COALESCE(grantee.rolname, 'PUBLIC') != 'postgres'
		ORDER BY n.nspname, COALESCE(grantee.rolname, 'PUBLIC'), acl.privilege_type`

	rows, err := r.db.Query(query, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query schema grants for schema %s: %w", schemaName, err)
	}
	defer rows.Close()

	var grants []types.DBGrant
	for rows.Next() {
		grant := types.DBGrant{ObjectType: "SCHEMA"}
		if err := rows.Scan(&grant.Role, &grant.Privilege, &grant.ObjectName, &grant.WithOption, &grant.GrantedBy); err != nil {
			return nil, fmt.Errorf("failed to scan schema grant for schema %s: %w", schemaName, err)
		}
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to read schema grants for schema %s: %w", schemaName, err)
	}
	return grants, nil
}

// readAllViews reads views from whichever catalog the server has. A view is not
// an object kind a preset rules out -- every dialect this reader serves has
// views -- so the choice is which catalog can answer, not whether to ask.
func (r *Reader) readAllViews() ([]types.DBView, error) {
	if r.caps.Has(capability.PostgresCatalogFunctions) {
		return r.readViews()
	}
	var views []types.DBView
	for _, schemaName := range r.schemasToRead() {
		schemaViews, err := r.readInformationSchemaViews(schemaName)
		if err != nil {
			return nil, err
		}
		views = append(views, schemaViews...)
	}
	return views, nil
}

// readCapabilityGatedObjects reads the object kinds whose presence a capability
// preset decides, so ReadSchema states the order and this states the gates.
//
// Splitting them apart is not only tidiness: four gates inline pushed
// ReadSchema past the cognitive-complexity limit, and the reads they guard are
// one idea -- ask only for what this server can have.
func (r *Reader) readCapabilityGatedObjects(schema *types.DBSchema) error {
	// Extensions are pg_extension, and pg_catalog is where that lives. A server
	// without it has no extension mechanism to describe -- but this read cannot
	// prove that, only that it could not look, so the kind is recorded as not
	// described. The comparator then withholds rather than concluding an
	// extension is missing from a read that never asked (stokaro/ptah#942).
	if r.caps.Has(capability.PostgresCatalogFunctions) {
		extensions, err := r.readExtensions()
		if err != nil {
			return fmt.Errorf("failed to read extensions: %w", err)
		}
		schema.Extensions = extensions
	} else {
		schema.NotDescribed = schema.NotDescribed.WithKind(coverage.Extension)
	}

	// Three object kinds a preset can rule out entirely, read only where it
	// does not. This is the gate readSequences, readRLSPolicies and
	// readRolesInto already carry, and it needs no not-described marker for the
	// same reason they do not: a server whose preset says it has no triggers
	// HAS none, so reporting none is the truth rather than a read that did not
	// look. Each of these reads is a pg_proc/pg_matviews/pg_trigger query, so
	// without the gate a server that has neither the objects nor the catalog
	// failed the whole read asking whether objects exist that its own preset
	// already says cannot.
	if r.caps.Has(capability.Functions) {
		functions, err := r.readFunctions()
		if err != nil {
			return fmt.Errorf("failed to read functions: %w", err)
		}
		schema.Functions = functions
	}

	views, err := r.readAllViews()
	if err != nil {
		return fmt.Errorf("failed to read views: %w", err)
	}
	schema.Views = views

	if r.caps.Has(capability.MaterializedViews) {
		matViews, err := r.readMaterializedViews()
		if err != nil {
			return fmt.Errorf("failed to read materialized views: %w", err)
		}
		schema.MatViews = matViews
	}

	if r.caps.Has(capability.Triggers) {
		triggers, err := r.readTriggers()
		if err != nil {
			return fmt.Errorf("failed to read triggers: %w", err)
		}
		schema.Triggers = triggers
	}

	return nil
}
