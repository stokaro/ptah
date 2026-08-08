package postgres

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/dbschema/types"
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

	// Read extensions (PostgreSQL-specific)
	extensions, err := r.readExtensions()
	if err != nil {
		return nil, fmt.Errorf("failed to read extensions: %w", err)
	}
	schema.Extensions = extensions

	// Read functions (PostgreSQL-specific)
	functions, err := r.readFunctions()
	if err != nil {
		return nil, fmt.Errorf("failed to read functions: %w", err)
	}
	schema.Functions = functions

	views, err := r.readViews()
	if err != nil {
		return nil, fmt.Errorf("failed to read views: %w", err)
	}
	schema.Views = views

	matViews, err := r.readMaterializedViews()
	if err != nil {
		return nil, fmt.Errorf("failed to read materialized views: %w", err)
	}
	schema.MatViews = matViews

	triggers, err := r.readTriggers()
	if err != nil {
		return nil, fmt.Errorf("failed to read triggers: %w", err)
	}
	schema.Triggers = triggers

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

func (r *Reader) readSchemas() ([]types.DBSchemaInfo, error) {
	if !r.scoped {
		return nil, nil
	}
	schemas := make([]types.DBSchemaInfo, 0, len(r.schemasToRead()))
	for _, schemaName := range r.schemasToRead() {
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

func (r *Reader) readTablesForSchema(schemaName string) ([]types.DBTable, error) {
	columnsByTable, err := r.readColumnsForSchema(schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to read columns for schema %s: %w", schemaName, err)
	}

	// Read tables, excluding system tables like schema_migrations
	tablesQuery := `
		SELECT table_schema, table_name, table_type,
		       COALESCE(obj_description(c.oid), '') as table_comment,
		       COALESCE(GREATEST(c.reltuples::bigint, st.n_live_tup, 0), 0) AS estimated_rows,
		       COALESCE(c.relrowsecurity, false) AS rls_enabled
			FROM information_schema.tables t
			LEFT JOIN pg_namespace n ON n.nspname = t.table_schema
			LEFT JOIN pg_class c ON c.relname = t.table_name AND c.relnamespace = n.oid
			LEFT JOIN pg_stat_all_tables st ON st.relid = c.oid
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
		err := rows.Scan(&table.Schema, &table.Name, &table.Type, &table.Comment, &table.EstimatedRows, &table.RLSEnabled)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}
		table.Schema = r.outputSchema(table.Schema)
		table.Columns = columnsByTable[table.Name]

		tables = append(tables, table)
	}

	return tables, nil
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
			CASE WHEN data_type = 'ARRAY' OR col.domain_name IS NOT NULL
				THEN format_type(a.atttypid, a.atttypmod)
				ELSE ''
			END AS formatted_type,
			-- The same format_type answer means two different things, and only
			-- this column separates them: for an array it is a TYPE, and for a
			-- domain it is the IDENTIFIER its author picked. A comparator that
			-- normalizes the two the same way lets a name decide whether a
			-- column changed -- a domain named "waypoint" contains "int" and one
			-- named "context" contains "text". A domain over an array is
			-- reported with data_type 'ARRAY' just like a plain array column, so
			-- the distinction cannot be recovered downstream (stokaro/ptah#1138).
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
			COALESCE(CASE WHEN a.attgenerated <> '' THEN pg_get_expr(ad.adbin, ad.adrelid) ELSE '' END, '') AS generated_expression,
			COALESCE(a.attidentity, '') AS identity_kind,
			COALESCE(
				pg_get_serial_sequence(
					format('%I.%I', col.table_schema, col.table_name),
					col.column_name
				),
				''
			) AS owned_sequence_name
		FROM information_schema.columns col
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
			Name:   name,
			Values: values,
		})
	}

	return enums, nil
}

// readUserTypesInto reads PostgreSQL domains, composite types, and range types
// and assigns them onto schema. Split out of ReadSchema to keep that method's
// cyclomatic complexity manageable.
func (r *Reader) readUserTypesInto(schema *types.DBSchema) error {
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
	const query = `
		SELECT
			n.nspname AS schema_name,
			t.typname AS domain_name,
			format_type(t.typbasetype, t.typtypmod) AS base_type,
			t.typnotnull AS not_null,
			COALESCE(t.typdefault, '') AS default_value,
			COALESCE((
				SELECT string_agg(pg_get_expr(c.conbin, c.conrelid), ' AND ')
				FROM pg_constraint c
				WHERE c.contypid = t.oid AND c.contype = 'c'
			), '') AS check_expr
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
	const query = `
		SELECT
			n.nspname AS schema_name,
			t.typname AS range_name,
			format_type(rng.rngsubtype, NULL) AS subtype
		FROM pg_type t
		JOIN pg_namespace n ON n.oid = t.typnamespace
		JOIN pg_range rng ON rng.rngtypid = t.oid
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
		if err := rows.Scan(&rawSchema, &rangeType.Name, &rangeType.Subtype); err != nil {
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
			COALESCE((
				SELECT json_agg(
					CASE WHEN op.opcdefault IS NOT FALSE THEN '' ELSE op.opcname::text END
					ORDER BY keys.ordinality
				)::text
				FROM unnest(ix.indclass) WITH ORDINALITY AS keys(opcoid, ordinality)
				LEFT JOIN pg_opclass op ON op.oid = keys.opcoid
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
			-- The extensions this index resolves to, from the resolved operator
			-- class OIDs and the access method OID rather than from either
			-- one's name -- including the class the DDL does print, which is
			-- the non-default one. See requiredExtensionsProjection.
			` + requiredExtensionsProjection("ix.indclass", "i.relam") + ` as index_required_extensions,
			COALESCE(pg_get_expr(ix.indpred, ix.indrelid), '') as predicate,
			ix.indisprimary,
			ix.indisunique
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
			&row.includeColumns, &row.method, &row.requiredExtensions,
			&row.predicate, &row.isPrimary, &row.isUnique,
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
	// requiredExtensions is the JSON array of extension names the index's
	// resolved operator classes and access method belong to.
	requiredExtensions string
	predicate          string
	isPrimary          bool
	isUnique           bool
}

// buildPostgresIndex maps one introspection row onto the dialect-neutral index
// model. It does not set Schema, which needs the reader's output-schema policy.
func buildPostgresIndex(row postgresIndexRow) (types.DBIndex, error) {
	index := types.DBIndex{
		Name:          row.indexName,
		TableName:     row.tableName,
		Definition:    row.indexDef,
		Condition:     row.predicate,
		IsUnique:      row.isUnique,
		IsPrimary:     row.isPrimary,
		Method:        row.method,
		NullsDistinct: postgresNullsDistinctFromDefinition(row.indexDef),
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

	return index, nil
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

// applyPostgresIndexOpclasses attaches the non-default operator class of each
// key to its part.
//
// A list that does not line up with the parts is dropped rather than applied
// off by one: an operator class on the wrong key builds a different index than
// the one being read, which is worse than not carrying it at all.
func applyPostgresIndexOpclasses(parts []types.DBIndexPart, opclassesJSON string) ([]types.DBIndexPart, error) {
	opclasses, err := decodePostgresKeyList[string](opclassesJSON, len(parts))
	if err != nil || opclasses == nil {
		return parts, err
	}
	for position := range parts {
		parts[position].Operator = opclasses[position]
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
	constraintsQuery := `
			SELECT
				tc.table_schema,
				tc.table_name,
				tc.constraint_name,
				tc.constraint_type,
				COALESCE(string_agg(kcu.column_name, ',' ORDER BY kcu.ordinal_position) FILTER (WHERE kcu.column_name IS NOT NULL), ''),
				COALESCE(max(ukcu.table_schema), ''),
				COALESCE(max(ukcu.table_name), ''),
				COALESCE(string_agg(ukcu.column_name, ',' ORDER BY kcu.ordinal_position) FILTER (WHERE ukcu.column_name IS NOT NULL), ''),
				COALESCE(rc.delete_rule, ''),
			COALESCE(rc.update_rule, ''),
			COALESCE(cc.check_clause, ''),
			COALESCE((
				SELECT pg_get_constraintdef(pc.oid)
				FROM pg_constraint pc
				JOIN pg_class pc_table ON pc_table.oid = pc.conrelid
				JOIN pg_namespace pc_schema ON pc_schema.oid = pc_table.relnamespace
				WHERE pc_schema.nspname = tc.table_schema
				AND pc_table.relname = tc.table_name
				AND pc.conname = tc.constraint_name
				LIMIT 1
			), '')
		FROM information_schema.table_constraints AS tc
		LEFT JOIN information_schema.key_column_usage AS kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
			AND tc.table_name = kcu.table_name
		LEFT JOIN information_schema.referential_constraints AS rc
			ON tc.constraint_name = rc.constraint_name
			AND tc.table_schema = rc.constraint_schema
		LEFT JOIN information_schema.key_column_usage AS ukcu
			ON ukcu.constraint_schema = rc.unique_constraint_schema
			AND ukcu.constraint_name = rc.unique_constraint_name
			AND ukcu.ordinal_position = kcu.position_in_unique_constraint
		LEFT JOIN information_schema.check_constraints AS cc
			ON tc.constraint_name = cc.constraint_name
			AND tc.table_schema = cc.constraint_schema
		WHERE tc.table_schema = $1
		AND tc.table_name NOT IN ('schema_migrations')
		GROUP BY
			tc.table_schema,
			tc.table_name,
			tc.constraint_name,
			tc.constraint_type,
			rc.delete_rule,
			rc.update_rule,
			cc.check_clause
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
		nullsDistinct := false
		return &nullsDistinct
	}
	if strings.Contains(upper, "NULLS DISTINCT") {
		nullsDistinct := true
		return &nullsDistinct
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
			col := &tables[i].Columns[j]           //nolint:gosec // G602: index bounded by `range tables[i].Columns`
			tableName := tables[i].QualifiedName() //nolint:gosec // G602: index bounded by `range tables`

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
		ORDER BY p.proname`

	rows, err := r.db.Query(functionsQuery, schemaName)
	if err != nil {
		return nil, fmt.Errorf("failed to query functions: %w", err)
	}
	defer rows.Close()

	var functions []types.DBFunction
	for rows.Next() {
		var fn types.DBFunction
		err := rows.Scan(
			&fn.Name,
			&fn.Parameters,
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
				WHEN pol.polroles = '{0}' THEN 'PUBLIC'
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

	if rolescope.DescribeAll() {
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
// statements with the same `NOT LIKE 'pg\_%' ESCAPE '\'` and `!= 'postgres'`
// exclusions, so the reserved roles and the bootstrap superuser are in neither
// list -- Ptah manages neither, in either direction. A desired schema that
// names one is therefore still compared against nothing and still planned as a
// CREATE ROLE the server refuses (SQLSTATE 42710 for postgres, 42939 for a
// reserved name); that predates this method, is unchanged by it, and is a
// separate refusal to build. Do not restate this as "every role the server
// has".
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
		-- The underscore is escaped because LIKE reads a bare _ as a
		-- single-character wildcard: 'pg_%' matches pgbouncer, pgadmin and
		-- pgpool, which are ordinary user roles. PostgreSQL reserves the
		-- prefix WITH the underscore (stokaro/ptah#1291).
		WHERE ` + membership + `
		AND r.rolname NOT LIKE 'pg\_%' ESCAPE '\'  -- Exclude system roles
		AND r.rolname != 'postgres'      -- Exclude postgres superuser
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
