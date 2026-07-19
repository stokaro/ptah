package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/platform/capability"
	"github.com/stokaro/ptah/dbschema/types"
)

// Reader reads schema from PostgreSQL databases
type Reader struct {
	db      *sql.DB
	schema  string
	schemas []string
	scoped  bool
	caps    capability.Capabilities
}

// NewPostgreSQLReader creates a new PostgreSQL schema reader
func NewPostgreSQLReader(db *sql.DB, schema string) *Reader {
	return NewPostgreSQLReaderWithCapabilities(db, schema, capability.Postgres16())
}

// NewPostgreSQLReaderWithCapabilities creates a PostgreSQL-family schema reader
// whose PostgreSQL-specific catalog reads are gated by target capabilities.
func NewPostgreSQLReaderWithCapabilities(db *sql.DB, schema string, caps capability.Capabilities) *Reader {
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
		roles, err := r.readRoles()
		if err != nil {
			return nil, fmt.Errorf("failed to read roles: %w", err)
		}
		schema.Roles = roles

		grants, err := r.readGrants()
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

		// Read columns for this table
		columns, err := r.readColumns(schemaName, table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to read columns for table %s: %w", table.QualifiedName(), err)
		}
		table.Columns = columns

		tables = append(tables, table)
	}

	return tables, nil
}

// readColumns reads all columns for a specific table
func (r *Reader) readColumns(schemaName, tableName string) ([]types.DBColumn, error) {
	columnsQuery := `
		SELECT
			column_name,
			data_type,
			udt_name,
			is_nullable,
			column_default,
			character_maximum_length,
			numeric_precision,
			numeric_scale,
			ordinal_position
		FROM information_schema.columns
		WHERE table_schema = $1 AND table_name = $2
		ORDER BY ordinal_position`

	rows, err := r.db.Query(columnsQuery, schemaName, tableName)
	if err != nil {
		return nil, fmt.Errorf("failed to query columns: %w", err)
	}
	defer rows.Close()

	var columns []types.DBColumn
	for rows.Next() {
		var col types.DBColumn
		err := rows.Scan(
			&col.Name,
			&col.DataType,
			&col.UDTName,
			&col.IsNullable,
			&col.ColumnDefault,
			&col.CharacterMaxLength,
			&col.NumericPrecision,
			&col.NumericScale,
			&col.OrdinalPosition,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan column: %w", err)
		}

		// Detect auto increment (SERIAL types)
		if col.ColumnDefault != nil {
			defaultVal := *col.ColumnDefault
			col.IsAutoIncrement = strings.Contains(defaultVal, "nextval(") &&
				strings.Contains(defaultVal, "_seq")
		}

		columns = append(columns, col)
	}

	return columns, nil
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

func (r *Reader) readIndexesForSchema(schemaName string) ([]types.DBIndex, error) {
	indexesQuery := `
		SELECT
			n.nspname as schemaname,
			t.relname as tablename,
			i.relname as indexname,
			pg_get_indexdef(i.oid) as indexdef,
			ix.indisprimary,
			ix.indisunique
		FROM pg_index ix
		JOIN pg_class i ON i.oid = ix.indexrelid
		JOIN pg_class t ON t.oid = ix.indrelid
		JOIN pg_namespace n ON n.oid = t.relnamespace
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
		var schemaName, tableName, indexName, indexDef string
		var isPrimary, isUnique bool
		err := rows.Scan(&schemaName, &tableName, &indexName, &indexDef, &isPrimary, &isUnique)
		if err != nil {
			return nil, fmt.Errorf("failed to scan index: %w", err)
		}

		// Parse index definition to extract columns and properties
		index := types.DBIndex{
			Name:          indexName,
			TableName:     tableName,
			Schema:        r.outputSchema(schemaName),
			Definition:    indexDef,
			IsUnique:      isUnique,
			IsPrimary:     isPrimary,
			NullsDistinct: postgresNullsDistinctFromDefinition(indexDef),
		}

		// Extract column names from index definition (simplified parsing)
		if strings.Contains(indexDef, "(") && strings.Contains(indexDef, ")") {
			start := strings.Index(indexDef, "(") + 1
			end := strings.LastIndex(indexDef, ")")
			if start < end {
				columnsStr := indexDef[start:end]
				columns := strings.Split(columnsStr, ",")
				for i, col := range columns {
					columns[i] = strings.TrimSpace(col)
				}
				index.Columns = columns
			}
		}

		indexes = append(indexes, index)
	}

	return indexes, nil
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
			pg_get_constraintdef(c.oid) AS constraint_definition
		FROM pg_constraint c
		JOIN pg_class cl ON c.conrelid = cl.oid
		JOIN pg_namespace n ON cl.relnamespace = n.oid
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
		err := rows.Scan(&schemaName, &constraintName, &tableName, &constraintType, &definition)
		if err != nil {
			return nil, fmt.Errorf("failed to scan PostgreSQL constraint: %w", err)
		}

		// Convert PostgreSQL constraint type to standard type
		var stdType string
		switch constraintType {
		case "x":
			stdType = "EXCLUDE"
		default:
			continue // Skip unknown types
		}

		constraint := types.DBConstraint{
			Name:      constraintName,
			TableName: tableName,
			Schema:    r.outputSchema(schemaName),
			Type:      stdType,
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

	return extensions, nil
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

// enhanceTablesWithIndexes adds primary key information from indexes
func (r *Reader) enhanceTablesWithIndexes(tables []types.DBTable, indexes []types.DBIndex) {
	// For auto-increment integer columns (originally SERIAL), automatically set them as primary keys
	// This is a PostgreSQL-specific behavior where SERIAL columns become auto-increment integers and are typically primary keys
	for i := range tables {
		for j := range tables[i].Columns {
			col := &tables[i].Columns[j]

			// If it's an auto-increment integer column, assume it's a primary key
			// PostgreSQL converts SERIAL to integer with auto-increment
			if col.IsAutoIncrement && (strings.Contains(strings.ToLower(col.DataType), "int") ||
				strings.Contains(strings.ToLower(col.UDTName), "int")) {
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
		AND p.proname NOT LIKE 'ptah_trigger_%'
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

// readRoles reads all PostgreSQL roles from the database
func (r *Reader) readRoles() ([]types.DBRole, error) {
	rolesQuery := `
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
		WHERE r.rolname NOT LIKE 'pg_%'  -- Exclude system roles
		AND r.rolname != 'postgres'      -- Exclude postgres superuser
		ORDER BY r.rolname`

	rows, err := r.db.Query(rolesQuery)
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

func (r *Reader) readGrants() ([]types.DBGrant, error) {
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
	}
	return grants, nil
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
		FROM information_schema.role_table_grants
		WHERE table_schema = $1
		AND grantee NOT LIKE 'pg_%'
		AND grantee != 'postgres'
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
		AND COALESCE(grantee.rolname, 'PUBLIC') NOT LIKE 'pg_%'
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
