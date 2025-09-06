package postgres

import (
	"database/sql"
	"fmt"
	"strings"

	"github.com/stokaro/ptah/dbschema/types"
)

// Reader reads schema from PostgreSQL databases
type Reader struct {
	db     *sql.DB
	schema string
}

// NewPostgreSQLReader creates a new PostgreSQL schema reader
func NewPostgreSQLReader(db *sql.DB, schema string) *Reader {
	if schema == "" {
		schema = "public"
	}
	return &Reader{
		db:     db,
		schema: schema,
	}
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

	// Read RLS policies (PostgreSQL-specific)
	rlsPolicies, err := r.readRLSPolicies()
	if err != nil {
		return nil, fmt.Errorf("failed to read RLS policies: %w", err)
	}
	schema.RLSPolicies = rlsPolicies

	// Read roles (PostgreSQL-specific)
	roles, err := r.readRoles()
	if err != nil {
		return nil, fmt.Errorf("failed to read roles: %w", err)
	}
	schema.Roles = roles

	// Enhance tables with constraint information
	r.enhanceTablesWithConstraints(schema.Tables, schema.Constraints)

	// Enhance tables with primary key information from indexes
	r.enhanceTablesWithIndexes(schema.Tables, schema.Indexes)

	return schema, nil
}

// readTables reads all tables and their columns
func (r *Reader) readTables() ([]types.DBTable, error) {
	// Read tables, excluding system tables like schema_migrations
	tablesQuery := `
		SELECT table_name, table_type,
		       COALESCE(obj_description(c.oid), '') as table_comment
		FROM information_schema.tables t
		LEFT JOIN pg_class c ON c.relname = t.table_name
		LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE t.table_schema = $1 AND (n.nspname = $1 OR n.nspname IS NULL)
		AND t.table_name NOT IN ('schema_migrations')
		ORDER BY table_name`

	rows, err := r.db.Query(tablesQuery, r.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query tables: %w", err)
	}
	defer rows.Close()

	var tables []types.DBTable
	for rows.Next() {
		var table types.DBTable
		err := rows.Scan(&table.Name, &table.Type, &table.Comment)
		if err != nil {
			return nil, fmt.Errorf("failed to scan table: %w", err)
		}

		// Read columns for this table
		columns, err := r.readColumns(table.Name)
		if err != nil {
			return nil, fmt.Errorf("failed to read columns for table %s: %w", table.Name, err)
		}
		table.Columns = columns

		tables = append(tables, table)
	}

	return tables, nil
}

// readColumns reads all columns for a specific table
func (r *Reader) readColumns(tableName string) ([]types.DBColumn, error) {
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

	rows, err := r.db.Query(columnsQuery, r.schema, tableName)
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

	rows, err := r.db.Query(enumsQuery, r.schema)
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

	rows, err := r.db.Query(indexesQuery, r.schema)
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
			Name:       indexName,
			TableName:  tableName,
			Definition: indexDef,
			IsUnique:   isUnique,
			IsPrimary:  isPrimary,
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

	// Then, enhance EXCLUDE constraints with detailed information from system catalogs
	constraints, err := r.enhanceExcludeConstraints(basicConstraints)
	if err != nil {
		return nil, err
	}

	return constraints, nil
}

// readBasicConstraints reads basic constraint information from information_schema
func (r *Reader) readBasicConstraints() ([]types.DBConstraint, error) {
	constraintsQuery := `
		SELECT
			tc.table_name,
			tc.constraint_name,
			tc.constraint_type,
			COALESCE(kcu.column_name, ''),
			COALESCE(ccu.table_name, ''),
			COALESCE(ccu.column_name, ''),
			COALESCE(rc.delete_rule, ''),
			COALESCE(rc.update_rule, ''),
			COALESCE(cc.check_clause, '')
		FROM information_schema.table_constraints AS tc
		LEFT JOIN information_schema.key_column_usage AS kcu
			ON tc.constraint_name = kcu.constraint_name
			AND tc.table_schema = kcu.table_schema
			AND tc.table_name = kcu.table_name
		LEFT JOIN information_schema.constraint_column_usage AS ccu
			ON ccu.constraint_name = tc.constraint_name
			AND ccu.table_schema = tc.table_schema
		LEFT JOIN information_schema.referential_constraints AS rc
			ON tc.constraint_name = rc.constraint_name
			AND tc.table_schema = rc.constraint_schema
		LEFT JOIN information_schema.check_constraints AS cc
			ON tc.constraint_name = cc.constraint_name
			AND tc.table_schema = cc.constraint_schema
		WHERE tc.table_schema = $1
		AND tc.table_name NOT IN ('schema_migrations')
		ORDER BY tc.table_name, tc.constraint_type, tc.constraint_name`

	rows, err := r.db.Query(constraintsQuery, r.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query constraints: %w", err)
	}
	defer rows.Close()

	var constraints []types.DBConstraint
	for rows.Next() {
		var constraint types.DBConstraint
		var foreignTable, foreignColumn, deleteRule, updateRule, checkClause string

		err := rows.Scan(
			&constraint.TableName,
			&constraint.Name,
			&constraint.Type,
			&constraint.ColumnName,
			&foreignTable,
			&foreignColumn,
			&deleteRule,
			&updateRule,
			&checkClause,
		)
		if err != nil {
			return nil, fmt.Errorf("failed to scan constraint: %w", err)
		}

		// Set optional fields
		if foreignTable != "" {
			constraint.ForeignTable = &foreignTable
		}
		if foreignColumn != "" {
			constraint.ForeignColumn = &foreignColumn
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

		constraints = append(constraints, constraint)
	}

	return constraints, nil
}

// enhanceExcludeConstraints enhances EXCLUDE constraints with detailed information from PostgreSQL system catalogs
func (r *Reader) enhanceExcludeConstraints(constraints []types.DBConstraint) ([]types.DBConstraint, error) {
	// Query PostgreSQL system catalogs for EXCLUDE constraint details
	excludeQuery := `
		SELECT
			c.conname AS constraint_name,
			cl.relname AS table_name,
			pg_get_constraintdef(c.oid) AS constraint_definition
		FROM pg_constraint c
		JOIN pg_class cl ON c.conrelid = cl.oid
		JOIN pg_namespace n ON cl.relnamespace = n.oid
		WHERE c.contype = 'x'  -- 'x' = exclusion constraint
		AND n.nspname = $1
		AND cl.relname NOT IN ('schema_migrations')
		ORDER BY cl.relname, c.conname`

	rows, err := r.db.Query(excludeQuery, r.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query EXCLUDE constraints: %w", err)
	}
	defer rows.Close()

	// Create a map of EXCLUDE constraint definitions for quick lookup
	excludeDefinitions := make(map[string]string)
	for rows.Next() {
		var constraintName, tableName, definition string
		err := rows.Scan(&constraintName, &tableName, &definition)
		if err != nil {
			return nil, fmt.Errorf("failed to scan EXCLUDE constraint: %w", err)
		}
		key := tableName + "." + constraintName
		excludeDefinitions[key] = definition
	}

	// Enhance EXCLUDE constraints with parsed details
	for i, constraint := range constraints {
		if constraint.Type == "EXCLUDE" {
			r.enhanceExcludeConstraint(&constraints[i], excludeDefinitions)
		}
	}

	return constraints, nil
}

// enhanceExcludeConstraint enhances a single EXCLUDE constraint with detailed information
func (r *Reader) enhanceExcludeConstraint(constraint *types.DBConstraint, excludeDefinitions map[string]string) {
	key := constraint.TableName + "." + constraint.Name
	definition, exists := excludeDefinitions[key]
	if !exists {
		return
	}

	parsed, err := r.ParseExcludeConstraintDefinition(definition)
	if err != nil {
		// Log the error but continue processing other constraints
		return
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
		if constraint.Type == "PRIMARY KEY" {
			if primaryKeys[constraint.TableName] == nil {
				primaryKeys[constraint.TableName] = make(map[string]bool)
			}
			primaryKeys[constraint.TableName][constraint.ColumnName] = true
		}
		if constraint.Type == "UNIQUE" {
			if uniqueKeys[constraint.TableName] == nil {
				uniqueKeys[constraint.TableName] = make(map[string]bool)
			}
			uniqueKeys[constraint.TableName][constraint.ColumnName] = true
		}
	}

	// Update table columns with constraint information
	for i := range tables {
		for j := range tables[i].Columns {
			col := &tables[i].Columns[j]
			tableName := tables[i].Name

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
		-- Exclude extension-owned functions to prevent migration issues
		-- Extension functions cannot be dropped independently and should be managed by the extension
		AND NOT EXISTS (
			SELECT 1 FROM pg_depend d
			JOIN pg_extension e ON e.oid = d.refobjid
			WHERE d.objid = p.oid AND d.deptype = 'e'
		)
		ORDER BY p.proname`

	rows, err := r.db.Query(functionsQuery, r.schema)
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
	rlsPoliciesQuery := `
		SELECT
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

	rows, err := r.db.Query(rlsPoliciesQuery, r.schema)
	if err != nil {
		return nil, fmt.Errorf("failed to query RLS policies: %w", err)
	}
	defer rows.Close()

	var policies []types.DBRLSPolicy
	for rows.Next() {
		var policy types.DBRLSPolicy
		err := rows.Scan(
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
