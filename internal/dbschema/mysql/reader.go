package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"github.com/stokaro/ptah/dbschema/types"
)

// Reader reads schema information from MySQL/MariaDB databases
type Reader struct {
	db     *sql.DB
	schema string
}

type checkConstraintClauses struct {
	byTableName map[string]string
	byName      map[string]string
}

// NewMySQLReader creates a new MySQL schema reader
func NewMySQLReader(db *sql.DB, schema string) *Reader {
	if schema == "" {
		schema = "information_schema"
	}
	return &Reader{
		db:     db,
		schema: schema,
	}
}

// ReadSchema reads the complete schema from MySQL/MariaDB
func (r *Reader) ReadSchema() (*types.DBSchema, error) {
	schema := &types.DBSchema{}

	// Get current database name
	var dbName string
	err := r.db.QueryRow("SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to get database name: %w", err)
	}

	// Read tables
	tables, err := r.readTables(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read tables: %w", err)
	}
	schema.Tables = tables

	// Read enums (MySQL stores them as column types)
	enums, err := r.readEnums(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read enums: %w", err)
	}
	schema.Enums = enums

	// Read indexes
	indexes, err := r.readIndexes(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read indexes: %w", err)
	}
	schema.Indexes = indexes

	// Read constraints
	constraints, err := r.readConstraints(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read constraints: %w", err)
	}
	schema.Constraints = constraints

	views, err := r.readViews(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read views: %w", err)
	}
	schema.Views = views

	triggers, err := r.readTriggers(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read triggers: %w", err)
	}
	schema.Triggers = triggers

	// Reconcile per-column flags after all catalog metadata is loaded.
	// information_schema.KEY_COLUMN_USAGE carries primary-key membership, and
	// information_schema.STATISTICS (NON_UNIQUE) is authoritative for unique
	// indexes. Keeping these derived flags in one post-pass avoids depending on
	// per-column metadata that is either absent or lossy across MySQL/MariaDB
	// versions.
	enhanceTablesWithPrimaryKeys(schema.Tables, schema.Constraints)
	reconcileColumnUniqueness(schema)

	return schema, nil
}

// readTables reads all tables and their columns using bulk information_schema
// queries.
func (r *Reader) readTables(dbName string) ([]types.DBTable, error) {
	columnsByTable, err := r.readColumnsByTable(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read columns: %w", err)
	}

	query := `
		SELECT TABLE_NAME, TABLE_TYPE, COALESCE(TABLE_COMMENT, '')
		FROM information_schema.TABLES
		WHERE TABLE_SCHEMA = ?
		AND TABLE_TYPE = 'BASE TABLE'
		AND TABLE_NAME NOT IN ('schema_migrations')
		ORDER BY TABLE_NAME`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []types.DBTable
	for rows.Next() {
		var table types.DBTable
		if err := rows.Scan(&table.Name, &table.Type, &table.Comment); err != nil {
			return nil, err
		}
		table.Columns = columnsByTable[table.Name]
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return tables, nil
}

func (r *Reader) readColumnsByTable(dbName string) (map[string][]types.DBColumn, error) {
	query := `
		SELECT
			TABLE_NAME,
			COLUMN_NAME,
			DATA_TYPE,
			COLUMN_TYPE,
			IS_NULLABLE,
			COLUMN_DEFAULT,
			CHARACTER_MAXIMUM_LENGTH,
			NUMERIC_PRECISION,
			NUMERIC_SCALE,
			ORDINAL_POSITION,
			CHARACTER_SET_NAME,
			COLLATION_NAME,
			EXTRA,
			GENERATION_EXPRESSION
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME NOT IN ('schema_migrations')
		ORDER BY TABLE_NAME, ORDINAL_POSITION`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnsByTable := make(map[string][]types.DBColumn)
	for rows.Next() {
		var col types.DBColumn
		var tableName string
		var defaultValue sql.NullString
		var characterMaxLength, numericPrecision, numericScale sql.NullInt64
		var charset, collate, extra, generatedExpression sql.NullString

		err := rows.Scan(
			&tableName,
			&col.Name,
			&col.DataType,
			&col.ColumnType,
			&col.IsNullable,
			&defaultValue,
			&characterMaxLength,
			&numericPrecision,
			&numericScale,
			&col.OrdinalPosition,
			&charset,
			&collate,
			&extra,
			&generatedExpression,
		)
		if err != nil {
			return nil, err
		}
		if col.ColumnType != "" {
			col.DataType = col.ColumnType
		}

		applyMySQLColumnMetadata(
			&col,
			defaultValue,
			characterMaxLength,
			numericPrecision,
			numericScale,
			charset,
			collate,
			extra,
			generatedExpression,
		)
		columnsByTable[tableName] = append(columnsByTable[tableName], col)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return columnsByTable, nil
}

func applyMySQLColumnMetadata(
	col *types.DBColumn,
	defaultValue sql.NullString,
	characterMaxLength sql.NullInt64,
	numericPrecision sql.NullInt64,
	numericScale sql.NullInt64,
	charset sql.NullString,
	collate sql.NullString,
	extra sql.NullString,
	generatedExpression sql.NullString,
) {
	if defaultValue.Valid {
		defaultSQL := normalizeMySQLColumnDefault(col, defaultValue.String)
		col.ColumnDefault = &defaultSQL
	}
	if characterMaxLength.Valid {
		length := int(characterMaxLength.Int64)
		col.CharacterMaxLength = &length
	}
	if numericPrecision.Valid {
		precision := int(numericPrecision.Int64)
		col.NumericPrecision = &precision
	}
	if numericScale.Valid {
		scale := int(numericScale.Int64)
		col.NumericScale = &scale
	}
	if charset.Valid {
		col.Charset = charset.String
	}
	if collate.Valid {
		col.Collate = collate.String
	}
	if extra.Valid {
		extraValue := strings.ToLower(extra.String)
		col.IsAutoIncrement = strings.Contains(extraValue, "auto_increment")
		switch {
		case strings.Contains(extraValue, "stored generated"):
			col.GeneratedKind = "STORED"
		case strings.Contains(extraValue, "virtual generated"):
			col.GeneratedKind = "VIRTUAL"
		}
	}
	if generatedExpression.Valid && generatedExpression.String != "" {
		expression := generatedExpression.String
		col.GeneratedExpression = &expression
	}
}

func normalizeMySQLColumnDefault(col *types.DBColumn, defaultValue string) string {
	value := strings.TrimSpace(defaultValue)
	if value == "" || isQuotedMySQLDefault(value) || !mysqlDefaultNeedsLiteralQuotes(col, value) {
		return defaultValue
	}
	return quoteMySQLDefaultLiteral(defaultValue)
}

func isQuotedMySQLDefault(value string) bool {
	return len(value) >= 2 &&
		((strings.HasPrefix(value, "'") && strings.HasSuffix(value, "'")) ||
			(strings.HasPrefix(value, `"`) && strings.HasSuffix(value, `"`)))
}

func mysqlDefaultNeedsLiteralQuotes(col *types.DBColumn, value string) bool {
	if isMySQLDefaultExpression(value) {
		return false
	}

	typeName := strings.ToLower(col.ColumnType)
	if typeName == "" {
		typeName = strings.ToLower(col.DataType)
	}
	switch {
	case strings.HasPrefix(typeName, "enum("), strings.HasPrefix(typeName, "set("):
		return true
	case strings.Contains(typeName, "char"), strings.Contains(typeName, "text"):
		return true
	case strings.Contains(typeName, "date"), strings.Contains(typeName, "time"), strings.Contains(typeName, "year"):
		return true
	default:
		return false
	}
}

func isMySQLDefaultExpression(value string) bool {
	normalized := strings.TrimSpace(strings.ToUpper(value))
	normalized = strings.TrimSuffix(normalized, "()")
	switch normalized {
	case "NULL", "CURRENT_TIMESTAMP", "CURRENT_DATE", "CURRENT_TIME", "LOCALTIME", "LOCALTIMESTAMP", "NOW", "UUID":
		return true
	default:
		return strings.HasPrefix(normalized, "CURRENT_TIMESTAMP(") ||
			strings.HasPrefix(normalized, "CURRENT_TIME(") ||
			strings.HasPrefix(normalized, "LOCALTIME(") ||
			strings.HasPrefix(normalized, "LOCALTIMESTAMP(") ||
			strings.HasPrefix(normalized, "NOW(")
	}
}

func quoteMySQLDefaultLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

// reconcileColumnUniqueness recomputes each column's IsUnique flag from the
// authoritative index metadata (information_schema.STATISTICS, captured in
// schema.Indexes with a correct NON_UNIQUE bit). A column is unique only when a
// non-primary, single-column unique index covers exactly that column. This
// overrides the DDL-parser heuristic, which cannot tell a plain non-unique
// KEY/INDEX from a UNIQUE one and would over-report uniqueness for FK-backing
// and other non-unique indexes.
func reconcileColumnUniqueness(schema *types.DBSchema) {
	// Set of table.column covered by a single-column unique (non-primary) index.
	uniqueColumns := make(map[string]struct{})
	for _, idx := range schema.Indexes {
		if idx.IsPrimary || !idx.IsUnique || len(idx.Columns) != 1 {
			continue
		}
		uniqueColumns[idx.TableName+"."+idx.Columns[0]] = struct{}{}
	}

	for ti := range schema.Tables {
		table := &schema.Tables[ti]
		for ci := range table.Columns {
			col := &table.Columns[ci]
			_, unique := uniqueColumns[table.Name+"."+col.Name]
			col.IsUnique = unique
		}
	}
}

func enhanceTablesWithPrimaryKeys(tables []types.DBTable, constraints []types.DBConstraint) {
	primaryKeys := make(map[string]map[string]struct{})
	for _, constraint := range constraints {
		if constraint.Type != "PRIMARY KEY" {
			continue
		}
		if primaryKeys[constraint.TableName] == nil {
			primaryKeys[constraint.TableName] = make(map[string]struct{})
		}
		for _, column := range constraint.ColumnNamesOrDefault() {
			primaryKeys[constraint.TableName][column] = struct{}{}
		}
	}

	for ti := range tables {
		table := &tables[ti]
		tablePrimaryKeys := primaryKeys[table.Name]
		if len(tablePrimaryKeys) == 0 {
			continue
		}
		for ci := range table.Columns {
			col := &table.Columns[ci]
			_, primary := tablePrimaryKeys[col.Name]
			col.IsPrimaryKey = primary
		}
	}
}

func (r *Reader) readViews(dbName string) ([]types.DBView, error) {
	query := `
		SELECT TABLE_NAME, VIEW_DEFINITION, CHECK_OPTION
		FROM information_schema.VIEWS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME NOT IN ('schema_migrations')
		ORDER BY TABLE_NAME`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []types.DBView
	for rows.Next() {
		var view types.DBView
		err := rows.Scan(&view.Name, &view.Body, &view.CheckOption)
		if err != nil {
			return nil, err
		}
		view.Schema = dbName
		views = append(views, view)
	}
	return views, nil
}

func (r *Reader) readTriggers(dbName string) ([]types.DBTrigger, error) {
	query := `
		SELECT
			TRIGGER_NAME,
			EVENT_OBJECT_TABLE,
			ACTION_TIMING,
			EVENT_MANIPULATION,
			ACTION_ORIENTATION,
			ACTION_STATEMENT
		FROM information_schema.TRIGGERS
		WHERE TRIGGER_SCHEMA = ?
		ORDER BY EVENT_OBJECT_TABLE, TRIGGER_NAME`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var triggers []types.DBTrigger
	for rows.Next() {
		var trigger types.DBTrigger
		err := rows.Scan(
			&trigger.Name,
			&trigger.Table,
			&trigger.Timing,
			&trigger.Event,
			&trigger.ForEach,
			&trigger.Body,
		)
		if err != nil {
			return nil, err
		}
		triggers = append(triggers, trigger)
	}
	return triggers, nil
}

// readEnums reads enum types from MySQL (stored as column types)
func (r *Reader) readEnums(dbName string) ([]types.DBEnum, error) {
	query := `
		SELECT DISTINCT
			COLUMN_TYPE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND DATA_TYPE = 'enum'`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var enums []types.DBEnum
	enumMap := make(map[string][]string)

	for rows.Next() {
		var columnType string
		err := rows.Scan(&columnType)
		if err != nil {
			return nil, err
		}

		// Parse enum values from column type like "enum('value1','value2','value3')"
		values := parseEnumValues(columnType)
		if len(values) > 0 {
			// Create a unique name for this enum based on its values
			enumName := fmt.Sprintf("enum_%s", strings.Join(values, "_"))
			enumMap[enumName] = values
		}
	}

	// Convert map to slice
	for name, values := range enumMap {
		enums = append(enums, types.DBEnum{
			Name:   name,
			Values: values,
		})
	}

	return enums, nil
}

// readIndexes reads all indexes
func (r *Reader) readIndexes(dbName string) ([]types.DBIndex, error) {
	query := `
		SELECT
			s.INDEX_NAME,
			s.TABLE_NAME,
			GROUP_CONCAT(s.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX) as COLUMNS,
			s.NON_UNIQUE,
			s.INDEX_TYPE
		FROM information_schema.STATISTICS s
		WHERE s.TABLE_SCHEMA = ?
		AND s.TABLE_NAME NOT IN ('schema_migrations')
		GROUP BY s.INDEX_NAME, s.TABLE_NAME, s.NON_UNIQUE, s.INDEX_TYPE
		ORDER BY s.TABLE_NAME, s.INDEX_NAME`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []types.DBIndex
	for rows.Next() {
		var index types.DBIndex
		var columnsStr string
		var nonUnique int
		var indexType string

		err := rows.Scan(&index.Name, &index.TableName, &columnsStr, &nonUnique, &indexType)
		if err != nil {
			return nil, err
		}

		index.Columns = strings.Split(columnsStr, ",")
		index.IsUnique = nonUnique == 0
		index.IsPrimary = index.Name == "PRIMARY"
		index.Definition = fmt.Sprintf("%s INDEX %s ON %s (%s)", indexType, index.Name, index.TableName, columnsStr)

		indexes = append(indexes, index)
	}

	return indexes, nil
}

// readConstraints reads all constraints
func (r *Reader) readConstraints(dbName string) ([]types.DBConstraint, error) {
	checkClauses, err := r.readCheckConstraintClauses(dbName)
	if err != nil {
		return nil, fmt.Errorf("read check constraint clauses: %w", err)
	}
	query := `
		SELECT
			tc.CONSTRAINT_NAME,
			tc.TABLE_NAME,
			tc.CONSTRAINT_TYPE,
			COALESCE(kcu.COLUMN_NAME, '') as COLUMN_NAME,
			COALESCE(kcu.REFERENCED_TABLE_NAME, '') as REFERENCED_TABLE_NAME,
			COALESCE(kcu.REFERENCED_COLUMN_NAME, '') as REFERENCED_COLUMN_NAME,
			COALESCE(rc.DELETE_RULE, '') as DELETE_RULE,
			COALESCE(rc.UPDATE_RULE, '') as UPDATE_RULE
		FROM information_schema.TABLE_CONSTRAINTS tc
		LEFT JOIN information_schema.KEY_COLUMN_USAGE kcu ON
			tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME AND
			tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA AND
			tc.TABLE_NAME = kcu.TABLE_NAME
		LEFT JOIN information_schema.REFERENTIAL_CONSTRAINTS rc ON
			tc.CONSTRAINT_NAME = rc.CONSTRAINT_NAME AND
			tc.TABLE_SCHEMA = rc.CONSTRAINT_SCHEMA
		WHERE tc.TABLE_SCHEMA = ?
		AND tc.TABLE_NAME NOT IN ('schema_migrations')
		ORDER BY tc.TABLE_NAME, tc.CONSTRAINT_NAME, kcu.ORDINAL_POSITION`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Use a map to group constraints by their unique identifier
	constraintMap := make(map[string]*types.DBConstraint)

	for rows.Next() {
		var constraintName, tableName, constraintType, columnName string
		var referencedTable, referencedColumn, deleteRule, updateRule string
		err := rows.Scan(
			&constraintName,
			&tableName,
			&constraintType,
			&columnName,
			&referencedTable,
			&referencedColumn,
			&deleteRule,
			&updateRule,
		)
		if err != nil {
			return nil, err
		}

		// Create a unique key for this constraint
		key := tableName + "." + constraintName

		// Get or create the constraint
		constraint, exists := constraintMap[key]
		if !exists {
			constraint = newConstraint(
				constraintName,
				tableName,
				constraintType,
				constraintRefs{
					referencedTable:  referencedTable,
					referencedColumn: referencedColumn,
					deleteRule:       deleteRule,
					updateRule:       updateRule,
				},
				checkClauses,
			)
			constraintMap[key] = constraint
		}

		// For multi-column constraints, we only store the first column name
		// in the legacy scalar field. ColumnNames / ForeignColumns retain the
		// full ordered list for composite keys.
		if constraint.ColumnName == "" && columnName != "" {
			constraint.ColumnName = columnName
		}
		if columnName != "" {
			constraint.ColumnNames = append(constraint.ColumnNames, columnName)
		}
		if referencedColumn != "" {
			constraint.ForeignColumns = append(constraint.ForeignColumns, referencedColumn)
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	// Convert map to slice
	var constraints []types.DBConstraint
	for _, constraint := range constraintMap {
		constraints = append(constraints, *constraint)
	}

	return constraints, nil
}

type constraintRefs struct {
	referencedTable  string
	referencedColumn string
	deleteRule       string
	updateRule       string
}

func newConstraint(name, tableName, constraintType string, refs constraintRefs, checkClauses checkConstraintClauses) *types.DBConstraint {
	constraint := &types.DBConstraint{
		Name:      name,
		TableName: tableName,
		Type:      constraintType,
	}
	if refs.referencedTable != "" {
		constraint.ForeignTable = &refs.referencedTable
	}
	if refs.referencedColumn != "" {
		constraint.ForeignColumn = &refs.referencedColumn
	}
	if refs.deleteRule != "" {
		constraint.DeleteRule = &refs.deleteRule
	}
	if refs.updateRule != "" {
		constraint.UpdateRule = &refs.updateRule
	}
	if checkClause := checkClauses.forConstraint(tableName, name); checkClause != "" {
		constraint.CheckClause = &checkClause
	}
	return constraint
}

func (c checkConstraintClauses) forConstraint(tableName, constraintName string) string {
	if checkClause := c.byTableName[tableName+"."+constraintName]; checkClause != "" {
		return checkClause
	}
	return c.byName[constraintName]
}

func (r *Reader) readCheckConstraintClauses(dbName string) (checkConstraintClauses, error) {
	clauses := checkConstraintClauses{
		byTableName: make(map[string]string),
		byName:      make(map[string]string),
	}
	err := r.readTableAwareCheckConstraintClauses(dbName, clauses.byTableName)
	if err == nil {
		return clauses, nil
	}
	if isMissingCheckConstraintsTable(err) {
		return clauses, nil
	}
	if !isMissingCheckConstraintTableNameColumn(err) {
		return clauses, err
	}

	err = r.readNameOnlyCheckConstraintClauses(dbName, clauses.byName)
	if err == nil {
		return clauses, nil
	}
	if isMissingCheckConstraintsTable(err) {
		return clauses, nil
	}
	return clauses, err
}

func isMissingCheckConstraintTableNameColumn(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	return mysqlErr.Number == 1054 && strings.Contains(strings.ToUpper(mysqlErr.Message), "TABLE_NAME")
}

func isMissingCheckConstraintsTable(err error) bool {
	var mysqlErr *mysqldriver.MySQLError
	if !errors.As(err, &mysqlErr) {
		return false
	}
	if mysqlErr.Number != 1109 && mysqlErr.Number != 1146 {
		return false
	}
	return strings.Contains(strings.ToUpper(mysqlErr.Message), "CHECK_CONSTRAINTS")
}

func (r *Reader) readTableAwareCheckConstraintClauses(dbName string, clauses map[string]string) error {
	rows, err := r.db.Query(`
		SELECT CONSTRAINT_NAME, TABLE_NAME, CHECK_CLAUSE
		FROM information_schema.CHECK_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ?`, dbName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var constraintName, tableName, checkClause string
		if err := rows.Scan(&constraintName, &tableName, &checkClause); err != nil {
			return err
		}
		clauses[tableName+"."+constraintName] = checkClause
	}
	return rows.Err()
}

func (r *Reader) readNameOnlyCheckConstraintClauses(dbName string, clauses map[string]string) error {
	rows, err := r.db.Query(`
		SELECT CONSTRAINT_NAME, CHECK_CLAUSE
		FROM information_schema.CHECK_CONSTRAINTS
		WHERE CONSTRAINT_SCHEMA = ?`, dbName)
	if err != nil {
		return err
	}
	defer rows.Close()

	for rows.Next() {
		var constraintName, checkClause string
		if err := rows.Scan(&constraintName, &checkClause); err != nil {
			return err
		}
		clauses[constraintName] = checkClause
	}
	return rows.Err()
}

// parseEnumValues parses enum values from MySQL column type
func parseEnumValues(columnType string) []string {
	// Remove "enum(" and ")" from the string
	if !strings.HasPrefix(columnType, "enum(") {
		return nil
	}

	valuesPart := strings.TrimPrefix(columnType, "enum(")
	valuesPart = strings.TrimSuffix(valuesPart, ")")

	// Split by comma and clean up quotes
	var values []string
	parts := strings.SplitSeq(valuesPart, ",")
	for part := range parts {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, "'\"")
		if part != "" {
			values = append(values, part)
		}
	}

	return values
}
