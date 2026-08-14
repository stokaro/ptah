package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/mysqlroutine"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

// Reader reads schema information from MySQL/MariaDB databases
type Reader struct {
	db     sqlrunner.Runner
	schema string
}

type checkConstraintClauses struct {
	byTableName map[constraintKey]string
	byName      map[string]string
}

type constraintKey struct {
	table string
	name  string
}

type tableColumnKey struct {
	table  string
	column string
}

// indexKey groups the information_schema.STATISTICS rows that belong to one
// key. MySQL scopes an index name to its table, so the table is part of the
// identity.
type indexKey struct {
	table string
	index string
}

// NewMySQLReader creates a new MySQL schema reader
func NewMySQLReader(db sqlrunner.Runner, schema string) *Reader {
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

	functions, err := r.readFunctions(dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read functions: %w", err)
	}
	schema.Functions = functions

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
	uniqueColumns := make(map[tableColumnKey]struct{})
	for _, idx := range schema.Indexes {
		if idx.IsPrimary || !idx.IsUnique || len(idx.Columns) != 1 {
			continue
		}
		uniqueColumns[tableColumnKey{table: idx.TableName, column: idx.Columns[0]}] = struct{}{}
	}

	for ti := range schema.Tables {
		table := &schema.Tables[ti]
		for ci := range table.Columns {
			col := &table.Columns[ci]
			_, unique := uniqueColumns[tableColumnKey{table: table.Name, column: col.Name}]
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

// readFunctions reads stored functions back from the live catalog.
//
// The parameter list is rebuilt from information_schema.PARAMETERS rather than
// taken from ROUTINES, because ROUTINES carries no argument list at all. In
// PARAMETERS, ordinal 0 is the RETURN type of a function and carries a NULL
// name, so only ordinals 1 and above are real arguments -- measured on MySQL
// 26.7.0, where `f_full(a INT, b VARCHAR(10)) RETURNS varchar(20)` reports
// three rows: (0, NULL, varchar(20)), (1, a, int), (2, b, varchar(10)).
//
// Volatility is derived from the two catalog columns the renderer encodes it
// into, IS_DETERMINISTIC and SQL_DATA_ACCESS. The mapping and the measurements
// that force it live in [mysqlroutine.VolatilityFromCatalog], next to the
// [mysqlroutine.Characteristic] it inverts, because the two halves drifting
// apart is exactly the defect this replaces: reading only IS_DETERMINISTIC
// reconstructed every non-deterministic routine as VOLATILE, so a declared
// STABLE function reported `volatility: VOLATILE -> STABLE` and planned the
// same destructive drop and create on every apply, forever. Measured on MySQL
// 26.7.0 and MariaDB 12.3.2, the diff was still there after a second apply.
//
// A body the connected account may not see is refused rather than reported as
// empty; see [errFunctionBodyHidden].
func (r *Reader) readFunctions(dbName string) ([]types.DBFunction, error) {
	parameters, err := r.readRoutineParameters(dbName)
	if err != nil {
		return nil, err
	}

	query := `
		SELECT
			ROUTINE_NAME,
			DTD_IDENTIFIER,
			IS_DETERMINISTIC,
			SQL_DATA_ACCESS,
			SECURITY_TYPE,
			DEFINER,
			CURRENT_USER(),
			ROUTINE_DEFINITION,
			COALESCE(ROUTINE_COMMENT, '')
		FROM information_schema.ROUTINES
		WHERE ROUTINE_SCHEMA = ?
		AND ROUTINE_TYPE = 'FUNCTION'
		ORDER BY ROUTINE_NAME`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []types.DBFunction
	for rows.Next() {
		var (
			fn              types.DBFunction
			isDeterministic string
			sqlDataAccess   string
			body            sql.NullString
		)
		if err := rows.Scan(
			&fn.Name, &fn.Returns, &isDeterministic, &sqlDataAccess,
			&fn.Security, &fn.Definer, &fn.CurrentAccount, &body, &fn.Comment,
		); err != nil {
			return nil, err
		}
		if !body.Valid {
			return nil, hiddenRoutineBodyError(dbName, fn.Name)
		}
		fn.Body = body.String
		fn.Schema = dbName
		fn.Language = "sql"
		fn.Returns = mysqlroutine.NormalizeType(fn.Returns)
		fn.Parameters = parameters[fn.Name]
		fn.Volatility = mysqlroutine.VolatilityFromCatalog(isDeterministic, sqlDataAccess)
		functions = append(functions, fn)
	}
	return functions, rows.Err()
}

// hiddenRoutineBodyError refuses a read whose function bodies the connected
// account may not see.
//
// information_schema.ROUTINES reports ROUTINE_DEFINITION as NULL, rather than
// omitting the row, when the account may see that a routine exists but lacks
// the privilege to inspect another definer's body. Measured on MySQL 26.7.0 and
// MariaDB 12.3.2 with an account holding only SELECT and EXECUTE on the
// database, every function it could name came back with a NULL body.
//
// Coalescing that to the empty string turned "I am not allowed to know" into
// "the author wrote nothing". [compare.FunctionDefinitions] then reported body
// drift against every desired function, and the planner answered it with the
// destructive DROP-then-CREATE pair -- so an account that may alter routines
// but not read them would replace a function that already matched, using a body
// it had never been able to compare against. The failure is silent and it
// destroys someone else's work, which is why this refuses the read instead of
// returning a value it knows is wrong.
//
// The trade is deliberate and it is a narrowing: a read that used to return a
// fabricated body now returns an error naming the routine and the privilege.
// Reporting a schema this account cannot actually read was never a capability.
func hiddenRoutineBodyError(dbName, routine string) error {
	return fmt.Errorf(
		"cannot read the body of function %s.%s: information_schema reports its "+
			"ROUTINE_DEFINITION as NULL, which means this account may see the routine "+
			"but not inspect another definer's body. Comparing against it would report "+
			"drift that is not there and plan a replacement of a function this account "+
			"cannot read. Grant SHOW_ROUTINE (or connect as the routine's definer), or "+
			"exclude the schema from this operation",
		dbName, routine)
}

// readRoutineParameters returns the rendered argument list of every FUNCTION in
// dbName, keyed by function name. See readFunctions for why ordinal 0 is
// skipped.
//
// The ROUTINE_TYPE filter is what restricts the rows to functions, and it is
// load-bearing rather than tidiness. MySQL and MariaDB let one schema hold a
// procedure and a function of the same name -- they are different routine
// types -- and information_schema.PARAMETERS keys both by SPECIFIC_NAME. A
// query filtered only by schema and ordinal therefore returned both sets under
// one key and appended them into the function's signature. Measured on MySQL
// 26.7.0 with `dual_name(a INT) RETURNS int` beside
// `dual_name(IN p_x VARCHAR(50), IN p_y DECIMAL(10,2))`, the old query returned
// three rows for that name, and the function's reconstructed parameters became
// `a int, p_x varchar(50), p_y decimal(10,2)` -- a signature it never had, and
// therefore parameter drift that no apply could ever resolve.
//
// The filter is PARAMETERS' own ROUTINE_TYPE column rather than a join onto
// ROUTINES, because a join cannot separate them: SPECIFIC_NAME is the SAME
// string for the procedure and the function, so joining on (schema,
// SPECIFIC_NAME) matches the procedure's parameter rows against the function's
// ROUTINES row and lets every one of them through. Measured: the joined form
// still produced `a int, p_x varchar(50), p_y decimal(10,2)` on both engines.
func (r *Reader) readRoutineParameters(dbName string) (map[string]string, error) {
	query := `
		SELECT SPECIFIC_NAME, PARAMETER_NAME, DTD_IDENTIFIER
		FROM information_schema.PARAMETERS
		WHERE SPECIFIC_SCHEMA = ?
		AND ROUTINE_TYPE = 'FUNCTION'
		AND ORDINAL_POSITION > 0
		ORDER BY SPECIFIC_NAME, ORDINAL_POSITION`

	rows, err := r.db.Query(query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	declarations := make(map[string][]string)
	for rows.Next() {
		var routine, name, dataType string
		if err := rows.Scan(&routine, &name, &dataType); err != nil {
			return nil, err
		}
		declarations[routine] = append(declarations[routine], name+" "+mysqlroutine.NormalizeType(dataType))
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	rendered := make(map[string]string, len(declarations))
	for routine, args := range declarations {
		rendered[routine] = strings.Join(args, ", ")
	}
	return rendered, nil
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

// indexKeyPartsQuery reads information_schema.STATISTICS one key part at a
// time, in key order, so the key can be assembled in Go.
//
// The projection this replaced was
// `GROUP_CONCAT(s.COLUMN_NAME ORDER BY s.SEQ_IN_INDEX)` split on a comma, and
// it cannot carry a key faithfully in two ways that a schema is free to hit:
//
//   - A comma is a legal character in a MySQL identifier. `KEY idx (`a,b`)`
//     came back as the two columns `a` and `b`.
//   - GROUP_CONCAT truncates at group_concat_max_len, 1024 bytes by default on
//     MySQL 9.7. A 16-part key of 64-character names is 1039 bytes, so the last
//     name arrived cut in half. MariaDB 11.8 defaults to 1048576 and does not
//     reach this one.
//
// Either way the reader reported column names the table does not have, and a
// comparison that trusts them plans a rebuild for a key that never changed.
// Measured on MySQL 9.7.1 and MariaDB 11.8.8, replaying a database's own
// `schema inspect` output ended in
// `Error 1072 (42000): Key column 'a' doesn't exist in table` where the pinned
// community binary v1.3.0 reported "Schema is synced" (issue #1245).
const indexKeyPartsQuery = `
		SELECT
			s.INDEX_NAME,
			s.TABLE_NAME,
			s.COLUMN_NAME,
			s.NON_UNIQUE,
			s.INDEX_TYPE
		FROM information_schema.STATISTICS s
		WHERE s.TABLE_SCHEMA = ?
		AND s.TABLE_NAME NOT IN ('schema_migrations')
		ORDER BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX`

// readIndexes reads all indexes, assembling each key from its parts.
func (r *Reader) readIndexes(dbName string) ([]types.DBIndex, error) {
	rows, err := r.db.Query(indexKeyPartsQuery, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		indexes    []types.DBIndex
		indexTypes []string
		positions  = make(map[indexKey]int)
	)
	for rows.Next() {
		var (
			name       string
			tableName  string
			columnName sql.NullString
			nonUnique  int
			indexType  string
		)
		err := rows.Scan(&name, &tableName, &columnName, &nonUnique, &indexType)
		if err != nil {
			return nil, err
		}
		key := indexKey{table: tableName, index: name}
		position, started := positions[key]
		if !started {
			position = len(indexes)
			positions[key] = position
			indexes = append(indexes, types.DBIndex{
				Name:      name,
				TableName: tableName,
				IsUnique:  nonUnique == 0,
				IsPrimary: name == "PRIMARY",
			})
			indexTypes = append(indexTypes, indexType)
		}
		addIndexKeyPart(&indexes[position], columnName)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for position := range indexes {
		index := &indexes[position]
		index.Definition = fmt.Sprintf(
			"%s INDEX %s ON %s (%s)",
			indexTypes[position],
			index.Name,
			index.TableName,
			strings.Join(index.Columns, ","),
		)
	}

	return indexes, nil
}

// addIndexKeyPart records one key part of an index.
//
// A NULL COLUMN_NAME is a functional key part -- `KEY idx ((b + 1))` on MySQL
// 8.0.13 and later -- whose expression lives in a STATISTICS column MariaDB
// does not have, so this reader cannot name it. It is recorded as a part that
// is missing from Columns rather than dropped silently: a comparison that read
// Columns as the whole key would plan a rebuild of a key that never changed.
// See [types.DBIndex.KeyPartsIncomplete].
func addIndexKeyPart(index *types.DBIndex, columnName sql.NullString) {
	if !columnName.Valid {
		index.KeyPartsIncomplete = true
		return
	}
	index.Columns = append(index.Columns, columnName.String)
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
	constraintMap := make(map[constraintKey]*types.DBConstraint)

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

		key := constraintKey{table: tableName, name: constraintName}

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
	if checkClause := c.byTableName[constraintKey{table: tableName, name: constraintName}]; checkClause != "" {
		return checkClause
	}
	return c.byName[constraintName]
}

func (r *Reader) readCheckConstraintClauses(dbName string) (checkConstraintClauses, error) {
	clauses := checkConstraintClauses{
		byTableName: make(map[constraintKey]string),
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

func (r *Reader) readTableAwareCheckConstraintClauses(dbName string, clauses map[constraintKey]string) error {
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
		clauses[constraintKey{table: tableName, name: constraintName}] = checkClause
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
