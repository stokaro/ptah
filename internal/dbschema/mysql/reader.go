package mysql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	mysqldriver "github.com/go-sql-driver/mysql"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/mysqlroutine"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

// Reader reads schema information from MySQL/MariaDB databases
type Reader struct {
	db     sqlrunner.Runner
	schema string
	// caps is the target's capability set, or nil where the caller has none.
	// Nil is not "no capabilities": it means the reader asks the catalog and
	// reads the shape off the answer, which is what every caller did before
	// stokaro/ptah#916 gave this reader a set.
	caps capability.Capabilities
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
	return NewMySQLReaderWithCapabilities(db, schema, nil)
}

// NewMySQLReaderWithCapabilities creates a MySQL schema reader that knows the
// shape of the target's catalog. Pass nil to keep the sniffing behavior: the
// set only decides which spelling of a catalog read is attempted first, and
// every fallback stays in place either way (stokaro/ptah#916).
func NewMySQLReaderWithCapabilities(db sqlrunner.Runner, schema string, caps capability.Capabilities) *Reader {
	if schema == "" {
		schema = "information_schema"
	}
	return &Reader{
		db:     db,
		schema: schema,
		caps:   caps,
	}
}

// ReadSchema is [Reader.ReadSchemaContext] under context.Background(), the
// context-free half of the pair [catalog.SchemaReader] declares. Prefer the
// Context form: only it can be told to stop.
func (r *Reader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}

// ReadSchemaContext reads the complete schema from MySQL/MariaDB
func (r *Reader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	schema := &catalog.Database{}

	// Get current database name
	var dbName string
	err := r.db.QueryRowContext(ctx, "SELECT DATABASE()").Scan(&dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to get database name: %w", err)
	}

	// Read tables
	tables, err := r.readTables(ctx, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read tables: %w", err)
	}
	schema.Tables = tables

	// Read enums (MySQL stores them as column types)
	enums, err := r.readEnums(ctx, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read enums: %w", err)
	}
	schema.Enums = enums

	// Read indexes
	indexes, err := r.readIndexes(ctx, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read indexes: %w", err)
	}
	schema.Indexes = indexes

	// Read constraints
	constraints, err := r.readConstraints(ctx, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read constraints: %w", err)
	}
	schema.Constraints = constraints

	views, err := r.readViews(ctx, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read views: %w", err)
	}
	schema.Views = views

	triggers, err := r.readTriggers(ctx, dbName)
	if err != nil {
		return nil, fmt.Errorf("failed to read triggers: %w", err)
	}
	schema.Triggers = triggers

	functions, err := r.readFunctions(ctx, dbName)
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
	// Read only where the preset claims the object. MySQL answers a sequence
	// question with a syntax error rather than an empty result, so asking
	// unconditionally would fail the whole description on the engine that has
	// none (stokaro/ptah#1759).
	sequences, err := r.readSequences(ctx, dbName)
	if err != nil {
		return nil, err
	}
	schema.Sequences = sequences
	// Roles and their grants are read only where the preset claims them, the
	// same gate the ClickHouse reader uses. mysql.user and mysql.tables_priv
	// need a privilege reading a table does not, so an account without it must
	// not lose the whole description over an object kind the schema may not
	// even declare.
	if r.caps.Has(capability.RoleManagement) {
		if err := r.readRolesInto(ctx, schema, dbName); err != nil {
			return nil, err
		}
	}

	enhanceTablesWithPrimaryKeys(schema.Tables, schema.Constraints)
	reconcileColumnUniqueness(schema)

	return schema, nil
}

// readTables reads all tables and their columns using bulk information_schema
// queries.
func (r *Reader) readTables(ctx context.Context, dbName string) ([]catalog.Table, error) {
	columnsByTable, err := r.readColumnsByTable(ctx, dbName)
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

	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []catalog.Table
	for rows.Next() {
		var table catalog.Table
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

func (r *Reader) readColumnsByTable(ctx context.Context, dbName string) (map[string][]catalog.Column, error) {
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
			GENERATION_EXPRESSION,
			COLUMN_COMMENT
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME NOT IN ('schema_migrations')
		ORDER BY TABLE_NAME, ORDINAL_POSITION`

	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columnsByTable := make(map[string][]catalog.Column)
	for rows.Next() {
		var col catalog.Column
		var tableName string
		var defaultValue sql.NullString
		var characterMaxLength, numericPrecision, numericScale sql.NullInt64
		var charset, collate, extra, generatedExpression sql.NullString
		var comment sql.NullString

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
			&comment,
		)
		if err != nil {
			return nil, err
		}
		if col.ColumnType != "" {
			col.DataType = col.ColumnType
		}
		// MySQL and MariaDB report a column with no comment as the empty
		// string rather than NULL, so there is nothing to distinguish "no
		// comment" from "a comment somebody set to nothing" -- and the second
		// is not a state the engines keep. Assigning unconditionally is
		// therefore the same thing, and reads as though it were not.
		col.Comment = comment.String

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
	col *catalog.Column,
	defaultValue sql.NullString,
	characterMaxLength,
	numericPrecision,
	numericScale sql.NullInt64,
	charset,
	collate,
	extra,
	generatedExpression sql.NullString,
) {
	if defaultValue.Valid && !isAbsentColumnDefault(defaultValue.String) {
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
		col.UpdateExpression = mysqlUpdateExpression(extra.String)
	}
	if generatedExpression.Valid && generatedExpression.String != "" {
		expression := generatedExpression.String
		col.GeneratedExpression = &expression
	}
}

// mysqlUpdateExpression reads the `ON UPDATE` clause out of a column's EXTRA.
//
// EXTRA is a list of facts about the column, and this is one of them: measured
// on MySQL 8.4, `DATETIME ON UPDATE CURRENT_TIMESTAMP` reports `on update
// CURRENT_TIMESTAMP`, and `TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE
// CURRENT_TIMESTAMP(3)` reports `DEFAULT_GENERATED on update
// CURRENT_TIMESTAMP(3)`. The expression is what follows the two words, taken as
// the server spelled it -- the engine writes the parameterized form back, and
// rewriting it here would be Ptah deciding what the column says.
//
// Matched case-insensitively on the keyword and returned verbatim, because the
// keyword's case is the server's convention and the expression's is the
// author's.
func mysqlUpdateExpression(extra string) string {
	const keyword = "on update "
	index := strings.Index(strings.ToLower(extra), keyword)
	if index < 0 {
		return ""
	}
	return strings.TrimSpace(extra[index+len(keyword):])
}

// isAbsentColumnDefault reports whether the catalog's answer means "no default"
// rather than a default of something.
//
// The two engines answer differently for a column that has none, and the
// difference is not visible until a description is replayed. Measured on
// MariaDB 12.3 and MySQL 26.7, the same table:
//
//	-- MariaDB                            -- MySQL
//	 COLUMN_NAME | COLUMN_DEFAULT          COLUMN_NAME | COLUMN_DEFAULT
//	 bio         | [NULL]  <- the TEXT     bio         | <SQL NULL>
//	 full_name   | [NULL]  <- the TEXT     full_name   | <SQL NULL>
//
// A default recorded from MariaDB's answer is rendered as `DEFAULT NULL`, which
// an ordinary column tolerates and a GENERATED column does not: the server
// refuses the pair with `Error 1064 ... near 'DEFAULT NULL'`, so no MariaDB
// database holding a generated column could be replayed at all. --dry-run
// against the source said `Schema is synced` throughout, because both sides of
// that comparison read through here (stokaro/ptah#2128).
//
// The bare form is what is folded, and the quotes are what make that safe: a
// default of the STRING "NULL" is stored as `'NULL'` and stays a default. For a
// nullable column `DEFAULT NULL` and no default are the same thing, and MariaDB
// does not store a NULL default for a NOT NULL column, so nothing else can
// reach this. It is also the compatible answer -- the pinned binary writes no
// default for either column above.
func isAbsentColumnDefault(value string) bool {
	return strings.EqualFold(strings.TrimSpace(value), "NULL")
}

func normalizeMySQLColumnDefault(col *catalog.Column, defaultValue string) string {
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

func mysqlDefaultNeedsLiteralQuotes(col *catalog.Column, value string) bool {
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
func reconcileColumnUniqueness(schema *catalog.Database) {
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

func enhanceTablesWithPrimaryKeys(tables []catalog.Table, constraints []catalog.Constraint) {
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

func (r *Reader) readViews(ctx context.Context, dbName string) ([]catalog.View, error) {
	query := `
		SELECT TABLE_NAME, VIEW_DEFINITION, CHECK_OPTION
		FROM information_schema.VIEWS
		WHERE TABLE_SCHEMA = ?
		AND TABLE_NAME NOT IN ('schema_migrations')
		ORDER BY TABLE_NAME`

	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []catalog.View
	for rows.Next() {
		var view catalog.View
		err := rows.Scan(&view.Name, &view.Body, &view.CheckOption)
		if err != nil {
			return nil, err
		}
		view.Schema = dbName
		views = append(views, view)
	}
	if err := rows.Err(); err != nil {
		return nil, err
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
func (r *Reader) readFunctions(ctx context.Context, dbName string) ([]catalog.Function, error) {
	parameters, err := r.readRoutineParameters(ctx, dbName)
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
			COALESCE(ROUTINE_COMMENT, ''),
			ROUTINE_TYPE
		FROM information_schema.ROUTINES
		WHERE ROUTINE_SCHEMA = ?
		-- Both routine kinds. A procedure used to be filtered out here with
		-- nothing saying so, so a comparison against a schema holding one
		-- reported no difference and no diagnostic (stokaro/ptah#1722).
		AND ROUTINE_TYPE IN ('FUNCTION', 'PROCEDURE')
		ORDER BY ROUTINE_TYPE, ROUTINE_NAME`

	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var functions []catalog.Function
	for rows.Next() {
		var (
			fn              catalog.Function
			isDeterministic string
			sqlDataAccess   string
			body            sql.NullString
			returns         sql.NullString
			routineType     string
		)
		if err := rows.Scan(
			&fn.Name, &returns, &isDeterministic, &sqlDataAccess,
			&fn.Security, &fn.Definer, &fn.CurrentAccount, &body, &fn.Comment, &routineType,
		); err != nil {
			return nil, err
		}
		if !body.Valid {
			return nil, hiddenRoutineBodyError(dbName, fn.Name)
		}
		// DTD_IDENTIFIER is NULL for a procedure and carries the type for a
		// function, which is the catalog saying the same thing the grammar
		// does: a procedure has no return type. Measured on MySQL 26.7.
		fn.Kind = routineKind(routineType)
		fn.Body = body.String
		fn.Schema = dbName
		fn.Language = "sql"
		fn.Returns = mysqlroutine.NormalizeType(returns.String)
		fn.Parameters = parameters[routineParameterKey(routineType, fn.Name)]
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
func (r *Reader) readRoutineParameters(ctx context.Context, dbName string) (map[string]string, error) {
	query := `
		SELECT ROUTINE_TYPE, SPECIFIC_NAME, PARAMETER_MODE, PARAMETER_NAME, DTD_IDENTIFIER
		FROM information_schema.PARAMETERS
		WHERE SPECIFIC_SCHEMA = ?
		AND ROUTINE_TYPE IN ('FUNCTION', 'PROCEDURE')
		AND ORDINAL_POSITION > 0
		ORDER BY ROUTINE_TYPE, SPECIFIC_NAME, ORDINAL_POSITION`

	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	declarations := make(map[string][]string)
	for rows.Next() {
		var routineType, routine, name, dataType string
		var mode sql.NullString
		if err := rows.Scan(&routineType, &routine, &mode, &name, &dataType); err != nil {
			return nil, err
		}
		key := routineParameterKey(routineType, routine)
		declarations[key] = append(declarations[key],
			parameterDeclaration(routineType, mode.String, name, mysqlroutine.NormalizeType(dataType)))
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

// routineParameterKey identifies a routine's parameter rows by kind and name.
//
// The kind is not decoration. MySQL and MariaDB let one schema hold a procedure
// and a function of the same name, and information_schema.PARAMETERS keys both
// by SPECIFIC_NAME, so a map keyed by name alone returns one routine's
// parameters for the other. That was safe only while the read filtered to
// functions; reading both kinds makes the collision reachable
// (stokaro/ptah#1722).
func routineParameterKey(routineType, name string) string {
	return strings.ToUpper(strings.TrimSpace(routineType)) + "\x00" + name
}

// routineKind maps a catalog ROUTINE_TYPE onto the kind the schema model uses.
func routineKind(routineType string) string {
	if strings.EqualFold(strings.TrimSpace(routineType), "PROCEDURE") {
		return "procedure"
	}
	return "function"
}

func (r *Reader) readTriggers(ctx context.Context, dbName string) ([]catalog.Trigger, error) {
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

	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var triggers []catalog.Trigger
	for rows.Next() {
		var trigger catalog.Trigger
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
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return triggers, nil
}

// readEnums reads the enums MySQL stores, which is to say the enum-typed
// COLUMNS: this engine has no enum type in its catalog, only a column whose
// type carries a value list.
//
// Each one is named after the column that holds it. It used to be named after
// its VALUES -- `enum_active_inactive` -- which made the identity a function of
// the thing most likely to change. Adding one value to a live column renamed
// the declaration `ptah schema inspect` printed, and renamed the Go type and
// every constant `ptah introspect` generated: EnumActiveInactive became
// EnumActiveInactiveArchived, EnumActiveInactiveActive became
// EnumActiveInactiveArchivedActive. A schema author who had committed those
// models got a rename across their code for adding a value (stokaro/ptah#1716).
//
// Two columns holding the same value list are therefore two enums here, not
// one. That is what the engine has: there is no shared type for them to be,
// and collapsing them under one synthesized name asserted a relationship the
// database does not record. The comparison is unaffected either way -- the
// MySQL family folds a declared enum into its column's type before comparing,
// so this list is what INSPECTION reports, not what convergence rests on.
func (r *Reader) readEnums(ctx context.Context, dbName string) ([]catalog.Enum, error) {
	query := `
		SELECT
			TABLE_NAME,
			COLUMN_NAME,
			COLUMN_TYPE
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = ?
		AND DATA_TYPE = 'enum'
		ORDER BY TABLE_NAME, COLUMN_NAME`

	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var enums []catalog.Enum
	for rows.Next() {
		var tableName, columnName, columnType string
		if err := rows.Scan(&tableName, &columnName, &columnType); err != nil {
			return nil, err
		}

		// Parse enum values from a column type like "enum('value1','value2')".
		values := parseEnumValues(columnType)
		if len(values) == 0 {
			continue
		}
		enums = append(enums, catalog.Enum{
			Name:   tableName + "_" + columnName,
			Values: values,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
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
			s.INDEX_TYPE,
			-- The number of leading characters this key indexes, NULL for a
			-- whole-column key. MySQL requires one for a BLOB or TEXT column,
			-- so a key that loses it produces a description the server refuses:
			-- used in key specification without a key length
			-- (stokaro/ptah#2112).
			s.SUB_PART
		FROM information_schema.STATISTICS s
		WHERE s.TABLE_SCHEMA = ?
		AND s.TABLE_NAME NOT IN ('schema_migrations')
		ORDER BY s.TABLE_NAME, s.INDEX_NAME, s.SEQ_IN_INDEX`

// readIndexes reads all indexes, assembling each key from its parts.
func (r *Reader) readIndexes(ctx context.Context, dbName string) ([]catalog.Index, error) {
	rows, err := r.db.QueryContext(ctx, indexKeyPartsQuery, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		indexes    []catalog.Index
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
			subPart    sql.NullInt64
		)
		err := rows.Scan(&name, &tableName, &columnName, &nonUnique, &indexType, &subPart)
		if err != nil {
			return nil, err
		}
		key := indexKey{table: tableName, index: name}
		position, started := positions[key]
		if !started {
			position = len(indexes)
			positions[key] = position
			indexes = append(indexes, catalog.Index{
				Name:      name,
				TableName: tableName,
				IsUnique:  nonUnique == 0,
				IsPrimary: name == "PRIMARY",
			})
			indexTypes = append(indexTypes, indexType)
		}
		addIndexKeyPart(&indexes[position], columnName, subPart)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for i := range indexes {
		dropUninformativeParts(&indexes[i])
	}
	for position := range indexes {
		index := &indexes[position]
		// The access method as the server spells it, in the structured field a
		// comparison can read. Carried only inside Definition below, a desired
		// SPATIAL index and a BTREE index of the same name over the same column
		// compared equal, and reconciliation reported InSync for a table that
		// did not have the requested method (stokaro/ptah#2721).
		index.Method = indexTypes[position]
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
// See [catalog.Index.KeyPartsIncomplete].
func addIndexKeyPart(index *catalog.Index, columnName sql.NullString, subPart sql.NullInt64) {
	if !columnName.Valid {
		index.KeyPartsIncomplete = true
		return
	}
	index.Columns = append(index.Columns, columnName.String)
	index.Parts = append(index.Parts, catalog.IndexPart{
		Name:   columnName.String,
		Prefix: indexKeyPrefix(subPart),
	})
}

// dropUninformativeParts clears Parts for a key whose parts say nothing Columns
// does not.
//
// Parts is assembled for every key because a prefix cannot be known until the
// last row of that key has been read, and a list missing its later parts would
// be worse than none: the renderer reads Parts when it has any, so a key whose
// second part was dropped would render as a key over the first column alone.
// Once the key is complete, a Parts list carrying nothing but the column names
// is dropped, which leaves every ordinary index reported exactly as it was.
func dropUninformativeParts(index *catalog.Index) {
	for _, part := range index.Parts {
		if part.Prefix != "" {
			return
		}
	}
	index.Parts = nil
}

// indexKeyPrefix spells SUB_PART the way the schema does, and answers empty for
// a key that covers the whole column.
//
// Parts is filled for every key rather than only the prefixed ones, because a
// partial list would be worse than none: the renderer reads Parts when it has
// any, and a key whose second part was dropped would render as a key over the
// first column alone.
func indexKeyPrefix(subPart sql.NullInt64) string {
	if !subPart.Valid || subPart.Int64 <= 0 {
		return ""
	}
	return strconv.FormatInt(subPart.Int64, 10)
}

// readConstraints reads all constraints
func (r *Reader) readConstraints(ctx context.Context, dbName string) ([]catalog.Constraint, error) {
	checkClauses, err := r.readCheckConstraintClauses(ctx, dbName)
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

	rows, err := r.db.QueryContext(ctx, query, dbName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Use a map to group constraints by their unique identifier
	constraintMap := make(map[constraintKey]*catalog.Constraint)
	// The order the rows arrive in, which the query's ORDER BY already fixed.
	// Ranging constraintMap to build the result discarded it: Go randomizes map
	// iteration, so two reads of one unchanged database returned the same
	// constraints in different orders (stokaro/ptah#2709). That is observable
	// rather than cosmetic -- catalog.Database.Constraints serializes as a JSON
	// slice, and schema fingerprints and a plan artifact's
	// current_schema_digest hash that serialization, so `schema apply` could
	// re-read an untouched database and report its own saved fingerprint stale.
	var constraintOrder []constraintKey

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
			constraintOrder = append(constraintOrder, key)
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

	// Convert map to slice, in the order the rows arrived.
	//
	// Declared nil rather than made with length zero on purpose:
	// catalog.Database.Constraints carries no `omitempty`, so a nil slice
	// serializes as `null` and an empty one as `[]`. Those are different bytes,
	// and the fingerprints this fix exists to stabilize hash exactly those
	// bytes -- building an empty slice here would change the digest of every
	// schema that has no constraints at all.
	var constraints []catalog.Constraint
	for _, key := range constraintOrder {
		constraints = append(constraints, *constraintMap[key])
	}

	return constraints, nil
}

type constraintRefs struct {
	referencedTable  string
	referencedColumn string
	deleteRule       string
	updateRule       string
}

func newConstraint(name, tableName, constraintType string, refs constraintRefs, checkClauses checkConstraintClauses) *catalog.Constraint {
	constraint := &catalog.Constraint{
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

func (r *Reader) readCheckConstraintClauses(ctx context.Context, dbName string) (checkConstraintClauses, error) {
	clauses := checkConstraintClauses{
		byTableName: make(map[constraintKey]string),
		byName:      make(map[string]string),
	}
	if !r.catalogNamesCheckConstraintTables() {
		// The target's set says TABLE_NAME is not there, so asking for it would
		// spend a round trip to be told error 1054. The error handling below is
		// still the authority -- a set that is wrong about a live server must
		// not cost the read -- so a missing view is absorbed here too.
		err := r.readNameOnlyCheckConstraintClauses(ctx, dbName, clauses.byName)
		if err == nil || isMissingCheckConstraintsTable(err) {
			return clauses, nil
		}
		return clauses, err
	}

	err := r.readTableAwareCheckConstraintClauses(ctx, dbName, clauses.byTableName)
	if err == nil {
		return clauses, nil
	}
	if isMissingCheckConstraintsTable(err) {
		return clauses, nil
	}
	if !isMissingCheckConstraintTableNameColumn(err) {
		return clauses, err
	}

	err = r.readNameOnlyCheckConstraintClauses(ctx, dbName, clauses.byName)
	if err == nil {
		return clauses, nil
	}
	if isMissingCheckConstraintsTable(err) {
		return clauses, nil
	}
	return clauses, err
}

// catalogNamesCheckConstraintTables answers which spelling of the
// CHECK_CONSTRAINTS read is attempted first.
//
// A reader built without a set answers true, which is the pre-capability
// behavior: ask the richer spelling and read the shape off the failure. That
// costs MySQL a failed round trip on every schema read -- the column is a
// MariaDB extension, not a newer MySQL -- which is what the set removes.
func (r *Reader) catalogNamesCheckConstraintTables() bool {
	if r.caps == nil {
		return true
	}
	return r.caps.Has(capability.CatalogCheckConstraintTableName)
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

func (r *Reader) readTableAwareCheckConstraintClauses(ctx context.Context, dbName string, clauses map[constraintKey]string) error {
	rows, err := r.db.QueryContext(ctx, `
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

func (r *Reader) readNameOnlyCheckConstraintClauses(ctx context.Context, dbName string, clauses map[string]string) error {
	rows, err := r.db.QueryContext(ctx, `
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

// parameterDeclaration spells one routine parameter the way CREATE accepts it.
//
// A procedure's parameter carries its mode, and losing one is not cosmetic: a
// parameter with no mode is IN, so a replayed procedure assigns to a local copy
// that is discarded at return. The procedure still exists, still accepts the
// same call, still succeeds, and no longer returns its result
// (stokaro/ptah#2208).
//
// A function's parameter carries none. MySQL reports IN for them in
// information_schema.PARAMETERS all the same, and writing that back is a syntax
// error -- measured on MySQL 9.7: `CREATE FUNCTION f(IN a INT)` answers
// ERROR 1064. So the mode is emitted for procedures only, and the catalog's
// answer for a function is deliberately discarded rather than trusted.
//
// IN is omitted even on a procedure. It is the default, the catalog reports it
// for every parameter that was written without one, and emitting it would
// rewrite a declaration somebody made into a longer one that means the same.
func parameterDeclaration(routineType, mode, name, dataType string) string {
	declaration := name + " " + dataType
	if !strings.EqualFold(routineType, "PROCEDURE") {
		return declaration
	}
	switch strings.ToUpper(strings.TrimSpace(mode)) {
	case "OUT", "INOUT":
		return strings.ToUpper(mode) + " " + declaration
	}
	return declaration
}
