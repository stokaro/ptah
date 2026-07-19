package sqlite

import (
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/stokaro/ptah/core/convert/fromschema"
	"github.com/stokaro/ptah/dbschema/types"
)

var triggerHeaderPattern = regexp.MustCompile(
	`(?is)\ACREATE\s+(?:TEMP(?:ORARY)?\s+)?TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?` +
		`(?:"[^"]+"|\S+)\s+(BEFORE|AFTER|INSTEAD\s+OF)\s+(INSERT|UPDATE|DELETE)\s+` +
		`ON\s+("[^"]+"|\S+)(?:\s+FOR\s+EACH\s+(ROW|STATEMENT))?\s+`,
)

// Reader reads schema information from SQLite databases.
type Reader struct {
	db     *sql.DB
	schema string
}

// NewSQLiteReader creates a SQLite schema reader.
func NewSQLiteReader(db *sql.DB, schema string) *Reader {
	if schema == "" {
		schema = "main"
	}
	return &Reader{db: db, schema: schema}
}

// ReadSchema reads user tables, indexes, constraints, views, and triggers.
func (r *Reader) ReadSchema() (*types.DBSchema, error) {
	tableNames, ddlByTable, err := r.readTableDefinitions()
	if err != nil {
		return nil, err
	}

	var schema types.DBSchema
	for _, tableName := range tableNames {
		table, err := r.readTable(tableName, ddlByTable[tableName])
		if err != nil {
			return nil, err
		}
		schema.Tables = append(schema.Tables, table)

		indexes, uniqueConstraints, err := r.readIndexes(tableName, ddlByTable[tableName])
		if err != nil {
			return nil, fmt.Errorf("sqlite: read indexes for %s: %w", tableName, err)
		}
		schema.Indexes = append(schema.Indexes, indexes...)
		schema.Constraints = append(schema.Constraints, uniqueConstraints...)

		constraints, err := r.readTableConstraints(tableName, table.Columns, ddlByTable[tableName])
		if err != nil {
			return nil, fmt.Errorf("sqlite: read constraints for %s: %w", tableName, err)
		}
		schema.Constraints = append(schema.Constraints, constraints...)
	}

	views, err := r.readViews()
	if err != nil {
		return nil, err
	}
	schema.Views = views

	triggers, err := r.readTriggers()
	if err != nil {
		return nil, err
	}
	schema.Triggers = triggers
	reconcileColumnUniqueness(&schema)

	return &schema, nil
}

func (r *Reader) readTableDefinitions() ([]string, map[string]string, error) {
	rows, err := r.db.Query(`
		SELECT name, sql
		FROM sqlite_schema
		WHERE type = 'table'
		  AND name NOT LIKE 'sqlite_%'
		  AND name <> 'schema_migrations'
		ORDER BY name
	`)
	if err != nil {
		return nil, nil, fmt.Errorf("sqlite: list tables: %w", err)
	}
	defer rows.Close()

	var names []string
	ddlByTable := make(map[string]string)
	for rows.Next() {
		var name string
		var ddl sql.NullString
		if err := rows.Scan(&name, &ddl); err != nil {
			return nil, nil, fmt.Errorf("sqlite: scan table definition: %w", err)
		}
		names = append(names, name)
		ddlByTable[name] = ddl.String
	}
	if err := rows.Err(); err != nil {
		return nil, nil, fmt.Errorf("sqlite: iterate table definitions: %w", err)
	}
	return names, ddlByTable, nil
}

func (r *Reader) readTable(name, ddl string) (types.DBTable, error) {
	columns, err := r.readColumns(name, ddl)
	if err != nil {
		return types.DBTable{}, fmt.Errorf("sqlite: read columns: %w", err)
	}
	return types.DBTable{
		Name:    name,
		Schema:  r.outputSchema(),
		Type:    "TABLE",
		Columns: columns,
	}, nil
}

func (r *Reader) outputSchema() string {
	if r.schema == "main" {
		return ""
	}
	return r.schema
}

func (r *Reader) readColumns(tableName, ddl string) ([]types.DBColumn, error) {
	rows, err := r.db.Query("PRAGMA table_xinfo(" + quotePragmaString(tableName) + ")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	autoIncrementColumn := autoincrementColumn(ddl)
	generatedExpressions := extractGeneratedExpressions(ddl)
	var columns []types.DBColumn
	for rows.Next() {
		var (
			cid        int
			name       string
			dataType   string
			notNull    int
			defaultVal sql.NullString
			pkOrdinal  int
			hidden     int
		)
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultVal, &pkOrdinal, &hidden); err != nil {
			return nil, err
		}
		if hidden == 1 {
			continue
		}
		column := types.DBColumn{
			Name:                name,
			DataType:            normalizeSQLiteType(dataType),
			ColumnType:          dataType,
			IsNullable:          sqliteNullable(notNull, pkOrdinal),
			OrdinalPosition:     cid + 1,
			IsPrimaryKey:        pkOrdinal > 0,
			IsAutoIncrement:     strings.EqualFold(name, autoIncrementColumn),
			GeneratedKind:       sqliteGeneratedKind(hidden),
			GeneratedExpression: nil,
		}
		if expression := generatedExpressions[name]; expression != "" {
			column.GeneratedExpression = &expression
		}
		if defaultVal.Valid {
			value := defaultVal.String
			column.ColumnDefault = &value
		}
		columns = append(columns, column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func sqliteNullable(notNull, pkOrdinal int) string {
	if notNull != 0 || pkOrdinal > 0 {
		return "NO"
	}
	return "YES"
}

func sqliteGeneratedKind(hidden int) string {
	switch hidden {
	case 2:
		return "VIRTUAL"
	case 3:
		return "STORED"
	default:
		return ""
	}
}

func normalizeSQLiteType(dataType string) string {
	dataType = strings.TrimSpace(dataType)
	if dataType == "" {
		return "BLOB"
	}
	return strings.ToUpper(dataType)
}

func autoincrementColumn(ddl string) string {
	parts := splitTopLevelComma(tableBody(ddl))
	for _, part := range parts {
		if !strings.Contains(strings.ToUpper(part), "AUTOINCREMENT") {
			continue
		}
		name, ok := leadingIdentifier(part)
		if ok {
			return name
		}
	}
	return ""
}

func (r *Reader) readIndexes(tableName, ddl string) ([]types.DBIndex, []types.DBConstraint, error) {
	rows, err := r.db.Query("PRAGMA index_list(" + quotePragmaString(tableName) + ")")
	if err != nil {
		return nil, nil, err
	}

	var entries []sqliteIndexEntry
	for rows.Next() {
		var entry sqliteIndexEntry
		if err := rows.Scan(&entry.seq, &entry.name, &entry.unique, &entry.origin, &entry.partial); err != nil {
			_ = rows.Close()
			return nil, nil, err
		}
		entries = append(entries, entry)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return nil, nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, nil, err
	}

	var indexes []types.DBIndex
	var constraints []types.DBConstraint
	uniqueDefs := extractUniqueDefinitions(ddl)
	uniqueOrdinal := 0
	for _, entry := range entries {
		columns, err := r.readIndexColumns(entry.name)
		if err != nil {
			return nil, nil, err
		}
		definition, err := r.schemaSQL(entry.name)
		if err != nil {
			return nil, nil, err
		}
		if len(columns) == 0 {
			columns = extractIndexColumns(definition)
		}
		constraintName := entry.name
		if entry.origin == "u" && uniqueOrdinal < len(uniqueDefs) {
			uniqueDef := uniqueDefs[uniqueOrdinal]
			uniqueOrdinal++
			if len(columns) == 0 {
				columns = uniqueDef.columns
			}
			if uniqueDef.name != "" {
				constraintName = uniqueDef.name
			}
		}
		index := types.DBIndex{
			Name:       entry.name,
			TableName:  tableName,
			Schema:     r.outputSchema(),
			Columns:    columns,
			IsUnique:   entry.unique != 0,
			IsPrimary:  entry.origin == "pk",
			Definition: definition,
		}
		indexes = append(indexes, index)
		if entry.origin == "u" {
			constraints = append(constraints, types.DBConstraint{
				Name:        constraintName,
				TableName:   tableName,
				Schema:      r.outputSchema(),
				Type:        "UNIQUE",
				ColumnName:  first(columns),
				ColumnNames: columns,
			})
		}
	}
	return indexes, constraints, nil
}

type sqliteIndexEntry struct {
	seq     int
	name    string
	unique  int
	origin  string
	partial int
}

func (r *Reader) readIndexColumns(indexName string) ([]string, error) {
	rows, err := r.db.Query("SELECT seqno, cid, name FROM pragma_index_info(?)", indexName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	type indexColumn struct {
		seqno int
		name  string
	}
	var columns []indexColumn
	for rows.Next() {
		var (
			seqno int
			cid   int
			name  string
		)
		if err := rows.Scan(&seqno, &cid, &name); err != nil {
			return nil, err
		}
		if cid < 0 || name == "" {
			continue
		}
		columns = append(columns, indexColumn{seqno: seqno, name: name})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	sort.Slice(columns, func(i, j int) bool { return columns[i].seqno < columns[j].seqno })
	out := make([]string, len(columns))
	for i, column := range columns {
		out[i] = column.name
	}
	return out, nil
}

func (r *Reader) readTableConstraints(tableName string, columns []types.DBColumn, ddl string) ([]types.DBConstraint, error) {
	var constraints []types.DBConstraint
	if primary := primaryKeyConstraint(tableName, r.outputSchema(), columns, ddl); primary != nil {
		constraints = append(constraints, *primary)
	}
	checks := extractCheckConstraints(tableName, r.outputSchema(), ddl)
	constraints = append(constraints, checks...)
	foreignKeys, err := r.readForeignKeys(tableName, ddl)
	if err != nil {
		return nil, err
	}
	constraints = append(constraints, foreignKeys...)
	return constraints, nil
}

func primaryKeyConstraint(tableName, schema string, columns []types.DBColumn, ddl string) *types.DBConstraint {
	if name, names := extractPrimaryKeyDefinition(ddl); len(names) > 0 {
		if name == "" {
			name = tableName + "_pkey"
		}
		return &types.DBConstraint{
			Name:        name,
			TableName:   tableName,
			Schema:      schema,
			Type:        "PRIMARY KEY",
			ColumnName:  first(names),
			ColumnNames: names,
		}
	}

	type pkColumn struct {
		name string
		pos  int
	}
	var pk []pkColumn
	for _, column := range columns {
		if column.IsPrimaryKey {
			pk = append(pk, pkColumn{name: column.Name, pos: column.OrdinalPosition})
		}
	}
	if len(pk) == 0 {
		return nil
	}
	sort.Slice(pk, func(i, j int) bool { return pk[i].pos < pk[j].pos })
	names := make([]string, len(pk))
	for i, column := range pk {
		names[i] = column.name
	}
	return &types.DBConstraint{
		Name:        tableName + "_pkey",
		TableName:   tableName,
		Schema:      schema,
		Type:        "PRIMARY KEY",
		ColumnName:  first(names),
		ColumnNames: names,
	}
}

func (r *Reader) readForeignKeys(tableName, ddl string) ([]types.DBConstraint, error) {
	rows, err := r.db.Query("PRAGMA foreign_key_list(" + quotePragmaString(tableName) + ")")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	named := extractForeignKeyNames(ddl)
	groups := make(map[int]*types.DBConstraint)
	for rows.Next() {
		var (
			id       int
			seq      int
			refTable string
			from     string
			to       sql.NullString
			onUpdate string
			onDelete string
			match    string
		)
		if err := rows.Scan(&id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, err
		}
		constraint := groups[id]
		if constraint == nil {
			refTableCopy := refTable
			deleteRule := normalizeSQLiteAction(onDelete)
			updateRule := normalizeSQLiteAction(onUpdate)
			constraint = &types.DBConstraint{
				TableName:    tableName,
				Schema:       r.outputSchema(),
				Type:         "FOREIGN KEY",
				ForeignTable: &refTableCopy,
				DeleteRule:   &deleteRule,
				UpdateRule:   &updateRule,
			}
			groups[id] = constraint
		}
		constraint.ColumnNames = append(constraint.ColumnNames, from)
		if to.Valid && to.String != "" {
			constraint.ForeignColumns = append(constraint.ForeignColumns, to.String)
		}
		constraint.ColumnName = first(constraint.ColumnNames)
		if len(constraint.ForeignColumns) > 0 {
			foreignColumn := constraint.ForeignColumns[0]
			constraint.ForeignColumn = &foreignColumn
		}
		_ = seq
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	ids := make([]int, 0, len(groups))
	for id := range groups {
		ids = append(ids, id)
	}
	sort.Ints(ids)
	constraints := make([]types.DBConstraint, 0, len(ids))
	for _, id := range ids {
		constraint := groups[id]
		if constraint.ForeignTable != nil {
			signature := foreignKeySignature{
				columns:        strings.Join(constraint.ColumnNames, ","),
				foreignTable:   *constraint.ForeignTable,
				foreignColumns: strings.Join(constraint.ForeignColumns, ","),
			}
			if explicitName := named[signature]; explicitName != "" {
				constraint.Name = explicitName
			}
		}
		if constraint.Name == "" {
			constraint.Name = fromschema.GenerateForeignKeyName(tableName, first(constraint.ColumnNames))
		}
		constraints = append(constraints, *constraint)
	}
	return constraints, nil
}

func normalizeSQLiteAction(action string) string {
	action = strings.ToUpper(strings.TrimSpace(action))
	if action == "" {
		return "NO ACTION"
	}
	return action
}

func (r *Reader) readViews() ([]types.DBView, error) {
	rows, err := r.db.Query(`
		SELECT name, sql
		FROM sqlite_schema
		WHERE type = 'view'
		  AND name <> 'schema_migrations'
		ORDER BY name
	`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: query views: %w", err)
	}
	defer rows.Close()

	var views []types.DBView
	for rows.Next() {
		var name string
		var ddl sql.NullString
		if err := rows.Scan(&name, &ddl); err != nil {
			return nil, fmt.Errorf("sqlite: scan view: %w", err)
		}
		views = append(views, types.DBView{
			Name:        name,
			Schema:      r.outputSchema(),
			Body:        viewBody(ddl.String),
			CheckOption: "NONE",
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate views: %w", err)
	}
	return views, nil
}

func (r *Reader) readTriggers() ([]types.DBTrigger, error) {
	rows, err := r.db.Query(`
		SELECT name, tbl_name, sql
		FROM sqlite_schema
		WHERE type = 'trigger'
		ORDER BY tbl_name, name
	`)
	if err != nil {
		return nil, fmt.Errorf("sqlite: query triggers: %w", err)
	}
	defer rows.Close()

	var triggers []types.DBTrigger
	for rows.Next() {
		var name, table string
		var ddl sql.NullString
		if err := rows.Scan(&name, &table, &ddl); err != nil {
			return nil, fmt.Errorf("sqlite: scan trigger: %w", err)
		}
		trigger := parseTriggerDDL(name, table, r.outputSchema(), ddl.String)
		triggers = append(triggers, trigger)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate triggers: %w", err)
	}
	return triggers, nil
}

func (r *Reader) schemaSQL(name string) (string, error) {
	var sqlText sql.NullString
	err := r.db.QueryRow(`
		SELECT sql
		FROM sqlite_schema
		WHERE name = ?
	`, name).Scan(&sqlText)
	if err == sql.ErrNoRows {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return sqlText.String, nil
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

func quotePragmaString(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "''") + "'"
}

func reconcileColumnUniqueness(schema *types.DBSchema) {
	uniqueColumns := make(map[string]struct{})
	for _, index := range schema.Indexes {
		if index.IsPrimary || !index.IsUnique || len(index.Columns) != 1 {
			continue
		}
		if strings.Contains(strings.ToUpper(index.Definition), " WHERE ") {
			continue
		}
		uniqueColumns[index.QualifiedTableName()+"."+index.Columns[0]] = struct{}{}
	}
	for tableIdx := range schema.Tables {
		table := &schema.Tables[tableIdx]
		for columnIdx := range table.Columns {
			column := &table.Columns[columnIdx]
			_, unique := uniqueColumns[table.QualifiedName()+"."+column.Name]
			column.IsUnique = unique
		}
	}
}

func tableBody(ddl string) string {
	start := strings.Index(ddl, "(")
	if start < 0 {
		return ""
	}
	depth := 0
	inQuote := false
	for i := start; i < len(ddl); i++ {
		ch := ddl[i]
		if ch == '\'' {
			inQuote = !inQuote
			continue
		}
		if inQuote {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return ddl[start+1 : i]
			}
		}
	}
	return ""
}

func splitTopLevelComma(value string) []string {
	var parts []string
	start := 0
	depth := 0
	inString := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		case ',':
			if depth == 0 {
				parts = append(parts, strings.TrimSpace(value[start:i]))
				start = i + 1
			}
		}
	}
	if tail := strings.TrimSpace(value[start:]); tail != "" {
		parts = append(parts, tail)
	}
	return parts
}

func extractCheckConstraints(tableName, schema, ddl string) []types.DBConstraint {
	var constraints []types.DBConstraint
	for idx, part := range splitTopLevelComma(tableBody(ddl)) {
		name, rest := optionalConstraintName(part)
		if name == "" {
			name = inlineCheckConstraintName(part)
		}
		expr, ok := checkExpression(rest)
		if !ok {
			continue
		}
		if name == "" {
			name = inferCheckName(tableName, expr, idx+1)
		}
		constraints = append(constraints, types.DBConstraint{
			Name:        name,
			TableName:   tableName,
			Schema:      schema,
			Type:        "CHECK",
			CheckClause: &expr,
		})
	}
	return constraints
}

func inlineCheckConstraintName(value string) string {
	idx := indexKeyword(value, " CONSTRAINT ")
	if idx < 0 {
		return ""
	}
	rest := strings.TrimSpace(value[idx+len(" CONSTRAINT "):])
	name, ok := leadingIdentifier(rest)
	if !ok {
		return ""
	}
	afterName := strings.TrimSpace(rest[len(consumedIdentifierPrefix(rest)):])
	if indexKeyword(afterName, "CHECK") < 0 {
		return ""
	}
	return name
}

type uniqueDefinition struct {
	name    string
	columns []string
}

func extractUniqueDefinitions(ddl string) []uniqueDefinition {
	var definitions []uniqueDefinition
	for _, part := range splitTopLevelComma(tableBody(ddl)) {
		name, rest := optionalConstraintName(part)
		if columns := tableUniqueColumns(rest); len(columns) > 0 {
			definitions = append(definitions, uniqueDefinition{name: name, columns: columns})
			continue
		}
		if column, ok := columnUnique(part); ok {
			definitions = append(definitions, uniqueDefinition{name: name, columns: []string{column}})
		}
	}
	return definitions
}

func extractGeneratedExpressions(ddl string) map[string]string {
	out := make(map[string]string)
	for _, part := range splitTopLevelComma(tableBody(ddl)) {
		column, ok := leadingIdentifier(part)
		if !ok {
			continue
		}
		if indexKeyword(part, "GENERATED") < 0 {
			continue
		}
		expression := generatedExpression(part)
		if expression != "" {
			out[column] = expression
		}
	}
	return out
}

func generatedExpression(value string) string {
	idx := indexKeyword(value, "AS")
	if idx < 0 {
		return ""
	}
	after := strings.TrimSpace(value[idx+len("AS"):])
	if !strings.HasPrefix(after, "(") {
		return ""
	}
	return balancedParenthesized(after)
}

func extractPrimaryKeyDefinition(ddl string) (string, []string) {
	for _, part := range splitTopLevelComma(tableBody(ddl)) {
		name, rest := optionalConstraintName(part)
		if columns := tablePrimaryKeyColumns(rest); len(columns) > 0 {
			return name, columns
		}
	}
	return "", nil
}

func tablePrimaryKeyColumns(value string) []string {
	idx := indexKeyword(value, "PRIMARY")
	if idx < 0 {
		return nil
	}
	prefix := strings.TrimSpace(value[:idx])
	if prefix != "" {
		return nil
	}
	after := strings.TrimSpace(value[idx+len("PRIMARY"):])
	if !strings.HasPrefix(strings.ToUpper(after), "KEY") {
		return nil
	}
	after = strings.TrimSpace(after[len("KEY"):])
	if !strings.HasPrefix(after, "(") {
		return nil
	}
	return splitIdentifierList(balancedParenthesized(after))
}

func tableUniqueColumns(value string) []string {
	idx := indexKeyword(value, "UNIQUE")
	if idx < 0 {
		return nil
	}
	prefix := strings.TrimSpace(value[:idx])
	if prefix != "" {
		return nil
	}
	after := strings.TrimSpace(value[idx+len("UNIQUE"):])
	if !strings.HasPrefix(after, "(") {
		return nil
	}
	return splitIdentifierList(balancedParenthesized(after))
}

func columnUnique(value string) (string, bool) {
	name, ok := leadingIdentifier(value)
	if !ok {
		return "", false
	}
	upper := strings.ToUpper(value)
	if !strings.Contains(upper, " UNIQUE") {
		return "", false
	}
	return name, true
}

func extractIndexColumns(definition string) []string {
	idx := indexKeyword(definition, " ON ")
	if idx < 0 {
		return nil
	}
	after := strings.TrimSpace(definition[idx+4:])
	open := strings.Index(after, "(")
	if open < 0 {
		return nil
	}
	return splitIdentifierList(balancedParenthesized(after[open:]))
}

func inferCheckName(tableName, expr string, fallback int) string {
	column := leadingCheckColumn(expr)
	if column != "" {
		return tableName + "_" + column + "_check"
	}
	return tableName + "_check_" + strconv.Itoa(fallback)
}

func leadingCheckColumn(expr string) string {
	expr = strings.TrimSpace(expr)
	name, ok := leadingIdentifier(expr)
	if ok {
		return name
	}
	return ""
}

func checkExpression(value string) (string, bool) {
	idx := indexKeyword(value, "CHECK")
	if idx < 0 {
		return "", false
	}
	after := strings.TrimSpace(value[idx+len("CHECK"):])
	if !strings.HasPrefix(after, "(") {
		return "", false
	}
	expr := balancedParenthesized(after)
	if expr == "" {
		return "", false
	}
	return expr, true
}

type foreignKeySignature struct {
	columns        string
	foreignTable   string
	foreignColumns string
}

func extractForeignKeyNames(ddl string) map[foreignKeySignature]string {
	out := make(map[foreignKeySignature]string)
	for _, part := range splitTopLevelComma(tableBody(ddl)) {
		if name, signature := inlineForeignKeyName(part); name != "" {
			out[signature] = name
			continue
		}
		name, rest := optionalConstraintName(part)
		if name == "" || indexKeyword(rest, "FOREIGN") < 0 {
			continue
		}
		columns := foreignKeyColumns(rest)
		table := foreignKeyTable(rest)
		foreignColumns := foreignKeyReferencedColumns(rest)
		if len(columns) == 0 || table == "" {
			continue
		}
		out[foreignKeySignature{
			columns:        strings.Join(columns, ","),
			foreignTable:   table,
			foreignColumns: strings.Join(foreignColumns, ","),
		}] = name
	}
	return out
}

func inlineForeignKeyName(value string) (string, foreignKeySignature) {
	column, ok := leadingIdentifier(value)
	if !ok || indexKeyword(value, "FOREIGN") >= 0 {
		return "", foreignKeySignature{}
	}
	constraintIdx := indexKeyword(value, " CONSTRAINT ")
	referencesIdx := indexKeyword(value, "REFERENCES")
	if constraintIdx < 0 || referencesIdx < 0 || constraintIdx > referencesIdx {
		return "", foreignKeySignature{}
	}
	afterConstraint := strings.TrimSpace(value[constraintIdx+len(" CONSTRAINT "):])
	name, ok := leadingIdentifier(afterConstraint)
	if !ok {
		return "", foreignKeySignature{}
	}
	foreignTable := foreignKeyTable(value)
	if foreignTable == "" {
		return "", foreignKeySignature{}
	}
	return name, foreignKeySignature{
		columns:        column,
		foreignTable:   foreignTable,
		foreignColumns: strings.Join(foreignKeyReferencedColumns(value), ","),
	}
}

func foreignKeyColumns(value string) []string {
	idx := indexKeyword(value, "FOREIGN")
	if idx < 0 {
		return nil
	}
	after := strings.TrimSpace(value[idx+len("FOREIGN"):])
	if !strings.HasPrefix(strings.ToUpper(after), "KEY") {
		return nil
	}
	after = strings.TrimSpace(after[len("KEY"):])
	if !strings.HasPrefix(after, "(") {
		return nil
	}
	return splitIdentifierList(balancedParenthesized(after))
}

func foreignKeyTable(value string) string {
	idx := indexKeyword(value, "REFERENCES")
	if idx < 0 {
		return ""
	}
	after := strings.TrimSpace(value[idx+len("REFERENCES"):])
	name, ok := leadingIdentifier(after)
	if !ok {
		return ""
	}
	return name
}

func foreignKeyReferencedColumns(value string) []string {
	idx := indexKeyword(value, "REFERENCES")
	if idx < 0 {
		return nil
	}
	after := strings.TrimSpace(value[idx+len("REFERENCES"):])
	tablePrefix := consumedIdentifierPrefix(after)
	if tablePrefix == "" {
		return nil
	}
	afterTable := strings.TrimSpace(after[len(tablePrefix):])
	if !strings.HasPrefix(afterTable, "(") {
		return nil
	}
	return splitIdentifierList(balancedParenthesized(afterTable))
}

func splitIdentifierList(value string) []string {
	parts := splitTopLevelComma(value)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if name, ok := leadingIdentifier(part); ok {
			out = append(out, name)
		}
	}
	return out
}

func optionalConstraintName(value string) (name string, rest string) {
	value = strings.TrimSpace(value)
	if !strings.HasPrefix(strings.ToUpper(value), "CONSTRAINT ") {
		return "", value
	}
	afterConstraint := strings.TrimSpace(value[len("CONSTRAINT "):])
	name, ok := leadingIdentifier(afterConstraint)
	if !ok {
		return "", value
	}
	afterName := strings.TrimSpace(afterConstraint[len(consumedIdentifierPrefix(afterConstraint)):])
	return name, afterName
}

func leadingIdentifier(value string) (string, bool) {
	value = strings.TrimSpace(value)
	switch {
	case value == "":
		return "", false
	case value[0] == '"':
		end := 1
		var b strings.Builder
		for end < len(value) {
			if value[end] == '"' {
				if end+1 < len(value) && value[end+1] == '"' {
					b.WriteByte('"')
					end += 2
					continue
				}
				return b.String(), true
			}
			b.WriteByte(value[end])
			end++
		}
		return "", false
	case value[0] == '`' || value[0] == '[':
		closeDelim := byte('`')
		if value[0] == '[' {
			closeDelim = ']'
		}
		end := strings.IndexByte(value[1:], closeDelim)
		if end < 0 {
			return "", false
		}
		return value[1 : end+1], true
	default:
		end := len(value)
		for i, ch := range value {
			if i > 0 && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '(' || ch == ',') {
				end = i
				break
			}
		}
		return strings.Trim(value[:end], `"`), end > 0
	}
}

func consumedIdentifierPrefix(value string) string {
	value = strings.TrimSpace(value)
	switch {
	case value == "":
		return ""
	case value[0] == '"':
		for i := 1; i < len(value); i++ {
			if value[i] == '"' {
				if i+1 < len(value) && value[i+1] == '"' {
					i++
					continue
				}
				return value[:i+1]
			}
		}
	case value[0] == '`':
		if end := strings.IndexByte(value[1:], '`'); end >= 0 {
			return value[:end+2]
		}
	}
	end := len(value)
	for i, ch := range value {
		if i > 0 && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r' || ch == '(' || ch == ',') {
			end = i
			break
		}
	}
	return value[:end]
}

func balancedParenthesized(value string) string {
	if !strings.HasPrefix(value, "(") {
		return ""
	}
	depth := 0
	inString := false
	for i := 0; i < len(value); i++ {
		ch := value[i]
		if ch == '\'' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch ch {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return strings.TrimSpace(value[1:i])
			}
		}
	}
	return ""
}

func indexKeyword(value, keyword string) int {
	if strings.TrimSpace(keyword) != keyword || strings.Contains(keyword, " ") {
		upper := strings.ToUpper(value)
		return strings.Index(upper, strings.ToUpper(keyword))
	}
	upper := strings.ToUpper(value)
	keyword = strings.ToUpper(keyword)
	for offset := 0; offset < len(upper); {
		idx := strings.Index(upper[offset:], keyword)
		if idx < 0 {
			return -1
		}
		idx += offset
		beforeOK := idx == 0 || !isIdentifierByte(upper[idx-1])
		after := idx + len(keyword)
		afterOK := after == len(upper) || !isIdentifierByte(upper[after])
		if beforeOK && afterOK {
			return idx
		}
		offset = idx + len(keyword)
	}
	return -1
}

func isIdentifierByte(ch byte) bool {
	return ch == '_' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z'
}

func viewBody(ddl string) string {
	idx := indexKeyword(ddl, "AS")
	if idx < 0 {
		return ""
	}
	return strings.TrimSpace(ddl[idx+len("AS"):])
}

func parseTriggerDDL(name, table, schema, ddl string) types.DBTrigger {
	trigger := types.DBTrigger{
		Name:    name,
		Schema:  schema,
		Table:   table,
		ForEach: "ROW",
		Body:    strings.TrimSpace(ddl),
	}
	matches := triggerHeaderPattern.FindStringSubmatch(ddl)
	if len(matches) == 0 {
		return trigger
	}
	trigger.Timing = strings.ToUpper(strings.Join(strings.Fields(matches[1]), " "))
	trigger.Event = strings.ToUpper(matches[2])
	trigger.Table = strings.Trim(matches[3], `"`)
	if strings.TrimSpace(matches[4]) != "" {
		trigger.ForEach = strings.ToUpper(matches[4])
	}
	return trigger
}
