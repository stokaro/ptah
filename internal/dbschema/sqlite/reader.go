package sqlite

import (
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/convert/fromschema"
	"go.5x5.cz/ptah/internal/sqlident"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

var triggerHeaderPattern = regexp.MustCompile(
	`(?is)\ACREATE\s+(?:TEMP(?:ORARY)?\s+)?TRIGGER\s+(?:IF\s+NOT\s+EXISTS\s+)?` +
		`(?:"[^"]+"|\S+)\s+(BEFORE|AFTER|INSTEAD\s+OF)\s+(INSERT|UPDATE|DELETE)\s+` +
		`ON\s+("[^"]+"|\S+)(?:\s+FOR\s+EACH\s+(ROW|STATEMENT))?\s+`,
)

// Reader reads schema information from SQLite databases.
type Reader struct {
	db     sqlrunner.Runner
	schema string
}

type tableColumnKey struct {
	table  string
	column string
}

// NewSQLiteReader creates a SQLite schema reader.
func NewSQLiteReader(db sqlrunner.Runner, schema string) *Reader {
	if schema == "" {
		schema = "main"
	}
	return &Reader{db: db, schema: schema}
}

// ReadSchema reads user tables, indexes, constraints, views, and triggers.
func (r *Reader) ReadSchema() (*types.DBSchema, error) {
	catalog, err := r.readSchemaCatalog()
	if err != nil {
		return nil, err
	}

	// Virtual tables and their shadow tables are excluded from every per-table
	// PRAGMA. A virtual table has no column list of its own to read, and
	// asking for one is not merely useless: pragma_table_xinfo has to load the
	// module to answer, so a single virtual table whose module this build does
	// not register fails the whole batch with `no such module: <name>` and
	// takes the rest of the schema down with it.
	skipped := catalog.nonOrdinaryTableNames()

	columnsByTable, err := r.readColumnsByTable(skipped)
	if err != nil {
		return nil, err
	}

	indexesByTable, uniqueConstraintsByTable, err := r.readIndexesByTable(
		catalog.indexDDLByName, catalog.tableDDLByName, skipped,
	)
	if err != nil {
		return nil, err
	}

	foreignKeysByTable, err := r.readForeignKeysByTable(catalog.tableDDLByName, skipped)
	if err != nil {
		return nil, err
	}

	var schema types.DBSchema
	for _, tableName := range catalog.tableNames {
		if spec, ok := catalog.virtualTables[tableName]; ok {
			schema.Tables = append(schema.Tables, r.readVirtualTable(tableName, spec))
			continue
		}

		ddl := catalog.tableDDLByName[tableName]
		table := r.readTable(tableName, columnsByTable[tableName], ddl)
		schema.Tables = append(schema.Tables, table)

		schema.Indexes = append(schema.Indexes, indexesByTable[tableName]...)
		schema.Constraints = append(schema.Constraints, uniqueConstraintsByTable[tableName]...)

		constraints := r.readTableConstraints(tableName, table.Columns, ddl, foreignKeysByTable[tableName])
		schema.Constraints = append(schema.Constraints, constraints...)
	}

	schema.Views = catalog.views(r.outputSchema())
	schema.Triggers = catalog.triggers(r.outputSchema())
	reconcileColumnUniqueness(&schema)

	return &schema, nil
}

type sqliteSchemaCatalog struct {
	tableNames     []string
	tableDDLByName map[string]string
	indexDDLByName map[string]string
	viewObjects    []sqliteSchemaObject
	triggerObjects []sqliteSchemaObject
	// virtualTables holds the module declaration of every name in tableNames
	// that a CREATE VIRTUAL TABLE statement created. Ordinary tables are
	// absent from it.
	virtualTables map[string]virtualTableSpec
	// shadowTableNames holds the tables a virtual table's module maintains.
	// They are never reported as schema objects; the field exists so the
	// per-table PRAGMA batches can skip them too.
	shadowTableNames []string
}

// nonOrdinaryTableNames lists the tables no per-table PRAGMA should be asked
// about: the virtual tables and the shadow tables their modules maintain.
func (c sqliteSchemaCatalog) nonOrdinaryTableNames() []string {
	names := make([]string, 0, len(c.virtualTables)+len(c.shadowTableNames))
	for name := range c.virtualTables {
		names = append(names, name)
	}
	names = append(names, c.shadowTableNames...)
	sort.Strings(names)
	return names
}

// excludeTablesFilter renders a WHERE fragment that removes the named tables
// from a query over sqlite_schema aliased as m, together with its arguments.
//
// The names are bound as parameters rather than interpolated: they come from
// the catalog of a database Ptah did not create, and a table name is allowed
// to contain a quote.
func excludeTablesFilter(names []string) (string, []any) {
	if len(names) == 0 {
		return "", nil
	}
	arguments := make([]any, 0, len(names))
	for _, name := range names {
		arguments = append(arguments, name)
	}
	placeholders := strings.TrimSuffix(strings.Repeat("?, ", len(names)), ", ")
	return "\n\t\t  AND m.name NOT IN (" + placeholders + ")", arguments
}

type sqliteSchemaObject struct {
	name      string
	tableName string
	ddl       string
}

func (c sqliteSchemaCatalog) views(schema string) []types.DBView {
	views := make([]types.DBView, 0, len(c.viewObjects))
	for _, object := range c.viewObjects {
		views = append(views, types.DBView{
			Name:        object.name,
			Schema:      schema,
			Body:        viewBody(object.ddl),
			CheckOption: "NONE",
		})
	}
	return views
}

func (c sqliteSchemaCatalog) triggers(schema string) []types.DBTrigger {
	triggers := make([]types.DBTrigger, 0, len(c.triggerObjects))
	for _, object := range c.triggerObjects {
		trigger := parseTriggerDDL(object.name, object.tableName, schema, object.ddl)
		triggers = append(triggers, trigger)
	}
	return triggers
}

func (r *Reader) readSchemaCatalog() (sqliteSchemaCatalog, error) {
	kinds, err := r.readTableKinds()
	if err != nil {
		return sqliteSchemaCatalog{}, err
	}

	query := fmt.Sprintf(`
		SELECT type, name, tbl_name, sql
		FROM %s
		WHERE type IN ('table', 'index', 'view', 'trigger')
		  AND NOT (type = 'table' AND name LIKE 'sqlite\_%%' ESCAPE '\')
		  AND NOT (type IN ('table', 'view') AND name = 'schema_migrations')
		ORDER BY type, tbl_name, name
	`, r.schemaObject("sqlite_schema"))
	rows, err := r.db.Query(query)
	if err != nil {
		return sqliteSchemaCatalog{}, fmt.Errorf("sqlite: read schema catalog: %w", err)
	}
	defer rows.Close()

	catalog := sqliteSchemaCatalog{
		tableDDLByName: make(map[string]string),
		indexDDLByName: make(map[string]string),
		virtualTables:  make(map[string]virtualTableSpec),
	}
	for rows.Next() {
		var objectType, name, tableName string
		var ddl sql.NullString
		if err := rows.Scan(&objectType, &name, &tableName, &ddl); err != nil {
			return sqliteSchemaCatalog{}, fmt.Errorf("sqlite: scan schema catalog: %w", err)
		}
		switch objectType {
		case "table":
			spec, virtual := parseVirtualTableDDL(ddl.String)
			switch {
			case kinds[name] == tableKindShadow:
				// A module's own bookkeeping table. Reporting it would put a
				// CREATE TABLE in front of an operator for an object SQLite
				// creates itself, which then collides with the virtual table
				// that owns it. See stokaro/ptah#1028.
				catalog.shadowTableNames = append(catalog.shadowTableNames, name)
				continue
			case virtual:
				catalog.virtualTables[name] = spec
			case kinds[name] == tableKindVirtual:
				// SQLite says this is a virtual table but its own recorded
				// statement does not parse as one. Ptah cannot say which
				// module owns it, and describing it as an ordinary table would
				// emit a statement that never created it.
				return sqliteSchemaCatalog{}, fmt.Errorf(
					"sqlite: %q is a virtual table whose CREATE VIRTUAL TABLE statement Ptah cannot read: %q",
					name, ddl.String,
				)
			}
			catalog.tableNames = append(catalog.tableNames, name)
			catalog.tableDDLByName[name] = ddl.String
		case "index":
			catalog.indexDDLByName[name] = ddl.String
		case "view":
			catalog.viewObjects = append(catalog.viewObjects, sqliteSchemaObject{
				name:      name,
				tableName: tableName,
				ddl:       ddl.String,
			})
		case "trigger":
			catalog.triggerObjects = append(catalog.triggerObjects, sqliteSchemaObject{
				name:      name,
				tableName: tableName,
				ddl:       ddl.String,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return sqliteSchemaCatalog{}, fmt.Errorf("sqlite: iterate schema catalog: %w", err)
	}
	sort.Strings(catalog.tableNames)
	sort.Slice(catalog.viewObjects, func(i, j int) bool {
		return catalog.viewObjects[i].name < catalog.viewObjects[j].name
	})
	sort.Slice(catalog.triggerObjects, func(i, j int) bool {
		if catalog.triggerObjects[i].tableName != catalog.triggerObjects[j].tableName {
			return catalog.triggerObjects[i].tableName < catalog.triggerObjects[j].tableName
		}
		return catalog.triggerObjects[i].name < catalog.triggerObjects[j].name
	})
	return catalog, nil
}

func (r *Reader) readTable(name string, columns []types.DBColumn, ddl string) types.DBTable {
	strict, withoutRowID := sqliteTableOptions(ddl)
	return types.DBTable{
		Name:         name,
		Schema:       r.outputSchema(),
		Type:         "TABLE",
		Columns:      columns,
		Strict:       strict,
		WithoutRowID: withoutRowID,
	}
}

// readVirtualTable describes a virtual table by the module declaration that
// created it.
//
// It carries no columns. A virtual table's columns are the module's answer to
// xConnect, not a column list an operator wrote, and CREATE VIRTUAL TABLE has
// nowhere to put them: the module arguments are what recreate the object. When
// the module is not registered in this build, SQLite cannot report the columns
// at all, so a description built from them would be empty for exactly the
// databases that need it most.
func (r *Reader) readVirtualTable(name string, spec virtualTableSpec) types.DBTable {
	return types.DBTable{
		Name:             name,
		Schema:           r.outputSchema(),
		Type:             "TABLE",
		VirtualModule:    spec.Module,
		VirtualArguments: spec.Arguments,
	}
}

func sqliteTableOptions(ddl string) (strict bool, withoutRowID bool) {
	idx := strings.LastIndex(ddl, ")")
	if idx < 0 {
		return false, false
	}
	tail := strings.ToUpper(ddl[idx+1:])
	return strings.Contains(tail, "STRICT"), strings.Contains(tail, "WITHOUT ROWID")
}

func (r *Reader) outputSchema() string {
	if r.schema == "main" {
		return ""
	}
	return r.schema
}

func (r *Reader) schemaObject(name string) string {
	schema := r.schema
	if schema == "" {
		schema = "main"
	}
	return sqlident.Qualified("sqlite", schema, name)
}

func (r *Reader) readColumnsByTable(skipped []string) (map[string][]types.DBColumn, error) {
	exclusion, exclusionArguments := excludeTablesFilter(skipped)
	query := fmt.Sprintf(`
		SELECT m.name, x.cid, x.name, x.type, x."notnull", x.dflt_value, x.pk, x.hidden, m.sql
		FROM %s AS m
		JOIN pragma_table_xinfo(m.name, ?) AS x
		WHERE m.type = 'table'
		  AND m.name NOT LIKE 'sqlite\_%%' ESCAPE '\'
		  AND m.name <> 'schema_migrations'%s
		ORDER BY m.name, x.cid
	`, r.schemaObject("sqlite_schema"), exclusion)
	rows, err := r.db.Query(query, append([]any{r.schema}, exclusionArguments...)...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read columns: %w", err)
	}
	defer rows.Close()

	type tableDDLMetadata struct {
		autoIncrementColumn  string
		generatedExpressions map[string]string
	}
	ddlMetadataByTable := make(map[string]tableDDLMetadata)
	columnsByTable := make(map[string][]types.DBColumn)
	for rows.Next() {
		var (
			tableName  string
			cid        int
			name       string
			dataType   string
			notNull    int
			defaultVal sql.NullString
			pkOrdinal  int
			hidden     int
			ddl        sql.NullString
		)
		if err := rows.Scan(&tableName, &cid, &name, &dataType, &notNull, &defaultVal, &pkOrdinal, &hidden, &ddl); err != nil {
			return nil, fmt.Errorf("sqlite: scan column: %w", err)
		}
		if hidden == 1 {
			continue
		}
		ddlMetadata := ddlMetadataByTable[tableName]
		if ddlMetadata.generatedExpressions == nil {
			ddlMetadata = tableDDLMetadata{
				autoIncrementColumn:  autoincrementColumn(ddl.String),
				generatedExpressions: extractGeneratedExpressions(ddl.String),
			}
			ddlMetadataByTable[tableName] = ddlMetadata
		}
		column := types.DBColumn{
			Name:                name,
			DataType:            normalizeSQLiteType(dataType),
			ColumnType:          dataType,
			IsNullable:          sqliteNullable(notNull),
			OrdinalPosition:     cid + 1,
			IsPrimaryKey:        pkOrdinal > 0,
			IsAutoIncrement:     strings.EqualFold(name, ddlMetadata.autoIncrementColumn),
			GeneratedKind:       sqliteGeneratedKind(hidden),
			GeneratedExpression: nil,
		}
		if expression := ddlMetadata.generatedExpressions[name]; expression != "" {
			column.GeneratedExpression = &expression
		}
		if defaultVal.Valid {
			value := defaultVal.String
			column.ColumnDefault = &value
		}
		columnsByTable[tableName] = append(columnsByTable[tableName], column)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate columns: %w", err)
	}
	return columnsByTable, nil
}

// sqliteNullable reports the column's nullability as SQLite itself reports it,
// from `pragma table_info.notnull` alone.
//
// Primary-key membership is deliberately not consulted. SQLite does not imply
// NOT NULL from PRIMARY KEY on a rowid table: `id INTEGER PRIMARY KEY` is a
// rowid alias with `notnull` 0 that accepts an explicit NULL insert and assigns
// a rowid for it. Measured on the pinned Atlas community v1.3.0 binary,
// `schema inspect --format '{{ json . }}'` over such a column reports
// `"null": true`, and it is the only column in the fixture whose flag Ptah
// disagreed about. Reading the flag from the key instead of from the catalog
// inverted it. See stokaro/ptah#1235 finding 6.3.
func sqliteNullable(notNull int) string {
	if notNull != 0 {
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

func (r *Reader) readIndexesByTable(
	indexDDLByName map[string]string,
	tableDDLByName map[string]string,
	skipped []string,
) (
	map[string][]types.DBIndex,
	map[string][]types.DBConstraint,
	error,
) {
	entriesByTable, err := r.readIndexEntriesByTable(skipped)
	if err != nil {
		return nil, nil, err
	}

	columnsByIndex, err := r.readIndexColumnsByIndex(skipped)
	if err != nil {
		return nil, nil, err
	}

	indexesByTable := make(map[string][]types.DBIndex, len(entriesByTable))
	constraintsByTable := make(map[string][]types.DBConstraint, len(entriesByTable))
	for tableName, entries := range entriesByTable {
		indexes, constraints := r.buildIndexesForTable(tableName, entries, indexDDLByName, tableDDLByName, columnsByIndex)
		indexesByTable[tableName] = indexes
		constraintsByTable[tableName] = constraints
	}
	return indexesByTable, constraintsByTable, nil
}

func (r *Reader) readIndexEntriesByTable(skipped []string) (map[string][]sqliteIndexEntry, error) {
	exclusion, exclusionArguments := excludeTablesFilter(skipped)
	query := fmt.Sprintf(`
		SELECT m.name, il.seq, il.name, il."unique", il.origin, il.partial
		FROM %s AS m
		JOIN pragma_index_list(m.name, ?) AS il
		WHERE m.type = 'table'
		  AND m.name NOT LIKE 'sqlite\_%%' ESCAPE '\'
		  AND m.name <> 'schema_migrations'%s
		ORDER BY m.name, il.seq
	`, r.schemaObject("sqlite_schema"), exclusion)
	rows, err := r.db.Query(query, append([]any{r.schema}, exclusionArguments...)...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read indexes: %w", err)
	}
	defer rows.Close()

	entriesByTable := make(map[string][]sqliteIndexEntry)
	for rows.Next() {
		var tableName string
		var entry sqliteIndexEntry
		if err := rows.Scan(&tableName, &entry.seq, &entry.name, &entry.unique, &entry.origin, &entry.partial); err != nil {
			return nil, fmt.Errorf("sqlite: scan index: %w", err)
		}
		entriesByTable[tableName] = append(entriesByTable[tableName], entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate indexes: %w", err)
	}
	return entriesByTable, nil
}

func (r *Reader) buildIndexesForTable(
	tableName string,
	entries []sqliteIndexEntry,
	indexDDLByName map[string]string,
	tableDDLByName map[string]string,
	columnsByIndex map[string]sqliteIndexColumns,
) ([]types.DBIndex, []types.DBConstraint) {
	ddl := tableDDLByName[tableName]
	var indexes []types.DBIndex
	var constraints []types.DBConstraint
	uniqueDefs := extractUniqueDefinitions(ddl)
	uniqueDefsByColumns := uniqueDefinitionsByColumns(uniqueDefs)
	uniqueOrdinal := 0
	for _, entry := range entries {
		definition := indexDDLByName[entry.name]
		indexColumns := columnsByIndex[entry.name]
		columns := indexColumns.names
		if indexColumns.needsDDLParsing || len(columns) == 0 {
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
		if entry.partial != 0 {
			index.Condition = extractIndexCondition(definition)
		}
		indexes = append(indexes, index)
		if entry.origin == "u" {
			if uniqueDef, ok := uniqueDefsByColumns[strings.Join(columns, ",")]; ok && uniqueDef.name != "" {
				constraintName = uniqueDef.name
			}
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
	return indexes, constraints
}

func uniqueDefinitionsByColumns(definitions []uniqueDefinition) map[string]uniqueDefinition {
	out := make(map[string]uniqueDefinition, len(definitions))
	for _, definition := range definitions {
		out[strings.Join(definition.columns, ",")] = definition
	}
	return out
}

type sqliteIndexColumns struct {
	names           []string
	needsDDLParsing bool
}

func (r *Reader) readIndexColumnsByIndex(skipped []string) (map[string]sqliteIndexColumns, error) {
	exclusion, exclusionArguments := excludeTablesFilter(skipped)
	query := fmt.Sprintf(`
		SELECT il.name, ix.seqno, ix.cid, ix.name, ix.key
		FROM %s AS m
		JOIN pragma_index_list(m.name, ?) AS il
		JOIN pragma_index_xinfo(il.name, ?) AS ix
		WHERE m.type = 'table'
		  AND m.name NOT LIKE 'sqlite\_%%' ESCAPE '\'
		  AND m.name <> 'schema_migrations'%s
		ORDER BY il.name, ix.seqno
	`, r.schemaObject("sqlite_schema"), exclusion)
	rows, err := r.db.Query(query, append([]any{r.schema, r.schema}, exclusionArguments...)...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read index columns: %w", err)
	}
	defer rows.Close()

	type indexColumn struct {
		seqno int
		name  string
	}
	columns := make(map[string][]indexColumn)
	needsDDLParsing := make(map[string]bool)
	for rows.Next() {
		var (
			indexName string
			seqno     int
			cid       int
			name      sql.NullString
			keyColumn int
		)
		if err := rows.Scan(&indexName, &seqno, &cid, &name, &keyColumn); err != nil {
			return nil, fmt.Errorf("sqlite: scan index column: %w", err)
		}
		if keyColumn == 0 {
			continue
		}
		if cid < 0 || !name.Valid || name.String == "" {
			needsDDLParsing[indexName] = true
			continue
		}
		columns[indexName] = append(columns[indexName], indexColumn{seqno: seqno, name: name.String})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate index columns: %w", err)
	}

	out := make(map[string]sqliteIndexColumns, len(columns)+len(needsDDLParsing))
	for indexName, indexColumns := range columns {
		sort.Slice(indexColumns, func(i, j int) bool { return indexColumns[i].seqno < indexColumns[j].seqno })
		names := make([]string, len(indexColumns))
		for i, column := range indexColumns {
			names[i] = column.name
		}
		out[indexName] = sqliteIndexColumns{
			names:           names,
			needsDDLParsing: needsDDLParsing[indexName],
		}
	}
	for indexName := range needsDDLParsing {
		if _, ok := out[indexName]; !ok {
			out[indexName] = sqliteIndexColumns{needsDDLParsing: true}
		}
	}
	return out, nil
}

func extractIndexCondition(definition string) string {
	whereIdx := indexTopLevelKeyword(definition, "WHERE")
	if whereIdx == -1 {
		return ""
	}
	return strings.TrimSpace(definition[whereIdx+len("WHERE"):])
}

func indexTopLevelKeyword(definition, keyword string) int {
	depth := 0
	var quote byte
	for i := 0; i < len(definition); i++ {
		ch := definition[i]
		if quote != 0 {
			if ch == quote {
				if i+1 < len(definition) && definition[i+1] == quote {
					i++
					continue
				}
				quote = 0
			}
			continue
		}

		switch ch {
		case '\'', '"', '`':
			quote = ch
		case '(':
			depth++
		case ')':
			if depth > 0 {
				depth--
			}
		default:
			if depth == 0 && hasKeywordAt(definition, keyword, i) {
				return i
			}
		}
	}
	return -1
}

func hasKeywordAt(input, keyword string, idx int) bool {
	if idx > 0 && isSQLIdentByte(input[idx-1]) {
		return false
	}
	if idx+len(keyword) > len(input) || !strings.EqualFold(input[idx:idx+len(keyword)], keyword) {
		return false
	}
	return idx+len(keyword) == len(input) || !isSQLIdentByte(input[idx+len(keyword)])
}

func isSQLIdentByte(ch byte) bool {
	return ch == '_' || ch == '$' || ch >= '0' && ch <= '9' || ch >= 'A' && ch <= 'Z' || ch >= 'a' && ch <= 'z'
}

type sqliteIndexEntry struct {
	seq     int
	name    string
	unique  int
	origin  string
	partial int
}

func (r *Reader) readTableConstraints(
	tableName string,
	columns []types.DBColumn,
	ddl string,
	foreignKeys []types.DBConstraint,
) []types.DBConstraint {
	var constraints []types.DBConstraint
	if primary := primaryKeyConstraint(tableName, r.outputSchema(), columns, ddl); primary != nil {
		constraints = append(constraints, *primary)
	}
	checks := extractCheckConstraints(tableName, r.outputSchema(), ddl)
	constraints = append(constraints, checks...)
	constraints = append(constraints, foreignKeys...)
	return constraints
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

func (r *Reader) readForeignKeysByTable(
	tableDDLByName map[string]string,
	skipped []string,
) (map[string][]types.DBConstraint, error) {
	exclusion, exclusionArguments := excludeTablesFilter(skipped)
	query := fmt.Sprintf(`
		SELECT m.name, fk.id, fk.seq, fk."table", fk."from", fk."to", fk.on_update, fk.on_delete, fk.match
		FROM %s AS m
		JOIN pragma_foreign_key_list(m.name, ?) AS fk
		WHERE m.type = 'table'
		  AND m.name NOT LIKE 'sqlite\_%%' ESCAPE '\'
		  AND m.name <> 'schema_migrations'%s
		ORDER BY m.name, fk.id, fk.seq
	`, r.schemaObject("sqlite_schema"), exclusion)
	rows, err := r.db.Query(query, append([]any{r.schema}, exclusionArguments...)...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read foreign keys: %w", err)
	}
	defer rows.Close()

	groupsByTable := make(map[string]map[int]*types.DBConstraint)
	for rows.Next() {
		var (
			tableName string
			id        int
			seq       int
			refTable  string
			from      string
			to        sql.NullString
			onUpdate  string
			onDelete  string
			match     string
		)
		if err := rows.Scan(&tableName, &id, &seq, &refTable, &from, &to, &onUpdate, &onDelete, &match); err != nil {
			return nil, fmt.Errorf("sqlite: scan foreign key: %w", err)
		}
		groups := groupsByTable[tableName]
		if groups == nil {
			groups = make(map[int]*types.DBConstraint)
			groupsByTable[tableName] = groups
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
		return nil, fmt.Errorf("sqlite: iterate foreign keys: %w", err)
	}

	out := make(map[string][]types.DBConstraint, len(groupsByTable))
	for tableName, groups := range groupsByTable {
		named := extractForeignKeyNames(tableDDLByName[tableName])
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
		out[tableName] = constraints
	}
	return out, nil
}

func normalizeSQLiteAction(action string) string {
	action = strings.ToUpper(strings.TrimSpace(action))
	if action == "" {
		return "NO ACTION"
	}
	return action
}

func first(values []string) string {
	if len(values) == 0 {
		return ""
	}
	return values[0]
}

// reconcileColumnUniqueness marks a column UNIQUE when the table declares a
// single-column UNIQUE **constraint** over it.
//
// The distinction is the whole point. SQLite's `pragma index_list` reports an
// `origin` for every index: `u` for the implicit index behind a declared UNIQUE
// constraint, and `c` for one an author created with `CREATE UNIQUE INDEX`.
// Only the first is part of the column's declaration; the reader turns those
// into [types.DBConstraint] rows of type UNIQUE, which is the signal used here.
//
// Keying off `schema.Indexes` instead folded a standalone `CREATE UNIQUE INDEX`
// into the column while leaving the index itself in the schema, so rendering the
// result emitted both the inline UNIQUE and the index. Measured against the
// pinned Atlas community v1.3.0 binary, replaying
// `schema inspect --format '{{ sql . }}'` over a table whose only uniqueness on
// a column came from a named index produced four indexes where the source had
// three: the extra one was a phantom `sqlite_autoindex` the source never had.
// See stokaro/ptah#1235 finding 5.2.
func reconcileColumnUniqueness(schema *types.DBSchema) {
	uniqueColumns := make(map[tableColumnKey]struct{})
	for _, constraint := range schema.Constraints {
		if constraint.Type != "UNIQUE" {
			continue
		}
		columns := constraint.ColumnNamesOrDefault()
		if len(columns) != 1 {
			continue
		}
		uniqueColumns[tableColumnKey{table: constraint.QualifiedTableName(), column: columns[0]}] = struct{}{}
	}
	for tableIdx := range schema.Tables {
		table := &schema.Tables[tableIdx]
		for columnIdx := range table.Columns {
			column := &table.Columns[columnIdx]
			_, unique := uniqueColumns[tableColumnKey{table: table.QualifiedName(), column: column.Name}]
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
	return splitIndexColumnList(balancedParenthesized(after[open:]))
}

func splitIndexColumnList(value string) []string {
	parts := splitTopLevelComma(value)
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		if name, ok := simpleIndexColumnName(part); ok {
			out = append(out, name)
			continue
		}
		out = append(out, part)
	}
	return out
}

func simpleIndexColumnName(value string) (string, bool) {
	name, ok := leadingIdentifier(value)
	if !ok {
		return "", false
	}
	rest := strings.TrimSpace(value[len(consumedIdentifierPrefix(value)):])
	if rest == "" {
		return name, true
	}
	fields := strings.Fields(strings.ToUpper(rest))
	if len(fields) == 0 {
		return name, true
	}
	switch fields[0] {
	case "ASC", "DESC":
		return name, true
	case "COLLATE":
		return name, true
	default:
		return "", false
	}
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
	matches := triggerHeaderPattern.FindStringSubmatchIndex(ddl)
	if len(matches) == 0 {
		return trigger
	}
	trigger.Body = strings.TrimSpace(ddl[matches[1]:])
	trigger.Timing = strings.ToUpper(strings.Join(strings.Fields(ddl[matches[2]:matches[3]]), " "))
	trigger.Event = strings.ToUpper(ddl[matches[4]:matches[5]])
	trigger.Table = strings.Trim(ddl[matches[6]:matches[7]], `"`)
	if matches[8] >= 0 {
		trigger.ForEach = strings.ToUpper(ddl[matches[8]:matches[9]])
	}
	return trigger
}
