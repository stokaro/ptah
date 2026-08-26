package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/catalog"
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

// ReadSchema is [Reader.ReadSchemaContext] under context.Background(), the
// context-free half of the pair [catalog.SchemaReader] declares. Prefer the
// Context form: only it can be told to stop.
func (r *Reader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}

// ReadSchemaContext reads user tables, indexes, constraints, views, and
// triggers.
func (r *Reader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	schemaCatalog, err := r.readSchemaCatalog(ctx)
	if err != nil {
		return nil, err
	}

	// Virtual tables and their shadow tables are excluded from every per-table
	// PRAGMA. A virtual table has no column list of its own to read, and
	// asking for one is not merely useless: pragma_table_xinfo has to load the
	// module to answer, so a single virtual table whose module this build does
	// not register fails the whole batch with `no such module: <name>` and
	// takes the rest of the schema down with it.
	skipped := schemaCatalog.nonOrdinaryTableNames()

	columnsByTable, err := r.readColumnsByTable(ctx, skipped)
	if err != nil {
		return nil, err
	}

	indexesByTable, uniqueConstraintsByTable, err := r.readIndexesByTable(ctx,
		schemaCatalog.indexDDLByName, schemaCatalog.tableDDLByName, skipped,
	)
	if err != nil {
		return nil, err
	}

	foreignKeysByTable, err := r.readForeignKeysByTable(ctx, schemaCatalog.tableDDLByName, skipped)
	if err != nil {
		return nil, err
	}

	var schema catalog.Database
	for _, tableName := range schemaCatalog.tableNames {
		if spec, ok := schemaCatalog.virtualTables[tableName]; ok {
			schema.Tables = append(schema.Tables, r.readVirtualTable(tableName, spec))
			continue
		}

		ddl := schemaCatalog.tableDDLByName[tableName]
		table := r.readTable(tableName, columnsByTable[tableName], ddl)
		schema.Tables = append(schema.Tables, table)

		schema.Indexes = append(schema.Indexes, indexesByTable[tableName]...)
		schema.Constraints = append(schema.Constraints, uniqueConstraintsByTable[tableName]...)

		constraints := r.readTableConstraints(tableName, table.Columns, ddl, foreignKeysByTable[tableName])
		schema.Constraints = append(schema.Constraints, constraints...)
	}

	schema.Views = schemaCatalog.views(r.outputSchema())
	schema.Triggers = schemaCatalog.triggers(r.outputSchema())
	reconcileColumnUniqueness(&schema)

	// Recorded last, on the schema rather than on any table, because it is a
	// statement about what this read could not classify. Where the module is
	// missing, the tables above include that module's private storage described
	// as ordinary tables, and nothing on those rows can say so -- SQLite did not
	// mark them, which is the whole condition. See stokaro/ptah#1028.
	unregistered, err := r.readUnregisteredVirtualTables(schemaCatalog.virtualTables)
	if err != nil {
		return nil, err
	}
	schema.UnregisteredVirtualTables = unregistered

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

func (c sqliteSchemaCatalog) views(schema string) []catalog.View {
	views := make([]catalog.View, 0, len(c.viewObjects))
	for _, object := range c.viewObjects {
		views = append(views, catalog.View{
			Name:        object.name,
			Schema:      schema,
			Body:        viewBody(object.ddl),
			CheckOption: "NONE",
		})
	}
	return views
}

func (c sqliteSchemaCatalog) triggers(schema string) []catalog.Trigger {
	triggers := make([]catalog.Trigger, 0, len(c.triggerObjects))
	for _, object := range c.triggerObjects {
		trigger := parseTriggerDDL(object.name, object.tableName, schema, object.ddl)
		triggers = append(triggers, trigger)
	}
	return triggers
}

func (r *Reader) readSchemaCatalog(ctx context.Context) (sqliteSchemaCatalog, error) {
	kinds, err := r.readTableKinds(ctx)
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
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return sqliteSchemaCatalog{}, fmt.Errorf("sqlite: read schema catalog: %w", err)
	}
	defer rows.Close()

	schemaCatalog := sqliteSchemaCatalog{
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
				schemaCatalog.shadowTableNames = append(schemaCatalog.shadowTableNames, name)
				continue
			case virtual:
				schemaCatalog.virtualTables[name] = spec
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
			schemaCatalog.tableNames = append(schemaCatalog.tableNames, name)
			schemaCatalog.tableDDLByName[name] = ddl.String
		case "index":
			if kinds[tableName] == tableKindShadow && ddl.Valid {
				return sqliteSchemaCatalog{}, fmt.Errorf(
					"sqlite: index %q targets virtual-table shadow table %q; "+
						"Ptah omits module-owned shadow tables and cannot replay the index without misrepresenting its owner",
					name,
					tableName,
				)
			}
			schemaCatalog.indexDDLByName[name] = ddl.String
		case "view":
			schemaCatalog.viewObjects = append(schemaCatalog.viewObjects, sqliteSchemaObject{
				name:      name,
				tableName: tableName,
				ddl:       ddl.String,
			})
		case "trigger":
			schemaCatalog.triggerObjects = append(schemaCatalog.triggerObjects, sqliteSchemaObject{
				name:      name,
				tableName: tableName,
				ddl:       ddl.String,
			})
		}
	}
	if err := rows.Err(); err != nil {
		return sqliteSchemaCatalog{}, fmt.Errorf("sqlite: iterate schema catalog: %w", err)
	}
	sort.Strings(schemaCatalog.tableNames)
	sort.Slice(schemaCatalog.viewObjects, func(i, j int) bool {
		return schemaCatalog.viewObjects[i].name < schemaCatalog.viewObjects[j].name
	})
	sort.Slice(schemaCatalog.triggerObjects, func(i, j int) bool {
		if schemaCatalog.triggerObjects[i].tableName != schemaCatalog.triggerObjects[j].tableName {
			return schemaCatalog.triggerObjects[i].tableName < schemaCatalog.triggerObjects[j].tableName
		}
		return schemaCatalog.triggerObjects[i].name < schemaCatalog.triggerObjects[j].name
	})
	return schemaCatalog, nil
}

func (r *Reader) readTable(name string, columns []catalog.Column, ddl string) catalog.Table {
	strict, withoutRowID := sqliteTableOptions(ddl)
	return catalog.Table{
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
func (r *Reader) readVirtualTable(name string, spec virtualTableSpec) catalog.Table {
	return catalog.Table{
		Name:             name,
		Schema:           r.outputSchema(),
		Type:             "TABLE",
		VirtualModule:    spec.Module,
		VirtualArguments: spec.Arguments,
	}
}

func sqliteTableOptions(ddl string) (strict, withoutRowID bool) {
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

func (r *Reader) readColumnsByTable(ctx context.Context, skipped []string) (map[string][]catalog.Column, error) {
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
	rows, err := r.db.QueryContext(ctx, query, append([]any{r.schema}, exclusionArguments...)...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read columns: %w", err)
	}
	defer rows.Close()

	type tableDDLMetadata struct {
		autoIncrementColumn  string
		generatedExpressions map[string]string
	}
	ddlMetadataByTable := make(map[string]tableDDLMetadata)
	columnsByTable := make(map[string][]catalog.Column)
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
		column := catalog.Column{
			Name:                name,
			DataType:            normalizeSQLiteType(dataType),
			TypeIsDeclaredText:  true,
			ColumnType:          dataType,
			IsNullable:          sqliteNullable(notNull),
			OrdinalPosition:     cid + 1,
			IsPrimaryKey:        pkOrdinal > 0,
			IsAutoIncrement:     strings.EqualFold(name, ddlMetadata.autoIncrementColumn),
			GeneratedKind:       sqliteGeneratedKind(hidden),
			GeneratedExpression: nil,
		}
		// PRAGMA table_xinfo is the authority on WHETHER a column is generated;
		// the DDL only supplies the text. Asking the DDL both questions is what
		// a default holding the characters `as (` would answer wrongly, and the
		// pragma cannot be fooled that way.
		if column.GeneratedKind != "" {
			if expression := ddlMetadata.generatedExpressions[name]; expression != "" {
				column.GeneratedExpression = &expression
			}
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

func (r *Reader) readIndexesByTable(ctx context.Context,
	indexDDLByName,
	tableDDLByName map[string]string,
	skipped []string,
) (
	map[string][]catalog.Index,
	map[string][]catalog.Constraint,
	error,
) {
	entriesByTable, err := r.readIndexEntriesByTable(ctx, skipped)
	if err != nil {
		return nil, nil, err
	}

	columnsByIndex, err := r.readIndexColumnsByIndex(ctx, skipped)
	if err != nil {
		return nil, nil, err
	}

	indexesByTable := make(map[string][]catalog.Index, len(entriesByTable))
	constraintsByTable := make(map[string][]catalog.Constraint, len(entriesByTable))
	for tableName, entries := range entriesByTable {
		indexes, constraints := r.buildIndexesForTable(tableName, entries, indexDDLByName, tableDDLByName, columnsByIndex)
		indexesByTable[tableName] = indexes
		constraintsByTable[tableName] = constraints
	}
	return indexesByTable, constraintsByTable, nil
}

func (r *Reader) readIndexEntriesByTable(ctx context.Context, skipped []string) (map[string][]sqliteIndexEntry, error) {
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
	rows, err := r.db.QueryContext(ctx, query, append([]any{r.schema}, exclusionArguments...)...)
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
	indexDDLByName,
	tableDDLByName map[string]string,
	columnsByIndex map[string]sqliteIndexColumns,
) ([]catalog.Index, []catalog.Constraint) {
	ddl := tableDDLByName[tableName]
	var indexes []catalog.Index
	var constraints []catalog.Constraint
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
		index := catalog.Index{
			Name:       entry.name,
			TableName:  tableName,
			Schema:     r.outputSchema(),
			Columns:    columns,
			Parts:      sqliteIndexParts(indexColumns.keys, columns),
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
			constraints = append(constraints, catalog.Constraint{
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
	// keys is every key of the index in key order, including the ones that are
	// expressions rather than columns. names holds the columns alone, which is
	// what the legacy columns-only representation wants; keys is what says
	// which position was which.
	keys []sqliteIndexKey
}

// sqliteIndexKey is one key of an index, and whether the catalog named a column
// for it.
//
// PRAGMA index_xinfo answers with a NULL name and a negative cid for a key that
// is an expression, which is the only place the distinction is recorded: the
// index's DDL carries the text of every key and says nothing about which kind
// each one is.
type sqliteIndexKey struct {
	name       string
	expression bool
	// desc is the key's own direction, which lives nowhere else: sqlite_schema
	// carries the CREATE INDEX text and index_list says nothing about ordering,
	// so a key read without this column is indistinguishable from an ascending
	// one (stokaro/ptah#2197).
	desc bool
}

func (r *Reader) readIndexColumnsByIndex(ctx context.Context, skipped []string) (map[string]sqliteIndexColumns, error) {
	exclusion, exclusionArguments := excludeTablesFilter(skipped)
	query := fmt.Sprintf(`
		SELECT il.name, ix.seqno, ix.cid, ix.name, ix.desc, ix.key
		FROM %s AS m
		JOIN pragma_index_list(m.name, ?) AS il
		JOIN pragma_index_xinfo(il.name, ?) AS ix
		WHERE m.type = 'table'
		  AND m.name NOT LIKE 'sqlite\_%%' ESCAPE '\'
		  AND m.name <> 'schema_migrations'%s
		ORDER BY il.name, ix.seqno
	`, r.schemaObject("sqlite_schema"), exclusion)
	rows, err := r.db.QueryContext(ctx, query, append([]any{r.schema, r.schema}, exclusionArguments...)...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read index columns: %w", err)
	}
	defer rows.Close()

	type indexColumn struct {
		seqno      int
		name       string
		expression bool
		desc       bool
	}
	columns := make(map[string][]indexColumn)
	needsDDLParsing := make(map[string]bool)
	for rows.Next() {
		var (
			indexName  string
			seqno      int
			cid        int
			name       sql.NullString
			descending int
			keyColumn  int
		)
		if err := rows.Scan(&indexName, &seqno, &cid, &name, &descending, &keyColumn); err != nil {
			return nil, fmt.Errorf("sqlite: scan index column: %w", err)
		}
		if keyColumn == 0 {
			continue
		}
		if cid < 0 || !name.Valid || name.String == "" {
			needsDDLParsing[indexName] = true
			columns[indexName] = append(columns[indexName],
				indexColumn{seqno: seqno, expression: true, desc: descending != 0})
			continue
		}
		columns[indexName] = append(columns[indexName],
			indexColumn{seqno: seqno, name: name.String, desc: descending != 0})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("sqlite: iterate index columns: %w", err)
	}

	out := make(map[string]sqliteIndexColumns, len(columns)+len(needsDDLParsing))
	for indexName, indexColumns := range columns {
		sort.Slice(indexColumns, func(i, j int) bool { return indexColumns[i].seqno < indexColumns[j].seqno })
		names := make([]string, 0, len(indexColumns))
		keys := make([]sqliteIndexKey, len(indexColumns))
		for i, column := range indexColumns {
			keys[i] = sqliteIndexKey{name: column.name, expression: column.expression, desc: column.desc}
			if !column.expression {
				names = append(names, column.name)
			}
		}
		out[indexName] = sqliteIndexColumns{
			names:           names,
			needsDDLParsing: needsDDLParsing[indexName],
			keys:            keys,
		}
	}
	for indexName := range needsDDLParsing {
		if _, ok := out[indexName]; !ok {
			out[indexName] = sqliteIndexColumns{needsDDLParsing: true}
		}
	}
	return out, nil
}

// sqliteIndexParts pairs the key texts the DDL carries with the kinds the
// catalog reported, so an expression key is recorded as an expression rather
// than as a column nothing will find.
//
// It answers nil unless every key lines up, which leaves catalog.Index.Parts empty
// and keeps the columns-only representation rather than guessing -- the same
// rule the PostgreSQL reader follows when its attnum list does not match.
//
// Without it an expression key reached the HCL renderer as a column name, and
// the document said `columns = [column["(lower(email))"]]`: a reference to a
// column that does not exist. The pinned Atlas community binary refuses that
// document outright, and Ptah replaying its own copy of it built an index over
// the STRING `"(lower(email))"` -- silently, because SQLite reads a
// double-quoted name that matches no column as a string literal. An index over
// a constant is the same value for every row (stokaro/ptah#2088).
func sqliteIndexParts(keys []sqliteIndexKey, keyTexts []string) []catalog.IndexPart {
	if len(keys) == 0 || len(keys) != len(keyTexts) {
		return nil
	}
	parts := make([]catalog.IndexPart, len(keys))
	for position, key := range keys {
		if key.expression {
			// An expression key's text comes from the DDL rather than from the
			// catalog, and the DDL spells the direction inside it -- the text
			// for `lower(c) DESC` IS "lower(c) DESC". Setting Desc as well
			// renders `lower(c) DESC DESC`, which SQLite refuses. Measured, and
			// the reason this branch does not carry the flag its neighbour does.
			parts[position] = catalog.IndexPart{Expr: keyTexts[position]}
			continue
		}
		parts[position] = catalog.IndexPart{Name: keyTexts[position], Desc: key.desc}
	}
	return parts
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
	columns []catalog.Column,
	ddl string,
	foreignKeys []catalog.Constraint,
) []catalog.Constraint {
	var constraints []catalog.Constraint
	if primary := primaryKeyConstraint(tableName, r.outputSchema(), columns, ddl); primary != nil {
		constraints = append(constraints, *primary)
	}
	checks := extractCheckConstraints(tableName, r.outputSchema(), ddl)
	constraints = append(constraints, checks...)
	constraints = append(constraints, foreignKeys...)
	return constraints
}

func primaryKeyConstraint(tableName, schema string, columns []catalog.Column, ddl string) *catalog.Constraint {
	if name, names := extractPrimaryKeyDefinition(ddl); len(names) > 0 {
		if name == "" {
			name = tableName + "_pkey"
		}
		return &catalog.Constraint{
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
	return &catalog.Constraint{
		Name:        tableName + "_pkey",
		TableName:   tableName,
		Schema:      schema,
		Type:        "PRIMARY KEY",
		ColumnName:  first(names),
		ColumnNames: names,
	}
}

func (r *Reader) readForeignKeysByTable(ctx context.Context,
	tableDDLByName map[string]string,
	skipped []string,
) (map[string][]catalog.Constraint, error) {
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
	rows, err := r.db.QueryContext(ctx, query, append([]any{r.schema}, exclusionArguments...)...)
	if err != nil {
		return nil, fmt.Errorf("sqlite: read foreign keys: %w", err)
	}
	defer rows.Close()

	groupsByTable := make(map[string]map[int]*catalog.Constraint)
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
			groups = make(map[int]*catalog.Constraint)
			groupsByTable[tableName] = groups
		}
		constraint := groups[id]
		if constraint == nil {
			refTableCopy := refTable
			deleteRule := normalizeSQLiteAction(onDelete)
			updateRule := normalizeSQLiteAction(onUpdate)
			constraint = &catalog.Constraint{
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

	out := make(map[string][]catalog.Constraint, len(groupsByTable))
	for tableName, groups := range groupsByTable {
		details := extractForeignKeyDetails(tableDDLByName[tableName])
		ids := make([]int, 0, len(groups))
		for id := range groups {
			ids = append(ids, id)
		}
		sort.Ints(ids)
		constraints := make([]catalog.Constraint, 0, len(ids))
		for _, id := range ids {
			constraint := groups[id]
			applyForeignKeyDetail(constraint, details)
			if constraint.Name == "" {
				constraint.Name = fromschema.GenerateForeignKeyName(tableName, first(constraint.ColumnNames))
			}
			constraints = append(constraints, *constraint)
		}
		out[tableName] = constraints
	}
	return out, nil
}

// applyForeignKeyDetail copies onto one read foreign key what only its own DDL
// says: the name the author gave it, and the deferral SQLite's catalog does not
// report at all (stokaro/ptah#2202).
func applyForeignKeyDetail(
	constraint *catalog.Constraint,
	details map[foreignKeySignature]foreignKeyDetail,
) {
	if constraint.ForeignTable == nil {
		return
	}
	detail, found := details[foreignKeySignature{
		columns:        strings.Join(constraint.ColumnNames, ","),
		foreignTable:   *constraint.ForeignTable,
		foreignColumns: strings.Join(constraint.ForeignColumns, ","),
	}]
	if !found {
		return
	}
	if detail.name != "" {
		constraint.Name = detail.name
	}
	constraint.Deferrable = detail.deferrable
	constraint.Initially = detail.initially
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
// into [catalog.Constraint] rows of type UNIQUE, which is the signal used here.
//
// Keying off `schema.Indexes` instead folded a standalone `CREATE UNIQUE INDEX`
// into the column while leaving the index itself in the schema, so rendering the
// result emitted both the inline UNIQUE and the index. Measured against the
// pinned Atlas community v1.3.0 binary, replaying
// `schema inspect --format '{{ sql . }}'` over a table whose only uniqueness on
// a column came from a named index produced four indexes where the source had
// three: the extra one was a phantom `sqlite_autoindex` the source never had.
// See stokaro/ptah#1235 finding 5.2.
func reconcileColumnUniqueness(schema *catalog.Database) {
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

func extractCheckConstraints(tableName, schema, ddl string) []catalog.Constraint {
	var constraints []catalog.Constraint
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
		constraints = append(constraints, catalog.Constraint{
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
		// The GENERATED ALWAYS prefix is OPTIONAL in SQLite's grammar:
		// `scaled REAL AS (raw * 100) VIRTUAL` declares the same column as the
		// spelling that carries it, and it is what the pinned Atlas community
		// binary writes. Gating on the keyword left the short form marked
		// generated -- PRAGMA table_xinfo's hidden value says so, and the reader
		// reads it -- with no expression, and a column that is generated with no
		// expression is rendered as a plain one. Replaying such a schema built a
		// column that holds NULL where the original computed a value
		// (stokaro/ptah#2090).
		//
		// No gate replaces it: generatedExpression requires AS followed by a
		// parenthesized expression, which a plain column does not have.
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

// foreignKeyDetail is what one foreign key's own DDL says that the catalog does
// not. The name has always been here; the deferral is here for the same reason
// (stokaro/ptah#2202).
type foreignKeyDetail struct {
	name       string
	deferrable bool
	initially  string
}

// extractForeignKeyDetails reads each foreign key's name and deferral out of the
// table's DDL, keyed by the signature PRAGMA foreign_key_list reports.
//
// An UNNAMED key gets an entry too. It carries no name, and it can still carry a
// deferral -- recording only the named ones is how `REFERENCES parent(id)
// DEFERRABLE INITIALLY DEFERRED` came back immediate.
func extractForeignKeyDetails(ddl string) map[foreignKeySignature]foreignKeyDetail {
	out := make(map[foreignKeySignature]foreignKeyDetail)
	for _, part := range splitTopLevelComma(tableBody(ddl)) {
		deferrable, initially := foreignKeyDeferral(part)
		if name, signature, ok := inlineForeignKey(part); ok {
			out[signature] = foreignKeyDetail{name: name, deferrable: deferrable, initially: initially}
			continue
		}
		name, rest := optionalConstraintName(part)
		if indexKeyword(rest, "FOREIGN") < 0 {
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
		}] = foreignKeyDetail{name: name, deferrable: deferrable, initially: initially}
	}
	return out
}

// foreignKeyDeferral reads the deferral off one constraint's text.
//
// SQLite has no catalog for it. PRAGMA foreign_key_list reports the columns, the
// referenced table, the actions and the match clause, and says nothing about
// DEFERRABLE, so the CREATE TABLE text is the only place the property exists --
// which is why the reader had nothing to read and reported every key as
// immediate.
//
// `NOT DEFERRABLE INITIALLY DEFERRED` is accepted by SQLite and behaves
// immediately. It is reported as not deferrable with no timing rather than as a
// deferral that would never happen: replaying the reported form behaves the way
// the source does, which is what a description is for.
func foreignKeyDeferral(value string) (deferrable bool, initially string) {
	idx := indexKeyword(value, "DEFERRABLE")
	if idx < 0 || precededByNot(value, idx) {
		return false, ""
	}
	timing := indexKeyword(value, "INITIALLY")
	if timing < 0 {
		return true, ""
	}
	after := strings.Fields(value[timing+len("INITIALLY"):])
	if len(after) == 0 {
		return true, ""
	}
	switch strings.ToUpper(after[0]) {
	case "DEFERRED":
		return true, "DEFERRED"
	case "IMMEDIATE":
		return true, "IMMEDIATE"
	}
	return true, ""
}

// precededByNot reports whether the word immediately before idx is NOT.
//
// `NOT DEFERRABLE` cannot be found by searching for the pair: the two words are
// separated by whatever whitespace the author wrote, and a description that
// matched only a single space would read `NOT  DEFERRABLE` as deferrable.
func precededByNot(value string, idx int) bool {
	before := strings.TrimRight(value[:idx], " \t\n\r")
	fields := strings.Fields(before)
	return len(fields) > 0 && strings.EqualFold(fields[len(fields)-1], "NOT")
}

// inlineForeignKey reports a column-level REFERENCES clause: its signature, and
// its constraint name when it has one.
//
// The name is optional and the signature is not. An unnamed inline key is still
// a key whose DDL may say something the catalog does not, so this answers ok for
// it and leaves the name empty (stokaro/ptah#2202).
func inlineForeignKey(value string) (name string, signature foreignKeySignature, ok bool) {
	column, ok := leadingIdentifier(value)
	if !ok || indexKeyword(value, "FOREIGN") >= 0 {
		return "", foreignKeySignature{}, false
	}
	if indexKeyword(value, "REFERENCES") < 0 {
		return "", foreignKeySignature{}, false
	}
	foreignTable := foreignKeyTable(value)
	if foreignTable == "" {
		return "", foreignKeySignature{}, false
	}
	signature = foreignKeySignature{
		columns:        column,
		foreignTable:   foreignTable,
		foreignColumns: strings.Join(foreignKeyReferencedColumns(value), ","),
	}

	constraintIdx := indexKeyword(value, " CONSTRAINT ")
	referencesIdx := indexKeyword(value, "REFERENCES")
	if constraintIdx < 0 || constraintIdx > referencesIdx {
		return "", signature, true
	}
	afterConstraint := strings.TrimSpace(value[constraintIdx+len(" CONSTRAINT "):])
	if named, found := leadingIdentifier(afterConstraint); found {
		name = named
	}
	return name, signature, true
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

func optionalConstraintName(value string) (name, rest string) {
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

func parseTriggerDDL(name, table, schema, ddl string) catalog.Trigger {
	trigger := catalog.Trigger{
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
