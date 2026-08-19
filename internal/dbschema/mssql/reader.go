package mssql

import (
	"database/sql"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

const schemaPredicatePlaceholder = "/* ptah:schema-predicate */"

type catalogTableKey struct {
	schema string
	table  string
}

type catalogObjectKey struct {
	table catalogTableKey
	name  string
}

// Reader reads schema information from Microsoft SQL Server databases.
type Reader struct {
	db      sqlrunner.Runner
	schema  string
	schemas []string
	scoped  bool
}

func NewSQLServerReader(db sqlrunner.Runner, schema string) *Reader {
	if schema == "" {
		schema = "dbo"
	}
	return &Reader{db: db, schema: schema, schemas: []string{schema}}
}

func (r *Reader) SetSchemas(schemas []string) {
	r.schemas = normalizeSchemas(schemas, r.schema)
	r.scoped = len(schemas) > 0
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
		fallback = "dbo"
	}
	return []string{fallback}
}

func (r *Reader) outputSchema(schemaName string) string {
	if r.scoped || !strings.EqualFold(schemaName, "dbo") {
		return schemaName
	}
	return ""
}

func (r *Reader) ReadSchema() (*types.DBSchema, error) {
	schema := &types.DBSchema{}

	tables, err := r.readTables()
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read tables: %w", err)
	}
	schema.Tables = tables

	indexes, err := r.readIndexes()
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read indexes: %w", err)
	}
	schema.Indexes = indexes

	constraints, err := r.readConstraints()
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read constraints: %w", err)
	}
	schema.Constraints = constraints

	views, err := r.readViews()
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read views: %w", err)
	}
	schema.Views = views

	sequences, err := r.readSequences()
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read sequences: %w", err)
	}
	schema.Sequences = sequences

	synonyms, err := r.readSynonyms()
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read synonyms: %w", err)
	}
	schema.Synonyms = synonyms

	triggers, err := r.readTriggers()
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read triggers: %w", err)
	}
	schema.Triggers = triggers

	reconcileColumnFlags(schema)
	return schema, nil
}

func (r *Reader) readTables() ([]types.DBTable, error) {
	columns, err := r.readColumnsByTable()
	if err != nil {
		return nil, err
	}

	query := `
		SELECT s.name, t.name, COALESCE(ep.value, '')
		FROM sys.tables AS t
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		LEFT JOIN sys.extended_properties AS ep
		  ON ep.major_id = t.object_id
		 AND ep.minor_id = 0
		 AND ep.name = 'MS_Description'
		WHERE t.is_ms_shipped = 0
		  AND t.name NOT IN ('schema_migrations', 'atlas_schema_revisions')
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []types.DBTable
	for rows.Next() {
		var table types.DBTable
		if err := rows.Scan(&table.Schema, &table.Name, &table.Comment); err != nil {
			return nil, err
		}
		scannedSchema := table.Schema
		table.Schema = r.outputSchema(scannedSchema)
		table.Type = "TABLE"
		table.Columns = columns[catalogTableKey{schema: scannedSchema, table: table.Name}]
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return tables, nil
}

func (r *Reader) readColumnsByTable() (map[catalogTableKey][]types.DBColumn, error) {
	query := `
		SELECT
			s.name,
			t.name,
			c.name,
			typ.name,
			c.max_length,
			c.precision,
			c.scale,
			c.is_nullable,
			COLUMNPROPERTY(c.object_id, c.name, 'IsIdentity'),
			IDENT_SEED(QUOTENAME(s.name) + '.' + QUOTENAME(t.name)),
			IDENT_INCR(QUOTENAME(s.name) + '.' + QUOTENAME(t.name)),
				COLUMNPROPERTY(c.object_id, c.name, 'ColumnId'),
				OBJECT_DEFINITION(c.default_object_id),
				cc.definition,
				cc.is_persisted,
				ep.value
		FROM sys.columns AS c
		JOIN sys.tables AS t ON t.object_id = c.object_id
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		JOIN sys.types AS typ ON typ.user_type_id = c.user_type_id
		LEFT JOIN sys.computed_columns AS cc
		  ON cc.object_id = c.object_id AND cc.column_id = c.column_id
		LEFT JOIN sys.extended_properties AS ep
		  ON ep.major_id = c.object_id
		 AND ep.minor_id = c.column_id
		 AND ep.name = 'MS_Description'
		WHERE t.is_ms_shipped = 0
		  AND t.name NOT IN ('schema_migrations', 'atlas_schema_revisions')
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name, c.column_id`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[catalogTableKey][]types.DBColumn)
	for rows.Next() {
		var (
			schemaName, tableName string
			typeName              string
			maxLength             int
			precision, scale      int
			nullable, identity    bool
			seed, increment       sql.NullFloat64
			defaultSQL            sql.NullString
			generatedExpression   sql.NullString
			generatedPersisted    sql.NullBool
			comment               sql.NullString
			column                types.DBColumn
		)
		if err := rows.Scan(
			&schemaName,
			&tableName,
			&column.Name,
			&typeName,
			&maxLength,
			&precision,
			&scale,
			&nullable,
			&identity,
			&seed,
			&increment,
			&column.OrdinalPosition,
			&defaultSQL,
			&generatedExpression,
			&generatedPersisted,
			&comment,
		); err != nil {
			return nil, err
		}
		column.DataType = strings.ToUpper(typeName)
		column.ColumnType = sqlServerColumnType(typeName, maxLength, precision, scale)
		column.IsNullable = "NO"
		if nullable {
			column.IsNullable = "YES"
		}
		column.IsAutoIncrement = identity
		if defaultSQL.Valid {
			normalized := normalizeDefault(defaultSQL.String)
			column.ColumnDefault = &normalized
		}
		if generatedExpression.Valid && generatedExpression.String != "" {
			expr := generatedExpression.String
			column.GeneratedExpression = &expr
			if generatedPersisted.Valid && generatedPersisted.Bool {
				column.GeneratedKind = "PERSISTED"
			}
		}
		if comment.Valid {
			_ = comment.String
		}
		if maxLength > 0 && supportsCharacterLength(typeName) {
			length := maxLength
			if isUnicodeType(typeName) {
				length /= 2
			}
			column.CharacterMaxLength = &length
		}
		if precision > 0 && supportsPrecision(typeName) {
			p := precision
			s := scale
			column.NumericPrecision = &p
			column.NumericScale = &s
		}
		key := catalogTableKey{schema: schemaName, table: tableName}
		columns[key] = append(columns[key], column)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return columns, nil
}

func (r *Reader) readIndexes() ([]types.DBIndex, error) {
	query := `
		SELECT s.name, t.name, i.name, i.is_unique, i.is_primary_key, c.name, ic.key_ordinal, ic.is_descending_key, COALESCE(i.filter_definition, '')
		FROM sys.indexes AS i
		JOIN sys.tables AS t ON t.object_id = i.object_id
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		JOIN sys.index_columns AS ic ON ic.object_id = i.object_id AND ic.index_id = i.index_id
		JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE t.is_ms_shipped = 0
		  AND i.is_hypothetical = 0
		  AND i.name IS NOT NULL
		  AND i.is_primary_key = 0
		  AND i.is_unique_constraint = 0
		  AND ic.is_included_column = 0
		  AND ic.key_ordinal > 0
		  AND t.name NOT IN ('schema_migrations', 'atlas_schema_revisions')
		  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name, i.name, ic.key_ordinal`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexByKey := make(map[catalogObjectKey]*types.DBIndex)
	var order []catalogObjectKey
	for rows.Next() {
		var (
			schemaName, tableName, indexName, columnName, filter string
			unique, primary, desc                                bool
			ordinal                                              int
		)
		if err := rows.Scan(&schemaName, &tableName, &indexName, &unique, &primary, &columnName, &ordinal, &desc, &filter); err != nil {
			return nil, err
		}
		key := catalogObjectKey{
			table: catalogTableKey{schema: schemaName, table: tableName},
			name:  indexName,
		}
		index := indexByKey[key]
		if index == nil {
			index = &types.DBIndex{
				Name:      indexName,
				TableName: tableName,
				Schema:    r.outputSchema(schemaName),
				IsUnique:  unique,
				IsPrimary: primary,
				Condition: filter,
			}
			indexByKey[key] = index
			order = append(order, key)
		}
		index.Columns = append(index.Columns, columnName)
		index.Parts = append(index.Parts, types.DBIndexPart{
			Name: columnName,
			Desc: desc,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	indexes := make([]types.DBIndex, 0, len(order))
	for _, key := range order {
		indexes = append(indexes, *indexByKey[key])
	}
	return indexes, nil
}

func (r *Reader) readConstraints() ([]types.DBConstraint, error) {
	constraints, err := r.readKeyConstraints()
	if err != nil {
		return nil, err
	}
	fks, err := r.readForeignKeys()
	if err != nil {
		return nil, err
	}
	checks, err := r.readChecks()
	if err != nil {
		return nil, err
	}
	constraints = append(constraints, fks...)
	constraints = append(constraints, checks...)
	return constraints, nil
}

func (r *Reader) readKeyConstraints() ([]types.DBConstraint, error) {
	query := `
		SELECT s.name, t.name, kc.name, kc.type_desc, c.name, ic.key_ordinal
		FROM sys.key_constraints AS kc
		JOIN sys.tables AS t ON t.object_id = kc.parent_object_id
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		JOIN sys.index_columns AS ic ON ic.object_id = kc.parent_object_id AND ic.index_id = kc.unique_index_id
		JOIN sys.columns AS c ON c.object_id = ic.object_id AND c.column_id = ic.column_id
		WHERE t.is_ms_shipped = 0
		  AND t.name NOT IN ('schema_migrations', 'atlas_schema_revisions')
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name, kc.name, ic.key_ordinal`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byKey := make(map[catalogObjectKey]*types.DBConstraint)
	var order []catalogObjectKey
	for rows.Next() {
		var schemaName, tableName, name, typeDesc, column string
		var ordinal int
		if err := rows.Scan(&schemaName, &tableName, &name, &typeDesc, &column, &ordinal); err != nil {
			return nil, err
		}
		key := catalogObjectKey{
			table: catalogTableKey{schema: schemaName, table: tableName},
			name:  name,
		}
		constraint := byKey[key]
		if constraint == nil {
			constraintType := "UNIQUE"
			if strings.EqualFold(typeDesc, "PRIMARY_KEY_CONSTRAINT") {
				constraintType = "PRIMARY KEY"
			}
			constraint = &types.DBConstraint{Name: name, TableName: tableName, Schema: r.outputSchema(schemaName), Type: constraintType}
			byKey[key] = constraint
			order = append(order, key)
		}
		constraint.ColumnNames = append(constraint.ColumnNames, column)
		if constraint.ColumnName == "" {
			constraint.ColumnName = column
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	constraints := make([]types.DBConstraint, 0, len(order))
	for _, key := range order {
		constraints = append(constraints, *byKey[key])
	}
	return constraints, nil
}

func (r *Reader) readForeignKeys() ([]types.DBConstraint, error) {
	query := `
		SELECT
			s.name, t.name, fk.name, c.name,
			rs.name, rt.name, rc.name,
			fk.delete_referential_action_desc,
			fk.update_referential_action_desc,
			fkc.constraint_column_id
		FROM sys.foreign_keys AS fk
		JOIN sys.tables AS t ON t.object_id = fk.parent_object_id
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		JOIN sys.foreign_key_columns AS fkc ON fkc.constraint_object_id = fk.object_id
		JOIN sys.columns AS c ON c.object_id = fkc.parent_object_id AND c.column_id = fkc.parent_column_id
		JOIN sys.tables AS rt ON rt.object_id = fkc.referenced_object_id
		JOIN sys.schemas AS rs ON rs.schema_id = rt.schema_id
		JOIN sys.columns AS rc ON rc.object_id = fkc.referenced_object_id AND rc.column_id = fkc.referenced_column_id
		WHERE t.is_ms_shipped = 0
		  AND t.name NOT IN ('schema_migrations', 'atlas_schema_revisions')
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name, fk.name, fkc.constraint_column_id`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byKey := make(map[catalogObjectKey]*types.DBConstraint)
	var order []catalogObjectKey
	for rows.Next() {
		var (
			schemaName, tableName, name, column string
			refSchema, refTable, refColumn      string
			deleteRule, updateRule              string
			ordinal                             int
		)
		if err := rows.Scan(&schemaName, &tableName, &name, &column, &refSchema, &refTable, &refColumn, &deleteRule, &updateRule, &ordinal); err != nil {
			return nil, err
		}
		key := catalogObjectKey{
			table: catalogTableKey{schema: schemaName, table: tableName},
			name:  name,
		}
		constraint := byKey[key]
		if constraint == nil {
			constraint = &types.DBConstraint{
				Name:          name,
				TableName:     tableName,
				Schema:        r.outputSchema(schemaName),
				Type:          "FOREIGN KEY",
				ForeignTable:  &refTable,
				ForeignSchema: r.outputSchema(refSchema),
				DeleteRule:    normalizeRule(deleteRule),
				UpdateRule:    normalizeRule(updateRule),
			}
			byKey[key] = constraint
			order = append(order, key)
		}
		constraint.ColumnNames = append(constraint.ColumnNames, column)
		constraint.ForeignColumns = append(constraint.ForeignColumns, refColumn)
		if constraint.ColumnName == "" {
			constraint.ColumnName = column
		}
		if constraint.ForeignColumn == nil {
			constraint.ForeignColumn = &refColumn
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	constraints := make([]types.DBConstraint, 0, len(order))
	for _, key := range order {
		constraints = append(constraints, *byKey[key])
	}
	return constraints, nil
}

func (r *Reader) readChecks() ([]types.DBConstraint, error) {
	query := `
		SELECT s.name, t.name, cc.name, cc.definition
		FROM sys.check_constraints AS cc
		JOIN sys.tables AS t ON t.object_id = cc.parent_object_id
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE t.is_ms_shipped = 0
		  AND t.name NOT IN ('schema_migrations', 'atlas_schema_revisions')
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name, cc.name`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var constraints []types.DBConstraint
	for rows.Next() {
		var constraint types.DBConstraint
		if err := rows.Scan(&constraint.Schema, &constraint.TableName, &constraint.Name, &constraint.CheckClause); err != nil {
			return nil, err
		}
		constraint.Schema = r.outputSchema(constraint.Schema)
		constraint.Type = "CHECK"
		constraints = append(constraints, constraint)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return constraints, nil
}

func (r *Reader) readViews() ([]types.DBView, error) {
	query := `
		SELECT s.name, v.name, OBJECT_DEFINITION(v.object_id)
		FROM sys.views AS v
		JOIN sys.schemas AS s ON s.schema_id = v.schema_id
		WHERE v.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, v.name`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []types.DBView
	for rows.Next() {
		var view types.DBView
		if err := rows.Scan(&view.Schema, &view.Name, &view.Body); err != nil {
			return nil, err
		}
		view.Schema = r.outputSchema(view.Schema)
		view.CheckOption = "NONE"
		views = append(views, view)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return views, nil
}

// readSequences reads the standalone sequences the connected database declares.
//
// The whole difficulty is that sys.sequences reports every option filled in,
// including the ones the author never wrote. `CREATE SEQUENCE s` with no
// clauses at all comes back with start_value and minimum_value both
// -9223372036854775808 and maximum_value 9223372036854775807 -- the bigint
// bounds -- because the engine resolved the defaults at creation time and there
// is no column recording which of them the statement actually named. Measured
// on SQL Server 2025 (RTM-CU8), 17.0.4075.5.
//
// That is safe here for one reason, and it is worth stating because it is the
// property that keeps an apply loop from re-planning the same sequence forever:
// the comparator only compares options the DECLARATION sets, and treats a nil
// one as unmanaged. So a fully populated read against a declaration that named
// nothing produces no change. A declaration that names an option is asking to
// manage it, and then the concrete catalog value is exactly what it has to be
// compared against.
//
// is_cached and cache_size are two facts, not one. is_cached = 1 with a NULL
// cache_size is the server choosing the size, which no declaration can ask for
// by number, so it reads as unset. is_cached = 0 is NO CACHE, which
// goschema.Sequence has no way to spell either; it also reads as unset, and the
// renderer's own NO CACHE stays reachable through a declared cache of zero.
func (r *Reader) readSequences() ([]types.DBSequence, error) {
	query := `
		SELECT s.name, sq.name, t.name,
			   CAST(sq.start_value AS bigint), CAST(sq.increment AS bigint),
			   CAST(sq.minimum_value AS bigint), CAST(sq.maximum_value AS bigint),
			   sq.is_cycling, sq.is_cached, sq.cache_size
		FROM sys.sequences AS sq
		JOIN sys.schemas AS s ON s.schema_id = sq.schema_id
		JOIN sys.types AS t ON t.user_type_id = sq.user_type_id
		WHERE sq.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, sq.name`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sequences []types.DBSequence
	for rows.Next() {
		sequence, scanErr := scanSequence(rows)
		if scanErr != nil {
			return nil, scanErr
		}
		sequence.Schema = r.outputSchema(sequence.Schema)
		sequences = append(sequences, sequence)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return sequences, nil
}

// scanSequence reads one sys.sequences row into the shared shape.
func scanSequence(rows *sql.Rows) (types.DBSequence, error) {
	var (
		sequence                         types.DBSequence
		start, increment, minVal, maxVal int64
		isCycling, isCached              bool
		cacheSize                        sql.NullInt64
	)
	if err := rows.Scan(&sequence.Schema, &sequence.Name, &sequence.DataType,
		&start, &increment, &minVal, &maxVal, &isCycling, &isCached, &cacheSize); err != nil {
		return types.DBSequence{}, err
	}
	sequence.Start = &start
	sequence.Increment = &increment
	sequence.MinValue = &minVal
	sequence.MaxValue = &maxVal
	sequence.Cycle = isCycling
	sequence.Cache = sequenceCacheFacts{cached: isCached, size: cacheSize}.managedOption()
	return sequence, nil
}

// sequenceCacheFacts is what sys.sequences reports about a sequence's cache,
// which is two facts rather than one.
type sequenceCacheFacts struct {
	cached bool
	size   sql.NullInt64
}

// managedOption decides which of the two facts becomes a managed option.
//
// A cached sequence with a size is the only combination a declaration can
// express, so it is the only one that reads as set. A cached sequence with a
// NULL size is the server choosing, and an uncached one is NO CACHE; neither
// has a spelling in goschema.Sequence, and reporting a number for either would
// make every such sequence compare unequal against a declaration that named
// one.
func (f sequenceCacheFacts) managedOption() *int64 {
	if !f.cached || !f.size.Valid {
		return nil
	}
	size := f.size.Int64
	return &size
}

// readSynonyms reads the synonyms the connected database declares.
//
// base_object_name is the target exactly as the server stored it, brackets and
// all, and it is kept unchanged: it is what the server will resolve, and
// rewriting it would change which object the alias names. The parsed parts are
// derived from it so that ordering can tell a local target from one in another
// database without parsing the string again at every call site.
func (r *Reader) readSynonyms() ([]types.DBSynonym, error) {
	query := `
		SELECT s.name, sy.name, sy.base_object_name
		FROM sys.synonyms AS sy
		JOIN sys.schemas AS s ON s.schema_id = sy.schema_id
		WHERE sy.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, sy.name`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var synonyms []types.DBSynonym
	for rows.Next() {
		var synonym types.DBSynonym
		if err := rows.Scan(&synonym.Schema, &synonym.Name, &synonym.Target); err != nil {
			return nil, err
		}
		synonym.Schema = r.outputSchema(synonym.Schema)
		applySynonymTargetParts(&synonym)
		synonyms = append(synonyms, synonym)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return synonyms, nil
}

// applySynonymTargetParts fills the parsed target parts from the raw catalog
// value.
//
// SQL Server writes base_object_name right-aligned: the last part is always the
// object, and the parts before it are schema, database and server in that order
// going left. A missing middle part is written as an empty pair of brackets, so
// `[srv]..[dbo].[t]` names a server and no database, and reading positionally
// from the left would take the server for a database.
func applySynonymTargetParts(synonym *types.DBSynonym) {
	parts := strings.Split(synonym.Target, ".")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		part = strings.TrimPrefix(part, "[")
		part = strings.TrimSuffix(part, "]")
		parts[i] = part
	}
	synonym.TargetObject = partFromRight(parts, 0)
	synonym.TargetSchema = partFromRight(parts, 1)
	synonym.TargetDatabase = partFromRight(parts, 2)
	synonym.TargetServer = partFromRight(parts, 3)
}

// partFromRight returns the nth part counting from the last one, or the empty
// string when the name has fewer parts than that.
func partFromRight(parts []string, n int) string {
	index := len(parts) - 1 - n
	if index < 0 {
		return ""
	}
	return parts[index]
}

func (r *Reader) readTriggers() ([]types.DBTrigger, error) {
	query := `
		SELECT s.name, tr.name, t.name, OBJECT_DEFINITION(tr.object_id)
		FROM sys.triggers AS tr
		JOIN sys.tables AS t ON t.object_id = tr.parent_id
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE tr.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name, tr.name`
	rows, err := r.db.Query(r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var triggers []types.DBTrigger
	for rows.Next() {
		var trigger types.DBTrigger
		if err := rows.Scan(&trigger.Schema, &trigger.Name, &trigger.Table, &trigger.Body); err != nil {
			return nil, err
		}
		trigger.Schema = r.outputSchema(trigger.Schema)
		trigger.Timing = "AFTER"
		trigger.Event = ""
		trigger.ForEach = "STATEMENT"
		triggers = append(triggers, trigger)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return triggers, nil
}

func (r *Reader) schemaPredicate(column string) string {
	if len(r.schemas) == 0 {
		return "1 = 1"
	}
	parts := make([]string, len(r.schemas))
	for i := range r.schemas {
		parts[i] = column + fmt.Sprintf(" = @p%d", i+1)
	}
	return strings.Join(parts, " OR ")
}

func (r *Reader) queryWithSchemaPredicate(query string) string {
	return strings.ReplaceAll(query, schemaPredicatePlaceholder, r.schemaPredicate("s.name"))
}

func (r *Reader) schemaArgs() []any {
	args := make([]any, len(r.schemas))
	for i, schema := range r.schemas {
		args[i] = schema
	}
	return args
}

func sqlServerColumnType(typeName string, maxLength, precision, scale int) string {
	upper := strings.ToUpper(typeName)
	switch {
	case supportsCharacterLength(typeName):
		if maxLength == -1 {
			return upper + "(MAX)"
		}
		length := maxLength
		if isUnicodeType(typeName) {
			length /= 2
		}
		return fmt.Sprintf("%s(%d)", upper, length)
	case supportsPrecision(typeName):
		return fmt.Sprintf("%s(%d,%d)", upper, precision, scale)
	default:
		return upper
	}
}

func supportsCharacterLength(typeName string) bool {
	switch strings.ToLower(typeName) {
	case "char", "varchar", "nchar", "nvarchar", "binary", "varbinary":
		return true
	default:
		return false
	}
}

func isUnicodeType(typeName string) bool {
	switch strings.ToLower(typeName) {
	case "nchar", "nvarchar":
		return true
	default:
		return false
	}
}

func supportsPrecision(typeName string) bool {
	switch strings.ToLower(typeName) {
	case "decimal", "numeric":
		return true
	default:
		return false
	}
}

func normalizeDefault(defaultSQL string) string {
	defaultSQL = strings.TrimSpace(defaultSQL)
	for hasSQLServerOuterParentheses(defaultSQL) {
		defaultSQL = strings.TrimSpace(defaultSQL[1 : len(defaultSQL)-1])
	}
	if len(defaultSQL) >= 3 && (defaultSQL[0] == 'N' || defaultSQL[0] == 'n') && defaultSQL[1] == '\'' && defaultSQL[len(defaultSQL)-1] == '\'' {
		defaultSQL = defaultSQL[1:]
	}
	return defaultSQL
}

func hasSQLServerOuterParentheses(value string) bool {
	if len(value) < 2 || value[0] != '(' || value[len(value)-1] != ')' {
		return false
	}

	depth := 0
	for i := 0; i < len(value); i++ {
		switch value[i] {
		case '\'':
			next, ok := skipSQLServerQuotedString(value, i)
			if !ok {
				return false
			}
			i = next
		case '(':
			depth++
		case ')':
			depth--
			if depth < 0 {
				return false
			}
			if depth == 0 && i != len(value)-1 {
				return false
			}
		}
	}
	return depth == 0
}

func skipSQLServerQuotedString(value string, start int) (int, bool) {
	for i := start + 1; i < len(value); i++ {
		if value[i] != '\'' {
			continue
		}
		if i+1 < len(value) && value[i+1] == '\'' {
			i++
			continue
		}
		return i, true
	}
	return 0, false
}

func normalizeRule(rule string) *string {
	normalized := strings.ReplaceAll(strings.ToUpper(rule), "_", " ")
	if normalized == "NO ACTION" {
		return nil
	}
	return &normalized
}

func reconcileColumnFlags(schema *types.DBSchema) {
	primary := map[catalogTableKey]map[string]struct{}{}
	unique := map[catalogTableKey]map[string]struct{}{}
	for _, constraint := range schema.Constraints {
		key := catalogTableKey{schema: constraint.Schema, table: constraint.TableName}
		switch constraint.Type {
		case "PRIMARY KEY":
			addColumns(primary, key, constraint.ColumnNamesOrDefault())
		case "UNIQUE":
			addColumns(unique, key, constraint.ColumnNamesOrDefault())
		}
	}
	for ti := range schema.Tables {
		key := catalogTableKey{schema: schema.Tables[ti].Schema, table: schema.Tables[ti].Name}
		for ci := range schema.Tables[ti].Columns {
			column := &schema.Tables[ti].Columns[ci]
			_, column.IsPrimaryKey = primary[key][column.Name]
			_, column.IsUnique = unique[key][column.Name]
		}
	}
}

func addColumns(set map[catalogTableKey]map[string]struct{}, table catalogTableKey, columns []string) {
	if set[table] == nil {
		set[table] = make(map[string]struct{}, len(columns))
	}
	for _, column := range columns {
		set[table][column] = struct{}{}
	}
}
