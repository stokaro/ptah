package mssql

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/sqlrunner"
)

const schemaPredicatePlaceholder = "/* ptah:schema-predicate */"

// grantObjectTypeSchema is the object type whose target is a schema rather than
// an object inside one.
const grantObjectTypeSchema = "SCHEMA"

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

// ReadSchema is [Reader.ReadSchemaContext] under context.Background(), the
// context-free half of the pair [catalog.SchemaReader] declares. Prefer the
// Context form: only it can be told to stop.
func (r *Reader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}

func (r *Reader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	schema := &catalog.Database{}

	tables, err := r.readTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read tables: %w", err)
	}
	schema.Tables = tables

	indexes, err := r.readIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read indexes: %w", err)
	}
	schema.Indexes = indexes

	constraints, err := r.readConstraints(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read constraints: %w", err)
	}
	schema.Constraints = constraints

	views, err := r.readViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read views: %w", err)
	}
	schema.Views = views

	sequences, err := r.readSequences(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read sequences: %w", err)
	}
	schema.Sequences = sequences

	roles, err := r.readRoles(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read roles: %w", err)
	}
	schema.Roles = roles

	grants, err := r.readGrants(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read grants: %w", err)
	}
	schema.Grants = grants

	memberships, err := r.readRoleMemberships(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read role memberships: %w", err)
	}
	schema.RoleMemberships = memberships

	owners, err := r.readObjectOwners(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read object owners: %w", err)
	}
	schema.ObjectOwners = owners

	synonyms, err := r.readSynonyms(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read synonyms: %w", err)
	}
	schema.Synonyms = synonyms

	extendedProperties, err := r.readExtendedProperties(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read extended properties: %w", err)
	}
	schema.ExtendedProperties = extendedProperties

	triggers, err := r.readTriggers(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read triggers: %w", err)
	}
	schema.Triggers = triggers

	functions, err := r.readFunctions(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read functions: %w", err)
	}
	schema.Functions = functions

	policies, err := r.readRLSPolicies(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read rls policies: %w", err)
	}
	schema.RLSPolicies = policies

	rlsEnabled, err := r.readRLSEnabledTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read rls enabled tables: %w", err)
	}
	applyRLSEnabled(schema, rlsEnabled)

	reconcileColumnFlags(schema)
	return schema, nil
}

func (r *Reader) readTables(ctx context.Context) ([]catalog.Table, error) {
	columns, err := r.readColumnsByTable(ctx)
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
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []catalog.Table
	for rows.Next() {
		var table catalog.Table
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

func (r *Reader) readColumnsByTable(ctx context.Context) (map[catalogTableKey][]catalog.Column, error) {
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
			-- Cast to text rather than scanning the NUMERIC: a float round
			-- trip renders IDENTITY(1000.000000,5.000000), and a
			-- DECIMAL(38,0) identity does not fit an int64 at all.
			CAST(IDENT_SEED(QUOTENAME(s.name) + '.' + QUOTENAME(t.name)) AS VARCHAR(41)),
			CAST(IDENT_INCR(QUOTENAME(s.name) + '.' + QUOTENAME(t.name)) AS VARCHAR(41)),
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
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[catalogTableKey][]catalog.Column)
	for rows.Next() {
		var (
			schemaName, tableName string
			typeName              string
			maxLength             int
			precision, scale      int
			nullable, identity    bool
			seed, increment       sql.NullString
			defaultSQL            sql.NullString
			generatedExpression   sql.NullString
			generatedPersisted    sql.NullBool
			comment               sql.NullString
			column                catalog.Column
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
		// The catalog answers with SQL Server's own type name, and the portable
		// mapping uses some of those names for something else: a declared
		// VARCHAR becomes NVARCHAR, which is two bytes per character rather
		// than one. Marking it is what stops a description of this database
		// from replaying as a Unicode column (stokaro/ptah#2147).
		column.TypeIsDeclaredText = true
		column.ColumnType = sqlServerColumnType(typeName, maxLength, precision, scale)
		column.IsNullable = "NO"
		if nullable {
			column.IsNullable = "YES"
		}
		column.IsAutoIncrement = identity
		// IDENT_SEED and IDENT_INCR answer for the TABLE, so the row carries
		// the same pair for every column of it. Only the identity column may
		// keep them, or a description would claim a seed for columns that have
		// none. SQL Server allows at most one identity column per table, which
		// is why the table-scoped functions are the right question to ask
		// (stokaro/ptah#2196).
		if identity {
			column.IdentityStart = strings.TrimSpace(seed.String)
			column.IdentityIncrement = strings.TrimSpace(increment.String)
		}
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
		// The query has always asked for MS_Description; there was nowhere to
		// put it until catalog.Column gained a Comment, so the value was read
		// and discarded. Assigning it is what lets the comparison see a
		// comment SQL Server already holds -- without it every column comment
		// reads as absent, and the planner calls sp_addextendedproperty on a
		// property that exists, which answers `Property cannot be added.
		// Property already exists` (stokaro/ptah#2168).
		column.Comment = comment.String
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

func (r *Reader) readIndexes(ctx context.Context) ([]catalog.Index, error) {
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
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	indexByKey := make(map[catalogObjectKey]*catalog.Index)
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
			index = &catalog.Index{
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
		index.Parts = append(index.Parts, catalog.IndexPart{
			Name: columnName,
			Desc: desc,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	indexes := make([]catalog.Index, 0, len(order))
	for _, key := range order {
		indexes = append(indexes, *indexByKey[key])
	}
	return indexes, nil
}

func (r *Reader) readConstraints(ctx context.Context) ([]catalog.Constraint, error) {
	constraints, err := r.readKeyConstraints(ctx)
	if err != nil {
		return nil, err
	}
	fks, err := r.readForeignKeys(ctx)
	if err != nil {
		return nil, err
	}
	checks, err := r.readChecks(ctx)
	if err != nil {
		return nil, err
	}
	constraints = append(constraints, fks...)
	constraints = append(constraints, checks...)
	return constraints, nil
}

func (r *Reader) readKeyConstraints(ctx context.Context) ([]catalog.Constraint, error) {
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
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byKey := make(map[catalogObjectKey]*catalog.Constraint)
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
			constraint = &catalog.Constraint{Name: name, TableName: tableName, Schema: r.outputSchema(schemaName), Type: constraintType}
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

	constraints := make([]catalog.Constraint, 0, len(order))
	for _, key := range order {
		constraints = append(constraints, *byKey[key])
	}
	return constraints, nil
}

func (r *Reader) readForeignKeys(ctx context.Context) ([]catalog.Constraint, error) {
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
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	byKey := make(map[catalogObjectKey]*catalog.Constraint)
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
			constraint = &catalog.Constraint{
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

	constraints := make([]catalog.Constraint, 0, len(order))
	for _, key := range order {
		constraints = append(constraints, *byKey[key])
	}
	return constraints, nil
}

func (r *Reader) readChecks(ctx context.Context) ([]catalog.Constraint, error) {
	query := `
		SELECT s.name, t.name, cc.name, cc.definition
		FROM sys.check_constraints AS cc
		JOIN sys.tables AS t ON t.object_id = cc.parent_object_id
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE t.is_ms_shipped = 0
		  AND t.name NOT IN ('schema_migrations', 'atlas_schema_revisions')
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name, cc.name`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var constraints []catalog.Constraint
	for rows.Next() {
		var constraint catalog.Constraint
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

func (r *Reader) readViews(ctx context.Context) ([]catalog.View, error) {
	query := `
		SELECT s.name, v.name, OBJECT_DEFINITION(v.object_id)
		FROM sys.views AS v
		JOIN sys.schemas AS s ON s.schema_id = v.schema_id
		WHERE v.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, v.name`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []catalog.View
	for rows.Next() {
		var view catalog.View
		if err := rows.Scan(&view.Schema, &view.Name, &view.Body); err != nil {
			return nil, err
		}
		// OBJECT_DEFINITION hands back the whole CREATE statement. The body is
		// what belongs in Body, and the header's WITH clause is what belongs in
		// Attributes -- read before Body is overwritten, because both come from
		// the same text. See [viewBody] and [viewAttributes].
		view.Attributes = viewAttributes(view.Body)
		view.Body = viewBody(view.Body)
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
// schemamodel.Sequence has no way to spell either; it also reads as unset, and the
// renderer's own NO CACHE stays reachable through a declared cache of zero.
func (r *Reader) readSequences(ctx context.Context) ([]catalog.Sequence, error) {
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
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sequences []catalog.Sequence
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
func scanSequence(rows *sql.Rows) (catalog.Sequence, error) {
	var (
		sequence                         catalog.Sequence
		start, increment, minVal, maxVal int64
		isCycling, isCached              bool
		cacheSize                        sql.NullInt64
	)
	if err := rows.Scan(&sequence.Schema, &sequence.Name, &sequence.DataType,
		&start, &increment, &minVal, &maxVal, &isCycling, &isCached, &cacheSize); err != nil {
		return catalog.Sequence{}, err
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
// has a spelling in schemamodel.Sequence, and reporting a number for either would
// make every such sequence compare unequal against a declaration that named
// one.
func (f sequenceCacheFacts) managedOption() *int64 {
	if !f.cached || !f.size.Valid {
		return nil
	}
	size := f.size.Int64
	return &size
}

// readRoles reads the database roles this database declares.
//
// Three exclusions, and each is a row that would otherwise be planned as
// something to drop.
//
// A SQL Server role is DATABASE-scoped, unlike a PostgreSQL role, so this reads
// one database's principals rather than a cluster's. is_fixed_role screens out
// db_owner and its siblings, which ship with every database. And `public` has
// to go on top of that: it reports is_fixed_role = 0, exists in every database,
// and cannot be dropped -- a plan naming it would emit a DROP ROLE the engine
// refuses.
//
// Attributes are deliberately absent. A database role has none of PostgreSQL's
// -- no login, no password, no superuser -- so reporting false for each is not
// a loss of information; it is the only truth available, and the renderer says
// so when a declaration asks for one.
func (r *Reader) readRoles(ctx context.Context) ([]catalog.Role, error) {
	query := `
		SELECT p.name
		FROM sys.database_principals AS p
		WHERE p.type = 'R' AND p.is_fixed_role = 0 AND p.name <> 'public'
		ORDER BY p.name`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var roles []catalog.Role
	for rows.Next() {
		var role catalog.Role
		if err := rows.Scan(&role.Name); err != nil {
			return nil, err
		}
		// Inherit is the one attribute whose T-SQL answer is not false: a
		// database role's members always receive its permissions.
		role.Inherit = true
		role.PasswordState = catalog.RolePasswordAbsent
		roles = append(roles, role)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return roles, nil
}

// readRoleMemberships reads who holds which database role.
//
// The same three exclusions readRoles applies are applied to the ROLE side:
// db_owner and its siblings ship with every database, and `public` holds every
// principal by definition, so both would report memberships nobody wrote. The
// MEMBER side is not filtered that way -- a fixed role can be a member of a
// user-defined one, and that edge is exactly the kind an analysis wants to see.
//
// SQL Server records no admin option: `ALTER ROLE ... ADD MEMBER` grants
// membership and nothing else, so AdminOption is false here rather than
// unknown (stokaro/ptah#1950).
func (r *Reader) readRoleMemberships(ctx context.Context) ([]catalog.RoleMembership, error) {
	query := `
		SELECT role_principal.name AS role_name, member_principal.name AS member_name
		FROM sys.database_role_members AS rm
		JOIN sys.database_principals AS role_principal
			ON role_principal.principal_id = rm.role_principal_id
		JOIN sys.database_principals AS member_principal
			ON member_principal.principal_id = rm.member_principal_id
		WHERE role_principal.is_fixed_role = 0 AND role_principal.name <> 'public'
		ORDER BY role_principal.name, member_principal.name`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	memberships := make([]catalog.RoleMembership, 0)
	for rows.Next() {
		var membership catalog.RoleMembership
		if err := rows.Scan(&membership.Role, &membership.Member); err != nil {
			return nil, err
		}
		memberships = append(memberships, membership)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return memberships, nil
}

// mssqlOwnerKinds maps sys.objects.type onto the vocabulary the rest of Ptah
// uses. A type with no entry is not reported: a constraint and a default have
// an owner in the catalog and are not objects an operator reasons about owning.
var mssqlOwnerKinds = map[string]string{
	"U":  "table",
	"V":  "view",
	"SO": "sequence",
}

// readObjectOwners reads who owns each table, view, sequence and schema.
//
// An object whose principal_id is NULL is owned by its SCHEMA's owner -- that
// is what "the schema owns it" means in T-SQL -- so the join resolves through
// COALESCE rather than dropping those rows, which on an ordinary database is
// every row.
//
// OwnerCanLogin asks authentication_type_desc rather than the principal type. A
// database role authenticates as nothing; a user created WITHOUT LOGIN reports
// NONE and cannot be authenticated as either, so neither is somebody whose
// password could be held. Measured on SQL Server 2025: dbo reports INSTANCE,
// `guest` and a `CREATE USER ... WITHOUT LOGIN` user both report NONE.
func (r *Reader) readObjectOwners(ctx context.Context) ([]catalog.ObjectOwner, error) {
	query := `
		SELECT o.type AS kind, s.name AS schema_name, o.name AS object_name,
		       owner.name AS owner_name,
		       CASE WHEN owner.authentication_type_desc <> 'NONE' THEN 1 ELSE 0 END AS owner_can_login
		FROM sys.objects AS o
		JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		JOIN sys.database_principals AS owner
			ON owner.principal_id = COALESCE(o.principal_id, s.principal_id)
		WHERE o.type IN ('U', 'V', 'SO') AND o.is_ms_shipped = 0
		UNION ALL
		SELECT 'schema' AS kind, '' AS schema_name, s.name AS object_name,
		       owner.name AS owner_name,
		       CASE WHEN owner.authentication_type_desc <> 'NONE' THEN 1 ELSE 0 END AS owner_can_login
		FROM sys.schemas AS s
		JOIN sys.database_principals AS owner ON owner.principal_id = s.principal_id
		WHERE s.name NOT IN ('sys', 'INFORMATION_SCHEMA')
		ORDER BY kind, schema_name, object_name`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	owners := make([]catalog.ObjectOwner, 0)
	for rows.Next() {
		var kind, schemaName, name, owner string
		var canLogin bool
		if err := rows.Scan(&kind, &schemaName, &name, &owner, &canLogin); err != nil {
			return nil, err
		}
		resolved := kind
		if mapped, ok := mssqlOwnerKinds[strings.TrimSpace(kind)]; ok {
			resolved = mapped
		}
		owners = append(owners, catalog.ObjectOwner{
			Kind: resolved, Schema: schemaName, Name: name, Owner: owner, OwnerCanLogin: canLogin,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return owners, nil
}

// readGrants reads the permissions this database's roles hold.
//
// state_desc carries three values, not two. GRANT and
// GRANT_WITH_GRANT_OPTION are the pair Grant.WithOption models. DENY is the
// third, and it SUBTRACTS: a role holding SELECT through a broader grant and
// DENY on one table cannot read that table. Ptah plans no such shape, so the
// row is reported with IsPartialRevoke set rather than dropped -- exactly what
// that field exists for, and what lets a live validation refuse a managed role
// whose effective privileges are quietly narrower than its grant rows say.
func (r *Reader) readGrants(ctx context.Context) ([]catalog.Grant, error) {
	query := `
		SELECT grantee.name, pe.permission_name, pe.state_desc, pe.class_desc,
			   COALESCE(s.name, ''), COALESCE(OBJECT_NAME(pe.major_id), SCHEMA_NAME(pe.major_id), '')
		FROM sys.database_permissions AS pe
		JOIN sys.database_principals AS grantee ON grantee.principal_id = pe.grantee_principal_id
		LEFT JOIN sys.objects AS o ON o.object_id = pe.major_id AND pe.class = 1
		LEFT JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		WHERE grantee.type = 'R' AND grantee.is_fixed_role = 0 AND grantee.name <> 'public'
		ORDER BY grantee.name, pe.permission_name`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		var grant catalog.Grant
		var state, class string
		if err := rows.Scan(&grant.Role, &grant.Privilege, &state, &class,
			&grant.Schema, &grant.ObjectName); err != nil {
			return nil, err
		}
		grant.Privilege = strings.ToUpper(strings.TrimSpace(grant.Privilege))
		grant.ObjectType = grantObjectTypeFor(class)
		grant.WithOption = state == "GRANT_WITH_GRANT_OPTION"
		grant.IsPartialRevoke = state == "DENY"
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
}

// grantObjectTypeFor maps a permission's class onto the object type the shared
// grant shape names.
//
// A class this reader does not model becomes the class name itself rather than
// a guess: naming an unmodeled target is what lets a comparison decline it,
// where defaulting to TABLE would compare a database-level permission against a
// table grant.
func grantObjectTypeFor(class string) string {
	switch class {
	case "OBJECT_OR_COLUMN":
		return "TABLE"
	case "SCHEMA":
		return grantObjectTypeSchema
	default:
		return strings.ToUpper(strings.TrimSpace(class))
	}
}

// readSynonyms reads the synonyms the connected database declares.
//
// base_object_name is the target exactly as the server stored it, brackets and
// all, and it is kept unchanged: it is what the server will resolve, and
// rewriting it would change which object the alias names. The parsed parts are
// derived from it so that ordering can tell a local target from one in another
// database without parsing the string again at every call site.
func (r *Reader) readSynonyms(ctx context.Context) ([]catalog.Synonym, error) {
	query := `
		SELECT s.name, sy.name, sy.base_object_name
		FROM sys.synonyms AS sy
		JOIN sys.schemas AS s ON s.schema_id = sy.schema_id
		WHERE sy.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, sy.name`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var synonyms []catalog.Synonym
	for rows.Next() {
		var synonym catalog.Synonym
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
func applySynonymTargetParts(synonym *catalog.Synonym) {
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

func (r *Reader) readTriggers(ctx context.Context) ([]catalog.Trigger, error) {
	query := `
		SELECT s.name, tr.name, t.name, OBJECT_DEFINITION(tr.object_id)
		FROM sys.triggers AS tr
		JOIN sys.tables AS t ON t.object_id = tr.parent_id
		JOIN sys.schemas AS s ON s.schema_id = t.schema_id
		WHERE tr.is_ms_shipped = 0
			  AND (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name, t.name, tr.name`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var triggers []catalog.Trigger
	for rows.Next() {
		var trigger catalog.Trigger
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

func reconcileColumnFlags(schema *catalog.Database) {
	primary := make(map[catalogTableKey]map[string]struct{})
	unique := make(map[catalogTableKey]map[string]struct{})
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
