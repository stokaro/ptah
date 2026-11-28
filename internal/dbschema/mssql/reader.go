package mssql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/internal/revisiontable"
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

// normalizeSchemas is schemas without blank names and repeats, in first-seen
// order, or fallback when nothing is left. A name keeps its bytes: ` a` is a
// schema of its own, not another spelling of `a`.
func normalizeSchemas(schemas []string, fallback string) []string {
	seen := make(map[string]struct{}, len(schemas)+1)
	out := make([]string, 0, len(schemas)+1)
	for _, schema := range schemas {
		if strings.TrimSpace(schema) == "" {
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

	if err := r.recordSchemas(ctx, schema); err != nil {
		return nil, err
	}

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

	if err := r.readPermissions(ctx, schema); err != nil {
		return nil, err
	}

	if err := r.readRoleGraph(ctx, schema); err != nil {
		return nil, err
	}

	owners, err := r.readObjectOwners(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read object owners: %w", err)
	}
	schema.ObjectOwners = owners

	synonyms, err := r.readSynonyms(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read synonyms: %w", err)
	}
	if err := recordSynonyms(schema, synonyms); err != nil {
		return nil, fmt.Errorf("sqlserver: record synonyms: %w", err)
	}

	extendedProperties, err := r.readExtendedProperties(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read extended properties: %w", err)
	}
	if err := recordExtendedProperties(schema, extendedProperties); err != nil {
		return nil, fmt.Errorf("sqlserver: record extended properties: %w", err)
	}

	triggers, err := r.readTriggers(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read triggers: %w", err)
	}
	schema.Triggers = triggers

	if err := r.recordFunctions(ctx, schema); err != nil {
		return nil, err
	}

	policies, coverage, err := r.readSecurityPolicies(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlserver: read security policies: %w", err)
	}
	// The extended properties are already recorded, so the policies join them
	// rather than replace them.
	if schema.FeatureObjects, err = schema.FeatureObjects.Merge(policies); err != nil {
		return nil, fmt.Errorf("sqlserver: record security policies: %w", err)
	}
	if schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(coverage); err != nil {
		return nil, fmt.Errorf("sqlserver: record security policies: %w", err)
	}

	reconcileColumnFlags(schema)
	return schema, nil
}

// recordSchemas records the schemas the read covers in schema, and the user
// schemas it leaves out.
func (r *Reader) recordSchemas(ctx context.Context, schema *catalog.Database) error {
	schemas, err := r.readSchemas(ctx)
	if err != nil {
		return fmt.Errorf("sqlserver: read schemas: %w", err)
	}
	schema.Schemas = schemas
	outside, err := r.readSchemasOutOfScope(ctx)
	if err != nil {
		return fmt.Errorf("sqlserver: read schemas outside the read: %w", err)
	}
	schema.SchemasOutOfScope = outside
	return nil
}

// recordFunctions reads the database's functions and procedures into schema,
// and claims the principal each one runs as is read: a routine the read gives
// no principal runs as its caller or its owner.
func (r *Reader) recordFunctions(ctx context.Context, schema *catalog.Database) error {
	functions, err := r.readFunctions(ctx)
	if err != nil {
		return fmt.Errorf("sqlserver: read functions: %w", err)
	}
	schema.Functions = functions
	executeAs, err := mssqlschema.RoutineOptionsCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil)
	if err != nil {
		return fmt.Errorf("sqlserver: record routine principals: %w", err)
	}
	if schema.FeatureCoverage, err = schema.FeatureCoverage.Combine(executeAs); err != nil {
		return fmt.Errorf("sqlserver: record routine principals: %w", err)
	}
	return nil
}

// readSchemas reads the schemas of the read's scope that the database holds, each
// spelled as the catalog spells it, with the comment an MS_Description extended
// property gives it. The comparison reads a declared schema missing from them as
// one the database lacks, so an empty schema has to appear here.
func (r *Reader) readSchemas(ctx context.Context) ([]catalog.Schema, error) {
	query := `
		SELECT s.name, COALESCE(CAST(ep.value AS nvarchar(max)), N'')
		FROM sys.schemas AS s
		LEFT JOIN sys.extended_properties AS ep
		  ON ep.class = 3
		 AND ep.major_id = s.schema_id
		 AND ep.minor_id = 0
		 AND ep.name = 'MS_Description'
		WHERE (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var schemas []catalog.Schema
	for rows.Next() {
		var schema catalog.Schema
		if err := rows.Scan(&schema.Name, &schema.Comment); err != nil {
			return nil, err
		}
		schemas = append(schemas, schema)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return schemas, nil
}

// readSchemasOutOfScope reads the schemas the database holds beyond the read's
// scope that can hold a user's objects, sorted by name, and an empty non-nil
// slice where there are none: dbo, and every schema a user created. The other
// schemas every database holds are left out: guest, INFORMATION_SCHEMA and
// sys, and the schema of each fixed database role, whose ID is 16384 or more.
func (r *Reader) readSchemasOutOfScope(ctx context.Context) ([]string, error) {
	query := `
		SELECT s.name
		FROM sys.schemas AS s
		WHERE (s.schema_id = 1 OR s.schema_id BETWEEN 5 AND 16383)
		  AND NOT (` + schemaPredicatePlaceholder + `)
		ORDER BY s.name`
	rows, err := r.db.QueryContext(ctx, r.queryWithSchemaPredicate(query), r.schemaArgs()...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	names := make([]string, 0)
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return names, nil
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
		  AND t.name NOT IN (` + revisiontable.DefaultSQLNames + `)
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
				ep.value,
				-- A column that names no COLLATE takes the database default,
				-- and so does one that restates it; the catalog keeps the
				-- same name for both.
				c.collation_name,
				CAST(DATABASEPROPERTYEX(DB_NAME(), 'Collation') AS NVARCHAR(128))
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
		  AND t.name NOT IN (` + revisiontable.DefaultSQLNames + `)
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
			collation             sql.NullString
			databaseCollation     sql.NullString
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
			&collation,
			&databaseCollation,
		); err != nil {
			return nil, err
		}
		if collation.Valid && !strings.EqualFold(collation.String, databaseCollation.String) {
			column.Collate, column.EffectiveCollate = collation.String, collation.String
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
			// A computed column the server does not store is computed on
			// read, which is what VIRTUAL names on every other target. Left
			// with no kind it reads as a plain column: the comparison would
			// not ask the server how it spells the expression, and a
			// declaration that names VIRTUAL would plan a change of kind.
			column.GeneratedKind = "VIRTUAL"
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
		SELECT s.name, t.name, i.name, i.is_unique, i.is_primary_key, c.name, ic.is_included_column, ic.is_descending_key, COALESCE(i.filter_definition, '')
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
		  AND (ic.key_ordinal > 0 OR ic.is_included_column = 1)
		  AND t.name NOT IN (` + revisiontable.DefaultSQLNames + `)
		  AND (` + schemaPredicatePlaceholder + `)
		-- Key columns in key order, then the payload in the order the index
		-- holds it: a payload column has no key ordinal, and index_column_id
		-- keeps the order the CREATE INDEX wrote, measured on 16.0.4295.3 and
		-- 17.0.5005.3.
		ORDER BY s.name, t.name, i.name, ic.is_included_column,
			CASE WHEN ic.is_included_column = 1 THEN ic.index_column_id ELSE ic.key_ordinal END`
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
			unique, primary, included, desc                      bool
		)
		if err := rows.Scan(&schemaName, &tableName, &indexName, &unique, &primary, &columnName, &included, &desc, &filter); err != nil {
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
		if included {
			index.IncludeColumns = append(index.IncludeColumns, columnName)
			continue
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
		  AND t.name NOT IN (` + revisiontable.DefaultSQLNames + `)
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
		  AND t.name NOT IN (` + revisiontable.DefaultSQLNames + `)
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
		  AND t.name NOT IN (` + revisiontable.DefaultSQLNames + `)
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
// refuses. It is the principal with principal_id 0, not every principal named
// public: a case-sensitive database accepts CREATE ROLE [PUBLIC] beside it, and
// any database a name with a space before it.
//
// A role named PUBLIC, and one with white space around its name that the
// server keeps, is refused ([refuseAmbiguousPrincipal]).
//
// Attributes are deliberately absent. A database role has none of PostgreSQL's
// -- no login, no password, no superuser -- so reporting false for each is not
// a loss of information; it is the only truth available, and the renderer says
// so when a declaration asks for one.
func (r *Reader) readRoles(ctx context.Context) ([]catalog.Role, error) {
	query := `
		SELECT p.name
		FROM sys.database_principals AS p
		WHERE p.type = 'R' AND p.is_fixed_role = 0 AND p.principal_id <> 0
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
		if err := refuseAmbiguousPrincipal(role.Name); err != nil {
			return nil, err
		}
		role.Name = strings.TrimRight(role.Name, " ")
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

// readRoleGraph reads who holds which database role, and which database user
// this connection runs as.
func (r *Reader) readRoleGraph(ctx context.Context, schema *catalog.Database) error {
	memberships, err := r.readRoleMemberships(ctx)
	if err != nil {
		return fmt.Errorf("sqlserver: read role memberships: %w", err)
	}
	schema.RoleMemberships = memberships
	// No row leaves the role unknown, and a plan then covers every membership.
	if err := r.db.QueryRowContext(ctx, `SELECT USER_NAME()`).Scan(&schema.ConnectedRole); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return fmt.Errorf("sqlserver: read the connected role: %w", err)
	}
	schema.ConnectedRole = strings.TrimRight(schema.ConnectedRole, " ")
	return nil
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
		WHERE role_principal.is_fixed_role = 0 AND role_principal.principal_id <> 0
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
		membership.Role = strings.TrimRight(membership.Role, " ")
		membership.Member = strings.TrimRight(membership.Member, " ")
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
//
// The schema apply log is left out, as the table read leaves it out. Its owner
// is part of a saved plan's source fingerprint otherwise, and a schema apply
// creates the log before its first statement, a run that fails included, so
// every plan saved before that run would read stale after it.
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
			AND NOT (o.type = 'U' AND o.name = N'` + revisiontable.ApplyLog + `')
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
			Kind: resolved, Schema: schemaName, Name: name, Owner: strings.TrimRight(owner, " "), OwnerCanLogin: new(canLogin),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return owners, nil
}

// readPermissions reads the permissions of the database into schema: the
// ones on a securable the schema models as its grants ([Reader.readGrants]),
// and the ones on any other securable as objects Ptah does not model
// ([Reader.readUnmodeledPermissions]).
func (r *Reader) readPermissions(ctx context.Context, schema *catalog.Database) error {
	grants, err := r.readGrants(ctx)
	if err != nil {
		return fmt.Errorf("sqlserver: read grants: %w", err)
	}
	schema.Grants = grants
	permissions, err := r.readUnmodeledPermissions(ctx)
	if err != nil {
		return fmt.Errorf("sqlserver: read permissions on other securables: %w", err)
	}
	schema.UnmodeledObjects = append(schema.UnmodeledObjects, permissions...)
	return nil
}

// readGrants reads the permissions this database's roles hold, and the ones a
// database user or public holds on the database's own objects and schemas.
//
// A user, an application role and public are read only there, because the
// rest of what they hold no statement of the schema's wrote. Measured on SQL
// Server 2025, a new database grants public SELECT on 232 system objects and
// two permissions on the database, and CREATE USER grants the user CONNECT on
// the database; none of them is a permission on an object of the schema. A
// fixed role holds no object permission at all. Every grantee is read on the
// schema's objects because a plan that drops one of them and creates it again
// takes every grantee's permissions, and gives back only what was read. The
// comparison leaves the permissions of a grantee the schema does not manage
// alone, as on a role it does not declare.
//
// state_desc carries four values, not two. GRANT and
// GRANT_WITH_GRANT_OPTION are the pair Grant.WithOption models. DENY and
// REVOKE subtract: a role holding SELECT through a broader grant and DENY on
// one table cannot read that table, and a role holding SELECT on a table and a
// REVOKE row on one of its columns cannot read that column. The REVOKE row is
// what `REVOKE SELECT (c) ON t` leaves under a grant on the whole table;
// measured on SQL Server 2019, 2022 and 2025, a member of the role then gets
// Msg 230 for a statement that reads c. Either row is reported with
// IsPartialRevoke set rather than dropped, which keeps the comparison from
// reading it as a privilege the role holds, and a DENY row with Denied set as
// well: a schema declares it as a DENY.
//
// A permission on one column of a table or a view is its own row, with the
// column_id in minor_id, and the reader names the column the way the catalog
// spells it. An object permission takes the place of the column rows of the
// same permission: measured on the same lines, `GRANT SELECT ON t` after
// `GRANT SELECT (id) ON t`, after `REVOKE SELECT (id) ON t` under a grant on
// t, or after `DENY SELECT (id) ON t`, leaves one row for the table, and
// `GRANT SELECT (id) ON t` under a table grant adds none. A column's rows go
// with the column when it is dropped.
//
// A column row of another state may sit under an object permission: a REVOKE
// row, `GRANT SELECT (c) ... WITH GRANT OPTION` under a plain grant, or the
// option revoked from c under a grantable one. The object row then holds for
// the columns without a row of their own and not for the others, so the
// reader reports it as that permission on each such column. Read as a
// permission on the whole object, a declaration of the whole object would
// compare equal while a member cannot read c, and `ptah db read` would print
// a grant that gives c back on replay.
//
// A DENY row on a column is the exception, and the object row is read as it
// is beside it. A schema declares the DENY, `DENY SELECT (c) ON t` after
// `GRANT SELECT ON t` keeps both rows, and `ptah db read` writes the grant
// before the DENY, so the replay holds both. Read as a grant on each other
// column, the table grant a schema declares never matches, and a plan that
// grants it again takes the DENY with it.
//
// The fixed role public is the principal with principal_id 0, and the read
// names it PUBLIC, as every other reader names the keyword and as a schema
// file's PUBLIC is recorded, so a grant to it compares with a declared one by
// name alone. Another principal named PUBLIC, and one with white space around
// its name that the server keeps, is refused ([refuseAmbiguousPrincipal]).
//
// Each row names its grantor, and the server keeps a row per grantor: measured
// on SQL Server 2025, a user granted SELECT by dbo and again by q2, who holds
// it WITH GRANT OPTION, has two rows, and `REVOKE GRANT OPTION FOR SELECT ...
// FROM q2 CASCADE` takes the second. The grantor is what lets a plan see which
// grants such a revoke takes with it.
//
// Each row also names the owner of its securable ([catalog.Grant.ObjectOwner]),
// resolved on the server for that row: the principal an object was given to,
// or else the owner of its schema, for an object or a column; the owner of
// the schema for a schema; and dbo for the database. A GRANT or a DENY run by
// sysadmin, by a member of db_owner or by a principal holding CONTROL names
// that owner as its grantor, so the owner is what tells the row such a
// statement takes the place of from one another principal gave. An inventory
// of owners by kind cannot say it: a procedure or a function given to another
// principal is owned by that principal and not by the owner of its schema.
//
// major_id names the securable in a number space of its class: a schema_id
// for a row of class SCHEMA, an object_id for an object or a column, and a
// principal_id, a user_type_id and so on for the other classes. The spaces
// overlap. Measured on SQL Server 2025, the first schema a new database
// creates has schema_id 5, OBJECT_NAME(5) is sysrowsets, and OBJECT_NAME of a
// role's principal_id reads sysclones. So the read names a securable by its
// class, and reads only the classes the schema models: the database, a
// schema, and an object or a column. A permission of another class, such as
// one on a role, a user or a type, is reported apart
// ([Reader.readUnmodeledPermissions]).
func (r *Reader) readGrants(ctx context.Context) ([]catalog.Grant, error) {
	query := `
		WITH permission AS (
			SELECT pe.grantee_principal_id, pe.grantor_principal_id, pe.class, pe.class_desc, pe.major_id, pe.minor_id,
				   pe.permission_name, pe.state_desc,
				   CASE WHEN pe.class = 1 AND pe.minor_id = 0 AND pe.state_desc IN ('GRANT', 'GRANT_WITH_GRANT_OPTION') AND EXISTS (
					   SELECT 1 FROM sys.database_permissions AS col
					   WHERE col.class = 1 AND col.major_id = pe.major_id AND col.minor_id <> 0
						 AND col.grantee_principal_id = pe.grantee_principal_id AND col.permission_name = pe.permission_name
						 AND col.state_desc <> 'DENY'
				   ) THEN 1 ELSE 0 END AS per_column
			FROM sys.database_permissions AS pe
		), effective AS (
			SELECT grantee_principal_id, grantor_principal_id, class, class_desc, major_id, minor_id, permission_name, state_desc
			FROM permission
			WHERE per_column = 0
			UNION ALL
			SELECT p.grantee_principal_id, p.grantor_principal_id, p.class, p.class_desc, p.major_id, c.column_id,
				   p.permission_name, p.state_desc
			FROM permission AS p
			JOIN sys.columns AS c ON c.object_id = p.major_id
			WHERE p.per_column = 1 AND NOT EXISTS (
				SELECT 1 FROM sys.database_permissions AS own
				WHERE own.class = 1 AND own.major_id = p.major_id AND own.minor_id = c.column_id
				  AND own.grantee_principal_id = p.grantee_principal_id AND own.permission_name = p.permission_name
			)
		)
		SELECT grantee.principal_id, grantee.name, pe.permission_name, pe.state_desc, pe.class_desc,
			   COALESCE(s.name, ''),
			   CASE pe.class WHEN 1 THEN COALESCE(OBJECT_NAME(pe.major_id), '')
					WHEN 3 THEN COALESCE(SCHEMA_NAME(pe.major_id), '') ELSE '' END,
			   CASE WHEN pe.class = 1 AND pe.minor_id <> 0 THEN COALESCE(COL_NAME(pe.major_id, pe.minor_id), '') ELSE '' END,
			   COALESCE(grantor.name, ''),
			   COALESCE(owner.name, '')
		FROM effective AS pe
		JOIN sys.database_principals AS grantee ON grantee.principal_id = pe.grantee_principal_id
		LEFT JOIN sys.database_principals AS grantor ON grantor.principal_id = pe.grantor_principal_id
		LEFT JOIN sys.objects AS o ON o.object_id = pe.major_id AND pe.class = 1
		LEFT JOIN sys.schemas AS s ON s.schema_id = o.schema_id
		LEFT JOIN sys.schemas AS target_schema ON target_schema.schema_id = pe.major_id AND pe.class = 3
		LEFT JOIN sys.database_principals AS owner ON owner.principal_id = CASE pe.class
			WHEN 0 THEN ` + dboPrincipalID + `
			WHEN 1 THEN COALESCE(o.principal_id, s.principal_id)
			WHEN 3 THEN target_schema.principal_id END
		WHERE grantee.is_fixed_role = 0 AND pe.class IN (0, 1, 3) AND (
			(grantee.type = 'R' AND grantee.principal_id <> 0)
			OR (pe.class = 1 AND o.is_ms_shipped = 0)
			OR pe.class = 3
		)
		ORDER BY grantee.name, pe.permission_name, pe.class, pe.major_id, pe.minor_id, grantor.name`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var grants []catalog.Grant
	for rows.Next() {
		var grant catalog.Grant
		var grantee int64
		var state, class string
		if err := rows.Scan(&grantee, &grant.Role, &grant.Privilege, &state, &class,
			&grant.Schema, &grant.ObjectName, &grant.Column, &grant.GrantedBy, &grant.ObjectOwner); err != nil {
			return nil, err
		}
		if grantee == publicPrincipalID {
			grant.Role = publicGrantee
		} else if err := refuseAmbiguousPrincipal(grant.Role); err != nil {
			return nil, err
		}
		grant.Role = strings.TrimRight(grant.Role, " ")
		grant.GrantedBy = strings.TrimRight(grant.GrantedBy, " ")
		grant.ObjectOwner = strings.TrimRight(grant.ObjectOwner, " ")
		grant.Privilege = strings.ToUpper(strings.TrimSpace(grant.Privilege))
		grant.ObjectType = grantObjectTypeFor(class)
		grant.WithOption = state == "GRANT_WITH_GRANT_OPTION"
		grant.IsPartialRevoke = state == "DENY" || state == "REVOKE"
		grant.Denied = state == "DENY"
		grants = append(grants, grant)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return grants, nil
}

// publicPrincipalID is the principal_id of the fixed role public in every
// database.
const publicPrincipalID = 0

// dboPrincipalID is the principal_id of dbo, the user the owner of the
// database maps to, in every database, as a query spells it.
const dboPrincipalID = "1"

// publicGrantee is how a read names the fixed role public: as every reader
// names the keyword PUBLIC, and as a schema file records it.
const publicGrantee = "PUBLIC"

// refuseAmbiguousPrincipal refuses a role named PUBLIC, which the schema
// reserves for the fixed role, and surrounding white space outside the
// trailing spaces SQL Server ignores. The SQL schema reader applies the same
// restriction. A case-sensitive database can hold a separate role named
// PUBLIC, so accepting it would confuse that role with the fixed one.
func refuseAmbiguousPrincipal(name string) error {
	trimmed := strings.TrimSpace(name)
	if trimmed == publicGrantee {
		return fmt.Errorf("%w: the database principal %q has the name a schema gives the fixed role public, "+
			"so a grant to one cannot be told from a grant to the other; rename the principal",
			ptaherr.ErrUnsupportedFeature, name)
	}
	if trimmed != strings.TrimRight(name, " ") {
		return fmt.Errorf("%w: the database principal %q uses surrounding white space that Ptah does not support on SQL Server; rename the principal",
			ptaherr.ErrUnsupportedFeature, name)
	}
	return nil
}

// grantObjectTypeFor maps a permission's class onto the object type the shared
// grant shape names.
//
// OBJECT_OR_COLUMN is every schema object, a procedure and a function as well
// as a table and a view, and it becomes TABLE: `GRANT EXECUTE ON dbo.p` names
// the object with no kind, so the declaration carries it OnTable. The
// comparison takes what ALL grants on it from the routine the catalog reads.
// DATABASE keeps its name. [Reader.readGrants] reads no other class.
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

// unmodeledPermissionReason is why a read leaves a permission of a class the
// schema does not model out of the grants it compares.
const unmodeledPermissionReason = "Ptah compares permissions on the database, a schema, an object or a column, " +
	"and no permission on another securable class, so no plan grants, denies or revokes it"

// readUnmodeledPermissions reports the permissions this database's roles hold
// on a securable of a class [Reader.readGrants] does not read, such as a
// role, a user, a type or a certificate. The schema cannot declare one, so
// the read keeps it out of the grants a comparison reads, where its major_id
// would name a securable of another class, and records it as an object
// Ptah does not model, named the way a statement names it: `GRANT ALTER ON
// ROLE::auditor TO app`. A class this reader cannot name is named by its
// class and the major_id the catalog holds, rather than by a name the
// catalog does not give it.
//
// The grantees are the ones readGrants reads on every class: the database's
// own roles, and not a user, public or a fixed role.
func (r *Reader) readUnmodeledPermissions(ctx context.Context) ([]catalog.UnmodeledObject, error) {
	query := `
		SELECT pe.class, pe.class_desc, pe.major_id, pe.permission_name, pe.state_desc, grantee.name,
			   COALESCE(target.type, ''),
			   COALESCE(CASE pe.class
				   WHEN 6 THEN SCHEMA_NAME(t.schema_id)
				   WHEN 10 THEN SCHEMA_NAME(x.schema_id) END COLLATE DATABASE_DEFAULT, ''),
			   COALESCE(CASE pe.class
				   WHEN 4 THEN target.name COLLATE DATABASE_DEFAULT
				   WHEN 5 THEN (SELECT a.name FROM sys.assemblies AS a WHERE a.assembly_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 6 THEN t.name COLLATE DATABASE_DEFAULT
				   WHEN 10 THEN x.name COLLATE DATABASE_DEFAULT
				   WHEN 15 THEN (SELECT m.name FROM sys.service_message_types AS m WHERE m.message_type_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 16 THEN (SELECT c.name FROM sys.service_contracts AS c WHERE c.service_contract_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 17 THEN (SELECT v.name FROM sys.services AS v WHERE v.service_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 18 THEN (SELECT b.name FROM sys.remote_service_bindings AS b WHERE b.remote_service_binding_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 19 THEN (SELECT ro.name FROM sys.routes AS ro WHERE ro.route_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 23 THEN (SELECT f.name FROM sys.fulltext_catalogs AS f WHERE f.fulltext_catalog_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 24 THEN (SELECT k.name FROM sys.symmetric_keys AS k WHERE k.symmetric_key_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 25 THEN (SELECT ce.name FROM sys.certificates AS ce WHERE ce.certificate_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 26 THEN (SELECT k.name FROM sys.asymmetric_keys AS k WHERE k.asymmetric_key_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 29 THEN (SELECT l.name FROM sys.fulltext_stoplists AS l WHERE l.stoplist_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 31 THEN (SELECT l.name FROM sys.registered_search_property_lists AS l WHERE l.property_list_id = pe.major_id) COLLATE DATABASE_DEFAULT
				   WHEN 32 THEN (SELECT d.name FROM sys.database_scoped_credentials AS d WHERE d.credential_id = pe.major_id) COLLATE DATABASE_DEFAULT
			   END, '')
		FROM sys.database_permissions AS pe
		JOIN sys.database_principals AS grantee ON grantee.principal_id = pe.grantee_principal_id
		LEFT JOIN sys.database_principals AS target ON target.principal_id = pe.major_id AND pe.class = 4
		LEFT JOIN sys.types AS t ON t.user_type_id = pe.major_id AND pe.class = 6
		LEFT JOIN sys.xml_schema_collections AS x ON x.xml_collection_id = pe.major_id AND pe.class = 10
		WHERE grantee.is_fixed_role = 0 AND grantee.type = 'R' AND grantee.principal_id <> 0
			AND pe.class NOT IN (0, 1, 3)
		ORDER BY grantee.name, pe.permission_name, pe.class, pe.major_id`
	rows, err := r.db.QueryContext(ctx, query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var objects []catalog.UnmodeledObject
	for rows.Next() {
		var permission unmodeledPermission
		if err := rows.Scan(&permission.class, &permission.classDesc, &permission.majorID, &permission.privilege,
			&permission.state, &permission.grantee, &permission.principalType, &permission.schema, &permission.name); err != nil {
			return nil, err
		}
		// The server ignores trailing spaces in a principal's name, as the
		// other reads of the principals here do.
		permission.grantee = strings.TrimRight(permission.grantee, " ")
		if permission.class == databasePrincipalClass {
			permission.name = strings.TrimRight(permission.name, " ")
		}
		objects = append(objects, catalog.UnmodeledObject{
			Kind: "permission", Name: permission.statement(), Reason: unmodeledPermissionReason,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return objects, nil
}

// unmodeledPermission is one row [Reader.readUnmodeledPermissions] reads.
type unmodeledPermission struct {
	class                                int
	majorID                              int64
	classDesc, privilege, state          string
	grantee, principalType, schema, name string
}

// securableClasses spells each class [Reader.readUnmodeledPermissions] names
// the way a GRANT names its securable. A database principal is spelled by
// its type ([unmodeledPermission.securable]).
var securableClasses = map[int]string{
	5: "ASSEMBLY", 6: "TYPE", 10: "XML SCHEMA COLLECTION", 15: "MESSAGE TYPE", 16: "CONTRACT",
	17: "SERVICE", 18: "REMOTE SERVICE BINDING", 19: "ROUTE", 23: "FULLTEXT CATALOG",
	24: "SYMMETRIC KEY", 25: "CERTIFICATE", 26: "ASYMMETRIC KEY", 29: "FULLTEXT STOPLIST",
	31: "SEARCH PROPERTY LIST", 32: "DATABASE SCOPED CREDENTIAL",
}

// statement names the permission as the statement that gives it does, such
// as `DENY VIEW DEFINITION ON ROLE::auditor TO app`.
func (p unmodeledPermission) statement() string {
	verb, option := strings.TrimSpace(p.state), ""
	if verb == "GRANT_WITH_GRANT_OPTION" {
		verb, option = "GRANT", " WITH GRANT OPTION"
	}
	return fmt.Sprintf("%s %s ON %s TO %s%s", verb, strings.TrimSpace(p.privilege), p.securable(), p.grantee, option)
}

// securable names the securable as a GRANT does, or by its class and
// major_id where the read has no name for it.
func (p unmodeledPermission) securable() string {
	name := p.name
	if p.schema != "" {
		name = p.schema + "." + name
	}
	class, spelled := securableClasses[p.class]
	if p.class == databasePrincipalClass {
		class, spelled = cmp.Or(principalSecurables[strings.TrimSpace(p.principalType)], "USER"), true
	}
	if !spelled || p.name == "" {
		return fmt.Sprintf("%s with major_id %d", strings.TrimSpace(p.classDesc), p.majorID)
	}
	return class + "::" + name
}

// databasePrincipalClass is the class of a permission on a database
// principal: a user, a role or an application role.
const databasePrincipalClass = 4

// principalSecurables spells a database principal's securable class by its
// type in sys.database_principals; every other type is a user.
var principalSecurables = map[string]string{"R": "ROLE", "A": "APPLICATION ROLE"}

func (r *Reader) readTriggers(ctx context.Context) ([]catalog.Trigger, error) {
	// is_instead_of_trigger and sys.trigger_events are the two facts a trigger
	// is. Without them an INSTEAD OF DELETE trigger and an AFTER UPDATE one read
	// identically, as AFTER INSERT, and that is not a spelling difference: an
	// INSTEAD OF trigger replaces the statement and an AFTER one runs behind it
	// (stokaro/ptah#2206).
	//
	// A trigger may name more than one event, so they are joined the way the
	// declaration spells them, ordered by the catalog's own code so the string
	// is stable rather than a function of row order.
	//
	// OBJECT_DEFINITION is NULL for an encrypted trigger. ISNULL keeps the read
	// from failing on one; what it leaves is an empty body, which the
	// comparator reports rather than a statement that would recreate the
	// trigger empty.
	//
	// A DML trigger's parent is a table or a view, and an INSTEAD OF trigger is
	// how a view takes writes. Joined to sys.tables alone, a view's trigger was
	// never read: `db read` left it out and a plan never saw it. A database
	// trigger has no parent object, so the join leaves it out.
	query := `
		SELECT s.name, tr.name, t.name,
			   ISNULL(OBJECT_DEFINITION(tr.object_id), ''),
			   CASE WHEN tr.is_instead_of_trigger = 1 THEN 'INSTEAD OF' ELSE 'AFTER' END,
			   ISNULL((SELECT STRING_AGG(te.type_desc, ' OR ') WITHIN GROUP (ORDER BY te.type)
					   FROM sys.trigger_events te WHERE te.object_id = tr.object_id), '')
		FROM sys.triggers AS tr
		JOIN sys.objects AS t ON t.object_id = tr.parent_id AND t.type IN ('U', 'V')
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
		var definition string
		if err := rows.Scan(
			&trigger.Schema, &trigger.Name, &trigger.Table,
			&definition, &trigger.Timing, &trigger.Event,
		); err != nil {
			return nil, err
		}
		trigger.Schema = r.outputSchema(trigger.Schema)
		// OBJECT_DEFINITION hands back the whole CREATE TRIGGER statement, and
		// Body is the body alone: everything downstream treats it that way, and
		// a renderer wraps it in a CREATE TRIGGER of its own. Kept whole, the
		// statement nests one CREATE TRIGGER inside another, which the server
		// refuses with `Incorrect syntax near the keyword 'TRIGGER'`.
		//
		// A trigger's header ends the same way a procedure's does: at the first
		// standalone AS outside brackets, skipping the one in WITH EXECUTE AS.
		trigger.Body = procedureBody(definition)
		// SQL Server has no row-level DML trigger -- FOR EACH ROW is not part of
		// its grammar -- so this constant states a property of the engine rather
		// than an unread column.
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
	case keepsFractionalSeconds(typeName) && scale != defaultFractionalSeconds:
		return fmt.Sprintf("%s(%d)", upper, scale)
	default:
		return upper
	}
}

// defaultFractionalSeconds is what a datetime2, a datetimeoffset or a time holds when
// it declares no precision. The read leaves it out, so an ordinary column reads as the
// document that built it did.
const defaultFractionalSeconds = 7

// keepsFractionalSeconds reports a type whose precision a column declares, which the
// catalog keeps as its scale. Without it a datetime2(3) column reads as datetime2: a
// declaration of datetime2(3) over it plans a change on every run, and one of datetime2
// plans none.
func keepsFractionalSeconds(typeName string) bool {
	switch strings.ToLower(typeName) {
	case "datetime2", "datetimeoffset", "time":
		return true
	default:
		return false
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
			// A key over several columns makes none of them unique on its own. Read as
			// if it did, every later plan holds a column change nothing declared.
			if columns := constraint.ColumnNamesOrDefault(); len(columns) == 1 {
				addColumns(unique, key, columns)
			}
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
