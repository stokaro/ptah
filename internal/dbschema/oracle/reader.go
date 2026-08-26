// Package oracle reads schema information from Oracle databases.
//
// Oracle's catalog is a third shape beside information_schema and pg_catalog:
// the ALL_* views, keyed by OWNER rather than by a schema column, with an
// object's owner and its schema being the same thing because in Oracle a schema
// IS a user.
//
// Two version differences decide which columns these queries may name, and both
// were measured rather than read. On 21.3, ALL_TAB_COLUMNS has no
// DATA_DEFAULT_VC and no DOMAIN_NAME -- both answer ORA-00904 -- while 23.26 has
// them. A reader written against the newer catalog fails on the older line at
// its very first column query, so the LONG column DATA_DEFAULT is what is read
// here. SEARCH_CONDITION_VC and TEXT_VC exist on both, and are used, because the
// LONG columns beside them are considerably worse to read.
package oracle

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

// Reader reads schema information from Oracle databases.
type Reader struct {
	db     sqlrunner.Runner
	schema string
	caps   capability.Capabilities
}

// NewOracleReader constructs a reader scoped to one schema.
//
// The schema is the owner every query filters on. An empty one is a
// programming error rather than a default: unlike PostgreSQL's `public` or SQL
// Server's `dbo`, Oracle has no schema every database carries, so there is no
// name to fall back to. The connection layer fills it from
// SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA').
func NewOracleReader(db sqlrunner.Runner, schema string) *Reader {
	return NewOracleReaderWithCapabilities(db, schema, capability.Oracle23())
}

// NewOracleReaderWithCapabilities is NewOracleReader told which release line it
// is reading.
//
// One read is gated on it, and the gate is not an optimization: ALL_DOMAINS
// does not exist on Oracle 21, and a query naming a relation the server does
// not have fails the STATEMENT -- which, inside a transaction, leaves every
// later read answering that the transaction is aborted rather than answering
// the question asked. Deciding from the preset means the statement is never
// sent (stokaro/ptah#1920).
func NewOracleReaderWithCapabilities(
	db sqlrunner.Runner,
	schema string,
	caps capability.Capabilities,
) *Reader {
	return &Reader{
		db:     db,
		schema: strings.ToUpper(strings.TrimSpace(schema)),
		caps:   caps.Clone(),
	}
}

// ReadSchema is [Reader.ReadSchemaContext] under context.Background(), the
// context-free half of the pair [catalog.SchemaReader] declares. Prefer the
// Context form: only it can be told to stop.
func (r *Reader) ReadSchema() (*catalog.Database, error) {
	return r.ReadSchemaContext(context.Background())
}

// ReadSchemaContext reads the objects Ptah renders for Oracle.
func (r *Reader) ReadSchemaContext(ctx context.Context) (*catalog.Database, error) {
	schema := &catalog.Database{
		Schemas: []catalog.Schema{{Name: r.schema}},
	}

	tables, err := r.readTables(ctx)
	if err != nil {
		return nil, fmt.Errorf("oracle: read tables: %w", err)
	}
	schema.Tables = tables

	constraints, generatedKeys, err := r.readConstraints(ctx)
	if err != nil {
		return nil, fmt.Errorf("oracle: read constraints: %w", err)
	}
	schema.Constraints = constraints

	indexes, err := r.readIndexes(ctx)
	if err != nil {
		return nil, fmt.Errorf("oracle: read indexes: %w", err)
	}
	schema.Indexes = indexes

	sequences, err := r.readSequences(ctx)
	if err != nil {
		return nil, fmt.Errorf("oracle: read sequences: %w", err)
	}
	schema.Sequences = sequences

	views, err := r.readViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("oracle: read views: %w", err)
	}
	schema.Views = views

	matViews, err := r.readMaterializedViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("oracle: read materialized views: %w", err)
	}
	schema.MatViews = matViews

	if err := r.readRolesInto(ctx, schema); err != nil {
		return nil, fmt.Errorf("oracle: %w", err)
	}

	if r.caps.Has(capability.DomainTypes) {
		domains, err := r.readDomains(ctx)
		if err != nil {
			return nil, fmt.Errorf("oracle: read domains: %w", err)
		}
		schema.Domains = domains
	}

	if r.caps.Has(capability.CompositeTypes) {
		composites, err := r.readComposites(ctx)
		if err != nil {
			return nil, fmt.Errorf("oracle: read composite types: %w", err)
		}
		schema.Composites = composites
	}

	// One read serves both keys, because one catalog row serves both objects:
	// ALL_PROCEDURES tells a function from a procedure by OBJECT_TYPE, and
	// asking for one kind and not the other would mean a second query for the
	// same view. A target declaring neither key is not asked at all.
	if r.caps.Has(capability.Functions) || r.caps.Has(capability.Procedures) {
		functions, err := r.readFunctions(ctx)
		if err != nil {
			return nil, fmt.Errorf("oracle: read functions: %w", err)
		}
		schema.Functions = functions
	}

	markKeyColumns(schema)
	schema.Constraints = withoutGeneratedKeys(schema.Constraints, generatedKeys)
	return schema, nil
}

// withoutGeneratedKeys drops the PRIMARY KEY and UNIQUE constraints Oracle named
// itself, after markKeyColumns has taken the fact off them.
//
// A key declared on the column is not a named constraint in the declaration, and
// Oracle gives it one anyway: `id INTEGER PRIMARY KEY` becomes SYS_C008644. Read
// back as a named constraint it has no counterpart to compare against, so a plan
// drops it, the next apply recreates the key inline, Oracle invents a fresh
// number, and the plan is non-empty again with a different name every time.
//
// The order matters. markKeyColumns runs first and copies IsPrimaryKey and
// IsUnique onto the columns, which is the shape the declared side uses, so
// nothing is lost by removing the row afterwards. A constraint the user named
// arrives with generated = 'USER NAME' and is kept, because that one does have a
// counterpart (stokaro/ptah#1890).
func withoutGeneratedKeys(constraints []catalog.Constraint, generated map[string]bool) []catalog.Constraint {
	if len(generated) == 0 {
		return constraints
	}
	kept := make([]catalog.Constraint, 0, len(constraints))
	for _, constraint := range constraints {
		if generated[constraint.Name] {
			continue
		}
		kept = append(kept, constraint)
	}
	return kept
}

const tableQuery = `
SELECT t.table_name, NVL(c.comments, ' ')
FROM all_tables t
LEFT JOIN all_tab_comments c
       ON c.owner = t.owner AND c.table_name = t.table_name
WHERE t.owner = :1
  AND NOT EXISTS (
        SELECT 1 FROM all_mviews m
        WHERE m.owner = t.owner AND m.container_name = t.table_name)
  AND t.dropped = 'NO'
ORDER BY t.table_name`

func (r *Reader) readTables(ctx context.Context) ([]catalog.Table, error) {
	rows, err := r.db.QueryContext(ctx, tableQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []catalog.Table
	for rows.Next() {
		var table catalog.Table
		var comment string
		if err := rows.Scan(&table.Name, &comment); err != nil {
			return nil, err
		}
		table.Schema = r.schema
		table.Type = "TABLE"
		table.Comment = strings.TrimSpace(comment)
		tables = append(tables, table)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	columns, err := r.readColumns(ctx)
	if err != nil {
		return nil, err
	}
	for i := range tables {
		tables[i].Columns = columns[tables[i].Name]
	}
	return tables, nil
}

// columnQuery reads every column of the schema in one statement.
//
// It reads ALL_TAB_COLS rather than ALL_TAB_COLUMNS, and the difference is one
// column that decides three fields. ALL_TAB_COLS has VIRTUAL_COLUMN -- measured
// present on 23.26 and 21.3 alike, and absent from ALL_TAB_COLUMNS on both --
// which is the only thing separating a generated column from an ordinary one
// with a default. HIDDEN_COLUMN comes with it and has to be filtered, because
// that view also lists the internal columns Oracle keeps for itself.
//
// DATA_DEFAULT is a LONG and is selected as itself rather than through
// DATA_DEFAULT_VC, which does not exist on 21.3. CHAR_LENGTH rather than
// DATA_LENGTH is the character count a VARCHAR2 was declared with; DATA_LENGTH
// is the byte width, and reports 22 for every NUMBER.
const columnQuery = `
SELECT c.table_name,
       c.column_name,
       c.data_type,
       c.char_length,
       c.data_precision,
       c.data_scale,
       c.nullable,
       c.column_id,
       c.identity_column,
       c.virtual_column,
       c.data_default,
       cc.comments
FROM all_tab_cols c
LEFT JOIN all_col_comments cc
       ON cc.owner = c.owner
      AND cc.table_name = c.table_name
      AND cc.column_name = c.column_name
WHERE c.owner = :1
  AND c.hidden_column = 'NO'
  AND c.table_name NOT LIKE 'BIN$%'
ORDER BY c.table_name, c.column_id`

func (r *Reader) readColumns(ctx context.Context) (map[string][]catalog.Column, error) {
	rows, err := r.db.QueryContext(ctx, columnQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string][]catalog.Column)
	for rows.Next() {
		var (
			table      string
			column     catalog.Column
			charLength sql.NullInt64
			precision  sql.NullInt64
			scale      sql.NullInt64
			nullable   string
			position   sql.NullInt64
			identity   string
			virtual    string
			def        sql.NullString
			comment    sql.NullString
		)
		if err := rows.Scan(&table, &column.Name, &column.DataType, &charLength,
			&precision, &scale, &nullable, &position, &identity, &virtual, &def,
			&comment); err != nil {
			return nil, err
		}
		// Oracle has no empty string: a column with no comment and one whose
		// comment was set to '' both read as NULL, so there is no state the
		// zero value could be mistaken for.
		column.Comment = comment.String
		// UDTName is deliberately left empty: it names a user-defined type,
		// and the raw catalog spelling is not one. Filling it with DATA_TYPE
		// made the comparison read `number` where the composed type says
		// `NUMBER(10)`, so a live schema did not match itself.
		column.DataType = formatColumnType(column.DataType, charLength, precision, scale)
		column.IsNullable = nullableWord(nullable)
		column.OrdinalPosition = int(position.Int64)
		column.IsAutoIncrement = identity == "YES"
		column.CharacterMaxLength = intPointer(charLength)
		column.NumericPrecision = intPointer(precision)
		column.NumericScale = intPointer(scale)
		assignDefault(&column, identity == "YES", virtual == "YES", def)
		columns[table] = append(columns[table], column)
	}
	return columns, rows.Err()
}

// constraintQuery reads the four constraint kinds Ptah renders.
//
// The C rows are filtered on GENERATED, and that filter is load-bearing: Oracle
// records a NOT NULL column as a CHECK constraint named SYS_C0012345 with the
// condition `"N" IS NOT NULL`. Reading those back as check constraints would
// make every NOT NULL column look like a constraint the declaration never
// wrote, and the comparison would plan a drop for each one on every run.
const constraintQuery = `
SELECT c.constraint_name,
       c.constraint_type,
       c.generated,
       c.table_name,
       NVL(c.search_condition_vc, ' '),
       NVL(c.r_constraint_name, ' '),
       NVL(c.delete_rule, ' '),
       c.deferrable,
       c.deferred,
       col.column_name,
       col.position
FROM all_constraints c
JOIN all_cons_columns col
     ON col.owner = c.owner AND col.constraint_name = c.constraint_name
WHERE c.owner = :1
  AND c.table_name NOT LIKE 'BIN$%'
  AND c.constraint_type IN ('P', 'U', 'R', 'C')
  AND NOT (c.constraint_type = 'C' AND c.generated = 'GENERATED NAME'
           AND REGEXP_LIKE(c.search_condition_vc, '^"[^"]+" IS NOT NULL$'))
ORDER BY c.table_name, c.constraint_name, col.position`

func (r *Reader) readConstraints(ctx context.Context) ([]catalog.Constraint, map[string]bool, error) {
	// The referenced keys are read BEFORE the constraint rows are opened, and
	// the order is load-bearing rather than tidy.
	//
	// go-ora v3 refuses a second query on the same *sql.DB while an earlier
	// result set still has rows to read, answering EOF. Measured, and the
	// distinction is undrained rather than open:
	//
	//	first drained, not closed, then second   ok
	//	first undrained, then second             EOF
	//
	// That is why readIndexes needs no such change: its loop drains before it
	// calls readIndexColumns. This read opened its second query first, with the
	// constraint rows untouched.
	//
	// v2 served the pair from a second pooled connection, so the old order
	// worked by accident of the driver rather than by design.
	referenced, err := r.readReferencedKeys(ctx)
	if err != nil {
		return nil, nil, err
	}

	rows, err := r.db.QueryContext(ctx, constraintQuery, r.schema)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var (
		constraints []catalog.Constraint
		index       = make(map[string]int)
	)
	generatedKeys := make(map[string]bool)
	for rows.Next() {
		var (
			name       string
			kind       string
			generated  string
			table      string
			condition  string
			refName    string
			deleteRule string
			deferrable string
			deferred   string
			columnName sql.NullString
			position   sql.NullInt64
		)
		if err := rows.Scan(&name, &kind, &generated, &table, &condition, &refName,
			&deleteRule, &deferrable, &deferred, &columnName, &position); err != nil {
			return nil, nil, err
		}
		if generated == "GENERATED NAME" && (kind == "P" || kind == "U") {
			generatedKeys[name] = true
		}

		at, seen := index[name]
		if !seen {
			constraints = append(constraints, r.newConstraint(constraintRow{
				name:       name,
				kind:       kind,
				table:      table,
				condition:  condition,
				refName:    refName,
				deleteRule: deleteRule,
				deferrable: deferrable,
				deferred:   deferred,
			}, referenced))
			at = len(constraints) - 1
			index[name] = at
		}
		if columnName.Valid {
			constraints[at].ColumnNames = append(constraints[at].ColumnNames, columnName.String)
			if constraints[at].ColumnName == "" {
				constraints[at].ColumnName = columnName.String
			}
		}
	}
	return constraints, generatedKeys, rows.Err()
}

// constraintRow is one ALL_CONSTRAINTS row, before its columns are joined on.
type constraintRow struct {
	name       string
	kind       string
	table      string
	condition  string
	refName    string
	deleteRule string
	deferrable string
	deferred   string
}

// newConstraint builds the constraint a row describes.
//
// The check clause and the foreign-key target are filled per kind, and the
// foreign-key half needs the second catalog read: Oracle records the target as
// the NAME of the unique or primary-key constraint it references rather than as
// a table and a column list.
func (r *Reader) newConstraint(row constraintRow, referenced map[string]referencedKey) catalog.Constraint {
	constraint := catalog.Constraint{
		Name:       row.name,
		TableName:  row.table,
		Schema:     r.schema,
		Type:       constraintTypeWord(row.kind),
		Deferrable: row.deferrable == "DEFERRABLE",
		Initially:  initiallyWord(row.deferred),
	}
	switch row.kind {
	case "C":
		clause := strings.TrimSpace(row.condition)
		constraint.CheckClause = &clause
	case "R":
		key := referenced[strings.TrimSpace(row.refName)]
		target := key.table
		constraint.ForeignTable = &target
		constraint.ForeignSchema = r.schema
		constraint.ForeignColumns = key.columns
		if len(key.columns) > 0 {
			first := key.columns[0]
			constraint.ForeignColumn = &first
		}
		if rule := strings.TrimSpace(row.deleteRule); rule != "" {
			constraint.DeleteRule = &rule
		}
	}
	return constraint
}

// referencedKey is the table and column list a foreign key points at.
//
// Oracle records the TARGET of a foreign key as the NAME of the unique or
// primary-key constraint it references, not as a table and a column list, so
// the target has to be resolved through a second read of the same catalog.
type referencedKey struct {
	table   string
	columns []string
}

const referencedKeyQuery = `
SELECT c.constraint_name, c.table_name, col.column_name
FROM all_constraints c
JOIN all_cons_columns col
     ON col.owner = c.owner AND col.constraint_name = c.constraint_name
WHERE c.owner = :1
  AND c.table_name NOT LIKE 'BIN$%'
  AND c.constraint_type IN ('P', 'U')
ORDER BY c.constraint_name, col.position`

func (r *Reader) readReferencedKeys(ctx context.Context) (map[string]referencedKey, error) {
	rows, err := r.db.QueryContext(ctx, referencedKeyQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	keys := make(map[string]referencedKey)
	for rows.Next() {
		var name, table, column string
		if err := rows.Scan(&name, &table, &column); err != nil {
			return nil, err
		}
		key := keys[name]
		key.table = table
		key.columns = append(key.columns, column)
		keys[name] = key
	}
	return keys, rows.Err()
}

// indexQuery reads indexes, excluding the ones Oracle created for a constraint.
//
// CONSTRAINT_INDEX exists on both measured lines and names exactly those: a
// PRIMARY KEY or UNIQUE constraint gets a backing index with the constraint's
// own name, and reading it back as a declared index would make the comparison
// see an index the declaration never wrote beside the constraint it did.
const indexQuery = `
SELECT i.index_name, i.table_name, i.uniqueness, i.index_type
FROM all_indexes i
WHERE i.owner = :1
  AND i.table_name NOT LIKE 'BIN$%'
  AND i.index_name NOT LIKE 'BIN$%'
  AND i.constraint_index = 'NO'
  AND i.index_type <> 'LOB'
ORDER BY i.table_name, i.index_name`

const indexColumnQuery = `
SELECT ic.index_name, ic.column_name, ic.descend, ic.column_position
FROM all_ind_columns ic
WHERE ic.index_owner = :1
ORDER BY ic.index_name, ic.column_position`

func (r *Reader) readIndexes(ctx context.Context) ([]catalog.Index, error) {
	rows, err := r.db.QueryContext(ctx, indexQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []catalog.Index
	for rows.Next() {
		var index catalog.Index
		var uniqueness, indexType string
		if err := rows.Scan(&index.Name, &index.TableName, &uniqueness, &indexType); err != nil {
			return nil, err
		}
		index.Schema = r.schema
		index.IsUnique = uniqueness == "UNIQUE"
		indexes = append(indexes, index)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	columns, err := r.readIndexColumns(ctx)
	if err != nil {
		return nil, err
	}
	for i := range indexes {
		parts := columns[indexes[i].Name]
		indexes[i].Parts = parts
		names := make([]string, 0, len(parts))
		for _, part := range parts {
			names = append(names, part.Name)
		}
		indexes[i].Columns = names
	}
	return indexes, nil
}

func (r *Reader) readIndexColumns(ctx context.Context) (map[string][]catalog.IndexPart, error) {
	rows, err := r.db.QueryContext(ctx, indexColumnQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	parts := make(map[string][]catalog.IndexPart)
	for rows.Next() {
		var index, column, descend string
		var position sql.NullInt64
		if err := rows.Scan(&index, &column, &descend, &position); err != nil {
			return nil, err
		}
		parts[index] = append(parts[index], catalog.IndexPart{
			Name: column,
			Desc: descend == "DESC",
		})
	}
	return parts, rows.Err()
}

// sequenceQuery lists the sequences a declaration could have written.
//
// The NOT EXISTS is load-bearing. An identity column is backed by a sequence
// Oracle creates and owns -- ISEQ$$_73294 and friends -- and ALL_SEQUENCES
// lists it beside the declared ones with no column marking it generated.
// Reading those back made `schema inspect` report two sequences nobody wrote
// for a schema Ptah had just applied from a file that declares none, and the
// comparison would plan a DROP for each on every run.
//
// ALL_TAB_IDENTITY_COLS.SEQUENCE_NAME names exactly those, which is a fact
// about ownership rather than the ISEQ$$_ prefix, which is a naming convention.
const sequenceQuery = `
SELECT s.sequence_name, s.min_value, s.max_value, s.increment_by, s.cycle_flag, s.cache_size,
	       -- Oracle keeps no START WITH. last_number is the next value the
	       -- sequence will issue, rounded UP to the end of the cached block, so
	       -- for an unused sequence it IS the declared start and for a used one
	       -- it is at or beyond where the source stands. Describing it never
	       -- re-issues a value the source already handed out, which is the safe
	       -- direction (stokaro/ptah#2207).
	       s.last_number
FROM all_sequences s
WHERE s.sequence_owner = :1
  AND NOT EXISTS (
        SELECT 1 FROM all_tab_identity_cols i
        WHERE i.owner = s.sequence_owner AND i.sequence_name = s.sequence_name)
ORDER BY s.sequence_name`

func (r *Reader) readSequences(ctx context.Context) ([]catalog.Sequence, error) {
	rows, err := r.db.QueryContext(ctx, sequenceQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sequences []catalog.Sequence
	for rows.Next() {
		var (
			sequence   catalog.Sequence
			minValue   sql.NullString
			maxValue   sql.NullString
			increment  sql.NullString
			cycle      string
			cache      sql.NullString
			lastNumber sql.NullString
		)
		if err := rows.Scan(&sequence.Name, &minValue, &maxValue, &increment, &cycle, &cache, &lastNumber); err != nil {
			return nil, err
		}
		sequence.Schema = r.schema
		sequence.MinValue = numberPointer(minValue)
		sequence.MaxValue = numberPointer(maxValue)
		sequence.Increment = numberPointer(increment)
		sequence.Cache = numberPointer(cache)
		sequence.Start = numberPointer(lastNumber)
		sequence.Cycle = cycle == "Y"
		sequences = append(sequences, sequence)
	}
	return sequences, rows.Err()
}

const viewQuery = `
SELECT v.view_name, NVL(v.text_vc, ' ')
FROM all_views v
WHERE v.owner = :1
ORDER BY v.view_name`

func (r *Reader) readViews(ctx context.Context) ([]catalog.View, error) {
	rows, err := r.db.QueryContext(ctx, viewQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []catalog.View
	for rows.Next() {
		var view catalog.View
		var body string
		if err := rows.Scan(&view.Name, &body); err != nil {
			return nil, err
		}
		view.Schema = r.schema
		view.Body = strings.TrimSpace(body)
		views = append(views, view)
	}
	return views, rows.Err()
}

// matViewQuery reads the materialized views the schema owns.
//
// QUERY is a LONG, so it is selected bare: NVL over it answers ORA-00932,
// expression is of data type CHAR, which is incompatible with expected data
// type LONG. That is the same shape DATA_DEFAULT has in columnQuery above.
const matViewQuery = `
SELECT m.mview_name, m.query
FROM all_mviews m
WHERE m.owner = :1
ORDER BY m.mview_name`

func (r *Reader) readMaterializedViews(ctx context.Context) ([]catalog.MaterializedView, error) {
	rows, err := r.db.QueryContext(ctx, matViewQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []catalog.MaterializedView
	for rows.Next() {
		var view catalog.MaterializedView
		var body sql.NullString
		if err := rows.Scan(&view.Name, &body); err != nil {
			return nil, err
		}
		view.Schema = r.schema
		view.Body = strings.TrimSpace(body.String)
		views = append(views, view)
	}
	return views, rows.Err()
}

// markKeyColumns copies the constraint facts onto the columns they describe.
//
// IsPrimaryKey and IsUnique are derived fields on catalog.Column, and every reader
// here fills them from its constraint read rather than from a column flag,
// because no catalog carries them on the column.
// formatColumnType composes the spelling a declaration would have written.
//
// The catalog splits a type across four columns, and DATA_TYPE alone is not the
// type: a VARCHAR2(200) and a VARCHAR2(4000) both report VARCHAR2, and a
// NUMBER(10) and a bare NUMBER both report NUMBER. Reading only DATA_TYPE made
// `schema inspect` answer `type = NUMBER` for every integer column, which
// compares unequal to the NUMBER(10) the renderer writes -- so a schema Ptah
// had just applied read back as one needing a change to every column.
//
// A type Oracle spells with its own argument is answered by the default arm
// below rather than by a guard above the switch. Measured: a column declared
// TIMESTAMP reports DATA_TYPE `TIMESTAMP(6)` and one declared INTERVAL DAY TO
// SECOND reports `INTERVAL DAY(2) TO SECOND(6)`, and neither name appears in
// the arms that append anything, so both come back whole. A guard was written
// here first and removed: reverting it changed no answer, which is the
// definition of dead code.
func formatColumnType(dataType string, charLength, precision, scale sql.NullInt64) string {
	switch dataType {
	case "NUMBER", "FLOAT":
		if !precision.Valid {
			return dataType
		}
		if scale.Valid && scale.Int64 != 0 {
			return fmt.Sprintf("%s(%d,%d)", dataType, precision.Int64, scale.Int64)
		}
		return fmt.Sprintf("%s(%d)", dataType, precision.Int64)
	case "VARCHAR2", "NVARCHAR2", "CHAR", "NCHAR", "RAW":
		if !charLength.Valid || charLength.Int64 == 0 {
			return dataType
		}
		return fmt.Sprintf("%s(%d)", dataType, charLength.Int64)
	default:
		return dataType
	}
}

func markKeyColumns(schema *catalog.Database) {
	primary := make(map[string]map[string]bool)
	unique := make(map[string]map[string]bool)
	for _, constraint := range schema.Constraints {
		target := unique
		switch constraint.Type {
		case "PRIMARY KEY":
			target = primary
		case "UNIQUE":
		default:
			continue
		}
		if target[constraint.TableName] == nil {
			target[constraint.TableName] = make(map[string]bool)
		}
		for _, column := range constraint.ColumnNames {
			target[constraint.TableName][column] = true
		}
	}
	for i := range schema.Tables {
		table := &schema.Tables[i]
		for j := range table.Columns {
			column := &table.Columns[j]
			column.IsPrimaryKey = primary[table.Name][column.Name]
			column.IsUnique = unique[table.Name][column.Name]
		}
	}
}

// assignDefault decides which of three fields DATA_DEFAULT belongs in.
//
// Oracle stores more than user defaults there, and reading it as one made a
// schema Ptah had just applied read back wrong in two ways at once:
//
//   - a virtual column reported `default = sql("\"size\"*2")` and nothing
//     saying it was generated, so a comparison would drop the generated-ness
//     and write the expression as an ordinary default;
//   - an identity column reported
//     `default = sql("\"PTAH\".\"ISEQ$$_73294\".nextval")`, naming the
//     sequence Oracle created for it -- a default nobody declared, whose name
//     is different in every database, so no two catalogs would ever compare
//     equal.
//
// VIRTUAL_COLUMN and IDENTITY_COLUMN separate the three cases, and in both of
// those two the value is Oracle's own bookkeeping rather than a declaration.
func assignDefault(column *catalog.Column, identity, virtual bool, def sql.NullString) {
	value := defaultPointer(def)
	switch {
	case virtual:
		column.GeneratedExpression = value
		column.GeneratedKind = "VIRTUAL"
	case identity:
		// IsAutoIncrement already carries this fact.
	default:
		column.ColumnDefault = value
	}
}

func constraintTypeWord(kind string) string {
	switch kind {
	case "P":
		return "PRIMARY KEY"
	case "U":
		return "UNIQUE"
	case "R":
		return "FOREIGN KEY"
	case "C":
		return "CHECK"
	default:
		return kind
	}
}

// nullableWord translates Oracle's Y/N into the YES/NO every other reader here
// records.
func nullableWord(nullable string) string {
	if nullable == "Y" {
		return "YES"
	}
	return "NO"
}

// initiallyWord translates DEFERRED/IMMEDIATE into the lower-case spelling the
// AST carries.
func initiallyWord(deferred string) string {
	switch deferred {
	case "DEFERRED":
		return "deferred"
	case "IMMEDIATE":
		return "immediate"
	default:
		return ""
	}
}

// numberPointer converts an Oracle NUMBER that this model stores as an int64,
// and answers nil for one that does not fit.
//
// The bounds are not hypothetical. A sequence created with no MAXVALUE gets
// Oracle's default of 10^28 - 1, and ALL_SEQUENCES reports it in full:
// 9999999999999999999999999999. Scanning that straight into an int64 fails --
// `converting driver.Value type string to a int64: value out of range` -- which
// is how the first live read of a schema Ptah had just applied ended.
//
// Answering nil says "this model does not carry a bound here", which is true,
// rather than a clamped number, which would be a bound nobody declared and
// which a comparison would then plan an ALTER to reach.
func numberPointer(value sql.NullString) *int64 {
	if !value.Valid {
		return nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value.String), 10, 64)
	if err != nil {
		return nil
	}
	return &parsed
}

func intPointer(value sql.NullInt64) *int {
	if !value.Valid {
		return nil
	}
	converted := int(value.Int64)
	return &converted
}

// defaultPointer trims the trailing newline Oracle stores with a column
// default.
//
// DATA_DEFAULT is a LONG holding the text exactly as it was parsed, which for
// `DEFAULT 0` is "0" and for a default written on its own line carries the
// newline with it. A trailing newline is not part of the expression and would
// make every comparison against a declared default report a difference.
func defaultPointer(value sql.NullString) *string {
	if !value.Valid {
		return nil
	}
	trimmed := strings.TrimSpace(value.String)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}
