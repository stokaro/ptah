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
	"database/sql"
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlrunner"
)

// Reader reads schema information from Oracle databases.
type Reader struct {
	db     sqlrunner.Runner
	schema string
}

// NewOracleReader constructs a reader scoped to one schema.
//
// The schema is the owner every query filters on. An empty one is a
// programming error rather than a default: unlike PostgreSQL's `public` or SQL
// Server's `dbo`, Oracle has no schema every database carries, so there is no
// name to fall back to. The connection layer fills it from
// SYS_CONTEXT('USERENV', 'CURRENT_SCHEMA').
func NewOracleReader(db sqlrunner.Runner, schema string) *Reader {
	return &Reader{db: db, schema: strings.ToUpper(strings.TrimSpace(schema))}
}

// ReadSchema reads the objects Ptah renders for Oracle.
func (r *Reader) ReadSchema() (*types.DBSchema, error) {
	schema := &types.DBSchema{
		Schemas: []types.DBSchemaInfo{{Name: r.schema}},
	}

	tables, err := r.readTables()
	if err != nil {
		return nil, fmt.Errorf("oracle: read tables: %w", err)
	}
	schema.Tables = tables

	constraints, generatedKeys, err := r.readConstraints()
	if err != nil {
		return nil, fmt.Errorf("oracle: read constraints: %w", err)
	}
	schema.Constraints = constraints

	indexes, err := r.readIndexes()
	if err != nil {
		return nil, fmt.Errorf("oracle: read indexes: %w", err)
	}
	schema.Indexes = indexes

	sequences, err := r.readSequences()
	if err != nil {
		return nil, fmt.Errorf("oracle: read sequences: %w", err)
	}
	schema.Sequences = sequences

	views, err := r.readViews()
	if err != nil {
		return nil, fmt.Errorf("oracle: read views: %w", err)
	}
	schema.Views = views

	matViews, err := r.readMaterializedViews()
	if err != nil {
		return nil, fmt.Errorf("oracle: read materialized views: %w", err)
	}
	schema.MatViews = matViews

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
func withoutGeneratedKeys(constraints []types.DBConstraint, generated map[string]bool) []types.DBConstraint {
	if len(generated) == 0 {
		return constraints
	}
	kept := make([]types.DBConstraint, 0, len(constraints))
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

func (r *Reader) readTables() ([]types.DBTable, error) {
	rows, err := r.db.Query(tableQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []types.DBTable
	for rows.Next() {
		var table types.DBTable
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

	columns, err := r.readColumns()
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
       c.data_default
FROM all_tab_cols c
WHERE c.owner = :1
  AND c.hidden_column = 'NO'
  AND c.table_name NOT LIKE 'BIN$%'
ORDER BY c.table_name, c.column_id`

func (r *Reader) readColumns() (map[string][]types.DBColumn, error) {
	rows, err := r.db.Query(columnQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	columns := make(map[string][]types.DBColumn)
	for rows.Next() {
		var (
			table      string
			column     types.DBColumn
			charLength sql.NullInt64
			precision  sql.NullInt64
			scale      sql.NullInt64
			nullable   string
			position   sql.NullInt64
			identity   string
			virtual    string
			def        sql.NullString
		)
		if err := rows.Scan(&table, &column.Name, &column.DataType, &charLength,
			&precision, &scale, &nullable, &position, &identity, &virtual, &def); err != nil {
			return nil, err
		}
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

func (r *Reader) readConstraints() ([]types.DBConstraint, map[string]bool, error) {
	// The referenced keys are read BEFORE the constraint rows are opened, and
	// the order is load-bearing rather than tidy.
	//
	// A second query issued while an earlier result set still has rows to read
	// is served only if the driver can take another connection from the pool.
	// go-ora v2.9.0 does; go-ora v3.0.1 answers EOF instead, and the
	// distinction there is undrained rather than open:
	//
	//	first drained, not closed, then second   ok
	//	first undrained, then second             EOF
	//
	// Depending on the pool for it is depending on an accident of the driver,
	// so this read does not: it takes its second query out of the nest rather
	// than trusting that a nested one is served. readIndexes needs no such
	// change and has the same property for the same reason -- its loop drains
	// before it calls readIndexColumns (stokaro/ptah#1888).
	referenced, err := r.readReferencedKeys()
	if err != nil {
		return nil, nil, err
	}

	rows, err := r.db.Query(constraintQuery, r.schema)
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	var (
		constraints []types.DBConstraint
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
func (r *Reader) newConstraint(row constraintRow, referenced map[string]referencedKey) types.DBConstraint {
	constraint := types.DBConstraint{
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

func (r *Reader) readReferencedKeys() (map[string]referencedKey, error) {
	rows, err := r.db.Query(referencedKeyQuery, r.schema)
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

func (r *Reader) readIndexes() ([]types.DBIndex, error) {
	rows, err := r.db.Query(indexQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var indexes []types.DBIndex
	for rows.Next() {
		var index types.DBIndex
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

	columns, err := r.readIndexColumns()
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

func (r *Reader) readIndexColumns() (map[string][]types.DBIndexPart, error) {
	rows, err := r.db.Query(indexColumnQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	parts := make(map[string][]types.DBIndexPart)
	for rows.Next() {
		var index, column, descend string
		var position sql.NullInt64
		if err := rows.Scan(&index, &column, &descend, &position); err != nil {
			return nil, err
		}
		parts[index] = append(parts[index], types.DBIndexPart{
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
SELECT s.sequence_name, s.min_value, s.max_value, s.increment_by, s.cycle_flag, s.cache_size
FROM all_sequences s
WHERE s.sequence_owner = :1
  AND NOT EXISTS (
        SELECT 1 FROM all_tab_identity_cols i
        WHERE i.owner = s.sequence_owner AND i.sequence_name = s.sequence_name)
ORDER BY s.sequence_name`

func (r *Reader) readSequences() ([]types.DBSequence, error) {
	rows, err := r.db.Query(sequenceQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sequences []types.DBSequence
	for rows.Next() {
		var (
			sequence  types.DBSequence
			minValue  sql.NullString
			maxValue  sql.NullString
			increment sql.NullString
			cycle     string
			cache     sql.NullString
		)
		if err := rows.Scan(&sequence.Name, &minValue, &maxValue, &increment, &cycle, &cache); err != nil {
			return nil, err
		}
		sequence.Schema = r.schema
		sequence.MinValue = numberPointer(minValue)
		sequence.MaxValue = numberPointer(maxValue)
		sequence.Increment = numberPointer(increment)
		sequence.Cache = numberPointer(cache)
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

func (r *Reader) readViews() ([]types.DBView, error) {
	rows, err := r.db.Query(viewQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []types.DBView
	for rows.Next() {
		var view types.DBView
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

func (r *Reader) readMaterializedViews() ([]types.DBMatView, error) {
	rows, err := r.db.Query(matViewQuery, r.schema)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var views []types.DBMatView
	for rows.Next() {
		var view types.DBMatView
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
// IsPrimaryKey and IsUnique are derived fields on DBColumn, and every reader
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

func markKeyColumns(schema *types.DBSchema) {
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
func assignDefault(column *types.DBColumn, identity, virtual bool, def sql.NullString) {
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
