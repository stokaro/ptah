package compare

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/catalogfield"
	"ptah.run/internal/chkey"
	"ptah.run/internal/chtype"
	"ptah.run/internal/constraintowner"
	"ptah.run/internal/exprkey"
	"ptah.run/internal/normalize"
	"ptah.run/internal/oracletype"
	"ptah.run/internal/sqlitekey"
	"ptah.run/internal/typechange"
	"ptah.run/internal/ydbsequence"
	"ptah.run/internal/ydbtype"
	"ptah.run/migration/internal/generatedschema"
	"ptah.run/migration/schemadiff/difftypes"
)

// TableColumns performs detailed column-level comparison within a specific table.
//
// This function is responsible for the complex task of comparing column structures
// between a generated table definition and an existing database table. It handles
// embedded field processing, column mapping, and detailed property comparison.
//
// # Embedded Field Processing
//
// The function's most complex aspect is handling embedded fields:
//  1. **Field Expansion**: Uses transform.ProcessEmbeddedFields() to expand embedded structs
//  2. **Field Combination**: Merges original fields with embedded-generated fields
//  3. **Struct Filtering**: Only processes fields belonging to the target struct
//
// This ensures that embedded fields (like timestamps, audit info) are properly
// compared against their corresponding database columns.
//
// # Comparison Algorithm
//
// The function performs comparison in three phases:
//  1. **Column Discovery**: Creates lookup maps for efficient column comparison
//  2. **Addition/Removal Detection**: Identifies new and removed columns
//  3. **Modification Analysis**: Compares properties of existing columns
//
// # Example Scenarios
//
// **Embedded field handling**:
//
//	```go
//	type User struct {
//	    ID   int    `db:"id"`
//	    Name string `db:"name"`
//	    Timestamps // Embedded struct with CreatedAt, UpdatedAt
//	}
//	```
//	The function expands Timestamps fields and compares them against database columns.
//
// **Column addition detection**:
//   - Generated schema has "email" column
//   - Database table doesn't have "email" column
//   - Result: "email" added to TableDiff.ColumnsAdded
//
// **Column modification detection**:
//   - Both have "name" column
//   - Generated: VARCHAR(255), Database: VARCHAR(100)
//   - Result: ColumnDiff added to TableDiff.ColumnsModified
//
// # Parameters
//
//   - genTable: Generated table definition from Go struct annotations
//   - dbTable: Current database table structure from introspection
//   - generated: Complete parse result containing all fields and embedded field definitions
//
// # Return Value
//
// Returns a TableDiff containing:
//   - ColumnsAdded: New columns that need to be added
//   - ColumnsRemoved: Existing columns that should be removed
//   - ColumnsModified: Columns with property differences
//
// # Performance Considerations
//
// - Time Complexity: O(n + m + k) where n=generated columns, m=database columns, k=embedded fields
// - Space Complexity: O(n + m) for lookup maps
// - Embedded field processing adds overhead but is necessary for accurate comparison
//
// # Output Consistency
//
// Column lists are sorted alphabetically for deterministic output and reliable testing.
func TableColumns(genTable schemamodel.Table, dbTable catalog.Table, desired *schemamodel.Database) difftypes.TableDiff {
	return TableColumnsWithDialect(genTable, dbTable, desired, "")
}

// TableColumnsWithDialect compares one table's columns with dialect-aware
// normalization for catalog-rewritten generated expressions.
func TableColumnsWithDialect(
	genTable schemamodel.Table,
	dbTable catalog.Table,
	desired *schemamodel.Database,
	dialect string,
) difftypes.TableDiff {
	return TableColumnsWithSemantics(
		genTable,
		dbTable,
		desired,
		dialect,
		identifier.ForDialect(dialect),
	)
}

// TableColumnsWithSemantics compares one table's columns using explicit
// catalog identifier rules.
func TableColumnsWithSemantics(
	genTable schemamodel.Table,
	dbTable catalog.Table,
	desired *schemamodel.Database,
	dialect string,
	semantics identifier.Semantics,
) difftypes.TableDiff {
	return tableColumnsWithSemantics(
		genTable,
		dbTable,
		desired,
		dialect,
		semantics,
		columnUniqueness{},
		ServerSpellings{},
		nil,
	)
}

func tableColumnsWithSemantics(
	genTable schemamodel.Table,
	dbTable catalog.Table,
	desired *schemamodel.Database,
	dialect string,
	semantics identifier.Semantics,
	uniqueness columnUniqueness,
	spellings ServerSpellings,
	caps capability.Capabilities,
) difftypes.TableDiff {
	tableDiff := difftypes.TableDiff{
		TableName: genTable.QualifiedName(),
		// Everything the declaration says about this table, for the rebuild a
		// dialect reaches for when ALTER TABLE cannot express the change.
		Desired: difftypes.TableDeclarationFor(desired, genTable, semantics),
	}

	// Create maps for quick lookup
	genFields := generatedschema.FieldsForTable(desired, genTable)
	genColumns := make(map[string]schemamodel.Field)
	for _, field := range genFields {
		genColumns[semantics.ColumnIdentityKey(field.Name)] = field
	}
	keyColumns := sqlitekey.KeyColumns(genTable, genFields)
	clickHouseKey, clickHouseKeyDeclared := clickHouseDeclaredKey(dialect, genTable, genFields)

	dbColumns := make(map[string]catalog.Column)
	for _, col := range dbTable.Columns {
		dbColumns[semantics.ColumnIdentityKey(col.Name)] = col
	}

	desiredDomains := desiredDomainIdentities(desired, semantics)

	// Find added and removed columns
	for identity, column := range genColumns {
		if _, exists := dbColumns[identity]; !exists {
			tableDiff.ColumnsAdded = append(tableDiff.ColumnsAdded, column.Clone())
		}
	}

	for identity, column := range dbColumns {
		if _, exists := genColumns[identity]; !exists {
			tableDiff.ColumnsRemoved = append(tableDiff.ColumnsRemoved, removedColumn(column, dialect))
		}
	}

	// Find modified columns
	for identity, genCol := range genColumns {
		if dbCol, exists := dbColumns[identity]; exists {
			switch {
			case clickHouseKeyDeclared:
				genCol.Primary = clickHouseKey[genCol.Name]
			case columnInTablePrimaryKey(genTable, genCol.Name) || columnInDeclaredPrimaryKey(desired, genTable, genCol.Name):
				genCol = normalizeTablePrimaryKeyColumn(genCol, dbCol, dialect)
			}
			if sqliteKeyColumnImpliesNotNull(dialect, genTable, keyColumns, genCol) {
				genCol.Nullable = false
			}
			if dbCol.IsPrimaryKey && sqliteRowidAlias(dialect, genTable, keyColumns, genCol) {
				genCol.Nullable = dbCol.IsNullable == "YES"
			}
			columnKey := newColumnIdentityForTable(genTable.Schema, genTable.Name, identity, semantics)
			genCol, dbCol = uniqueness.compared(columnKey, genCol, dbCol)
			colDiff := columnsWithDesiredDomains(genCol, dbCol, dialect, desiredDomains, columnContext{
				schema:               genTable.Schema,
				table:                genTable.Name,
				generatedExpressions: spellings.Generated,
				columnSpellings:      spellings.Columns,
				defaultIntSize:       spellings.DefaultIntSize,
				serialSequences:      caps.Has(capability.SerialSequenceOptions),
			})
			// A comment-only difference has no entry in Changes, and it is
			// still a difference: without the second condition a column whose
			// comment was rewritten in the declaration never reached the
			// planner (stokaro/ptah#2168).
			if len(colDiff.Changes) > 0 || colDiff.CommentChange != nil ||
				colDiff.NotNullConstraintNameChange != nil {
				tableDiff.ColumnsModified = append(tableDiff.ColumnsModified, colDiff)
			}
		}
	}

	// Sort for consistent output
	sortColumns(tableDiff.ColumnsAdded)
	sortColumns(tableDiff.ColumnsRemoved)
	sort.Slice(tableDiff.ColumnsModified, func(i, j int) bool {
		return tableDiff.ColumnsModified[i].ColumnName < tableDiff.ColumnsModified[j].ColumnName
	})
	tableDiff.ColumnKeyNames = gainedKeyNames(tableDiff, genTable, desired, dialect)

	return tableDiff
}

// commentChange reports a comment transition, or nil when there is none.
//
// It takes both sides because absence is a state a planner has to act on: a
// comment the database holds and the declaration does not is a removal, and the
// desired side alone cannot say so. This is the shape [rowTTLChange] uses, for
// the same reason.
// notNullConstraintNameChange applies the repository's omitted-attribute rule
// to the NOT NULL constraint name: an explicit desired name is compared, an
// omitted one leaves the actual name unmanaged.
//
// The empty-desired guard is load-bearing rather than defensive. PostgreSQL 18
// names EVERY NOT NULL and offers no catalog flag separating an author-supplied
// name from a generated one, so on that target the current side is populated
// for every non-nullable column in the database. Comparing an omitted
// declaration against it would report a rename on every column of every table
// nobody touched, and no apply could settle it: the next read would return the
// new generated name and report the difference again.
//
// It has the cost the rule always has, stated in stokaro/ptah#2260 for the
// other optional attributes: removing a previously managed name by deleting it
// from the declaration is not supported. Deleting it makes the name unmanaged;
// it does not request a rename back to a generated one (stokaro/ptah#2161).
func notNullConstraintNameChange(desired, current string) *difftypes.NotNullConstraintNameChange {
	if desired == "" || desired == current {
		return nil
	}
	return &difftypes.NotNullConstraintNameChange{Current: current, Desired: desired}
}

func commentChange(desired, current string) *difftypes.CommentChange {
	if desired == current {
		return nil
	}
	return &difftypes.CommentChange{Current: current, Desired: desired}
}

// Columns performs detailed property-level comparison between a generated column and database column.
//
// This function is the most granular level of schema comparison, analyzing individual
// column properties to detect differences that require migration. It handles complex
// cross-database type normalization and property comparison logic.
//
// # Property Comparison Categories
//
// The function compares five main categories of column properties:
//  1. **Data Types**: Handles cross-database type normalization and comparison
//  2. **Nullability**: Considers primary key implications and explicit nullable settings
//  3. **Primary Key**: Compares primary key constraint status
//  4. **Uniqueness**: Compares unique constraint status
//  5. **Default Values**: Handles auto-increment special cases and type-specific normalization
//
// # Complex Logic Areas
//
// **Type Normalization**:
//   - Uses Type() to handle cross-database type variations
//   - Considers both DataType and UDTName from database introspection
//   - Handles PostgreSQL user-defined types vs standard types
//
// **Nullability Logic**:
//   - Primary key columns are always NOT NULL regardless of field definition
//   - Explicit nullable settings override default behavior
//   - Database "YES"/"NO" strings converted to boolean for comparison
//
// **Auto-increment Handling**:
//   - SERIAL columns have special default value handling
//   - Database shows sequence defaults, but entities expect empty defaults
//   - Prevents false positives for auto-increment columns
//
// # Example Comparisons
//
// **Type difference detection**:
//
//	```
//	Generated: VARCHAR(255)
//	Database:  VARCHAR(100)
//	Result:    Changes["type"] = "varchar -> varchar" (normalized)
//	```
//
// **Nullability change**:
//
//	```
//	Generated: nullable=false
//	Database:  nullable=true
//	Result:    Changes["nullable"] = "true -> false"
//	```
//
// **Primary key promotion**:
//
//	```
//	Generated: primary=true
//	Database:  primary=false
//	Result:    Changes["primary_key"] = "false -> true"
//	```
//
// **Default value normalization**:
//
//	```
//	Generated: default=""
//	Database:  default_expr="NULL"
//	Result:    No change (both normalize to empty string)
//	```
//
// # Parameters
//
//   - genCol: Generated column definition from Go struct field
//   - dbCol: Current database column from introspection
//
// # Return Value
//
// Returns a ColumnDiff with:
//   - ColumnName: Name of the column being compared
//   - Changes: Map of property changes in "old -> new" format
//
// # Cross-Database Considerations
//
// The function handles database-specific variations:
//   - **PostgreSQL**: UDT names, SERIAL types, native boolean types
//   - **MySQL/MariaDB**: TINYINT boolean representation, AUTO_INCREMENT
//   - **Type mapping**: Intelligent normalization for accurate comparison
func Columns(genCol schemamodel.Field, dbCol catalog.Column) difftypes.ColumnDiff {
	return ColumnsWithDialect(genCol, dbCol, "")
}

// ColumnsWithDialect compares two columns using dialect-specific expression
// normalization where catalog readback rewrites equivalent SQL.
//
// It knows nothing about which type names the desired schema declares as
// domains, so it decides a domain from the DATABASE column alone. Every
// comparison that has a desired schema to consult goes through
// tableColumnsWithSemantics, which passes that set down.
func ColumnsWithDialect(genCol schemamodel.Field, dbCol catalog.Column, dialect string) difftypes.ColumnDiff {
	return columnsWithDesiredDomains(genCol, dbCol, dialect, nil, columnContext{})
}

// columnContext carries what a column comparison needs about the table it sits
// in and about the server that answered for the schema.
//
// It is a struct rather than three more parameters because the two facts arrive
// together and are read together: the generated-expression map is keyed by the
// column's qualified name, so a comparison that knows the map and not the table
// can look nothing up.
type columnContext struct {
	// schema and table qualify the column for
	// [config.CompareOptions.GeneratedExpressions].
	schema string
	table  string
	// generatedExpressions is that map, nil when nobody asked a server.
	generatedExpressions map[string]config.GeneratedExpression
	// columnSpellings is [config.CompareOptions.ColumnSpellings], nil when
	// nobody asked a server.
	columnSpellings map[string]config.ColumnSpelling
	// defaultIntSize is [ServerSpellings.DefaultIntSize].
	defaultIntSize int
	// serialSequences compares the start and the increment of a Serial
	// column's sequence, on a target with [capability.SerialSequenceOptions].
	serialSequences bool
}

// columnSpelling returns the server's spelling of the column's type and
// default, or false when no server answered for it.
func (ctx columnContext) columnSpelling(dialect, column string) (config.ColumnSpelling, bool) {
	spelling, ok := ctx.columnSpellings[exprkey.Column(dialect, ctx.schema, ctx.table, column)]
	return spelling, ok && spelling.Resolved
}

func columnsWithDesiredDomains(
	genCol schemamodel.Field,
	dbCol catalog.Column,
	dialect string,
	desiredDomains map[string]domainIdentity,
	ctx columnContext,
) difftypes.ColumnDiff {
	colDiff := difftypes.ColumnDiff{
		ColumnName:    genCol.Name,
		Desired:       genCol.Clone(),
		Changes:       make(map[string]string),
		CommentChange: commentChange(genCol.Comment, dbCol.Comment),
		NotNullConstraintNameChange: notNullConstraintNameChange(
			genCol.NotNullConstraintName, dbCol.NotNullConstraintName),
	}

	// ClickHouse-only guard: older goschema models cannot express
	// MATERIALIZED / ALIAS / EPHEMERAL columns. Once the schema side carries a
	// generated expression, compare it normally below.
	if dbCol.GeneratedKind != "" && genCol.GeneratedExpression == "" {
		return colDiff
	}

	dbRawType := dbCol.RawType()

	// The default comparison below asks what CATEGORY each side's type is, so it
	// keeps using the normalizer even where the type comparison above does not:
	// a default is normalized as a boolean or a number.
	//
	// For a domain column both sides receive the DOMAIN NAME, not the domain's
	// base type, because Column.RawType answers with the domain and the desired
	// side spells the column as the domain too. So `d_bool` reaches the
	// normalizer, which folds by substring and lands on "boolean" by luck, while
	// `positive` folds to nothing and the boolean/decimal/temporal branches of
	// normalize.DefaultValue are skipped for that column.
	//
	// Passing the base type on the database side alone would be worse, not
	// better: the desired side is a schemamodel.Field carrying only a type name and
	// has no base type to reach for, so the two sides would land in different
	// categories and a database would stop being in sync with itself -- the
	// asymmetry stokaro/ptah#1242 is about. Both sides receiving the same
	// category, right or wrong, is what keeps a self-diff quiet, and no live
	// churn from the folding has been measured. Fixing it properly means giving
	// the desired side a base type to answer with.
	// dbType is deliberately discarded: both defaults below are normalized
	// under the DESIRED type, for the reason stated there.
	genType, _ := normalizeColumnTypesForDialect(genCol, dbRawType, dialect)

	// A type the server spells the way the catalog does is the same type,
	// whatever the declaration wrote: varchar(10)[] is stored and read back as
	// character varying(10)[]. A spelling that differs is not decided here; the
	// comparison below still folds what it knows how to fold.
	spelling, spelled := ctx.columnSpelling(dialect, genCol.Name)
	sameType := spelled && strings.EqualFold(spelling.Type, strings.TrimSpace(dbRawType))
	declared := genCol
	declared.Type = resolvedDeclaredType(genCol.Type, dialect, ctx.defaultIntSize)
	if change := columnTypeChange(declared, dbCol, dbRawType, dialect, desiredDomains); change != "" && !sameType {
		colDiff.Changes["type"] = change
	}

	// Compare nullable. On the engines that enforce it, a primary key column is
	// NOT NULL whatever the field says, and the reader reports it that way, so
	// the generated side is normalized to match or every primary key would show
	// a permanent diff.
	genNullable := genCol.Nullable
	if genCol.Primary && primaryKeyImpliesNotNull(dialect) {
		genNullable = false
	}
	if writesYDBSerial(genCol, dialect) {
		genNullable = false
	}
	dbNullable := dbCol.IsNullable == "YES"
	if genNullable != dbNullable {
		colDiff.Changes["nullable"] = fmt.Sprintf("%t -> %t", dbNullable, genNullable)
	}
	if ctx.serialSequences && serialSequenceChanges(colDiff.Changes, genCol, dbCol) {
		colDiff.CurrentSequenceRestart = dbCol.SequenceRestart
	}

	// Compare primary key
	genPrimary := genCol.Primary
	dbPrimary := dbCol.IsPrimaryKey
	if genPrimary != dbPrimary {
		colDiff.Changes["primary_key"] = fmt.Sprintf("%t -> %t", dbPrimary, genPrimary)
	}

	// Compare unique
	genUnique := genCol.Unique
	dbUnique := dbCol.IsUnique
	if genUnique != dbUnique {
		colDiff.Changes["unique"] = fmt.Sprintf("%t -> %t", dbUnique, genUnique)
	}
	var resolution *config.GeneratedExpression
	if entry, ok := ctx.generatedExpressions[exprkey.Generated(dialect, ctx.schema, ctx.table, genCol.Name)]; ok {
		resolution = &entry
	}
	if diff := generatedColumnDiff(genCol, dbCol, dialect, resolution); diff != "" {
		colDiff.Changes["generated"] = diff
	}

	if key, change := columnDefaultChange(genCol, dbCol, genType, dialect, spelling, spelled); change != "" {
		colDiff.Changes[key] = change
	}

	return colDiff
}

// columnDefaultChange compares a column's declared default with the live one
// and returns the change key and text, or an empty change.
func columnDefaultChange(
	genCol schemamodel.Field,
	dbCol catalog.Column,
	genType, dialect string,
	spelling config.ColumnSpelling,
	spelled bool,
) (key, change string) {
	// Compare default values (simplified)
	genDefault := genCol.Default
	if genDefault == "" {
		genDefault = genCol.DefaultExpr
	}
	if platform.NormalizeDialect(dialect) == platform.YDB {
		dbDefault := ""
		if dbCol.ColumnDefault != nil {
			dbDefault = *dbCol.ColumnDefault
		}
		return ydbDefaultChange(genCol, dbCol, genDefault, dbDefault)
	}
	// Default is a literal value, while DefaultExpr is SQL. The renderer quotes
	// the literal NULL, so feed its quoted form to the normalizer too. Otherwise
	// a real text default compares equal to no default at all.
	if (genCol.DefaultSet || genCol.Default != "") && strings.EqualFold(strings.TrimSpace(genCol.Default), "NULL") {
		genDefault = "'" + genCol.Default + "'"
	}
	genDefault = renderedDefaultForDialect(genDefault, genCol.Type, dialect)
	dbDefault := ""
	if dbCol.ColumnDefault != nil {
		dbDefault = *dbCol.ColumnDefault
	}

	// Skip the sequence-backed default only when the desired column declares no
	// default of its own AND the database treats it as an auto-increment/SERIAL
	// column — the type implies the sequence, so the database's nextval(...)
	// default is expected and not a difference. When the desired declares an
	// explicit default (e.g. a column that draws from a standalone sequence via
	// default_expr="nextval('seq')"), compare it normally; normalize.DefaultValue
	// reconciles the ::regclass read-back form (issue #675). The genDefault==""
	// guard alone carries the feature, so a genuine sequence default that the
	// model does not declare is still reported as drift.
	skipImplicitSequenceDefault := genDefault == "" &&
		(dbCol.IsAutoIncrement || strings.Contains(strings.ToUpper(genCol.Type), "SERIAL"))
	// A default the server spells as the catalog reports it is the same
	// default: '2020-01-01'::timestamp with time zone is stored with its time
	// and zone filled in.
	sameDefault := spelled && spelling.Default == strings.TrimSpace(dbDefault)
	if !skipImplicitSequenceDefault && !sameDefault {
		// Both sides are normalized under the DESIRED type, not each under its
		// own. A default's meaning depends on the column's type -- `0` is
		// `false` on a boolean and `0` on an integer -- and after this plan
		// runs the column has the desired type, so "does the live default
		// already say what we would write" is a question asked in the target's
		// terms.
		//
		// Normalized under two types it could answer no for two spellings of
		// one value. Measured on SQLite: a hand-made `b BOOLEAN DEFAULT 0`
		// compared against Ptah's own description of it, whose rendered type is
		// `integer`, reported `default_expr: 0 -> 0` -- a change between a
		// value and itself, beside the type change that was real
		// (stokaro/ptah#2041).
		//
		// It changes nothing where the two types agree, which is every
		// comparison that does not already report a type change.
		normalizedDbDefault := normalize.DefaultValue(dbDefault, genType)

		idxName := "default"
		if normalize.IsDefaultExpr(dbDefault) {
			idxName = "default_expr"
		}

		normalizeGenDefaultFn := normalize.DefaultValue(genDefault, genType)

		if normalizeGenDefaultFn != normalizedDbDefault {
			return idxName, fmt.Sprintf("%s -> %s", dbDefault, genDefault)
		}
	}

	return "", ""
}

// primaryKeyImpliesNotNull reports whether the dialect makes a primary key
// column NOT NULL on its own, so the comparator may normalize the generated
// side to the reader's answer.
//
// SQLite does not decide this from the dialect at all. On a rowid table
// `id INTEGER PRIMARY KEY` is a rowid alias: `pragma table_info.notnull` is 0,
// an explicit NULL insert is accepted, and a rowid is assigned for it.
// Normalizing anyway made a schema whose key column SQLite reports as nullable
// diff forever against the very DDL that created it (stokaro/ptah#1235).
//
// SQLite's answer depends on the table's shape, which this predicate cannot see,
// so it answers with the shape that has no NOT NULL to normalize -- the ordinary
// rowid table. [sqliteKeyColumnImpliesNotNull] carries the STRICT and
// WITHOUT ROWID halves, where SQLite does enforce NOT NULL on a key column, and
// the caller applies both.
//
// YDB answers true for a different reason than the SQL engines. Its key does
// not imply NOT NULL -- a key column declared without it is nullable and takes
// one row whose key is NULL -- but the YDB renderer writes NOT NULL on every key
// column, because no Ptah declaration can ask for a nullable key and every
// other engine makes a key NOT NULL. Normalizing the declared side to that
// rendering is what keeps a key column declared without not_null from
// differing from the column Ptah built; and a catalog key the server reports
// as nullable, built outside Ptah, still differs from the declaration and is
// reported. Measured on 26.2.1.14: `ALTER COLUMN id DROP NOT NULL` on a key
// column is accepted, so a comparison that treated the declared key as
// nullable would plan exactly that on the second apply.
func primaryKeyImpliesNotNull(dialect string) bool {
	return platform.NormalizeDialect(dialect) != platform.SQLite
}

// sqliteKeyColumnImpliesNotNull reports whether SQLite -- not SQL in general --
// enforces NOT NULL on this key column because of the table's shape.
//
// A STRICT or WITHOUT ROWID table makes its key columns NOT NULL, and the reader
// reports them that way from `pragma table_info`, so the generated side has to
// be normalized to match or the table drifts against the DDL that created it and
// every plan is another full table rebuild. The rowid alias of a STRICT table
// stays nullable; see [sqlitekey] for the measured shape table.
func sqliteKeyColumnImpliesNotNull(
	dialect string,
	table schemamodel.Table,
	keyColumns []string,
	field schemamodel.Field,
) bool {
	if platform.NormalizeDialect(dialect) != platform.SQLite {
		return false
	}
	return sqlitekey.ImpliesNotNull(table, keyColumns, field)
}

// sqliteRowidAlias reports whether field is the rowid alias of a SQLite table,
// the one key column whose NOT NULL flag changes nothing the server does; see
// [sqlitekey.IsRowidAlias]. The caller takes the catalog's flag for it when the
// catalog column is the key too, so `id INTEGER PRIMARY KEY` and `id integer
// NOT NULL PRIMARY KEY` compare equal rather than rebuilding the table. A column
// that becomes the key keeps its nullability change beside the key change.
func sqliteRowidAlias(
	dialect string,
	table schemamodel.Table,
	keyColumns []string,
	field schemamodel.Field,
) bool {
	if platform.NormalizeDialect(dialect) != platform.SQLite {
		return false
	}
	return sqlitekey.IsRowidAlias(table, keyColumns, field)
}

// columnTypeChange returns the "database -> desired" row for a column's type,
// or "" when the two sides describe the same type.
//
// A column whose declared type is a DOMAIN is decided by identity and never by
// normalize.Type. That matcher works by substring -- anything containing "int"
// is "integer" and anything containing "text" is "text" -- which is safe for
// type names and wrong for a name a schema author picked. Measured on
// PostgreSQL 17.10:
//
//	CREATE DOMAIN waypoint AS integer CHECK (VALUE > 0);
//	CREATE DOMAIN context  AS integer;
//	CREATE TABLE t (id serial PRIMARY KEY, a waypoint NOT NULL, b context NOT NULL);
//
// "waypoint" contains "int" and "context" contains "text", so against a desired
// `a bigint, b text` both columns compared EQUAL and neither
// ALTER COLUMN ... TYPE was planned -- while the plan kept its
// DROP DOMAIN ... CASCADE. Applying it exited 0, said "Schema apply completed
// successfully", and left the table with only its id column: the CASCADE took
// the two columns and their data because nothing had converted them first
// (stokaro/ptah#1138).
//
// A domain therefore agrees only with a desired type that names the SAME
// domain. Anything else -- a base type, a different domain, a plain type that
// happens to share a substring with the name -- is a change and is reported.
//
// The rule holds when only the DESIRED side names a domain, too. A plain
// integer column against a desired schema that declares `waypoint` and types
// the column with it is a change the pinned Atlas community binary v1.3.0 also
// plans, measured on the same two databases:
//
//	ALTER TABLE "t" ALTER COLUMN "a" TYPE waypoint;
//
// where both sides of this comparator's normalization say "integer" and Ptah
// reported the schemas synced.
//
// A domain's identity is (schema, name), and the name alone is not it. Measured
// on the same server, one database holding public.status and one holding
// other.status, with a row in the table:
//
//	ptah-compat schema diff --from <public.status> --to <other.status>
//	  DROP DOMAIN IF EXISTS "status" CASCADE;      <- and no ALTER
//
// so `schema apply --auto-approve` exited 0, said "Schema apply completed
// successfully", and left the table with only its id column. Comparing the two
// halves of the identity is what makes that a reported change.
func columnTypeChange(
	genCol schemamodel.Field,
	dbCol catalog.Column,
	dbRawType, dialect string,
	desiredDomains map[string]domainIdentity,
) string {
	dbDomain, dbIsDomain := dbColumnDomainIdentity(dbCol)
	desiredDomain, desiredIsDomain := desiredColumnDomainIdentity(genCol.Type, desiredDomains)
	if dbIsDomain || desiredIsDomain {
		if dbIsDomain && domainIdentitiesMatch(dbDomain, desiredDomain) {
			return ""
		}
		return fmt.Sprintf("%s -> %s", dbRawType, strings.TrimSpace(genCol.Type))
	}

	if sameQualifiedTypeName(genCol.Type, dbRawType, dbCol.UDTSchema, dialect) {
		return ""
	}

	switch platform.NormalizeDialect(dialect) {
	case platform.YDB:
		return ydbColumnTypeChange(genCol, dbCol, dbRawType)
	case platform.ClickHouse:
		return clickHouseColumnTypeChange(genCol, dbCol, dbRawType)
	}
	genType, dbType := normalizeColumnTypesForDialect(genCol, dbRawType, dialect)
	switch {
	case genType != dbType:
		return typeChangeText(dbRawType, genCol.Type, dbType, genType, dialect)
	case shouldReportSizedTypeChange(dbRawType, genCol.Type, dialect):
		return fmt.Sprintf("%s -> %s", dbRawType, genCol.Type)
	}
	return ""
}

// sameQualifiedTypeName reports whether a declared type written with a schema
// or in quotes -- `app.mood`, `"app"."Mood"[]`, as pg_dump writes a type
// outside the search path -- names the type the catalog reports.
//
// The catalog spells such a type its own way: unquoted where no quote is
// needed, and by its bare name, udt_name, unless it is an array. Compared as
// text, `"s".mood` against `mood` planned an ALTER COLUMN ... TYPE on every run
// (stokaro/ptah#3620). So the two are compared as names: the same bare name,
// the same array depth, and the same schema, which the catalog reports as
// udtSchema when its spelling leaves it out. A reported type whose schema
// nothing says is not matched, because two schemas may each hold a type of
// that name and moving a column between them is a change.
//
// A declared type without a schema, or with a modifier list, is left to the
// comparison below, which is what that comparison is for.
func sameQualifiedTypeName(declared, reported, udtSchema, dialect string) bool {
	if !isPostgresFamilyDialect(dialect) {
		return false
	}
	declared, reported = strings.TrimSpace(declared), strings.TrimSpace(reported)
	if !strings.ContainsAny(declared, `."`) || strings.ContainsAny(declared+reported, "()") {
		return false
	}
	declaredBase, declaredDepth := splitArrayDepth(declared)
	reportedBase, reportedDepth := splitArrayDepth(reported)
	if declaredDepth != reportedDepth {
		return false
	}
	declaredSchema, declaredName := splitQualifiedTypeName(declaredBase)
	reportedSchema, reportedName := splitQualifiedTypeName(reportedBase)
	if reportedSchema == "" {
		reportedSchema = udtSchema
	}
	if foldDomainPart(declaredName) != foldDomainPart(reportedName) {
		return false
	}
	return declaredSchema == "" || foldDomainPart(declaredSchema) == foldDomainPart(reportedSchema)
}

// splitArrayDepth splits the trailing [] pairs off a type name.
func splitArrayDepth(typeName string) (base string, depth int) {
	for {
		trimmed, found := strings.CutSuffix(strings.TrimSpace(typeName), "[]")
		if !found {
			return typeName, depth
		}
		typeName, depth = trimmed, depth+1
	}
}

func isPostgresFamilyDialect(dialect string) bool {
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres, platform.CockroachDB, platform.YugabyteDB:
		return true
	default:
		return false
	}
}

// domainIdentity is what a domain IS: the schema that holds it and its own
// name, both case-folded. An empty schema means "not said, and nothing here can
// resolve it" -- never "the default schema", which is a value this type carries
// spelled out.
type domainIdentity struct {
	schema string
	name   string
}

// foldDomainPart canonicalizes one half of an identity. PostgreSQL folds an
// unquoted identifier to lower case on the way in, so the catalog spelling and
// the spelling a schema author typed differ in case and name one domain.
func foldDomainPart(value string) string {
	return strings.ToLower(unquoteIdentifier(value))
}

// dbColumnDomainIdentity returns the identity of the DOMAIN a database column
// is declared with, and false when its declared type is not a domain.
//
// DomainName/DomainSchema are the fact: information_schema records them for
// exactly the columns whose declared type is a domain, and nothing else in a
// column's catalog row separates a domain from a plain column of the same base
// type. They are read together, because half an identity is not one: a
// comparator holding only "status" for a column of public.status calls it equal
// to a desired other.status, reports no change, and lets the plan's
// DROP DOMAIN ... CASCADE take the column (stokaro/ptah#1138).
//
// FormattedType is the server's own format_type of the same domain. It is
// consulted for the schema qualifier the server writes when the search path
// forces one, and it still counts on its own, because a caller may carry it
// without the catalog columns and because the failure it guards is destructive
// while its cost is not: reading a spelling as an identity can only ever REPORT
// a change that normalization would have folded away, never hide one. The one
// shape it must not claim is an array, whose spelling is a type rather than an
// identifier -- and format_type spells every array with a trailing "[]",
// including an array of a domain, while a column whose declared type IS a
// domain is spelled with the domain's own name.
func dbColumnDomainIdentity(dbCol catalog.Column) (domainIdentity, bool) {
	qualifier, spelled := domainColumnSpelling(dbCol)
	if name := foldDomainPart(dbCol.DomainName); name != "" {
		schema := foldDomainPart(dbCol.DomainSchema)
		if schema == "" {
			schema = foldDomainPart(qualifier)
		}
		return domainIdentity{schema: schema, name: name}, true
	}
	if name := foldDomainPart(spelled); name != "" {
		return domainIdentity{schema: foldDomainPart(qualifier), name: name}, true
	}
	return domainIdentity{}, false
}

// domainColumnSpelling splits the server's own spelling of a domain column's
// declared type, and returns an empty name for every column that has none --
// including an array, whose spelling is a type.
func domainColumnSpelling(dbCol catalog.Column) (schema, name string) {
	formatted := strings.TrimSpace(dbCol.FormattedType)
	if formatted == "" || strings.HasSuffix(formatted, "[]") {
		return "", ""
	}
	return splitQualifiedTypeName(formatted)
}

// desiredDomainIdentities indexes, by folded bare name, the domains the desired
// schema declares. It is what lets the comparator see a domain on the side
// where a type is only a string: schemamodel.Field carries a type name and nothing
// that says the name belongs to a domain.
//
// The index is by BARE name because that is how a column references a domain
// declared in the same schema, and its value carries the declared schema so an
// unqualified reference resolves to the domain it actually names rather than to
// any domain of that name. A name two declarations share is left unresolved:
// which one an unqualified reference means is a search-path question this
// comparator has no answer for, and guessing one would be the miss again.
//
// semantics.DefaultSchema is the explicit default rule. A domain declared with
// no schema of its own lives in the schema the connection reads, which is the
// same schema the database side reports for it.
func desiredDomainIdentities(
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) map[string]domainIdentity {
	if desired == nil {
		return nil
	}
	identities := make(map[string]domainIdentity, len(desired.Domains))
	ambiguous := make(map[string]struct{})
	for _, domain := range desired.Domains {
		name := foldDomainPart(domain.Name)
		if name == "" {
			continue
		}
		schema := foldDomainPart(domain.Schema)
		if schema == "" {
			schema = foldDomainPart(semantics.DefaultSchema)
		}
		if declared, seen := identities[name]; seen && declared.schema != schema {
			ambiguous[name] = struct{}{}
		}
		identities[name] = domainIdentity{schema: schema, name: name}
	}
	for name := range ambiguous {
		identities[name] = domainIdentity{name: name}
	}
	return identities
}

// desiredColumnDomainIdentity returns the identity a desired column type names,
// and whether the desired schema declares that name as a domain.
//
// A qualified spelling says its own schema. An unqualified one is resolved
// through the declaration when there is exactly one, and otherwise left with an
// empty schema -- the search path decides it, and this comparator does not read
// the search path.
func desiredColumnDomainIdentity(
	genType string,
	desiredDomains map[string]domainIdentity,
) (domainIdentity, bool) {
	schema, bare := splitQualifiedTypeName(strings.TrimSpace(genType))
	identity := domainIdentity{schema: foldDomainPart(schema), name: foldDomainPart(bare)}
	if identity.name == "" {
		return identity, false
	}
	declared, isDomain := desiredDomains[identity.name]
	if isDomain && identity.schema == "" {
		identity.schema = declared.schema
	}
	return identity, isDomain
}

// domainIdentitiesMatch reports whether two identities name one domain.
//
// Both halves must agree when both are known. An empty schema on either side is
// a spelling that did not say and that nothing resolved, which the search path
// decides at the server; it is matched by name so that a database compared
// against itself stays synced. Two DIFFERENT schemas name two different
// domains and never agree -- that is the half whose absence lost a column.
func domainIdentitiesMatch(dbDomain, desiredDomain domainIdentity) bool {
	if dbDomain.name == "" || desiredDomain.name == "" {
		return false
	}
	if dbDomain.name != desiredDomain.name {
		return false
	}
	return dbDomain.schema == "" || desiredDomain.schema == "" ||
		dbDomain.schema == desiredDomain.schema
}

// splitQualifiedTypeName splits schema.name and drops the quotes PostgreSQL
// writes around an identifier that needs them.
func splitQualifiedTypeName(name string) (schema, bare string) {
	if dot := strings.LastIndex(name, "."); dot >= 0 {
		schema = unquoteIdentifier(name[:dot])
		name = name[dot+1:]
	}
	return schema, unquoteIdentifier(name)
}

func unquoteIdentifier(name string) string {
	return strings.Trim(strings.TrimSpace(name), `"`)
}

// renderedDefaultForDialect answers the default the renderer would write, where
// that differs from the declared one.
//
// Two dialects need it, and both for booleans.
//
// Oracle has no boolean: BOOLEAN becomes NUMBER(1) there, so a column declared
// `default="true"` is written as `DEFAULT 1` and read back as `1`. Comparing
// the declared `true` against the catalog's `1` reported a default change on a
// column that matched, on every run.
//
// SQLite has no boolean either, and stores what the affinity converts: writing
// `DEFAULT 'true'` on a numeric column stored the TEXT "true" rather than 1,
// so the renderer writes the number (stokaro/ptah#2092) and the comparison has
// to read the declaration the same way.
//
// It is the same question the type comparison asks one function above -- not
// "are these the same word" but "would rendering this declaration produce what
// the catalog holds" -- and it is answered by the renderer's own mapping rather
// than a second copy of it.
func renderedDefaultForDialect(declaredDefault, declaredType, dialect string) string {
	switch platform.NormalizeDialect(dialect) {
	case platform.Oracle:
		if base := oracletype.Base(declaredType); base != "BOOLEAN" && base != "BOOL" {
			return declaredDefault
		}
	case platform.SQLite:
		// The affinity decides, because the affinity is what SQLite converts
		// by. A TEXT or BLOB column keeps the characters, so `true` there is
		// the word rather than the number.
		switch normalize.SQLiteAffinity(declaredType) {
		case "TEXT", "BLOB":
			return declaredDefault
		}
	default:
		return declaredDefault
	}
	switch strings.ToLower(strings.Trim(declaredDefault, "'")) {
	case "true":
		return "1"
	case "false":
		return "0"
	}
	return declaredDefault
}

func normalizeColumnTypesForDialect(
	genCol schemamodel.Field,
	dbType, dialect string,
) (generatedType, databaseType string) {
	genType := genCol.Type
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLite:
		// SQLite stores the declaration and never resolves it, so two spellings
		// are the same type exactly when they yield the same AFFINITY. Comparing
		// the canonical spellings instead reported a change between
		// `VARCHAR(80)` and `TEXT`, and the plan that followed rebuilt the whole
		// table to change nothing an application can observe
		// (stokaro/ptah#2040).
		//
		// The declared side is asked in the spelling the RENDERER would write,
		// which is what the question has always been here: a Go schema saying
		// BOOLEAN produces an INTEGER column, so its affinity is INTEGER and
		// not the NUMERIC that `BOOLEAN` would give. A type a catalog stored
		// verbatim is written as it stands and keeps its own affinity.
		return normalize.SQLiteAffinity(renderedSQLiteType(genCol)),
			normalize.SQLiteAffinity(dbType)
	case platform.Spanner:
		// Spanner's PostgreSQL interface has ONE string type. A `text` column
		// and an unbounded `character varying` are the same STRING(MAX), and
		// the catalog reports the second whichever of the two was declared --
		// measured on the PGAdapter emulator v0.55.2, a column applied as
		// `text` reads back as `character varying`.
		//
		// So comparing the spellings planned `ALTER COLUMN ... TYPE text` on
		// every run of a document the database already matched, and the plan
		// could never be applied: the emulator answers that ALTER with a
		// GOOGLESQL_RET_CHECK failure (stokaro/ptah#2074).
		//
		// A width is still a distinction: STRING(200) is not STRING(MAX). So
		// the fold is between the UNBOUNDED spellings only, and a sized string
		// keeps a category of its own -- which is what makes a declared
		// varchar(200) against an unbounded column a change in both
		// directions. Two sized strings land in one category and their widths
		// are asked about by shouldReportSizedTypeChange, as everywhere else.
		return spannerStringType(genType, normalize.Type(genType)),
			spannerStringType(dbType, normalize.Type(dbType))
	case platform.Oracle:
		// Oracle has no counterpart for most declared type names, so the
		// declaration and the catalog never agree on the spelling: a declared
		// TEXT is a CLOB, an INT is a NUMBER(10), a BOOLEAN is a NUMBER(1).
		// Comparing them raw reported an ALTER for every column of a database
		// Ptah had just built from that declaration.
		//
		// The declared side goes through the same mapping the renderer writes,
		// which is what makes the two comparable: the question the comparison
		// has to answer is not "are these the same word" but "would rendering
		// this declaration produce the type the catalog holds".
		return normalize.Type(oracletype.Map(genType)), normalize.Type(dbType)
	case platform.YDB:
		return ydbComparableTypes(genType, dbType)
	case platform.CockroachDB:
		return normalize.Type(cockroachCatalogTypeName(genType)), normalize.Type(cockroachCatalogTypeName(dbType))
	default:
		return normalize.Type(genType), normalize.Type(dbType)
	}
}

// cockroachCatalogTypeName spells a CockroachDB type by the name its catalog
// reports for it, where the two differ.
//
// CockroachDB names its string and byte types STRING and BYTES and reports them
// as text and bytea: measured on v26.3.2, a column declared STRING reads back
// as text, STRING[] as text[], and BYTES as bytea. Compared as declared, each
// planned ALTER COLUMN ... TYPE to the type the column had, on every run
// (stokaro/ptah#4059). A sized STRING(n) is not folded: it is a text column
// with a width, read back as STRING(n), not the varchar(n) it resembles.
func cockroachCatalogTypeName(typeName string) string {
	base, depth := splitArrayDepth(typeName)
	switch strings.ToLower(strings.TrimSpace(base)) {
	case "string":
		base = "text"
	case "bytes":
		base = "bytea"
	default:
		return typeName
	}
	return base + strings.Repeat("[]", depth)
}

// resolvedDeclaredType is the type a server builds for a declaration, where it
// resolves the declared spelling through a session setting rather than by its
// name.
//
// CockroachDB is the case. A column declared INT or INTEGER, with no width, is
// built with the width the session's default_int_size names, 8 unless the
// session sets it: measured on v26.3.2, `n integer` and `k int` both read back
// as INT8, and INT4 in a session with default_int_size = 4. Taken at
// PostgreSQL's width, a declared `integer` differs from the INT8 the same
// statement built, and the plan carries an ALTER COLUMN ... TYPE on every run
// (stokaro/ptah#3922). So the declaration is compared as the width the server
// gives it, and a declaration that does name a width, `int4` or `bigint`, is
// compared as written.
//
// defaultIntSize is 0 where no connection read the setting; that is taken as
// CockroachDB's own default. Every other dialect's declaration is returned
// unchanged.
func resolvedDeclaredType(declared, dialect string, defaultIntSize int) string {
	if platform.NormalizeDialect(dialect) != platform.CockroachDB {
		return declared
	}
	if name := strings.ToLower(strings.TrimSpace(declared)); name != "int" && name != "integer" {
		return declared
	}
	if defaultIntSize == 4 {
		return "int4"
	}
	return "int8"
}

// typeChangeText spells a reported type change for a person reading the plan.
//
// The normalized forms are what the comparison DECIDED on and they are usually
// the more useful of the two -- `varchar -> text` says the category changed
// without a width in the way. On SQLite they are affinities, and an operator
// told `NUMERIC -> INTEGER` has to work out which of their columns that was,
// so there the raw spellings are reported instead (stokaro/ptah#2040).
func typeChangeText(dbRawType, genRawType, dbNormalized, genNormalized, dialect string) string {
	if platform.NormalizeDialect(dialect) == platform.SQLite {
		return fmt.Sprintf("%s -> %s", strings.TrimSpace(dbRawType), strings.TrimSpace(genRawType))
	}
	return fmt.Sprintf("%s -> %s", dbNormalized, genNormalized)
}

// spannerStringType names a string column by whether it is bounded, which is
// the only distinction Spanner's single string type has.
//
// normalize.Type has already dropped the width by the time it is called, so the
// raw spelling is what answers the question. `text`, `varchar` and
// `character varying` with no width are one type; anything carrying a width is
// another, and two of those are compared on their widths further down.
func spannerStringType(raw, normalized string) string {
	if normalized != "varchar" && normalized != "text" {
		return normalized
	}
	if strings.Contains(raw, "(") {
		return "varchar"
	}
	return "text"
}

// ydbColumnTypeChange compares a YDB column's type. An integer that increments,
// on either side, is read as the Serial type the renderer writes for it: a
// declared BIGINT with auto_increment is a BigSerial, and so is a column the
// other side reports as an incrementing Int64.
func ydbColumnTypeChange(genCol schemamodel.Field, dbCol catalog.Column, dbRawType string) string {
	declared, current := genCol.Type, dbRawType
	if genCol.AutoInc || genCol.IdentityGeneration != "" {
		declared = ydbSerialOf(declared)
	}
	if dbCol.IsAutoIncrement {
		current = ydbSerialOf(current)
	}
	desiredType, currentType := ydbComparableTypes(declared, current)
	if desiredType == currentType {
		return ""
	}
	return fmt.Sprintf("%s -> %s", currentType, desiredType)
}

// clickHouseColumnTypeChange compares a ClickHouse column's type as the
// server stores it, width included.
//
// The generic comparison folds every type whose name contains "int" to
// "integer" and asks about width in SQL's terms, where Int8 is PostgreSQL's
// 64-bit int8. A live Int32 against a declared Int64 recorded no change that
// way, and neither did UInt8 against UInt16 or Int32 against UInt32
// (stokaro/ptah#4105). Measured on 24.10 and 26.9, the server takes
// `MODIFY COLUMN d Int64` over an Int32 and keeps every value.
//
// Each side is spelled the way the renderer writes it: as it stands when it
// came verbatim from a catalog or from sql(), and through [chtype.Map]
// otherwise. The current side is asked too, because a file-to-file comparison
// hands it a declaration rather than a catalog type. The two spellings are
// then compared as [chtype.Canonical] keys, so a name the server stores under
// another, Decimal32(2) for Decimal(9, 2), is not a change.
//
// Nullability is compared on its own, so an outer Nullable is taken off both
// sides first: a declaration that writes Nullable(Int64) and one that writes
// Int64 with the nullable flag set ask for the same type.
func clickHouseColumnTypeChange(genCol schemamodel.Field, dbCol catalog.Column, dbRawType string) string {
	declared := strings.TrimSpace(genCol.Type)
	if !genCol.TypeIsDeclaredText && !genCol.TypeRawSQL {
		declared = clickHouseMappedType(declared)
	}
	current := strings.TrimSpace(dbRawType)
	if !dbCol.TypeIsDeclaredText {
		current = clickHouseMappedType(current)
	}
	declared, current = chtype.StripNullable(declared), chtype.StripNullable(current)
	if chtype.Canonical(declared) == chtype.Canonical(current) {
		return ""
	}
	return fmt.Sprintf("%s -> %s", current, declared)
}

// clickHouseMappedType is the type the ClickHouse renderer writes for a
// portable declaration. A type the map refuses is compared as written.
func clickHouseMappedType(declared string) string {
	mapping, err := chtype.Map(declared)
	if err != nil {
		return declared
	}
	return mapping.Type
}

// ydbDefaultChange compares a YDB column's default as YQL literals. YDB
// stores a default as a typed value, and the reader reports it as the literal
// ydbtype.Literal writes for it; the declaration is written through the same
// function in the type the column has, so `5`, `+5` and `05` on an Int8 column
// all compare as `5t`, and a declared TIMESTAMP default compares in the
// Timestamp64 or Timestamp spelling the server built. A Serial column takes
// its value from its sequence, so the reader reports no default for one and
// there is nothing to compare.
func ydbDefaultChange(genCol schemamodel.Field, dbCol catalog.Column, declared, current string) (key, change string) {
	if strings.TrimSpace(declared) == "" && current == "" {
		return "", ""
	}
	if ydbDeclaredDefault(genCol, dbCol.RawType(), declared) == current {
		return "", ""
	}
	return "default", fmt.Sprintf("%s -> %s", current, declared)
}

// ydbDeclaredDefault writes a declared default as the literal the YDB renderer
// writes for it: in the catalog's type where the declaration lands on that
// type, and in the declaration's own type otherwise. A NULL default writes
// nothing, as the renderer writes none. An expression, and a value the type
// cannot hold, are compared as written.
func ydbDeclaredDefault(genCol schemamodel.Field, catalogType, declared string) string {
	if strings.TrimSpace(genCol.Default) == "" && strings.TrimSpace(genCol.DefaultExpr) != "" {
		return declared
	}
	value, isNull := ydbtype.DeclaredValue(declared)
	if isNull {
		return ""
	}
	columnType, _ := ydbComparableTypes(genCol.Type, catalogType)
	literal, err := ydbtype.Literal(columnType, value, capability.Capabilities{
		capability.SmallIntegerDefaults: true,
		capability.DocumentTypeDefaults: true,
	})
	if err != nil {
		return declared
	}
	return literal
}

// ydbSerialOf answers the Serial type an incrementing integer of columnType
// is written as, and columnType itself where YDB has no Serial for it.
func ydbSerialOf(columnType string) string {
	renderings := ydbtype.Renderings(columnType)
	if len(renderings) == 0 {
		return columnType
	}
	if serial, ok := ydbtype.SerialFor(renderings[0]); ok {
		return serial
	}
	return columnType
}

// serialSequenceChanges records a difference in the start or the increment of
// a Serial column's sequence, under the attribute names a declaration spells
// them with. It compares only where both sides are Serial: a column that is
// Serial on one side only has a type difference, which says more.
//
// Each side is read through [ydbsequence.Canonical], so an omitted setting is
// the 1 a sequence nobody altered has, and `0100` is 100. Both settings are
// compared and recorded on their own, while the plan writes both whatever
// changed, so a value is never left to whatever the server keeps. It reports
// whether it recorded either.
func serialSequenceChanges(changes map[string]string, genCol schemamodel.Field, dbCol catalog.Column) bool {
	declaredSerial := genCol.AutoInc || genCol.IdentityGeneration != "" || ydbtype.DeclaresSerial(genCol.Type)
	if !declaredSerial || !dbCol.IsAutoIncrement {
		return false
	}
	recorded := false
	for _, setting := range []struct{ key, declared, current string }{
		{"identity_start", genCol.IdentityStart, dbCol.IdentityStart},
		{"identity_increment", genCol.IdentityIncrement, dbCol.IdentityIncrement},
	} {
		declared, current := ydbsequence.Canonical(setting.declared), ydbsequence.Canonical(setting.current)
		if declared != current {
			changes[setting.key] = current + " -> " + declared
			recorded = true
		}
	}
	return recorded
}

// writesYDBSerial reports a column the YDB renderer writes as a Serial type,
// which it writes NOT NULL whatever the field says: a Serial column always
// holds a value from its sequence. Without this a Serial column declared
// without not_null would plan DROP NOT NULL against the column Ptah built, on
// every run.
func writesYDBSerial(genCol schemamodel.Field, dialect string) bool {
	if platform.NormalizeDialect(dialect) != platform.YDB {
		return false
	}
	return ydbsequence.DeclaresSerialColumn(genCol.Type, genCol.AutoInc, genCol.IdentityGeneration)
}

// ydbComparableTypes reads both sides through the YDB type map, the one the
// renderer writes from, and calls them equal when any type the declaration can
// land on is a type the other side can be. A declared TIMESTAMP is Timestamp64
// on a line with the wide types and Timestamp on one without, and a table
// built on either answers the declaration. A declared VARCHAR(255) is Utf8,
// whose length YDB does not keep, so the length is not a difference here; the
// render reports it as dropped instead.
//
// The current side goes through the map too because it is not always a
// catalog: a file-to-file comparison hands it a declaration, VARCHAR(255)
// where a server would say Utf8. A catalog spelling maps to itself, since the
// map reads YDB's own names case-sensitively -- Int8 is YDB's 8-bit integer,
// INT8 is SQL's 64-bit one. The full spelling is compared, Decimal(10,2)
// against Decimal(12,2) included, so no width check runs after this.
func ydbComparableTypes(declared, current string) (desiredType, currentType string) {
	want := ydbtype.Renderings(declared)
	have := ydbtype.Renderings(current)
	if len(have) == 0 {
		have = []string{strings.TrimSpace(current)}
	}
	for _, rendering := range want {
		for _, candidate := range have {
			if strings.EqualFold(rendering, candidate) {
				return candidate, candidate
			}
		}
	}
	if len(want) == 0 {
		return strings.TrimSpace(declared), have[0]
	}
	return want[0], have[0]
}

// renderedSQLiteType is the type SQLite's renderer would write for a
// declaration, which is the side of the comparison the affinity is taken from.
//
// A type the catalog stored verbatim is written as it stands; anything else
// goes through the canonical spelling, which is the rule that existed before
// affinities were compared at all.
func renderedSQLiteType(genCol schemamodel.Field) string {
	// TypeRawSQL counts as the same fact, for the reason the SQLite renderer
	// gives: a document carries a catalog's type as `sql("BOOLEAN")`, and both
	// sides have to read that as the declaration or the comparison would ask
	// about a type the renderer is not going to write.
	if genCol.TypeIsDeclaredText || genCol.TypeRawSQL {
		return genCol.Type
	}
	return normalize.SQLiteColumnType(genCol.Type)
}

// shouldReportSizedTypeChange reports a within-category change that the type
// normalizer folds away — a change in integer width, string length, or decimal
// precision, in either direction. Narrowing (e.g. BIGINT -> INTEGER) can lose
// data; widening (e.g. INTEGER -> BIGINT, VARCHAR(50) -> VARCHAR(100)) cannot,
// but it is still a real ALTER that a database built directly from the desired
// schema would carry, so both are reported. The SQLite guard suppresses these
// for SQLite's type affinity, where such distinctions do not exist.
func shouldReportSizedTypeChange(dbType, genType, dialect string) bool {
	if platform.NormalizeDialect(dialect) == platform.SQLite &&
		normalize.Type(dbType) == normalize.Type(normalize.SQLiteColumnType(genType)) {
		return false
	}
	// The suppression Oracle needs, and it compares the FULL rendered type
	// rather than the normalized one.
	//
	// normalize.Type strips the width, which is what SQLite's arm above wants:
	// there, type affinity means a width is not a type distinction at all.
	// Using it here suppressed real width changes -- a declared VARCHAR(200)
	// against a catalog VARCHAR2(400) normalizes to one string, so an ALTER
	// that a database built from the declaration would carry stopped being
	// reported. Comparing what the renderer would actually write keeps the
	// suppression to the case it is for: a declaration that already produces
	// exactly the catalog's type.
	if platform.NormalizeDialect(dialect) == platform.Oracle {
		// The declaration is asked in the renderer's spelling on both counts.
		// typechange compares type FAMILIES, and it has no reading of a
		// declared VARCHAR against a catalog VARCHAR2 -- so without the
		// mapping it answered neither narrowing nor widening, and a width
		// change went unreported in either direction.
		rendered := oracletype.Map(genType)
		if strings.EqualFold(strings.TrimSpace(dbType), rendered) {
			return false
		}
		return typechange.IsNarrowing(dbType, rendered) || typechange.IsWidening(dbType, rendered)
	}
	return typechange.IsNarrowing(dbType, genType) || typechange.IsWidening(dbType, genType)
}

func normalizeTablePrimaryKeyColumn(genCol schemamodel.Field, dbCol catalog.Column, dialect string) schemamodel.Field {
	if primaryKeyImpliesNotNull(dialect) {
		genCol.Nullable = false
	}
	genCol.Primary = dbCol.IsPrimaryKey
	return genCol
}

// clickHouseDeclaredKey returns the columns a ClickHouse table's declared key
// clauses use, and whether the table declares one, so a declared column is a
// key column exactly when the server would flag it is_in_primary_key.
//
// A SQL, YAML or annotation declaration states a ClickHouse key as PRIMARY KEY
// or ORDER BY on the engine, never on the column, while the reader marks every
// column the key uses. Compared column by column, a table identical to its
// declaration reported `primary_key: true -> false` for each key column and
// planned a MODIFY COLUMN that changes nothing, on every run
// (stokaro/ptah#4104). Read from the clauses, the two sides agree, and a key
// that really differs still shows on the columns it moves.
func clickHouseDeclaredKey(dialect string, table schemamodel.Table, fields []schemamodel.Field) (map[string]bool, bool) {
	if platform.NormalizeDialect(dialect) != platform.ClickHouse {
		return nil, false
	}
	names := make([]string, 0, len(fields))
	for _, field := range fields {
		names = append(names, field.Name)
	}
	return chkey.PrimaryKeyColumns(table, names)
}

func columnInTablePrimaryKey(table schemamodel.Table, column string) bool {
	return slices.Contains(tablePrimaryKeyColumns(table), column)
}

// columnInDeclaredPrimaryKey reports whether a PRIMARY KEY constraint of table
// covers the column. The key is compared as a constraint, as the table's own
// key is, so the column is normalized like one of the table's key columns.
// Compared on the column, the key the database holds reads as the column's
// own, and every plan changes `primary_key: true -> false` (stokaro/ptah#3959).
func columnInDeclaredPrimaryKey(desired *schemamodel.Database, table schemamodel.Table, column string) bool {
	for _, constraint := range desired.Constraints {
		if !strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") || !slices.Contains(constraint.Columns, column) {
			continue
		}
		if owner, found := constraintowner.Table(constraint, desired.Tables); found && owner.QualifiedName() == table.QualifiedName() {
			return true
		}
	}
	return false
}

func tablePrimaryKeyColumns(table schemamodel.Table) []string {
	if len(table.PrimaryKeyParts) == 0 {
		return nonEmptyNames(table.PrimaryKey)
	}
	columns := make([]string, 0, len(table.PrimaryKeyParts))
	for _, part := range table.PrimaryKeyParts {
		if name := strings.TrimSpace(part.Name); name != "" {
			columns = append(columns, name)
		}
	}
	return columns
}

// generatedExpressionIsRewritten reports that the target stores a rewrite of a
// generated column's expression rather than the text it was given.
//
// Oracle is the one such target, measured on 23.26.2.0.0 and 21.3.0.0.0: every
// column reference is quoted and upper-cased, the spaces around operators are
// dropped, and parentheses appear that the declaration did not carry --
// `CASE WHEN n > 0 AND n < 10 THEN 1 ELSE 0 END` is stored as
// `CASE  WHEN ("N">0 AND "N"<10) THEN 1 ELSE 0 END`, doubled space included.
//
// It decides what happens when nobody resolved the declaration: elsewhere the
// stored text is the declared text and a comparison is sound, and here it is
// not, so the attribute is left uncompared rather than reported as a change
// that a MODIFY would not make (stokaro/ptah#1915).
func generatedExpressionIsRewritten(dialect string) bool {
	return platform.NormalizeDialect(dialect) == platform.Oracle
}

// generatedColumnDiff compares a generated column, using the server's own
// spelling of the declaration where one was resolved.
//
// resolution is nil when nobody asked a server about this column, and non-nil
// with Resolved false when one was asked and refused the declaration. Those two
// are one case here rather than two, deliberately: neither yields a stored form
// to compare against, so on a rewriting target both leave the expression
// uncompared, and on every other target both leave today's textual comparison
// alone. An arm that separated them would be an arm nothing could reach.
func generatedColumnDiff(
	genCol schemamodel.Field,
	dbCol catalog.Column,
	dialect string,
	resolution *config.GeneratedExpression,
) string {
	declared := genCol.GeneratedExpression
	switch {
	case resolution != nil && resolution.Resolved:
		// The server's own spelling of the declaration, so the two sides are
		// compared as like with like.
		declared = resolution.Expression
	case generatedExpressionIsRewritten(dialect):
		// A rewriting target with no usable answer. Reporting the textual
		// difference here is what plans a MODIFY that changes nothing on every
		// run; the kind is still compared, because VIRTUAL against nothing is a
		// real change whatever the expression says.
		return generatedKindDiff(genCol, dbCol)
	}

	genExpr := normalizeGeneratedExpression(declared, dialect)
	dbExpr := ""
	if dbCol.GeneratedExpression != nil {
		dbExpr = normalizeGeneratedExpression(*dbCol.GeneratedExpression, dialect)
	}
	genKind := strings.ToUpper(strings.TrimSpace(genCol.GeneratedKind))
	dbKind := strings.ToUpper(strings.TrimSpace(dbCol.GeneratedKind))
	if genExpr == dbExpr && genKind == dbKind {
		return ""
	}
	return fmt.Sprintf("%s %s -> %s %s", dbKind, dbExpr, genKind, genExpr)
}

// generatedKindDiff compares only whether the column is generated, and how.
//
// It is what remains comparable on a rewriting target with no resolution: a
// column that stops being generated, or changes between STORED and VIRTUAL, is
// a change no spelling question can hide.
func generatedKindDiff(genCol schemamodel.Field, dbCol catalog.Column) string {
	genKind := strings.ToUpper(strings.TrimSpace(genCol.GeneratedKind))
	dbKind := strings.ToUpper(strings.TrimSpace(dbCol.GeneratedKind))
	if genKind == dbKind {
		return ""
	}
	return fmt.Sprintf("%s -> %s", dbKind, genKind)
}

func normalizeGeneratedExpression(expression, dialect string) string {
	expression = normalize.Expression(expression)
	switch platform.NormalizeDialect(dialect) {
	case platform.Postgres:
		return normalizeCatalogGeneratedExpression(stripPostgresGeneratedTypeCasts(expression), dialect)
	case platform.MySQL, platform.MariaDB:
		return normalizeMySQLGeneratedExpression(normalizeCatalogGeneratedExpression(expression, dialect))
	case platform.SQLServer:
		return normalizeSQLServerGeneratedExpression(expression)
	default:
		return expression
	}
}

func normalizeMySQLGeneratedExpression(expression string) string {
	return replaceSQLFunctionOutsideSingleQuotedSQL(expression, "lcase(", "lower(")
}

func normalizeSQLServerGeneratedExpression(expression string) string {
	var b strings.Builder
	inString := false
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch == '\'' {
			b.WriteByte(ch)
			if inString && i+1 < len(expression) && expression[i+1] == '\'' {
				i++
				b.WriteByte('\'')
				continue
			}
			inString = !inString
			continue
		}
		if !inString && ch == '[' {
			i = writeSQLServerBracketedIdentifier(&b, expression, i)
			continue
		}
		if !inString && ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		if !inString && (ch == ' ' || ch == '\t' || ch == '\n' || ch == '\r') {
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func normalizeCatalogGeneratedExpression(expression, dialect string) string {
	var b strings.Builder
	mysqlFamily := isMySQLFamilyDialect(dialect)
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch == '\'' || (mysqlFamily && ch == '"') {
			i = copyQuotedSQL(&b, expression, i)
			continue
		}
		switch ch {
		case '`', '"', ' ', '\t', '\n', '\r':
			continue
		default:
			if ch >= 'A' && ch <= 'Z' {
				ch += 'a' - 'A'
			}
			b.WriteByte(ch)
		}
	}
	return collapseParenthesizedIdentifiers(b.String())
}

func stripPostgresGeneratedTypeCasts(expression string) string {
	var b strings.Builder
	inString := false
	inIdentifier := false
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch == '\'' && !inIdentifier {
			b.WriteByte(ch)
			if inString && i+1 < len(expression) && expression[i+1] == '\'' {
				i++
				b.WriteByte('\'')
				continue
			}
			inString = !inString
			continue
		}
		if ch == '"' && !inString {
			b.WriteByte(ch)
			if inIdentifier && i+1 < len(expression) && expression[i+1] == '"' {
				i++
				b.WriteByte('"')
				continue
			}
			inIdentifier = !inIdentifier
			continue
		}
		if !inString && !inIdentifier && i+1 < len(expression) && ch == ':' && expression[i+1] == ':' {
			i = skipPostgresGeneratedTypeCast(expression, i)
			i--
			continue
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func skipPostgresGeneratedTypeCast(expression string, start int) int {
	i := start + 2
	for i < len(expression) && isPostgresTypeCastCharacter(expression[i]) {
		i++
	}
	if i >= len(expression) || expression[i] != '(' {
		return i
	}
	depth := 0
	for i < len(expression) {
		switch expression[i] {
		case '(':
			depth++
		case ')':
			depth--
			if depth == 0 {
				return i + 1
			}
		}
		i++
	}
	return i
}

func isPostgresTypeCastCharacter(ch byte) bool {
	return ch == ' ' || ch == '.' || ch == '_' ||
		(ch >= '0' && ch <= '9') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z')
}

func collapseParenthesizedIdentifiers(expression string) string {
	for {
		next, changed := collapseOneParenthesizedIdentifier(expression)
		if !changed {
			return expression
		}
		expression = next
	}
}

func collapseOneParenthesizedIdentifier(expression string) (string, bool) {
	for start := 0; start < len(expression); start++ {
		if expression[start] != '(' {
			continue
		}
		if start > 0 && isIdentifierExpression(expression[start-1:start]) {
			continue
		}
		end := strings.IndexByte(expression[start+1:], ')')
		if end < 0 {
			return expression, false
		}
		end += start + 1
		inner := expression[start+1 : end]
		if !isIdentifierExpression(inner) {
			continue
		}
		return expression[:start] + inner + expression[end+1:], true
	}
	return expression, false
}

func isIdentifierExpression(expression string) bool {
	if expression == "" {
		return false
	}
	for i := 0; i < len(expression); i++ {
		ch := expression[i]
		if ch != '_' && ch != '.' &&
			(ch < '0' || ch > '9') &&
			(ch < 'a' || ch > 'z') &&
			(ch < 'A' || ch > 'Z') {
			return false
		}
	}
	return true
}

func writeSQLServerBracketedIdentifier(b *strings.Builder, expression string, start int) int {
	for i := start + 1; i < len(expression); i++ {
		ch := expression[i]
		if ch == ']' {
			if i+1 < len(expression) && expression[i+1] == ']' {
				b.WriteByte(']')
				i++
				continue
			}
			return i
		}
		if ch >= 'A' && ch <= 'Z' {
			ch += 'a' - 'A'
		}
		b.WriteByte(ch)
	}

	b.WriteByte('[')
	return start
}

// removedColumn describes a column the database reported and the desired schema
// does not declare, in the shape a rollback renders it from.
//
// The description is [catalogfield.Field], the same one the conversion that
// writes a document uses, so the two cannot answer differently. What it is NOT
// given is the column's keys, and that omission is the point.
//
// A dropped column's PRIMARY KEY and FOREIGN KEY are reported by the CONSTRAINT
// comparison, which turns them back into additions in the same reversal. A
// column that also carried them would have them restored twice: measured, the
// rollback of a column with a foreign key emitted `ADD CONSTRAINT` for it once
// from the column and once from the constraint, and PostgreSQL answers the
// second with `constraint ... already exists` (stokaro/ptah#2404).
//
// The forward direction is unaffected: a DECLARED field carries its own foreign
// key, the constraint comparison reports none for it, and the column path is
// the only one that emits it.
func removedColumn(reported catalog.Column, dialect string) schemamodel.Field {
	field := catalogfield.Field(reported, catalogfield.Options{Dialect: dialect})
	field.Primary = false
	field.Foreign = ""
	field.ForeignKeyName = ""
	field.OnDelete = ""
	field.OnUpdate = ""
	field.Deferrable = false
	field.Initially = ""
	field.ForeignKeyMatch = ""
	field.ForeignKeyNotEnforced = false
	return field
}

// sortColumns orders by the key the name list was sorted on.
func sortColumns(columns difftypes.ColumnChanges) {
	sort.Slice(columns, func(i, j int) bool { return columns[i].Name < columns[j].Name })
}

// gainedKeyNames answers the name each column of tableDiff that gains its own
// UNIQUE takes, keyed by column: a column added with UNIQUE, and a column whose
// uniqueness changes to UNIQUE. A column declaring unique_expr takes no key over
// the raw column and is left out. It is nil where no column gains a key, and on
// an engine whose naming is not measured; see [ownKeyName].
func gainedKeyNames(
	tableDiff difftypes.TableDiff, table schemamodel.Table, desired *schemamodel.Database, dialect string,
) map[string]string {
	var columns []string
	for _, field := range tableDiff.ColumnsAdded {
		if field.Unique && strings.TrimSpace(field.UniqueExpr) == "" {
			columns = append(columns, field.Name)
		}
	}
	for _, column := range tableDiff.ColumnsModified {
		_, changed := column.Changes["unique"]
		if changed && column.Desired.Unique && strings.TrimSpace(column.Desired.UniqueExpr) == "" {
			columns = append(columns, column.ColumnName)
		}
	}
	var names map[string]string
	for _, column := range columns {
		name, named := ownKeyName(desired, table, column, dialect)
		if !named {
			return nil
		}
		if names == nil {
			names = make(map[string]string, len(columns))
		}
		names[column] = name
	}
	return names
}
