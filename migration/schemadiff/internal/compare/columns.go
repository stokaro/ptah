package compare

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/sqlitekey"
	"go.5x5.cz/ptah/migration/internal/generatedschema"
	"go.5x5.cz/ptah/migration/internal/typechange"
	"go.5x5.cz/ptah/migration/schemadiff/internal/normalize"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
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
func TableColumns(genTable goschema.Table, dbTable types.DBTable, generated *goschema.Database) difftypes.TableDiff {
	return TableColumnsWithDialect(genTable, dbTable, generated, "")
}

// TableColumnsWithDialect compares one table's columns with dialect-aware
// normalization for catalog-rewritten generated expressions.
func TableColumnsWithDialect(
	genTable goschema.Table,
	dbTable types.DBTable,
	generated *goschema.Database,
	dialect string,
) difftypes.TableDiff {
	return TableColumnsWithSemantics(
		genTable,
		dbTable,
		generated,
		dialect,
		identifier.ForDialect(dialect),
	)
}

// TableColumnsWithSemantics compares one table's columns using explicit
// catalog identifier rules.
func TableColumnsWithSemantics(
	genTable goschema.Table,
	dbTable types.DBTable,
	generated *goschema.Database,
	dialect string,
	semantics identifier.Semantics,
) difftypes.TableDiff {
	return tableColumnsWithSemantics(
		genTable,
		dbTable,
		generated,
		dialect,
		semantics,
		nil,
	)
}

func tableColumnsWithSemantics(
	genTable goschema.Table,
	dbTable types.DBTable,
	generated *goschema.Database,
	dialect string,
	semantics identifier.Semantics,
	objectOwnedUniqueColumns map[columnIdentity]struct{},
) difftypes.TableDiff {
	tableDiff := difftypes.TableDiff{TableName: genTable.QualifiedName()}

	// Create maps for quick lookup
	genFields := generatedschema.FieldsForTable(generated, genTable)
	genColumns := make(map[string]goschema.Field)
	for _, field := range genFields {
		genColumns[semantics.ColumnIdentityKey(field.Name)] = field
	}
	keyColumns := sqlitekey.KeyColumns(genTable, genFields)

	dbColumns := make(map[string]types.DBColumn)
	for _, col := range dbTable.Columns {
		dbColumns[semantics.ColumnIdentityKey(col.Name)] = col
	}

	desiredDomains := desiredDomainIdentities(generated, semantics)

	// Find added and removed columns
	for identity, column := range genColumns {
		if _, exists := dbColumns[identity]; !exists {
			tableDiff.ColumnsAdded = append(tableDiff.ColumnsAdded, column.Name)
		}
	}

	for identity, column := range dbColumns {
		if _, exists := genColumns[identity]; !exists {
			tableDiff.ColumnsRemoved = append(tableDiff.ColumnsRemoved, column.Name)
		}
	}

	// Find modified columns
	for identity, genCol := range genColumns {
		if dbCol, exists := dbColumns[identity]; exists {
			if columnInTablePrimaryKey(genTable, genCol.Name) {
				genCol = normalizeTablePrimaryKeyColumn(genCol, dbCol, dialect)
			}
			if sqliteKeyColumnImpliesNotNull(dialect, genTable, keyColumns, genCol) {
				genCol.Nullable = false
			}
			columnKey := columnIdentity{
				table:  newTableIdentity(genTable.Schema, genTable.Name, semantics),
				column: identity,
			}
			if _, objectOwned := objectOwnedUniqueColumns[columnKey]; objectOwned {
				genCol.Unique = false
				dbCol.IsUnique = false
			}
			colDiff := columnsWithDesiredDomains(genCol, dbCol, dialect, desiredDomains)
			if len(colDiff.Changes) > 0 {
				tableDiff.ColumnsModified = append(tableDiff.ColumnsModified, colDiff)
			}
		}
	}

	// Sort for consistent output
	sort.Strings(tableDiff.ColumnsAdded)
	sort.Strings(tableDiff.ColumnsRemoved)
	sort.Slice(tableDiff.ColumnsModified, func(i, j int) bool {
		return tableDiff.ColumnsModified[i].ColumnName < tableDiff.ColumnsModified[j].ColumnName
	})

	return tableDiff
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
func Columns(genCol goschema.Field, dbCol types.DBColumn) difftypes.ColumnDiff {
	return ColumnsWithDialect(genCol, dbCol, "")
}

// ColumnsWithDialect compares two columns using dialect-specific expression
// normalization where catalog readback rewrites equivalent SQL.
//
// It knows nothing about which type names the desired schema declares as
// domains, so it decides a domain from the DATABASE column alone. Every
// comparison that has a desired schema to consult goes through
// tableColumnsWithSemantics, which passes that set down.
func ColumnsWithDialect(genCol goschema.Field, dbCol types.DBColumn, dialect string) difftypes.ColumnDiff {
	return columnsWithDesiredDomains(genCol, dbCol, dialect, nil)
}

func columnsWithDesiredDomains(
	genCol goschema.Field,
	dbCol types.DBColumn,
	dialect string,
	desiredDomains map[string]domainIdentity,
) difftypes.ColumnDiff {
	colDiff := difftypes.ColumnDiff{
		ColumnName: genCol.Name,
		Changes:    make(map[string]string),
	}

	// ClickHouse-only guard: older goschema models cannot express
	// MATERIALIZED / ALIAS / EPHEMERAL columns. Once the schema side carries a
	// generated expression, compare it normally below.
	if dbCol.GeneratedKind != "" && genCol.GeneratedExpression == "" {
		return colDiff
	}

	dbRawType := rawDBColumnType(dbCol)

	// The default comparison below asks what CATEGORY each side's type is, so it
	// keeps using the normalizer even where the type comparison above does not:
	// a default is normalized as a boolean or a number.
	//
	// For a domain column both sides receive the DOMAIN NAME, not the domain's
	// base type, because rawDBColumnType answers with the domain and the desired
	// side spells the column as the domain too. So `d_bool` reaches the
	// normalizer, which folds by substring and lands on "boolean" by luck, while
	// `positive` folds to nothing and the boolean/decimal/temporal branches of
	// normalize.DefaultValue are skipped for that column.
	//
	// Passing the base type on the database side alone would be worse, not
	// better: the desired side is a goschema.Field carrying only a type name and
	// has no base type to reach for, so the two sides would land in different
	// categories and a database would stop being in sync with itself -- the
	// asymmetry stokaro/ptah#1242 is about. Both sides receiving the same
	// category, right or wrong, is what keeps a self-diff quiet, and no live
	// churn from the folding has been measured. Fixing it properly means giving
	// the desired side a base type to answer with.
	genType, dbType := normalizeColumnTypesForDialect(genCol.Type, dbRawType, dialect)

	if change := columnTypeChange(genCol, dbCol, dbRawType, dialect, desiredDomains); change != "" {
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
	dbNullable := dbCol.IsNullable == "YES"
	if genNullable != dbNullable {
		colDiff.Changes["nullable"] = fmt.Sprintf("%t -> %t", dbNullable, genNullable)
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
	if diff := generatedColumnDiff(genCol, dbCol, dialect); diff != "" {
		colDiff.Changes["generated"] = diff
	}

	// Compare default values (simplified)
	genDefault := genCol.Default
	if genDefault == "" {
		genDefault = genCol.DefaultExpr
	}
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
	if !skipImplicitSequenceDefault {
		normalizedDbDefault := normalize.DefaultValue(dbDefault, dbType)

		idxName := "default"
		if normalize.IsDefaultExpr(dbDefault) {
			idxName = "default_expr"
		}

		normalizeGenDefaultFn := normalize.DefaultValue(genDefault, genType)

		if normalizeGenDefaultFn != normalizedDbDefault {
			colDiff.Changes[idxName] = fmt.Sprintf("%s -> %s", dbDefault, genDefault)
		}
	}

	return colDiff
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
	table goschema.Table,
	keyColumns []string,
	field goschema.Field,
) bool {
	if platform.NormalizeDialect(dialect) != platform.SQLite {
		return false
	}
	return sqlitekey.ImpliesNotNull(table, keyColumns, field)
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
	genCol goschema.Field,
	dbCol types.DBColumn,
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

	genType, dbType := normalizeColumnTypesForDialect(genCol.Type, dbRawType, dialect)
	switch {
	case genType != dbType:
		return fmt.Sprintf("%s -> %s", dbType, genType)
	case shouldReportSizedTypeChange(dbRawType, genCol.Type, dialect):
		return fmt.Sprintf("%s -> %s", dbRawType, genCol.Type)
	}
	return ""
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
func dbColumnDomainIdentity(dbCol types.DBColumn) (domainIdentity, bool) {
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
func domainColumnSpelling(dbCol types.DBColumn) (schema, name string) {
	formatted := strings.TrimSpace(dbCol.FormattedType)
	if formatted == "" || strings.HasSuffix(formatted, "[]") {
		return "", ""
	}
	return splitQualifiedTypeName(formatted)
}

// desiredDomainIdentities indexes, by folded bare name, the domains the desired
// schema declares. It is what lets the comparator see a domain on the side
// where a type is only a string: goschema.Field carries a type name and nothing
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
	generated *goschema.Database,
	semantics identifier.Semantics,
) map[string]domainIdentity {
	if generated == nil {
		return nil
	}
	identities := make(map[string]domainIdentity, len(generated.Domains))
	ambiguous := make(map[string]struct{})
	for _, domain := range generated.Domains {
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

func normalizeColumnTypesForDialect(genType, dbType, dialect string) (generatedType, databaseType string) {
	switch platform.NormalizeDialect(dialect) {
	case platform.SQLite:
		return normalize.Type(sqliteRenderedColumnType(genType)), normalize.Type(dbType)
	default:
		return normalize.Type(genType), normalize.Type(dbType)
	}
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
		normalize.Type(dbType) == normalize.Type(sqliteRenderedColumnType(genType)) {
		return false
	}
	return typechange.IsNarrowing(dbType, genType) || typechange.IsWidening(dbType, genType)
}

func sqliteRenderedColumnType(rawType string) string {
	upper := strings.ToUpper(strings.TrimSpace(rawType))
	base := upper
	if idx := strings.Index(base, "("); idx >= 0 {
		base = strings.TrimSpace(base[:idx])
	}
	switch base {
	case "":
		return "blob"
	case "BOOLEAN", "BOOL":
		return "integer"
	case "SERIAL", "BIGSERIAL", "SMALLSERIAL", "AUTO_INCREMENT":
		return "integer"
	case "VARCHAR", "CHARACTER VARYING", "CHAR", "CHARACTER", "TEXT", "CITEXT", "ENUM":
		return "text"
	case "BYTEA", "BLOB":
		return "blob"
	case "DOUBLE PRECISION":
		return "real"
	default:
		return rawType
	}
}

func normalizeTablePrimaryKeyColumn(genCol goschema.Field, dbCol types.DBColumn, dialect string) goschema.Field {
	if primaryKeyImpliesNotNull(dialect) {
		genCol.Nullable = false
	}
	genCol.Primary = dbCol.IsPrimaryKey
	return genCol
}

func columnInTablePrimaryKey(table goschema.Table, column string) bool {
	return slices.Contains(tablePrimaryKeyColumns(table), column)
}

func tablePrimaryKeyColumns(table goschema.Table) []string {
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

func generatedColumnDiff(genCol goschema.Field, dbCol types.DBColumn, dialect string) string {
	genExpr := normalizeGeneratedExpression(genCol.GeneratedExpression, dialect)
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
