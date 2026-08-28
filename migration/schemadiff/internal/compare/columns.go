package compare

import (
	"fmt"
	"slices"
	"sort"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/config"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/catalogfield"
	"go.5x5.cz/ptah/internal/exprkey"
	"go.5x5.cz/ptah/internal/normalize"
	"go.5x5.cz/ptah/internal/oracletype"
	"go.5x5.cz/ptah/internal/sqlitekey"
	"go.5x5.cz/ptah/internal/typechange"
	"go.5x5.cz/ptah/migration/internal/generatedschema"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
		nil,
		nil,
	)
}

func tableColumnsWithSemantics(
	genTable schemamodel.Table,
	dbTable catalog.Table,
	desired *schemamodel.Database,
	dialect string,
	semantics identifier.Semantics,
	objectOwnedUniqueColumns map[columnIdentity]struct{},
	generatedExpressions map[string]config.GeneratedExpression,
) difftypes.TableDiff {
	tableDiff := difftypes.TableDiff{TableName: genTable.QualifiedName()}

	// Create maps for quick lookup
	genFields := generatedschema.FieldsForTable(desired, genTable)
	genColumns := make(map[string]schemamodel.Field)
	for _, field := range genFields {
		genColumns[semantics.ColumnIdentityKey(field.Name)] = field
	}
	keyColumns := sqlitekey.KeyColumns(genTable, genFields)

	dbColumns := make(map[string]catalog.Column)
	for _, col := range dbTable.Columns {
		dbColumns[semantics.ColumnIdentityKey(col.Name)] = col
	}

	desiredDomains := desiredDomainIdentities(desired, semantics)

	// Find added and removed columns
	for identity, column := range genColumns {
		if _, exists := dbColumns[identity]; !exists {
			tableDiff.ColumnsAdded = append(tableDiff.ColumnsAdded, column)
		}
	}

	for identity, column := range dbColumns {
		if _, exists := genColumns[identity]; !exists {
			tableDiff.ColumnsRemoved = append(tableDiff.ColumnsRemoved, removedColumn(column))
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
			columnKey := newColumnIdentityForTable(genTable.Schema, genTable.Name, identity, semantics)
			if _, objectOwned := objectOwnedUniqueColumns[columnKey]; objectOwned {
				genCol.Unique = false
				dbCol.IsUnique = false
			}
			colDiff := columnsWithDesiredDomains(genCol, dbCol, dialect, desiredDomains, columnContext{
				schema:               genTable.Schema,
				table:                genTable.Name,
				generatedExpressions: generatedExpressions,
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
	var resolution *config.GeneratedExpression
	if entry, ok := ctx.generatedExpressions[exprkey.Generated(dialect, ctx.schema, ctx.table, genCol.Name)]; ok {
		resolution = &entry
	}
	if diff := generatedColumnDiff(genCol, dbCol, dialect, resolution); diff != "" {
		colDiff.Changes["generated"] = diff
	}

	// Compare default values (simplified)
	genDefault := genCol.Default
	if genDefault == "" {
		genDefault = genCol.DefaultExpr
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
	if !skipImplicitSequenceDefault {
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
	table schemamodel.Table,
	keyColumns []string,
	field schemamodel.Field,
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

	genType, dbType := normalizeColumnTypesForDialect(genCol, dbRawType, dialect)
	switch {
	case genType != dbType:
		return typeChangeText(dbRawType, genCol.Type, dbType, genType, dialect)
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
	default:
		return normalize.Type(genType), normalize.Type(dbType)
	}
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

func columnInTablePrimaryKey(table schemamodel.Table, column string) bool {
	return slices.Contains(tablePrimaryKeyColumns(table), column)
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
func removedColumn(reported catalog.Column) schemamodel.Field {
	field := catalogfield.Field(reported, catalogfield.Options{})
	field.Primary = false
	field.Foreign = ""
	field.ForeignKeyName = ""
	field.OnDelete = ""
	field.OnUpdate = ""
	field.Deferrable = false
	field.Initially = ""
	return field
}

// sortColumns orders by the key the name list was sorted on.
func sortColumns(columns difftypes.ColumnChanges) {
	sort.Slice(columns, func(i, j int) bool { return columns[i].Name < columns[j].Name })
}
