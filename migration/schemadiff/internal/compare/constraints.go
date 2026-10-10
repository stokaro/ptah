package compare

import (
	"cmp"
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/constraintowner"
	"ptah.run/internal/exprkey"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/mysqlindex"
	"ptah.run/migration/schemadiff/difftypes"
)

// Constraints compares constraint definitions between generated and database schemas.
//
// This function identifies differences in table-level constraints such as EXCLUDE,
// CHECK, UNIQUE, PRIMARY KEY, and FOREIGN KEY constraints. It compares constraints
// defined through Go struct annotations with constraints that exist in the database.
//
// # Constraint Types Supported
//
//   - EXCLUDE: PostgreSQL EXCLUDE constraints for preventing conflicts
//   - CHECK: Table-level CHECK constraints for data validation
//   - UNIQUE: Table-level UNIQUE constraints spanning multiple columns
//   - PRIMARY KEY: Composite primary key constraints
//   - FOREIGN KEY: Table-level foreign key constraints
//
// # Comparison Logic
//
// The function performs constraint comparison by:
//  1. **Constraint Discovery**: Creates lookup maps for efficient constraint comparison
//  2. **Addition Detection**: Identifies constraints in generated schema but not in database
//  3. **Removal Detection**: Identifies constraints in database but not in generated schema
//
// # Database Schema Constraints
//
// The function currently focuses on constraints defined through schema annotations.
// Database-introspected constraints are not yet fully supported, so this function
// primarily detects constraint additions from the generated schema.
//
// # Example Usage
//
//	// Compare constraints between schemas
//	compare.Constraints(generated, database, diff)
//
//	// Check for constraint changes
//	if len(diff.ConstraintsAdded) > 0 {
//		log.Printf("Found %d new constraints to add", len(diff.ConstraintsAdded))
//	}
//
// # Parameters
//
//   - generated: Target schema parsed from Go struct annotations
//   - database: Current database schema from database introspection
//   - diff: Schema difference structure to populate with constraint changes
//
// # Limitations
//
//   - Database constraint introspection is not yet fully implemented
//   - Currently focuses on constraint additions from generated schema
//   - Constraint modifications are not yet detected
func Constraints(desired *schemamodel.Database, current *catalog.Database, diff *difftypes.SchemaDiff, opts *config.CompareOptions) {
	dialect := ""
	if opts != nil {
		dialect = opts.Dialect
	}
	ConstraintsWithSemantics(desired, current, diff, opts, identifier.ForDialect(dialect))
}

// ConstraintsWithSemantics is [Constraints] told which identifier rules the
// target actually has, rather than the offline defaults its dialect name
// implies. Table and index comparison already take the resolved rules; this
// entry point exists so constraint comparison stops being the one side that
// rebuilds them from the dialect string.
//
// The difference is not cosmetic on MySQL and MariaDB. A schema there is a
// database, so [identifier.ForDialect] cannot name the one that owns an
// unqualified table and returns an empty DefaultSchema; only a connection can
// supply it. Re-deriving the offline rules here discarded that value, the
// normalization in [tableMemberKey] became a no-op, and every constraint the
// database had was reported as removed -- the exact failure #1232 fixed on
// PostgreSQL and SQLite, still live on MySQL because it could never see a
// default schema (stokaro/ptah#1244).
func ConstraintsWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	opts *config.CompareOptions,
	semantics identifier.Semantics,
) {
	dialect := ""
	if opts != nil {
		dialect = opts.Dialect
	}
	genConstraints, dbConstraints := pairedConstraints(desired, database, dialect, semantics)

	// Find added constraints (constraints in generated schema but not in database)
	for constraintKey, genConstraint := range genConstraints {
		if _, exists := dbConstraints[constraintKey]; !exists {
			diff.ConstraintsAdded = appendConstraintAddition(diff.ConstraintsAdded, genConstraint, semantics)
		}
	}

	// Find removed constraints (constraints in database but not in generated schema)
	for constraintKey, dbConstraint := range dbConstraints {
		if _, exists := genConstraints[constraintKey]; !exists {
			diff.ConstraintsRemoved = appendConstraintRemoval(diff.ConstraintsRemoved, dbConstraint, semantics)
			diff.ForeignKeysRemovedWithTables = appendForeignKeyRemoval(diff.ForeignKeysRemovedWithTables, dbConstraint, semantics)
		}
	}

	// Find modified constraints (constraints that exist in both but have different definitions)
	for constraintKey, genConstraint := range genConstraints {
		if dbConstraint, exists := dbConstraints[constraintKey]; exists {
			if constraintDefinitionsChanged(genConstraint, dbConstraint, dialect, semantics, opts,
				indexTableRowFormat(database.Tables, catalog.Index{TableName: dbConstraint.TableName, Schema: dbConstraint.Schema}, semantics)) {
				// For now, treat modified constraints as removed + added
				// In the future, we could add a ConstraintsModified field to SchemaDiff
				diff.ConstraintsRemoved = appendConstraintRemoval(diff.ConstraintsRemoved, dbConstraint, semantics)
				diff.ForeignKeysRemovedWithTables = appendForeignKeyRemoval(diff.ForeignKeysRemovedWithTables, dbConstraint, semantics)
				diff.ConstraintsAdded = appendConstraintAddition(diff.ConstraintsAdded, genConstraint, semantics)
				continue
			}
			if needsValidation(genConstraint, dbConstraint) {
				diff.ConstraintsValidated = append(diff.ConstraintsValidated, difftypes.ConstraintValidation{
					TableName: dbConstraint.QualifiedTableName(),
					Name:      dbConstraint.Name,
				})
			}
		}
	}
	slices.SortFunc(diff.ConstraintsValidated, func(a, b difftypes.ConstraintValidation) int {
		return cmp.Or(strings.Compare(a.TableName, b.TableName), strings.Compare(a.Name, b.Name))
	})

	// Sort for consistent output. One list per direction now, ordered by host
	// then name, so a plan lists its constraint work the same way whatever the
	// map iteration produced (stokaro/ptah#2315). Unnamed constraints share a
	// name, so their definitions order them.
	slices.SortFunc(diff.ConstraintsAdded, func(a, b difftypes.ConstraintAdditionInfo) int {
		return cmp.Or(
			strings.Compare(a.TableName, b.TableName),
			strings.Compare(a.Name, b.Name),
			strings.Compare(a.Type, b.Type),
			strings.Compare(a.CheckExpression, b.CheckExpression),
			slices.Compare(a.Columns, b.Columns),
			strings.Compare(a.ExcludeElements, b.ExcludeElements),
			strings.Compare(a.ForeignTable, b.ForeignTable),
			slices.Compare(a.ForeignColumns, b.ForeignColumns),
		)
	})
	sort.Slice(diff.ConstraintsRemoved, func(i, j int) bool {
		a, b := diff.ConstraintsRemoved[i], diff.ConstraintsRemoved[j]
		if a.TableName != b.TableName {
			return a.TableName < b.TableName
		}
		return a.Name < b.Name
	})
	sort.Slice(diff.ForeignKeysRemovedWithTables, func(i, j int) bool {
		a, b := diff.ForeignKeysRemovedWithTables[i], diff.ForeignKeysRemovedWithTables[j]
		if a.TableName != b.TableName {
			return a.TableName < b.TableName
		}
		return a.Name < b.Name
	})
}

// pairedConstraints keys both sides' constraints the way the comparison pairs
// them: the desired side's declared constraints with the ones it synthesizes
// from columns and tables, and the database's constraints less the ones
// another representation owns. A key on both sides is one constraint.
//
// It is the one pairing, because the definitions and the comments of a
// constraint are compared by two functions that must agree on which database
// constraint a declaration is.
func pairedConstraints(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) (genConstraints map[tableMemberKey]schemamodel.Constraint, dbConstraints map[tableMemberKey]catalog.Constraint) {
	// Create maps for detailed constraint comparison
	genConstraints = declaredAndCheckConstraints(desired, database, dialect, semantics)

	recordSynthesized(genConstraints, synthesizeTablePrimaryKeyConstraints(desired, database, dialect, semantics), semantics)

	// Synthesize table-level Constraint entries from field-level `foreign=`
	// annotations so on_delete / on_update drift on an existing field-level
	// FK participates in comparison (issue #189), mirroring the field-level
	// CHECK synthesis above. Only synthesized for columns that already exist
	// in the database — new tables/columns get their FK inline via CREATE
	// TABLE / ALTER TABLE ADD CONSTRAINT, so emitting an ADD CONSTRAINT here
	// would double-create it in the same migration step.
	//
	// Every database foreign key stays in the comparison and is paired by
	// name, whichever form declares its counterpart; see
	// [isFieldLevelConstraint] for why none is excused by its column.
	recordSynthesized(genConstraints, synthesizeFieldLevelForeignKeyConstraints(desired, database, semantics), semantics)

	dbConstraints = collectDatabaseConstraints(
		desired,
		database,
		genConstraints,
		dialect,
		semantics,
	)
	pairNumberedForeignKeys(genConstraints, dbConstraints, dialect, semantics)

	return genConstraints, dbConstraints
}

// collectDatabaseConstraints keys the database's constraints by table and name,
// leaving out the ones another representation of the same object already owns.
//
// Two exclusions, and they are different in kind. A field-level constraint is
// carried by the column it belongs to, so the column's lifecycle creates and
// drops it; for a UNIQUE, [readColumnKeys] says which one that is. A UNIQUE constraint the desired state names as an *index* is the
// same catalog object as that index, so index comparison creates and drops it;
// see [uniqueConstraintOwnedByDeclaredIndex]. A desired state that names the
// object as a constraint keeps it here, which is why the hand-off asks about
// genConstraints first: a schema that declares both spellings of one name has
// its constraint honored rather than being handed to a pool that would then
// plan an ADD CONSTRAINT on top of the existing index.
func collectDatabaseConstraints(
	desired *schemamodel.Database,
	database *catalog.Database,
	genConstraints map[tableMemberKey]schemamodel.Constraint,
	dialect string,
	semantics identifier.Semantics,
) map[tableMemberKey]catalog.Constraint {
	declaredIndexes := generatedIndexIdentities(desired, semantics)
	ownedByColumns := readColumnKeys(desired, database, dialect, semantics).owned
	dbConstraints := make(map[tableMemberKey]catalog.Constraint, len(database.Constraints))
	for _, constraint := range database.Constraints {
		key := newCatalogConstraintKey(constraint, semantics)
		if _, owned := ownedByColumns[key]; owned {
			continue
		}
		if isFieldLevelConstraint(constraint, desired, semantics) {
			continue
		}
		if _, declaredAsConstraint := genConstraints[key]; !declaredAsConstraint &&
			uniqueConstraintOwnedByDeclaredIndex(
				constraint,
				dialect,
				declaredIndexes,
				semantics,
			) {
			continue
		}
		dbConstraints[key] = constraint
	}
	return dbConstraints
}

// appendConstraintRemoval records the table-qualified removal info for a
// database constraint that is being dropped (or modified, which is expressed as
// remove + add). Dialects that need the owning table and a type-specific drop
// syntax — MySQL/MariaDB FOREIGN KEY uses DROP FOREIGN KEY rather than DROP
// CONSTRAINT — read this parallel slice instead of the bare name list.
func appendConstraintRemoval(
	infos difftypes.ConstraintRemovals,
	dbConstraint catalog.Constraint,
	semantics identifier.Semantics,
) difftypes.ConstraintRemovals {
	return append(infos, difftypes.ConstraintRemovalInfo{
		Name:      dbConstraint.Name,
		TableName: dbConstraint.QualifiedTableName(),
		Type:      dbConstraint.Type,
		Identity:  constraintIdentity(dbConstraint.QualifiedTableName(), dbConstraint.Name, semantics),
	})
}

func appendForeignKeyRemoval(
	infos []difftypes.ForeignKeyRemovalInfo,
	dbConstraint catalog.Constraint,
	semantics identifier.Semantics,
) []difftypes.ForeignKeyRemovalInfo {
	if !strings.EqualFold(dbConstraint.Type, "FOREIGN KEY") {
		return infos
	}
	return append(infos, difftypes.ForeignKeyRemovalInfo{
		Name:           dbConstraint.Name,
		TableName:      dbConstraint.QualifiedTableName(),
		Identity:       constraintIdentity(dbConstraint.QualifiedTableName(), dbConstraint.Name, semantics),
		Columns:        append([]string(nil), dbConstraint.ColumnNamesOrDefault()...),
		ForeignTable:   dbConstraint.QualifiedForeignTableName(),
		ForeignColumns: append([]string(nil), dbConstraint.ForeignColumnsOrDefault()...),
	})
}

// appendConstraintAddition records the table-qualified definition of a
// constraint that is being added (or modified, which is expressed as remove +
// add). The bare ConstraintsAdded name list cannot disambiguate a field-level
// FK whose name repeats across several tables — the canonical case being an
// embedded inline-relation mixin (e.g. fk_entity_tenant on every table that
// embeds a tenant-aware base struct, issue #197). Planners read this parallel
// slice to emit one correctly-targeted ALTER TABLE per real host table instead
// of re-deriving the table from a field's Go struct name (which, for a mixin,
// is not a table at all). For a unique-named constraint this carries exactly
// one entry and matches the previous field-scan behavior.
func appendConstraintAddition(
	infos difftypes.ConstraintAdditions,
	genConstraint schemamodel.Constraint,
	semantics identifier.Semantics,
) difftypes.ConstraintAdditions {
	return append(infos, difftypes.ConstraintAdditionInfo{
		Name:            genConstraint.Name,
		TableName:       genConstraint.Table,
		Type:            genConstraint.Type,
		Columns:         append([]string(nil), genConstraint.Columns...),
		IncludeColumns:  append([]string(nil), genConstraint.IncludeColumns...),
		NullsDistinct:   cloneBoolPtr(genConstraint.NullsDistinct),
		Comment:         genConstraint.Comment,
		CheckExpression: genConstraint.CheckExpression,
		UsingMethod:     genConstraint.UsingMethod,
		KeyBlockSize:    genConstraint.KeyBlockSize,
		ExcludeElements: genConstraint.ExcludeElements,
		WhereCondition:  genConstraint.WhereCondition,
		ForeignTable:    genConstraint.ForeignTable,
		ForeignColumn:   genConstraint.ForeignColumn,
		ForeignColumns:  append([]string(nil), genConstraint.ForeignColumnsOrDefault()...),
		OnDelete:        genConstraint.OnDelete,
		OnUpdate:        genConstraint.OnUpdate,
		OnDeleteColumns: append([]string(nil), genConstraint.OnDeleteColumns...),
		Deferrable:      genConstraint.Deferrable,
		Initially:       genConstraint.Initially,
		Match:           genConstraint.Match,
		NotEnforced:     genConstraint.NotEnforced,
		NotValid:        genConstraint.NotValid,
		Identity:        constraintIdentity(genConstraint.Table, genConstraint.Name, semantics),
	})
}

// needsValidation reports whether the database holds NOT VALID a CHECK or
// foreign key the declaration holds validated. The other direction is no
// change: a declaration that allows NOT VALID is satisfied by a constraint the
// server has validated, and one it has not.
func needsValidation(genConstraint schemamodel.Constraint, dbConstraint catalog.Constraint) bool {
	return dbConstraint.NotValid && !genConstraint.NotValid
}

// constraintDefinitionsChanged compares constraint definitions between generated and database schemas
// to detect if a constraint needs to be recreated due to definition changes.
func constraintDefinitionsChanged(
	genConstraint schemamodel.Constraint,
	dbConstraint catalog.Constraint,
	dialect string,
	semantics identifier.Semantics,
	opts *config.CompareOptions,
	rowFormat string,
) bool {
	// Basic constraint type comparison
	if genConstraint.Type != dbConstraint.Type {
		return true
	}

	// Type-specific comparisons
	switch genConstraint.Type {
	case "EXCLUDE":
		return excludeConstraintChanged(genConstraint, dbConstraint, excludeExpressionsOf(opts), semantics)
	case "CHECK":
		return checkConstraintChanged(genConstraint, dbConstraint, checkExpressionsOf(opts), semantics)
	case "UNIQUE":
		return uniqueConstraintChanged(genConstraint, dbConstraint, semantics)
	case "PRIMARY KEY":
		return primaryKeyConstraintChanged(genConstraint, dbConstraint, dialect, semantics, rowFormat)
	case "FOREIGN KEY":
		return foreignKeyConstraintChanged(genConstraint, dbConstraint, dialect, semantics)
	default:
		// For unknown constraint types, assume no change
		return false
	}
}

// primaryKeyConstraintChanged compares primary keys. The columns compare as
// the dialect resolves names: Oracle reports an unquoted `id` as ID, and
// compared as text, a key read back from Oracle 23 was dropped and added on
// every plan. The access method compares as any MySQL-family index's does,
// where the server keeps it; see [mysqlindex.MethodSatisfiedBy].
func primaryKeyConstraintChanged(
	genConstraint schemamodel.Constraint,
	dbConstraint catalog.Constraint,
	dialect string,
	semantics identifier.Semantics,
	rowFormat string,
) bool {
	if platform.NormalizeDialect(dialect) == platform.MySQL || platform.NormalizeDialect(dialect) == platform.MariaDB {
		if genConstraint.Comment != dbConstraint.Comment ||
			(mysqlindex.KeepsBlockSize(dialect, rowFormat) && genConstraint.KeyBlockSize != dbConstraint.KeyBlockSize) {
			return true
		}
	}
	return !sameColumnNames(semantics, genConstraint.Columns, dbConstraint.ColumnNamesOrDefault()) ||
		!stringSetsEqual(genConstraint.IncludeColumns, dbConstraint.IncludeColumns) ||
		deferralChanged(genConstraint, dbConstraint) ||
		!mysqlindex.MethodSatisfiedBy(dialect, genConstraint.UsingMethod, getStringValue(dbConstraint.UsingMethod))
}

// excludeConstraintChanged compares EXCLUDE constraint definitions.
//
// A resolved entry holds the declared elements and WHERE clause as the server
// prints them, split the way the reader splits a live constraint, so the two
// are compared as like with like. PostgreSQL 18.6 stores `WHERE (s > 0)` as
// `WHERE ((s > 0))` and casts a literal to the type of the column it meets, so
// a text comparison planned an unchanged constraint again on every run
// (stokaro/ptah#3767). Without a resolver the texts are compared as they are.
func excludeConstraintChanged(
	genConstraint schemamodel.Constraint,
	dbConstraint catalog.Constraint,
	excludes map[string]config.ExcludeExpression,
	semantics identifier.Semantics,
) bool {
	if !strings.EqualFold(genConstraint.UsingMethod, getStringValue(dbConstraint.UsingMethod)) ||
		deferralChanged(genConstraint, dbConstraint) {
		return true
	}
	elements, where := genConstraint.ExcludeElements, genConstraint.WhereCondition
	if resolved, ok := excludes[exprkey.Exclude(semantics, genConstraint.Table, genConstraint.Name)]; ok && resolved.Resolved {
		elements, where = resolved.Elements, resolved.Where
	}
	return elements != getStringValue(dbConstraint.ExcludeElements) ||
		where != getStringValue(dbConstraint.WhereCondition)
}

// excludeExpressionsOf reads the resolved map out of the options, which may be
// nil on every offline path.
func excludeExpressionsOf(opts *config.CompareOptions) map[string]config.ExcludeExpression {
	if opts == nil {
		return nil
	}
	return opts.ExcludeExpressions
}

// declaredAndCheckConstraints collects the desired side's declared constraints
// and the CHECK constraints the comparison synthesizes, keyed as the comparison
// keys them. A declared constraint wins over a synthesized one with its name.
func declaredAndCheckConstraints(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) map[tableMemberKey]schemamodel.Constraint {
	constraints := make(map[tableMemberKey]schemamodel.Constraint)
	for _, constraint := range desired.Constraints {
		constraint, key := comparedDeclaredConstraint(constraint, desired.Tables, database, dialect, semantics)
		constraints[key] = constraint
	}

	// Synthesize table-level Constraint entries from field-level `check=`
	// annotations so they participate in drift comparison alongside table
	// constraints from `//ptah:schema:constraint`. Only synthesized for
	// columns that already exist in the database — new tables/columns get
	// their CHECK inline via CREATE TABLE / ALTER TABLE ADD COLUMN, and
	// double-emitting an ALTER TABLE ADD CONSTRAINT would fail because the
	// constraint is created in the same migration step.
	recordSynthesized(constraints, synthesizeFieldLevelCheckConstraints(desired, database, dialect, semantics), semantics)

	// Synthesize table-level Constraint entries from the table's own `checks`
	// list, which renders as a named CHECK. Without this the constraint the
	// render created is reported as one to drop on every run after the first
	// (stokaro/ptah#2590).
	recordSynthesized(constraints, synthesizeTableLevelCheckConstraints(desired, database, dialect, semantics), semantics)
	return constraints
}

// declaredConstraint returns a constraint the declaration states, with its
// table named the way the comparison names it, and the key it pairs by. The
// constraint comparison and the comment comparison both key a declared
// constraint through it, so the two cannot disagree about which constraint a
// declaration is.
func declaredConstraint(
	constraint schemamodel.Constraint, tables []schemamodel.Table, semantics identifier.Semantics,
) (schemamodel.Constraint, tableMemberKey) {
	constraint.Table = constraintowner.TableName(constraint, tables)
	return constraint, newDeclaredConstraintKey(constraint, semantics)
}

// comparedDeclaredConstraint is [declaredConstraint] for a comparison with
// database on dialect. A declared primary key takes the name of the database's
// primary key where its own name cannot find it: always on MySQL and MariaDB,
// which call every primary key PRIMARY whatever the declaration says, and on
// any dialect when the declaration names none. Keyed by the declared name, the
// key the first apply built never meets its declaration, and every later apply
// plans it again (stokaro/ptah#3959).
//
// The constraint comparison and the comment comparison both key a declaration
// through it, so the two pair a declared key with the same database key.
func comparedDeclaredConstraint(
	constraint schemamodel.Constraint,
	tables []schemamodel.Table,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) (schemamodel.Constraint, tableMemberKey) {
	constraint, key := declaredConstraint(constraint, tables, semantics)
	if !strings.EqualFold(strings.TrimSpace(constraint.Type), "PRIMARY KEY") || database == nil {
		return constraint, key
	}
	if !isMySQLFamily(dialect) && strings.TrimSpace(constraint.Name) != "" {
		return constraint, key
	}
	identity := newQualifiedTableIdentity(constraint.Table, semantics)
	name, found := livePrimaryKeyName(database.Constraints, identity, semantics)
	if !found {
		return constraint, key
	}
	constraint.Name = name
	return constraint, newDeclaredConstraintKey(constraint, semantics)
}

// ComparedExcludeConstraints returns every EXCLUDE constraint the desired side
// declares, with its table named the way the comparison names it, ordered by
// table and name. The resolver that asks the server to spell each one reads
// this set, so it keys each answer as the comparison looks it up.
func ComparedExcludeConstraints(
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) []schemamodel.Constraint {
	if desired == nil {
		return nil
	}
	var excludes []schemamodel.Constraint
	for _, constraint := range desired.Constraints {
		if !strings.EqualFold(constraint.Type, "EXCLUDE") {
			continue
		}
		constraint, _ = declaredConstraint(constraint, desired.Tables, semantics)
		excludes = append(excludes, constraint)
	}
	slices.SortFunc(excludes, func(a, b schemamodel.Constraint) int {
		return cmp.Or(strings.Compare(a.Table, b.Table), strings.Compare(a.Name, b.Name))
	})
	return excludes
}

// ComparedCheckConstraints returns every CHECK constraint the constraint
// comparison holds for the desired side against database on dialect: the
// declared ones, and the ones it synthesizes from a column's check and a
// table's checks list. They are ordered by table and name.
//
// The resolver that asks the server to spell each CHECK reads this set, so the
// set it resolves is the set compared. The resolver read the declared
// constraints alone, and a column-level CHECK the server rewrites -- `kind IN
// ('plugin')`, stored by PostgreSQL 18.6 as `(kind = 'plugin'::text)` -- was
// never asked about and was dropped and re-added on every run
// (stokaro/ptah#3643).
func ComparedCheckConstraints(
	desired *schemamodel.Database,
	database *catalog.Database,
	dialect string,
	semantics identifier.Semantics,
) []schemamodel.Constraint {
	if desired == nil {
		return nil
	}
	var checks []schemamodel.Constraint
	for _, constraint := range declaredAndCheckConstraints(desired, database, dialect, semantics) {
		if strings.EqualFold(constraint.Type, "CHECK") {
			checks = append(checks, constraint)
		}
	}
	slices.SortFunc(checks, func(a, b schemamodel.Constraint) int {
		return cmp.Or(
			strings.Compare(a.Table, b.Table),
			strings.Compare(a.Name, b.Name),
			strings.Compare(a.CheckExpression, b.CheckExpression),
		)
	})
	return checks
}

// checkConstraintChanged compares CHECK constraint definitions.
//
// A resolved entry answers it outright: the declaration was put through the
// same server that printed the catalog's form, so the two are compared as like
// with like. PostgreSQL stores a parse tree rather than the text it was given,
// and the textual normalizer below folds some of that rewrite and cannot fold
// the rest -- `price >= 0` comes back as `(price >= (0)::numeric)` and
// `score BETWEEN 1 AND 10` as two comparisons, so a check nobody had changed
// was dropped and re-added on every run (stokaro/ptah#2044).
//
// Without a resolver the old rule stands, which is right for the shapes it
// folds and declines the one it cannot.
func checkConstraintChanged(
	genConstraint schemamodel.Constraint,
	dbConstraint catalog.Constraint,
	checks map[string]config.CheckExpression,
	semantics identifier.Semantics,
) bool {
	// A CHECK the server does not enforce is another constraint from one it
	// does. PostgreSQL 18.6 cannot change it in place (`cannot alter
	// enforceability of constraint`), so a difference is a drop and an add
	// (stokaro/ptah#3853).
	if genConstraint.NotEnforced != dbConstraint.NotEnforced {
		return true
	}
	dbClause := getStringValue(dbConstraint.CheckClause)
	if strings.TrimSpace(genConstraint.CheckExpression) == "" || strings.TrimSpace(dbClause) == "" {
		return false
	}
	if resolved, ok := resolvedCheckExpression(checks, genConstraint, semantics); ok {
		return normalizeCheckExpression(resolved) != normalizeCheckExpression(dbClause)
	}
	if checkExpressionHasUnsupportedRewrite(genConstraint.CheckExpression, dbClause) {
		return false
	}
	return normalizeCheckExpression(genConstraint.CheckExpression) != normalizeCheckExpression(dbClause)
}

// resolvedCheckExpression looks up the server's spelling of one declared
// check, and reports whether there is one to use.
//
// A present key that is not Resolved is a declaration the server refused, and
// it falls back rather than being read as a difference: the refusal says
// nothing about whether the two expressions agree.
func resolvedCheckExpression(
	checks map[string]config.CheckExpression,
	constraint schemamodel.Constraint,
	semantics identifier.Semantics,
) (string, bool) {
	resolved, ok := checks[exprkey.Check(semantics, constraint.Table, constraint.Name)]
	if !ok || !resolved.Resolved {
		return "", false
	}
	return resolved.Expression, true
}

// checkExpressionsOf reads the resolved map out of the options, which may be
// nil on every offline path.
func checkExpressionsOf(opts *config.CompareOptions) map[string]config.CheckExpression {
	if opts == nil {
		return nil
	}
	return opts.CheckExpressions
}

// uniqueConstraintChanged compares UNIQUE constraint definitions. The columns
// compare as the dialect resolves names, for the reason
// primaryKeyConstraintChanged gives.
func uniqueConstraintChanged(
	genConstraint schemamodel.Constraint,
	dbConstraint catalog.Constraint,
	semantics identifier.Semantics,
) bool {
	return !columnSetsMatch(genConstraint.Columns, dbConstraint.ColumnNamesOrDefault(), semantics) ||
		!stringSetsEqual(genConstraint.IncludeColumns, dbConstraint.IncludeColumns) ||
		!nullsDistinctEqual(genConstraint.NullsDistinct, dbConstraint.NullsDistinct) ||
		deferralChanged(genConstraint, dbConstraint)
}

// indexTableRowFormat resolves the index owner using the comparison's name semantics.
func indexTableRowFormat(tables []catalog.Table, index catalog.Index, semantics identifier.Semantics) string {
	ref := indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{TableName: index.QualifiedTableName(), Name: index.Name})
	for _, table := range tables {
		owner := indexscope.IdentityKeyWithSemantics(semantics, difftypes.IndexRef{TableName: table.QualifiedName(), Name: index.Name})
		if owner == ref {
			return table.RowFormat
		}
	}
	return ""
}
