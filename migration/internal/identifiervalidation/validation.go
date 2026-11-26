// Package identifiervalidation validates generated identifiers before schema
// comparison or migration planning.
package identifiervalidation

import (
	"fmt"
	"strings"

	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/generatedschema"
)

// ValidateCoverage rejects catalog-resolved snapshots that omit a candidate.
func ValidateCoverage(
	semantics identifier.Semantics,
	names []string,
) error {
	for _, name := range names {
		if name != "" && !semantics.Resolves(name) {
			return fmt.Errorf(
				"%w: identifier semantics snapshot does not resolve %q",
				ptaherr.ErrInvalidSchemaDiff,
				name,
			)
		}
	}
	return nil
}

// ValidateTarget rejects unresolved or colliding target identifiers.
func ValidateTarget(
	desired *schemamodel.Database,
	dialect string,
	semantics identifier.Semantics,
) error {
	if desired == nil {
		return nil
	}
	if err := ValidateCoverage(
		semantics,
		targetIdentifierNames(desired, semantics.DefaultSchema),
	); err != nil {
		return err
	}
	if err := validateTablesAndColumns(desired, semantics); err != nil {
		return err
	}
	if err := validateSchemaObjects(desired, semantics); err != nil {
		return err
	}
	return indexscope.ValidateDeclared(dialect, semantics, difftypes.IndexDeclarationsOf(desired))
}

// ObjectNames lists the names of the views, materialized views, routines,
// sequences and triggers desired declares, with the table each trigger is on.
// A schema-qualified name contributes its schema and its name, each without
// its quotes. A nil desired lists nothing.
//
// It is the one list of these names. The comparison asks a server how it
// compares them, and [ValidateTarget] checks that the server was asked about
// every one, so the two cannot disagree about which names those are.
func ObjectNames(desired *schemamodel.Database) []string {
	if desired == nil {
		return nil
	}
	var names []string
	for _, view := range desired.Views {
		names = appendQualifiedIdentifier(names, view.Name)
	}
	for _, view := range desired.MaterializedViews {
		names = appendQualifiedIdentifier(names, view.Name)
	}
	for _, function := range desired.Functions {
		names = appendQualifiedIdentifier(names, function.Name)
	}
	for _, sequence := range desired.Sequences {
		if sequence.Schema != "" {
			names = append(names, sequence.Schema)
		}
		names = appendQualifiedIdentifier(names, sequence.Name)
	}
	for _, trigger := range desired.Triggers {
		names = appendQualifiedIdentifier(names, trigger.Table)
		names = appendQualifiedIdentifier(names, trigger.Name)
	}
	return names
}

func targetIdentifierNames(
	desired *schemamodel.Database,
	defaultSchema string,
) []string {
	names := []string{defaultSchema}
	for _, field := range desired.Fields {
		names = append(names, field.Name)
	}
	for _, table := range desired.Tables {
		names = append(names, table.Schema, table.Name)
		for _, field := range generatedschema.FieldsForTable(desired, table) {
			names = append(names, field.Name)
		}
	}
	for _, index := range desired.Indexes {
		names = append(names, index.Name)
		names = appendQualifiedIdentifier(names, index.TableName)
		names = append(names, index.Fields...)
		names = append(names, index.IncludeColumns...)
		for _, part := range index.Parts {
			names = append(names, part.Name)
		}
	}
	return append(names, ObjectNames(desired)...)
}

func appendQualifiedIdentifier(names []string, value string) []string {
	ref, ok := tableref.Parse(value)
	if !ok {
		return append(names, value)
	}
	if !ref.Qualified {
		return append(names, ref.Name)
	}
	return append(names, ref.Schema, ref.Name)
}

func validateTablesAndColumns(
	desired *schemamodel.Database,
	semantics identifier.Semantics,
) error {
	semantics = indexscope.ConflictSemantics(semantics)
	tables := make([]spelledName, 0, len(desired.Tables))
	for _, table := range desired.Tables {
		tables = append(tables, spelledName{spelling: table.QualifiedName(), key: semantics.QualifiedTableConflictKey(table.QualifiedName())})
	}
	if previous, current, conflict := firstConflict(tables); conflict {
		return conflictError("tables", previous, current)
	}

	for _, table := range desired.Tables {
		if previous, current, conflict := conflictingColumn(semantics, desired, table); conflict {
			return fmt.Errorf(
				"%w: target columns %s.%s and %s.%s may have the same catalog identity",
				ptaherr.ErrInvalidSchemaDiff,
				table.QualifiedName(),
				previous,
				table.QualifiedName(),
				current,
			)
		}
	}
	return nil
}

// unresolvedTableKey is the part every conflict key of a table name the target
// cannot place contains, whichever of the schema and the name is unresolved.
var unresolvedTableKey = identifier.ComparisonCatalogUnknown.ConflictKey("")

// spelledName is a declared name as a conflict check reads it: spelling
// says which declaration it is, and key is its conflict key.
type spelledName struct {
	spelling string
	key      string
}

// firstConflict returns the first pair of names the target cannot keep
// apart, in declaration order. Two names spelled alike are one declaration,
// not a pair.
//
// A name whose key has no equivalence class is compared against every name
// already seen, for the reason [conflictingColumn] gives: under an
// accent-insensitive collation `örders` is the table `orders`, and the two
// conflict keys are different.
func firstConflict(names []spelledName) (previous, current string, conflict bool) {
	byKey := make(map[string]string, len(names))
	var seen []string
	unresolved := ""
	for _, name := range names {
		if strings.Contains(name.key, unresolvedTableKey) {
			if earlier, found := firstOther(seen, name.spelling); found {
				return earlier, name.spelling, true
			}
			if unresolved == "" {
				unresolved = name.spelling
			}
			seen = append(seen, name.spelling)
			continue
		}
		if unresolved != "" && unresolved != name.spelling {
			return unresolved, name.spelling, true
		}
		if earlier, exists := byKey[name.key]; exists && earlier != name.spelling {
			return earlier, name.spelling, true
		}
		byKey[name.key] = name.spelling
		seen = append(seen, name.spelling)
	}
	return "", "", false
}

// validateSchemaObjects refuses two declared views, materialized views,
// routines, sequences or triggers of one kind that the target may read as
// one object.
//
// The comparison keys each of these kinds by the table rule, so two such
// declarations share one key, and one of them is compared while the other is
// dropped in silence. Measured on SQL Server 2025 under a case-insensitive
// collation, the views MixedView and MIXEDVIEW are one view: a plan created
// one of them, and the next comparison found nothing to do.
//
// A routine declared twice under one name is two overloads, which is how
// PostgreSQL holds them, so two routines are one declaration when they are
// in one schema and spelled alike. Triggers are named per table, as the
// comparison names them.
func validateSchemaObjects(desired *schemamodel.Database, semantics identifier.Semantics) error {
	semantics = indexscope.ConflictSemantics(semantics)
	kinds := []struct {
		noun  string
		names []string
	}{
		{noun: "views", names: nameList(desired.Views, func(view schemamodel.View) string { return view.Name })},
		{noun: "materialized views", names: nameList(desired.MaterializedViews, func(view schemamodel.MaterializedView) string { return view.Name })},
		{noun: "routines", names: nameList(desired.Functions, func(function schemamodel.Function) string { return function.Name })},
		{noun: "sequences", names: nameList(desired.Sequences, schemamodel.Sequence.QualifiedName)},
	}
	for _, kind := range kinds {
		spelled := make([]spelledName, 0, len(kind.names))
		for _, name := range kind.names {
			spelled = append(spelled, spelledName{spelling: schemaSpelling(name, semantics), key: semantics.QualifiedTableConflictKey(name)})
		}
		if previous, current, conflict := firstConflict(spelled); conflict {
			return conflictError(kind.noun, previous, current)
		}
	}
	triggers := make([]spelledName, 0, len(desired.Triggers))
	for _, trigger := range desired.Triggers {
		triggers = append(triggers, spelledName{
			spelling: schemaSpelling(trigger.Table, semantics) + " trigger " + trigger.Name,
			key:      semantics.QualifiedTableConflictKey(trigger.Table) + "\x00" + semantics.TableConflictKey(trigger.Name),
		})
	}
	if previous, current, conflict := firstConflict(triggers); conflict {
		return conflictError("triggers", previous, current)
	}
	return nil
}

// schemaSpelling is name with the schema it is in written out, so that
// `orders`, `dbo.orders` and `[dbo].[orders]` read as one spelling on a
// target whose default schema is dbo.
func schemaSpelling(name string, semantics identifier.Semantics) string {
	ref, ok := tableref.Parse(name)
	if !ok {
		return name
	}
	if !ref.Qualified {
		ref.Schema = semantics.DefaultSchema
	}
	return tableref.Canonical(ref.Schema, ref.Name)
}

// nameList lists the name of every element.
func nameList[T any](elements []T, name func(T) string) []string {
	listed := make([]string, 0, len(elements))
	for _, element := range elements {
		listed = append(listed, name(element))
	}
	return listed
}

// conflictError names two declarations of one kind the target may read as
// one object.
func conflictError(kind, previous, current string) error {
	return fmt.Errorf(
		"%w: target %s %s and %s may have the same catalog identity",
		ptaherr.ErrInvalidSchemaDiff,
		kind,
		previous,
		current,
	)
}

// firstOther returns the first name in seen spelled differently from name.
func firstOther(seen []string, name string) (string, bool) {
	for _, earlier := range seen {
		if earlier != name {
			return earlier, true
		}
	}
	return "", false
}

// conflictingColumn returns the first pair of column names one table cannot
// keep apart, in declaration order.
//
// A name whose equivalence class the target cannot resolve is compared against
// every column already seen rather than against its own conflict bucket. That
// is not a refinement: MySQL treats `İ` and ASCII `i` as one column, and one
// side of that pair is ASCII, so bucketing the unresolved names together
// leaves the two in different buckets and misses the collision the server
// actually reports (stokaro/ptah#2771).
func conflictingColumn(
	semantics identifier.Semantics,
	desired *schemamodel.Database,
	table schemamodel.Table,
) (previous, current string, conflict bool) {
	columns := make(map[string]string)
	seen := make([]string, 0, len(desired.Fields))
	unresolved := ""
	for _, field := range generatedschema.FieldsForTable(desired, table) {
		if semantics.ColumnConflictUnresolved(field.Name) {
			if len(seen) > 0 {
				return seen[0], field.Name, true
			}
			unresolved = field.Name
			seen = append(seen, field.Name)
			continue
		}
		if unresolved != "" {
			return unresolved, field.Name, true
		}
		conflictKey := semantics.ColumnConflictKey(field.Name)
		if earlier, exists := columns[conflictKey]; exists && earlier != field.Name {
			return earlier, field.Name, true
		}
		columns[conflictKey] = field.Name
		seen = append(seen, field.Name)
	}
	return "", "", false
}
