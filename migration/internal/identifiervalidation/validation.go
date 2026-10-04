// Package identifiervalidation validates generated identifiers before schema
// comparison or migration planning.
package identifiervalidation

import (
	"fmt"
	"strings"

	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/indexscope"
	"ptah.run/internal/tableref"
	"ptah.run/migration/internal/generatedschema"
	"ptah.run/migration/schemadiff/difftypes"
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
	return indexscope.ValidateDeclared(dialect, semantics, difftypes.IndexDeclarationsOf(desired))
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
	return names
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
	if previous, current, conflict := conflictingTable(semantics, desired); conflict {
		return fmt.Errorf(
			"%w: target tables %s and %s may have the same catalog identity",
			ptaherr.ErrInvalidSchemaDiff,
			previous,
			current,
		)
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

// conflictingTable returns the first pair of tables the target cannot keep
// apart, in declaration order. A table declared twice under one spelling is
// one table, not a pair.
//
// A table whose name has no equivalence class is compared against every table
// already seen, for the reason [conflictingColumn] gives: under an
// accent-insensitive collation `örders` is the table `orders`, and the two
// conflict keys are different.
func conflictingTable(
	semantics identifier.Semantics,
	desired *schemamodel.Database,
) (previous, current string, conflict bool) {
	tables := make(map[string]string, len(desired.Tables))
	var seen []string
	unresolved := ""
	for _, table := range desired.Tables {
		rawName := table.QualifiedName()
		conflictKey := semantics.QualifiedTableConflictKey(rawName)
		if strings.Contains(conflictKey, unresolvedTableKey) {
			if earlier, found := firstOther(seen, rawName); found {
				return earlier, rawName, true
			}
			if unresolved == "" {
				unresolved = rawName
			}
			seen = append(seen, rawName)
			continue
		}
		if unresolved != "" && unresolved != rawName {
			return unresolved, rawName, true
		}
		if earlier, exists := tables[conflictKey]; exists && earlier != rawName {
			return earlier, rawName, true
		}
		tables[conflictKey] = rawName
		seen = append(seen, rawName)
	}
	return "", "", false
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
