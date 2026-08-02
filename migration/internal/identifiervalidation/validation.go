// Package identifiervalidation validates generated identifiers before schema
// comparison or migration planning.
package identifiervalidation

import (
	"fmt"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/internal/indexscope"
	"go.5x5.cz/ptah/internal/tableref"
	"go.5x5.cz/ptah/migration/internal/generatedschema"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
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
	generated *goschema.Database,
	dialect string,
	semantics identifier.Semantics,
) error {
	if generated == nil {
		return nil
	}
	if err := ValidateCoverage(
		semantics,
		targetIdentifierNames(generated, semantics.DefaultSchema),
	); err != nil {
		return err
	}
	if err := validateTablesAndColumns(generated, semantics); err != nil {
		return err
	}
	_, err := indexscope.NewResolverWithSemantics(
		dialect,
		semantics,
		&difftypes.SchemaDiff{},
		generated,
	)
	return err
}

func targetIdentifierNames(
	generated *goschema.Database,
	defaultSchema string,
) []string {
	names := []string{defaultSchema}
	for _, field := range generated.Fields {
		names = append(names, field.Name)
	}
	for _, table := range generated.Tables {
		names = append(names, table.Schema, table.Name)
		for _, field := range generatedschema.FieldsForTable(generated, table) {
			names = append(names, field.Name)
		}
	}
	for _, index := range generated.Indexes {
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
	generated *goschema.Database,
	semantics identifier.Semantics,
) error {
	tables := make(map[string]string, len(generated.Tables))
	for _, table := range generated.Tables {
		rawName := table.QualifiedName()
		conflictKey := semantics.QualifiedTableConflictKey(rawName)
		if previous, exists := tables[conflictKey]; exists &&
			previous != rawName {
			return fmt.Errorf(
				"%w: target tables %s and %s may have the same catalog identity",
				ptaherr.ErrInvalidSchemaDiff,
				previous,
				rawName,
			)
		}
		tables[conflictKey] = rawName
	}

	for _, table := range generated.Tables {
		columns := make(map[string]string)
		for _, field := range generatedschema.FieldsForTable(generated, table) {
			conflictKey := semantics.ColumnConflictKey(field.Name)
			if previous, exists := columns[conflictKey]; exists &&
				previous != field.Name {
				return fmt.Errorf(
					"%w: target columns %s.%s and %s.%s may have the same catalog identity",
					ptaherr.ErrInvalidSchemaDiff,
					table.QualifiedName(),
					previous,
					table.QualifiedName(),
					field.Name,
				)
			}
			columns[conflictKey] = field.Name
		}
	}
	return nil
}
