package pgtablesource

import (
	"context"
	"encoding/csv"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/hclext"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/postgres/pgtable"
)

const deleteColumnsAttribute = "on_delete_columns"
const foreignKeyDirective = "ptah:schema:constraint"

func deleteColumnsCoverage() (schemaext.Coverage, error) {
	return pgtable.DeleteColumnsCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)
}

// DeleteColumnsAnnotations reads and writes a CSV list of logical local
// column names on the common constraint directive. An omitted list selects
// every local column, as a SQL foreign key without a list does.
func DeleteColumnsAnnotations() annotation.Extension {
	return annotation.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.DeleteColumnsKind},
		Attributes: []annotation.DirectiveAttributes{{Directive: foreignKeyDirective,
			Attributes: []annotation.Attribute{{Name: deleteColumnsAttribute, Value: "list", Description: "CSV list of local columns set by PostgreSQL ON DELETE SET NULL or SET DEFAULT."}},
			Reads:      []string{"type"}, Decode: decodeDeleteColumnsAnnotation}},
		Coverage: annotation.Unlimited(deleteColumnsCoverage), Encoder: deleteColumnsExporter{}, Encodes: []schemaext.Kind{pgtable.DeleteColumnsKind}}
}
func decodeDeleteColumnsAnnotation(attributes map[string]string) (schemaext.Facets, error) {
	text, present := attributes[deleteColumnsAttribute]
	if !present {
		return schemaext.Facets{}, nil
	}
	if strings.ToUpper(strings.TrimSpace(attributes["type"])) != "FOREIGN KEY" {
		return schemaext.Facets{}, fmt.Errorf("%w: on_delete_columns requires a FOREIGN KEY constraint", schemaext.ErrInvalidValue)
	}
	records, err := csv.NewReader(strings.NewReader(text)).ReadAll()
	if err != nil || len(records) != 1 || len(records[0]) == 0 {
		return schemaext.Facets{}, fmt.Errorf("%w: on_delete_columns requires one nonempty CSV list", schemaext.ErrInvalidValue)
	}
	return pgtable.WithDeleteColumns(schemaext.Facets{}, records[0])
}

type deleteColumnsExporter struct{}

func (deleteColumnsExporter) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if err := deleteColumnsExportContext(ctx, request.Coverage); err != nil {
		return annotation.EncodeResult{}, err
	}
	result := annotation.EncodeResult{}
	for _, slot := range request.Slots {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		columns, _, err := pgtable.DeleteColumnsOf(slot.Facets)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		if len(columns) == 0 {
			continue
		}
		if slot.Slot.Directive != foreignKeyDirective || slot.Reads["type"] != "FOREIGN KEY" {
			return annotation.EncodeResult{}, fmt.Errorf("%w: ON DELETE column selection requires a FOREIGN KEY annotation", schemaext.ErrInvalidValue)
		}
		var text strings.Builder
		writer := csv.NewWriter(&text)
		if err := writer.Write(columns); err != nil {
			return annotation.EncodeResult{}, err
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Attributes = append(result.Attributes, annotation.SlotAttributes{Slot: slot.Slot,
			Attributes: []annotation.EncodedAttribute{{Name: deleteColumnsAttribute, Value: strings.TrimSuffix(text.String(), "\n")}}})
	}
	return result, ctx.Err()
}

// DeleteColumnsHCL describes default selection on foreign keys and refuses an
// export that narrows the action, because Atlas HCL has no column-list syntax.
// An explicit full-key selection is equivalent to the action without a list.
func DeleteColumnsHCL() hclext.Extension {
	return hclext.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.DeleteColumnsKind}, Encoder: deleteColumnsExporter{},
		Coverage: func(hclext.CoverageRequest) (schemaext.Coverage, error) { return deleteColumnsCoverage() }}
}
func (deleteColumnsExporter) EncodeHCL(ctx context.Context, request hclext.EncodeRequest) (hclext.EncodeResult, error) {
	if err := deleteColumnsExportContext(ctx, request.Coverage); err != nil {
		return hclext.EncodeResult{}, err
	}
	for _, slot := range request.Keys {
		if err := ctx.Err(); err != nil {
			return hclext.EncodeResult{}, err
		}
		columns, _, err := pgtable.DeleteColumnsOf(slot.Facets)
		if err != nil {
			return hclext.EncodeResult{}, err
		}
		if len(columns) == 0 {
			continue
		}
		if slot.Key == nil || slot.Key.Type != "FOREIGN KEY" {
			return hclext.EncodeResult{}, fmt.Errorf("%w: ON DELETE column selection requires captured foreign-key columns", schemaext.ErrInvalidValue)
		}
		for _, column := range columns {
			if !slices.Contains(slot.Key.Columns, column) {
				return hclext.EncodeResult{}, fmt.Errorf("%w: ON DELETE column %q is outside foreign key %q", schemaext.ErrInvalidValue, column, slot.Slot.Name)
			}
		}
		for _, column := range slot.Key.Columns {
			if len(columns) > 0 && !slices.Contains(columns, column) {
				return hclext.EncodeResult{}, fmt.Errorf("%w: foreign key %q limits ON DELETE to columns %s, which Atlas HCL cannot represent", ptaherr.ErrUnsupportedFeature, slot.Slot.Name, strings.Join(columns, ", "))
			}
		}
	}
	return hclext.EncodeResult{}, ctx.Err()
}

// DeleteColumnsYAML declares that YAML foreign keys select their entire local
// key. The frontend has no column-list spelling, so an omitted list means all.
func DeleteColumnsYAML() yamlext.Extension {
	return yamlext.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.DeleteColumnsKind}, Coverage: deleteColumnsCoverage}
}
func deleteColumnsExportContext(ctx context.Context, coverage schemaext.Coverage) error {
	if ctx == nil {
		return fmt.Errorf("%w: ON DELETE export requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	coverage = coverage.SelectKinds([]schemaext.Kind{pgtable.DeleteColumnsKind})
	for _, record := range coverage.SubjectRecords() {
		if record.Knowledge.State == schemaext.Uninspected || record.Knowledge.State == schemaext.Unrepresentable {
			return fmt.Errorf("%w: ON DELETE selection for %s cannot be exported with unknown coverage: %s", ptaherr.ErrUnsupportedFeature, record.Subject, record.Knowledge.Reason)
		}
	}
	for _, record := range coverage.KindRecords() {
		if record.Knowledge.Unscoped().State == schemaext.Unrepresentable {
			return fmt.Errorf("%w: ON DELETE column selections cannot be exported: %s", ptaherr.ErrUnsupportedFeature, record.Knowledge.Unscoped().Reason)
		}
	}
	return nil
}
