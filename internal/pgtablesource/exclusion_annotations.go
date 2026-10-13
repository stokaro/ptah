package pgtablesource

import (
	"context"
	"encoding/csv"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/annotation"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/postgres/pgtable"
)

// ExclusionDirective declares a named PostgreSQL EXCLUDE constraint.
const ExclusionDirective = "ptah:schema:exclude"

// ExclusionAnnotations reads and writes independently named EXCLUDE objects.
// Declarations resolve table references after the whole Go file is read.
func ExclusionAnnotations() annotation.Extension {
	return annotation.Extension{Owner: pgtable.Owner, Kinds: []schemaext.Kind{pgtable.ExclusionKind},
		Directives: []annotation.Directive{{Name: ExclusionDirective, Description: "Declares a PostgreSQL EXCLUDE constraint.",
			Scopes: []annotation.Scope{annotation.ScopeFile, annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
				{Name: "name", Description: "Constraint name.", Value: "string", Required: true},
				{Name: "table", Description: "Owning table; defaults to the struct's table.", Value: "string"},
				{Name: "using", Description: "Index access method.", Value: "string", Required: true},
				{Name: "elements", Description: "Expressions and their exclusion operators.", Value: "SQL", Required: true},
				{Name: "condition", Description: "Optional index predicate.", Value: "SQL"},
				{Name: "deferrable", Description: "Allows checking at transaction commit.", Value: "boolean", Boolean: true},
				{Name: "initially", Description: "Initial check timing: immediate or deferred.", Value: "string"},
				{Name: "comment", Description: "Constraint comment.", Value: "string"},
				{Name: "requires", Description: "CSV list of exact required extension names.", Value: "list"},
			}}},
		File:     func() annotation.FileDecoder { return &exclusionFile{} },
		Coverage: annotation.Unlimited(exclusionCoverage), Encoder: exclusionAnnotations{}, Retainer: exclusionAnnotations{}, Encodes: []schemaext.Kind{pgtable.ExclusionKind},
	}
}

type exclusionFile struct{ declarations []annotation.Declaration }

func (f *exclusionFile) Decode(declaration annotation.Declaration) ([]annotation.Contribution, error) {
	f.declarations = append(f.declarations, declaration)
	return nil, nil
}

func (f *exclusionFile) Finish(tables annotation.Tables) ([]annotation.Contribution, error) {
	var result []annotation.Contribution
	var seen schemaext.Objects
	for _, declaration := range f.declarations {
		object, err := exclusionAnnotation(declaration, tables)
		if err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
		}
		if seen, err = seen.Declare(object); err != nil {
			return nil, &annotation.DeclarationError{Declaration: declaration, Err: err}
		}
		result = append(result, annotation.Contribution{Object: &object, Source: declaration, Label: "EXCLUDE constraint " + object.Ref.String()})
	}
	return result, nil
}

func exclusionAnnotation(declaration annotation.Declaration, tables annotation.Tables) (schemaext.Object, error) {
	if declaration.Directive != ExclusionDirective {
		return schemaext.Object{}, fmt.Errorf("%w: unexpected EXCLUDE directive %q", schemaext.ErrInvalidValue, declaration.Directive)
	}
	attributes := declaration.Attributes
	schema, table, err := tables.Reference(declaration.Struct, attributes["table"], "an EXCLUDE constraint")
	if err != nil {
		return schemaext.Object{}, err
	}
	value := pgtable.DesiredExclusion{Exclusion: pgtable.Exclusion{
		UsingMethod: attributes["using"], Elements: attributes["elements"], WhereCondition: attributes["condition"],
		Initially: attributes["initially"], Comment: attributes["comment"],
	}, StructName: declaration.Struct}
	if text, present := attributes["deferrable"]; present {
		value.Deferrable, err = strconv.ParseBool(text)
		if err != nil {
			return schemaext.Object{}, fmt.Errorf("%w: EXCLUDE deferrable must be a boolean", schemaext.ErrInvalidValue)
		}
	}
	if text, present := attributes["requires"]; present {
		records, readErr := csv.NewReader(strings.NewReader(text)).ReadAll()
		if readErr != nil || len(records) != 1 {
			return schemaext.Object{}, fmt.Errorf("%w: EXCLUDE requires must be one CSV list", schemaext.ErrInvalidValue)
		}
		value.Dependencies = exclusionRequirements(records[0])
	}
	return exclusionObject(schema, table, attributes["name"], value, declaration.Targets)
}

type exclusionAnnotations struct{}

func (exclusionAnnotations) EncodeAnnotations(ctx context.Context, request annotation.EncodeRequest) (annotation.EncodeResult, error) {
	if ctx == nil {
		return annotation.EncodeResult{}, fmt.Errorf("%w: EXCLUDE annotations require a context", schemaext.ErrInvalidValue)
	}
	if err := exclusionExportCoverage(request.Coverage); err != nil {
		return annotation.EncodeResult{}, err
	}
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.Postgres))
	tables := make(map[objectidentity.Key]annotation.Table, len(request.Tables))
	for _, table := range request.Tables {
		tables[builder.TableParts(table.Schema, table.Name).Key()] = table
	}
	result := annotation.EncodeResult{}
	for _, object := range request.Objects {
		if err := ctx.Err(); err != nil {
			return annotation.EncodeResult{}, err
		}
		directive, err := exclusionEncodedAnnotation(object, tables)
		if err != nil {
			return annotation.EncodeResult{}, err
		}
		result.Directives = append(result.Directives, directive)
	}
	if err := ctx.Err(); err != nil {
		return annotation.EncodeResult{}, err
	}
	return result, nil
}

func exclusionEncodedAnnotation(object schemaext.Object, tables map[objectidentity.Key]annotation.Table) (annotation.EncodedDirective, error) {
	value, err := exclusionValue(object)
	if err != nil {
		return annotation.EncodedDirective{}, err
	}
	table, found := tables[pgtable.ExclusionTable(object.Ref).Key()]
	if !found {
		return annotation.EncodedDirective{}, fmt.Errorf("%w: EXCLUDE %s names a table the export does not declare", ptaherr.ErrUnsupportedFeature, object.Ref)
	}
	attributes := []annotation.EncodedAttribute{{Name: "name", Value: object.Ref.Name.Source}, {Name: "table", Value: table.QualifiedName()},
		{Name: "using", Value: value.UsingMethod}, {Name: "elements", Value: value.Elements}}
	if value.Deferrable {
		attributes = append(attributes, annotation.EncodedAttribute{Name: "deferrable", Value: "true"})
	}
	for _, pair := range []annotation.EncodedAttribute{{Name: "condition", Value: value.WhereCondition}, {Name: "initially", Value: value.Initially}, {Name: "comment", Value: value.Comment}} {
		if pair.Value != "" {
			attributes = append(attributes, pair)
		}
	}
	names, err := exclusionRequirementNames(value.Dependencies)
	if err != nil {
		return annotation.EncodedDirective{}, err
	}
	if len(names) > 0 {
		var text strings.Builder
		writer := csv.NewWriter(&text)
		if err := writer.Write(names); err != nil {
			return annotation.EncodedDirective{}, err
		}
		writer.Flush()
		if err := writer.Error(); err != nil {
			return annotation.EncodedDirective{}, err
		}
		attributes = append(attributes, annotation.EncodedAttribute{Name: "requires", Value: strings.TrimSuffix(text.String(), "\n")})
	}
	return annotation.EncodedDirective{Directive: ExclusionDirective, Placement: annotation.AfterTable, Table: &table, Attributes: attributes, Targets: slices.Clone(object.Targets)}, nil
}

func (exclusionAnnotations) RetainAnnotations(ctx context.Context, request annotation.RetentionRequest) ([]bool, error) {
	if ctx == nil {
		return nil, fmt.Errorf("%w: EXCLUDE retention requires a context", schemaext.ErrInvalidValue)
	}
	result := make([]bool, len(request.Declarations))
	for i, declaration := range request.Declarations {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		expected, err := exclusionAnnotation(declaration, request.Tables)
		if err != nil {
			return nil, err
		}
		actual, found, err := request.Objects.Get(expected.Ref)
		if err != nil {
			return nil, err
		}
		if !found {
			continue
		}
		value, err := exclusionValue(actual)
		if err != nil {
			return nil, err
		}
		declared, err := exclusionValue(expected)
		if err != nil {
			return nil, err
		}
		value.StructName, value.Normalized = declared.StructName, nil
		result[i] = declared.Equal(value) && slices.Equal(expected.Targets, actual.Targets)
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
