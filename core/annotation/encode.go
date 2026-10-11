package annotation

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/internal/dialectscope"
)

// Encoder writes the models of an extension's Encodes that a schema holds as
// directives for the Go annotation writer. It returns data and never Go
// source: the writer quotes every value, writes every comment and places every
// directive, so a source keeps one syntax whichever owner wrote a directive of
// it.
//
// The Go annotation writer reports no losses. A value the owner cannot write,
// a facet on a declaration it has no spelling for, and a coverage record a
// source cannot carry are the owner's to refuse with an error, which refuses
// the export.
type Encoder interface {
	EncodeAnnotations(ctx context.Context, request EncodeRequest) (EncodeResult, error)
}

// EncodeRequest is what one schema holds of an extension's models.
type EncodeRequest struct {
	// Objects are the schema's objects of the extension's Encodes, in the
	// order of their identities.
	Objects []schemaext.Object
	// Tables are every table the source declares, in the order the writer
	// writes them. A directive placed at a table names one of them.
	Tables Tables
	// Slots are the declarations of the frontend's own directives the writer
	// writes that hold a facet of the extension's Encodes, each with those
	// facets alone, in the order the writer writes them.
	Slots []SlotFacets
	// Coverage is what the schema knows of the extension's Kinds.
	Coverage schemaext.Coverage
}

// Slot names one declaration of the frontend's own directives the writer
// writes: a table, its primary key, a field, an index or a constraint of it,
// or a materialized view. It is comparable, and two declarations of one
// source do not share one.
type Slot struct {
	// Directive is the directive, such as "ptah:schema:index".
	Directive string
	// Table is the table the declaration is, or is in. It is zero for a
	// materialized view.
	Table Table
	// Name is the name of a field, an index, a constraint or a materialized
	// view, and empty for a table and its primary key.
	Name string
	// PrimaryKey marks the primary key the table directive declares, whose
	// settings [DirectiveAttributes.PrimaryKey] attributes spell.
	PrimaryKey bool
}

// SlotFacets is one declaration of the frontend's own directives and facets
// it holds.
type SlotFacets struct {
	// Slot is the declaration.
	Slot Slot
	// Facets are the facets.
	Facets schemaext.Facets
}

// EncodeResult is an owner's writing of an [EncodeRequest].
type EncodeResult struct {
	// Directives are the directives the owner writes. At one placement the
	// writer writes them in this order.
	Directives []EncodedDirective
}

// Placement is where among the writer's own directives an encoded directive
// stands. The writer writes the directives of one placement owner by owner,
// in the order of the owners' names, and each owner's in the order it
// returns them, so the order of a source does not depend on the order owners
// were registered in.
type Placement string

const (
	// AfterSchemas places a directive in the comment of the struct the
	// writer hangs a schema's standalone objects on, after the schemas and
	// before the types and sequences the writer writes there: among the
	// objects the rest of the schema may name, such as a type a column is
	// declared over.
	AfterSchemas Placement = "after-schemas"
	// AfterPrivileges places a directive in that comment after the default
	// privileges, the last of the writer's own directives there: an object
	// that stands for a relation over the tables, such as a continuous
	// aggregate.
	AfterPrivileges Placement = "after-privileges"
	// AfterTable places a directive in the comment of the struct of the
	// table [EncodedDirective.Table] names, after the table's own directive:
	// a setting of the table that has a directive of its own, such as a
	// hypertable.
	AfterTable Placement = "after-table"
)

// EncodedDirective is one directive an owner writes.
type EncodedDirective struct {
	// Directive is the directive's name, such as "ptah:schema:hypertable":
	// one the owner declares, or one of the frontend's own that the owner
	// reads where Targets scope it (see [TargetScope]). Reading the source
	// back hands the declaration to the owner that wrote it.
	Directive string
	// Placement is where the directive stands.
	Placement Placement
	// Table is the table an [AfterTable] directive stands at: one of
	// [EncodeRequest.Tables]. It is nil for every other placement.
	Table *Table
	// Attributes are the attributes the directive writes, in order. An
	// attribute the owner leaves out is not written.
	Attributes []EncodedAttribute
	// Targets are the targets the declaration is scoped to, which the writer
	// writes as its dialects attribute, after the others. Empty writes none.
	Targets []string
}

// EncodedAttribute is one attribute an encoded directive writes.
type EncodedAttribute struct {
	// Name is the attribute's name.
	Name string
	// Value is its value, as the frontend hands it to a decoder: the writer
	// quotes it, and a bare boolean is written "true".
	Value string
}

// Encodes reports whether an extension of the set writes objects and facets
// of kind.
func (s Set) Encodes(kind schemaext.Kind) bool {
	return slices.ContainsFunc(s.extensions, func(extension Extension) bool {
		return extension.Encoder != nil && slices.Contains(extension.Encodes, kind)
	})
}

// Encode hands each extension that has an [Encoder] what schema holds of its
// models -- the objects and facets of its Encodes and the coverage of its
// Kinds, beside every table -- in one call per extension, also when the
// schema holds none of them, since an owner may refuse a coverage record it
// cannot write. It returns every directive they write, owner by owner, in the
// order of the owners' names.
//
// It refuses a directive without a name, one that reading the source back
// would hand to another owner or to none, one at an unknown placement, at a
// table the request did not hold, or with a table at a placement that takes
// none, and an attribute without a name, written twice or spelling the
// dialects attribute, which Targets write. An encoder's own error is returned
// as it is: it refuses the export, as a value the owner cannot write would
// otherwise be written wrong.
func (s Set) Encode(ctx context.Context, schema EncodeRequest) (EncodeResult, error) {
	order := make([]int, 0, len(s.extensions))
	for index, extension := range s.extensions {
		if extension.Encoder != nil {
			order = append(order, index)
		}
	}
	slices.SortStableFunc(order, func(a, b int) int { return cmp.Compare(s.extensions[a].Owner, s.extensions[b].Owner) })
	var encoded EncodeResult
	for _, index := range order {
		extension := s.extensions[index]
		request := extension.selectRequest(schema)
		result, err := extension.Encoder.EncodeAnnotations(ctx, request)
		if err != nil {
			return EncodeResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return EncodeResult{}, err
		}
		for _, directive := range result.Directives {
			if err := s.checkEncodedDirective(index, request, directive); err != nil {
				return EncodeResult{}, err
			}
		}
		encoded.Directives = append(encoded.Directives, result.Directives...)
	}
	return encoded, nil
}

// selectRequest is what schema holds of the extension's models: the objects
// and the facets of its Encodes, the coverage of its Kinds, and every table.
func (e Extension) selectRequest(schema EncodeRequest) EncodeRequest {
	request := EncodeRequest{Tables: slices.Clone(schema.Tables), Coverage: schema.Coverage.SelectKinds(e.Kinds)}
	for _, object := range schema.Objects {
		if slices.Contains(e.Encodes, schemaext.Kind(object.Ref.Kind)) {
			request.Objects = append(request.Objects, object)
		}
	}
	for _, slot := range schema.Slots {
		facets := slot.Facets
		for _, kind := range facets.Kinds() {
			if !slices.Contains(e.Encodes, kind) {
				facets = facets.Without(kind)
			}
		}
		if facets.Len() > 0 {
			request.Slots = append(request.Slots, SlotFacets{Slot: slot.Slot, Facets: facets})
		}
	}
	return request
}

// checkEncodedDirective refuses a directive the writer cannot write for the
// extension at index: one without a name, one that does not read back to the
// extension, a placement it cannot place, and an attribute without a name,
// written twice or spelling the dialects attribute.
func (s Set) checkEncodedDirective(index int, request EncodeRequest, directive EncodedDirective) error {
	owner := s.extensions[index].Owner
	if strings.TrimSpace(directive.Directive) == "" {
		return fmt.Errorf("%w: Go annotation encoder of %s wrote a directive without a name", schemaext.ErrInvalidValue, owner)
	}
	reader, found := s.owners[directive.Directive]
	if !found {
		var err error
		if reader, found, err = s.scopeIndex(directive.Directive, directive.Targets); err != nil {
			return fmt.Errorf("%w: Go annotation encoder of %s wrote directive %q: %w", schemaext.ErrInvalidValue, owner, directive.Directive, err)
		}
	}
	if !found || reader != index {
		return fmt.Errorf("%w: Go annotation encoder of %s wrote directive %q, which reading the source back does not hand to it",
			schemaext.ErrInvalidValue, owner, directive.Directive)
	}
	if err := checkPlacement(owner, request, directive); err != nil {
		return err
	}
	names := make(map[string]bool, len(directive.Attributes))
	for _, attribute := range directive.Attributes {
		if strings.TrimSpace(attribute.Name) == "" || names[attribute.Name] || attribute.Name == dialectscope.Attribute {
			return fmt.Errorf("%w: Go annotation encoder of %s wrote directive %q with attribute %q unnamed, twice or as its targets",
				schemaext.ErrInvalidValue, owner, directive.Directive, attribute.Name)
		}
		names[attribute.Name] = true
	}
	return nil
}

// checkPlacement refuses a directive at an unknown placement, an [AfterTable]
// directive without a table the request holds, and a table on a directive at
// any other placement.
func checkPlacement(owner string, request EncodeRequest, directive EncodedDirective) error {
	switch directive.Placement {
	case AfterTable:
		if directive.Table == nil || !slices.Contains(request.Tables, *directive.Table) {
			return fmt.Errorf("%w: Go annotation encoder of %s placed directive %q at a table the schema does not declare",
				schemaext.ErrInvalidValue, owner, directive.Directive)
		}
	case AfterSchemas, AfterPrivileges:
		if directive.Table != nil {
			return fmt.Errorf("%w: Go annotation encoder of %s placed directive %q at %s with a table",
				schemaext.ErrInvalidValue, owner, directive.Directive, directive.Placement)
		}
	default:
		return fmt.Errorf("%w: Go annotation encoder of %s placed directive %q at unknown placement %q",
			schemaext.ErrInvalidValue, owner, directive.Directive, directive.Placement)
	}
	return nil
}
