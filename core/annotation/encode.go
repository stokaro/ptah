package annotation

import (
	"cmp"
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/coverage"
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
	// Fields are every field of the schema, in the schema's order. An owner
	// may give one a Go type of the target's own (see
	// [EncodeResult.FieldTypes]).
	Fields []FieldDeclaration
	// Target is the target the source is written for, as
	// platform.NormalizeDialect spells it, or empty when the export names
	// none. An owner may leave out what only another target reads, such as
	// the limits of models the target never holds.
	Target string
}

// Slot names one declaration of the frontend's own directives the writer
// writes: a table, its primary key, a field, an index or a constraint of it,
// a materialized view, or a routine. It is comparable, and two declarations of one
// source do not share one.
type Slot struct {
	// Directive is the directive, such as "ptah:schema:index".
	Directive string
	// Table is the table the declaration is, or is in. It is zero for a
	// materialized view.
	Table Table
	// Name is the name of a field, an index, a constraint or a materialized
	// view or routine, and empty for a table and its primary key.
	Name string
	// Parameters distinguishes overloaded routine declarations with the same name.
	// It is empty for other directives.
	Parameters string
	// PrimaryKey marks a primary key. On the table directive it is the key
	// the table declares, whose settings [DirectiveAttributes.PrimaryKey]
	// attributes spell. On the constraint directive it is a PRIMARY KEY
	// constraint, whose settings the directive's other attributes spell, so
	// an owner can refuse a setting only a primary key takes on any other
	// constraint.
	PrimaryKey bool
}

// SlotFacets is one declaration of the frontend's own directives and facets
// it holds.
type SlotFacets struct {
	// Slot is the declaration.
	Slot Slot
	// Facets are the facets.
	Facets schemaext.Facets
	// Options are the declaration's options, such as an index's WITH
	// options, keyed by name. An owner whose attributes of the directive
	// spell options (see [DirectiveAttributes.Parameters]) is handed them,
	// and claims the ones it writes in [SlotAttributes.Options]. A
	// declaration is handed to an owner that reads one of its options or its
	// attributes even where it holds none of the owner's facets.
	Options map[string]string
	// Reads are the attributes of the frontend's own that the declaration
	// writes and the owner reads beside its own (see
	// [DirectiveAttributes.Reads]), such as an index's type, keyed by name.
	Reads map[string]string
}

// FieldDeclaration is one field the writer writes, as an owner may type it.
type FieldDeclaration struct {
	// Slot is the field's declaration.
	Slot Slot
	// Type is the field's type as the schema names it.
	Type string
	// Nullable reports a field that holds NULL.
	Nullable bool
	// Key reports a field of its table's primary key.
	Key bool
}

// FieldType is the Go type an owner gives a field.
type FieldType struct {
	// Slot is the field's declaration, one of [EncodeRequest.Fields].
	Slot Slot
	// Name is the type as the source spells it, such as "uint64" or
	// "types.Decimal". The writer writes a pointer to it for a nullable
	// field whose type is not a slice.
	Name string
	// Import is the package the type needs, or empty.
	Import string
}

// EncodeResult is an owner's writing of an [EncodeRequest].
type EncodeResult struct {
	// Directives are the directives the owner writes. At one placement the
	// writer writes them in this order.
	Directives []EncodedDirective
	// Attributes are what the owner adds to the frontend's own declarations
	// it was handed, one entry for each declaration and position.
	Attributes []SlotAttributes
	// Limits are the not-described declarations the owner writes: kinds of
	// its models, or one object of them, that the source does not describe,
	// so reading it back leaves them unknown rather than absent (see
	// [Extension.Limits]). The writer writes them at the start of every file
	// it generates, owner by owner in the order of the owners' names, so a
	// file read alone claims no more than the whole source does.
	Limits []EncodedLimit
	// FieldTypes are the Go types the owner gives fields of its target, in
	// place of the writer's own reading of their types.
	FieldTypes []FieldType
}

// EncodedLimit is one not-described declaration an owner writes.
type EncodedLimit struct {
	// Kind is the kind the declaration names, one of the owner's
	// [Extension.Limits].
	Kind string
	// Name is the object the limit covers, or empty for the whole kind.
	Name string
	// Reason records why the source leaves this object undescribed.
	Reason coverage.Reason
	// Provenance records how the limit was learned. The writer preserves an
	// explicitly unknown value instead of inferring authored evidence.
	Provenance coverage.Provenance
}

// SlotAttributes are the attributes an owner adds to one of the frontend's
// own declarations it was handed, such as a materialized view's refresh
// schedule. The writer writes them where Position says, before the
// directive's dialects attribute, owner by owner in the order of the owners'
// names. An owner writes at most one entry for each declaration and
// position.
type SlotAttributes struct {
	// Slot is the declaration.
	Slot Slot
	// Position is where in the directive the attributes stand.
	Position Position
	// Attributes are the attributes, in order. Each is one the owner adds to
	// the declaration's directive (see [DirectiveAttributes]), so reading the
	// source back hands it to the owner, or a platform property written
	// platform.<target>.<key>, which the owner's property decoding reads.
	Attributes []EncodedAttribute
	// Options are the declaration's options the attributes spell, which
	// reading the source back hands to the owner as the declaration's
	// options again. The writer refuses an option no owner claims.
	Options []string
}

// Position is where in one of the frontend's own directives the attributes an
// owner adds stand.
type Position string

const (
	// WithOwnAttributes places them with the directive's own attributes, and
	// before its platform properties: after the attributes that name the
	// key a table's primary key or a constraint states, and after the own
	// attributes of any other directive. It is the zero value.
	WithOwnAttributes Position = ""
	// AfterProperties places them after the directive's platform
	// properties, on a table or an index, the two directives that carry
	// them.
	AfterProperties Position = "after-properties"
)

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
	// AfterViews places a directive in that comment after the views and
	// materialized views and before the roles: an object that reads or
	// moves data rather than declaring a relation, such as a topic or an
	// external table.
	AfterViews Placement = "after-views"
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
	// AfterPolicies places a directive in the comment of the struct of the
	// table [EncodedDirective.Table] names, after the table's own row-level
	// security policies: a policy of the owner's own, such as a ClickHouse
	// row policy.
	AfterPolicies Placement = "after-policies"
	// AfterSwitches places a directive in the comment of the struct of the
	// table [EncodedDirective.Table] names, after the table's own row-level
	// security switches: the switch of the owner's own, such as a
	// PostgreSQL table's.
	AfterSwitches Placement = "after-switches"
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
// cannot write. It returns every directive they write and every attribute
// they add to a declaration, owner by owner, in the order of the owners'
// names.
//
// It refuses a directive without a name, one that reading the source back
// would hand to another owner or to none, one at an unknown placement, at a
// table the request did not hold, or with a table at a placement that takes
// none, and an attribute without a name, written twice or spelling the
// dialects attribute, which Targets write. It refuses attributes added to a
// declaration the extension was not handed or added twice, and an attribute
// without a name, written twice, or one the extension does not add to the
// declaration's directive. It refuses a limit of a kind reading the source
// back does not hand to the extension, and one written twice. An option claim
// must name an option the extension receives, and each option has one writer.
// A field type must name a field the request holds and a nonempty Go type;
// each field has at most one type writer. Every owner is
// handed the request's Target. An encoder's own error is returned
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
	writers := make(map[slotAttribute]string)
	claims := make(map[slotOption]string)
	typed := make(map[Slot]string)
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
		if err := s.checkSlotAttributes(index, request, result.Attributes, writers); err != nil {
			return EncodeResult{}, err
		}
		if err := s.checkLimits(index, result.Limits); err != nil {
			return EncodeResult{}, err
		}
		if err := s.checkOptions(index, request, result.Attributes, claims); err != nil {
			return EncodeResult{}, err
		}
		if err := s.checkFieldTypes(index, request, result.FieldTypes, typed); err != nil {
			return EncodeResult{}, err
		}
		encoded.Directives = append(encoded.Directives, result.Directives...)
		encoded.Attributes = append(encoded.Attributes, result.Attributes...)
		encoded.Limits = append(encoded.Limits, result.Limits...)
		encoded.FieldTypes = append(encoded.FieldTypes, result.FieldTypes...)
	}
	return encoded, nil
}

// slotOption is one option of one declaration.
type slotOption struct {
	slot Slot
	name string
}

// checkOptions refuses an option the extension at index claims that its
// declaration does not hold, or that another extension claims too. claims
// records which extension claims each option.
func (s Set) checkOptions(index int, request EncodeRequest, slots []SlotAttributes, claims map[slotOption]string) error {
	owner := s.extensions[index].Owner
	for _, slot := range slots {
		at := slices.IndexFunc(request.Slots, func(entry SlotFacets) bool { return entry.Slot == slot.Slot })
		for _, name := range slot.Options {
			if _, held := request.Slots[at].Options[name]; !held {
				return fmt.Errorf("%w: Go annotation encoder of %s claimed option %q of %s, which it was not handed",
					schemaext.ErrInvalidValue, owner, name, slot.Slot)
			}
			key := slotOption{slot: slot.Slot, name: name}
			if claimer, taken := claims[key]; taken {
				return fmt.Errorf("%w: Go annotation encoders of %s and %s both claim option %q of %s",
					schemaext.ErrDuplicate, claimer, owner, name, slot.Slot)
			}
			claims[key] = owner
		}
	}
	return nil
}

// checkFieldTypes refuses a Go type the extension at index gives a field
// the request does not hold, one without a name, and one another extension
// gives the field too. typed records which extension types each field.
func (s Set) checkFieldTypes(index int, request EncodeRequest, types []FieldType, typed map[Slot]string) error {
	owner := s.extensions[index].Owner
	for _, fieldType := range types {
		held := slices.ContainsFunc(request.Fields, func(field FieldDeclaration) bool { return field.Slot == fieldType.Slot })
		if !held || strings.TrimSpace(fieldType.Name) == "" {
			return fmt.Errorf("%w: Go annotation encoder of %s typed %s, which it was not handed, or gave it no type",
				schemaext.ErrInvalidValue, owner, fieldType.Slot)
		}
		if typer, taken := typed[fieldType.Slot]; taken {
			return fmt.Errorf("%w: Go annotation encoders of %s and %s both type %s", schemaext.ErrDuplicate, typer, owner, fieldType.Slot)
		}
		typed[fieldType.Slot] = owner
	}
	return nil
}

// checkLimits refuses a limit of the extension at index that reading the
// source back would hand to another owner or to none, and one it writes
// twice.
func (s Set) checkLimits(index int, limits []EncodedLimit) error {
	written := make(map[[2]string]bool, len(limits))
	for _, limit := range limits {
		key := [2]string{strings.ToLower(strings.TrimSpace(limit.Kind)), limit.Name}
		reader, found := s.limits[key[0]]
		if !found || reader != index || written[key] {
			return fmt.Errorf("%w: Go annotation encoder of %s wrote the limit of kind %q and name %q, which reading the source back does not hand to it, or wrote it twice",
				schemaext.ErrInvalidValue, s.extensions[index].Owner, limit.Kind, limit.Name)
		}
		if !limit.Reason.Valid() || !limit.Provenance.Valid() {
			return fmt.Errorf("%w: Go annotation encoder of %s wrote invalid limit evidence", schemaext.ErrInvalidValue, s.extensions[index].Owner)
		}
		written[key] = true
	}
	return nil
}

// selectRequest is what schema holds of the extension's models: the objects
// and the facets of its Encodes, the coverage of its Kinds, and every table.
func (e Extension) selectRequest(schema EncodeRequest) EncodeRequest {
	request := EncodeRequest{Tables: slices.Clone(schema.Tables), Coverage: schema.Coverage.SelectKinds(e.Kinds), Target: schema.Target}
	for _, object := range schema.Objects {
		if slices.Contains(e.Encodes, schemaext.Kind(object.Ref.Kind)) {
			request.Objects = append(request.Objects, object)
		}
	}
	for _, slot := range schema.Slots {
		selected := e.selectSlot(slot)
		if selected.Facets.Len() > 0 || len(selected.Options) > 0 || len(selected.Reads) > 0 {
			request.Slots = append(request.Slots, selected)
		}
	}
	// A field is any owner's to type, so every owner is handed every field.
	request.Fields = slices.Clone(schema.Fields)
	return request
}

// selectSlot copies the facets, options and frontend attributes the extension
// reads from one declaration.
func (e Extension) selectSlot(slot SlotFacets) SlotFacets {
	facets := slot.Facets
	for _, kind := range facets.Kinds() {
		if !slices.Contains(e.Encodes, kind) {
			facets = facets.Without(kind)
		}
	}
	selected := SlotFacets{Slot: slot.Slot, Facets: facets}
	for _, group := range e.Attributes {
		if group.Directive != slot.Slot.Directive {
			continue
		}
		if group.Parameters != nil && len(slot.Options) > 0 {
			selected.Options = maps.Clone(slot.Options)
		}
		for _, name := range group.Reads {
			if value, found := slot.Reads[name]; found {
				if selected.Reads == nil {
					selected.Reads = make(map[string]string, len(group.Reads))
				}
				selected.Reads[name] = value
			}
		}
	}
	return selected
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

// checkPlacement refuses a directive at an unknown placement, a directive at
// a table without a table the request holds, and a table on a directive at
// any other placement.
func checkPlacement(owner string, request EncodeRequest, directive EncodedDirective) error {
	switch directive.Placement {
	case AfterTable, AfterPolicies, AfterSwitches:
		if directive.Table == nil || !slices.Contains(request.Tables, *directive.Table) {
			return fmt.Errorf("%w: Go annotation encoder of %s placed directive %q at a table the schema does not declare",
				schemaext.ErrInvalidValue, owner, directive.Directive)
		}
	case AfterSchemas, AfterViews, AfterPrivileges:
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

// slotAttribute is one attribute of one declaration.
type slotAttribute struct {
	slot Slot
	name string
}

// checkSlotAttributes refuses the attributes the extension at index adds to
// declarations where the writer cannot write them: to a declaration the
// extension was not handed, or to one twice at one position, at an unknown
// position, and an attribute without a name, written twice, neither one the
// extension adds to the declaration's directive nor a platform property, or
// one another extension writes to the declaration. writers records which
// extension writes each attribute.
func (s Set) checkSlotAttributes(index int, request EncodeRequest, slots []SlotAttributes, writers map[slotAttribute]string) error {
	extension := s.extensions[index]
	type slotPosition struct {
		slot     Slot
		position Position
	}
	written := make(map[slotPosition]bool, len(slots))
	for _, slot := range slots {
		handed := slices.ContainsFunc(request.Slots, func(entry SlotFacets) bool { return entry.Slot == slot.Slot })
		place := slotPosition{slot: slot.Slot, position: slot.Position}
		if !handed || written[place] || (slot.Position != WithOwnAttributes && slot.Position != AfterProperties) {
			return fmt.Errorf("%w: Go annotation encoder of %s wrote attributes of %s, which it was not handed, wrote twice or placed at %q",
				schemaext.ErrInvalidValue, extension.Owner, slot.Slot, slot.Position)
		}
		written[place] = true
		for _, attribute := range slot.Attributes {
			key := slotAttribute{slot: slot.Slot, name: attribute.Name}
			owned := extension.adds(slot.Slot, attribute.Name) || strings.HasPrefix(attribute.Name, "platform.")
			if strings.TrimSpace(attribute.Name) == "" || writers[key] == extension.Owner || !owned {
				return fmt.Errorf("%w: Go annotation encoder of %s wrote attribute %q of %s unnamed, twice or as one it does not add",
					schemaext.ErrInvalidValue, extension.Owner, attribute.Name, slot.Slot)
			}
			if writer, taken := writers[key]; taken {
				return fmt.Errorf("%w: Go annotation encoders of %s and %s both write attribute %q of %s",
					schemaext.ErrDuplicate, writer, extension.Owner, attribute.Name, slot.Slot)
			}
			writers[key] = extension.Owner
		}
	}
	return nil
}

// adds reports whether the extension adds the attribute name to the
// directive of slot, in a group of the table's primary key exactly where the
// slot is that key. A PRIMARY KEY constraint takes the constraint
// directive's ordinary groups.
func (e Extension) adds(slot Slot, name string) bool {
	tableKey := slot.PrimaryKey && slot.Directive == tableDirective
	return slices.ContainsFunc(e.Attributes, func(group DirectiveAttributes) bool {
		return group.Directive == slot.Directive && group.PrimaryKey == tableKey &&
			slices.ContainsFunc(group.Attributes, func(attribute Attribute) bool { return attribute.Name == name })
	})
}

// String names the declaration in a diagnostic, such as `index "i" of table
// "app.t"` or `matview "hourly"`. The kind is the last part of the directive's
// name.
func (s Slot) String() string {
	kind := s.Directive[strings.LastIndex(s.Directive, ":")+1:]
	table := s.Table.QualifiedName()
	switch {
	case s.PrimaryKey && s.Name == "":
		return fmt.Sprintf("the primary key of table %q", table)
	case s.Name == "":
		return fmt.Sprintf("%s %q", kind, table)
	case table == "":
		return fmt.Sprintf("%s %q", kind, s.Name)
	default:
		return fmt.Sprintf("%s %q of table %q", kind, s.Name, table)
	}
}
