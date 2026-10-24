package annotation

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/coverage"
	"ptah.run/core/schemaext"
	"ptah.run/internal/targetscope"
)

// ErrUnselected is returned for a [Set] nobody built: the zero value. A
// caller that wants no owner directives passes [None] instead, so a forgotten
// selection is refused rather than read as a decision to drop them.
var ErrUnselected = errors.New("no annotation owners were selected")

// Declaration is one owner directive as the frontend read it.
type Declaration struct {
	// Directive is the directive's name, such as "ptah:schema:hypertable".
	Directive string
	// Attributes are the directive's attributes as written, after the
	// frontend unquoted them and refused the ones the directive does not
	// declare. A bare boolean attribute reads "true".
	Attributes map[string]string
	// Struct is the Go struct the comment is attached to, or empty for a
	// file-level comment.
	Struct string
	// Line is the line of the file the directive is written on, counting
	// from 1, or 0 where the frontend does not know it. A refusal that names
	// the declaration is reported there.
	Line int
	// File names the file the directive is written in, as the frontend names
	// it in a refusal.
	File string
	// Targets are the targets the declaration's dialects attribute scopes it
	// to, for a declaration of one of the frontend's own directives that an
	// owner reads by its [TargetScope]. Empty means the declaration names none.
	Targets []string
	// Start and End bound the declaration's comment in its file, End being
	// the position after the last byte. Both are zero where the frontend reads
	// source it has no positions for.
	Start, End SourcePosition
	// FileSize is the length in bytes of the file the declaration is written
	// in, or 0 where the frontend does not know it.
	FileSize int
	// Subject is the table, column or index the declaration describes, for a
	// directive whose [Directive.Describes] names a column or an index and for
	// attributes an owner reads through [DirectiveAttributes.PerFile]. It is
	// the zero value for any other declaration.
	Subject Subject
}

// SubjectKind names a part of a table that a declaration describes.
type SubjectKind string

// The parts a declaration may describe.
const (
	// SubjectTable is the table of the declaration's struct.
	SubjectTable SubjectKind = "table"
	// SubjectColumn is a column of the declaration's struct.
	SubjectColumn SubjectKind = "column"
	// SubjectIndex is an index declared by the declaration's struct.
	SubjectIndex SubjectKind = "index"
)

// Subject is the part of a table a declaration describes, in the terms the
// author wrote it. The frontend fills it from the field or index directive the
// declaration is written beside, so an owner never maps a Go field to a column
// or looks for an index itself.
type Subject struct {
	// Kind says which part this is. The zero value means no part.
	Kind SubjectKind
	// Struct is the Go struct the part belongs to.
	Struct string
	// Name is the column name the field directive writes, or the index name
	// the index directive writes. It is empty for a table.
	Name string
	// Table is the table an index directive names with its table attribute,
	// as written. It is empty where the index belongs to the table of Struct.
	Table string
}

// SourcePosition is a place in a Go source file, in the units a source span uses.
type SourcePosition struct {
	// Offset counts bytes from the start of the file, from 0.
	Offset int
	// Line counts lines from 1. Zero means the position is unknown.
	Line int
	// Column counts bytes from the start of the line, from 1.
	Column int
}

// Contribution is one thing a declaration adds to the schema: a standalone
// feature object, or a facet of a table. Exactly one of Object and Facet is
// set.
type Contribution struct {
	// Object is a standalone feature object.
	Object *schemaext.Object
	// Facet is a value attached to the table Table names.
	Facet schemaext.Value
	// Table names the table a facet belongs to, as the declaration wrote it,
	// optionally schema-qualified. Empty means the table of the declaration's
	// struct. The frontend attaches the facet once the whole file is read, so
	// the table may be declared after the directive.
	Table string
	// Label names what the contribution declares in a refusal, such as
	// `continuous aggregate "app.hourly"` or `a hypertable`.
	Label string
	// Source is the declaration the contribution comes from. A contribution
	// [FileDecoder.Finish] returns sets it: the frontend places a facet by
	// Source's struct and reports a refusal at Source. A contribution a
	// decoder returns for the declaration it is decoding leaves it zero.
	Source Declaration
	// Targets scope a facet to the targets it holds on, as a declaration's
	// dialects attribute scopes it. Empty holds on every target.
	Targets []string
	// Subject says that a facet belongs to a column or an index of Source's
	// struct instead of to a table. The zero value and [SubjectTable] attach
	// the facet to the table Table names. A facet whose column or index the
	// file does not declare is refused.
	Subject Subject
}

// TargetScope hands an owner the declarations of one of the frontend's own
// directives whose target scope makes them the owner's, such as a row-level
// security policy scoped to PostgreSQL: the owner reads them in place of the
// frontend, as it reads its own directives.
type TargetScope struct {
	// Directive names the frontend's directive, such as
	// "ptah:schema:rls:policy".
	Directive string
	// Targets are the dialects whose declarations the owner reads. A target
	// belongs to one owner for a directive.
	Targets []string
	// Unscoped hands the owner the declarations that name no target too. One
	// owner of a directive may set it.
	Unscoped bool
	// Label names Targets in a refusal, such as "PostgreSQL-family targets".
	Label string
}

// DirectiveAttributes are attributes an owner adds to one of the frontend's
// own directives, such as a schedule on a materialized view. The frontend
// validates them beside the directive's own attributes and hands the ones a
// declaration wrote to Decode, whose facets join the object the directive
// declares.
type DirectiveAttributes struct {
	// Directive names the frontend's directive, such as
	// "ptah:schema:matview".
	Directive string
	// Attributes are the owner's attributes of the directive. An attribute
	// name belongs to one owner, and never to the directive itself.
	Attributes []Attribute
	// Reads are attributes of the directive itself that the owner's decoders
	// read beside its own, such as an index's type. A declaration that
	// writes one of them is decoded even where it writes none of the owner's
	// attributes, since the directive's own attribute may be what makes the
	// object one of the owner's.
	Reads []string
	// Decode reads the owner's attributes a declaration wrote, and the ones
	// of Reads it wrote, keyed by name. It is called only when the
	// declaration wrote at least one of them, and it returns facets of the
	// owner's models, with their target scopes.
	Decode func(attributes map[string]string) (schemaext.Facets, error)
	// Parameters reads the same attributes into options of the object the
	// directive declares, such as the WITH options of an index, for an owner
	// whose attributes spell options the common model already carries. It is
	// optional, and it is called when Decode is.
	Parameters func(attributes map[string]string) (map[string]string, error)
	// PrimaryKey attaches the facets Decode returns to the primary key of
	// the table the directive declares instead of to the table, for
	// attributes that are settings of the key, such as an index hint. Only
	// the table directive, "ptah:schema:table", declares a table, and such a
	// group takes no Parameters. [Set.DecodePrimaryKeyAttributes] decodes
	// these groups, and [Set.DecodeAttributes] the others.
	PrimaryKey bool
	// PerFile hands the declarations that write these attributes to the
	// owner's [FileDecoder] instead of Decode, which the group leaves unset,
	// since the owner needs the file's tables to answer. The declaration
	// carries the frontend's directive name, only the attributes of the
	// group, and the [Subject] they describe. The facets the file decoder
	// returns from [FileDecoder.Finish] attach to that subject. The frontend
	// reads it for the table, field and index directives, and the set refuses
	// it for any other, since a group nobody calls would drop its attributes
	// without an error. A group that sets it needs [Extension.File], and sets
	// neither Decode, Parameters nor PrimaryKey.
	PerFile bool
}

// The frontend's own directives that declare a column and an index.
const (
	fieldDirective = "ptah:schema:field"
	indexDirective = "ptah:schema:index"
)

// tableDirective is the frontend's directive that declares a table, the one
// whose primary key [DirectiveAttributes.PrimaryKey] attributes describe.
const tableDirective = "ptah:schema:table"

// Extension is what one feature owner contributes to the Go annotation
// frontend.
type Extension struct {
	// Owner names the owner, as its provider ID does.
	Owner string
	// Directives are the directives the owner decodes. A directive name
	// belongs to one owner, and never to the frontend's own grammar.
	Directives []Directive
	// Kinds are the models the owner's contributions carry. The provider that
	// registers the extension owns their desired codecs.
	Kinds []schemaext.Kind
	// Attributes are the owner's attributes on the frontend's own
	// directives.
	Attributes []DirectiveAttributes
	// Decode turns one declaration of one of Directives into what it
	// declares. It must not keep the declaration, and it returns an error for
	// a value it refuses, a [DeclarationError] to name the attribute.
	Decode func(Declaration) ([]Contribution, error)
	// File starts a [FileDecoder] for one file, for an owner whose
	// declarations refer to each other or to the file's tables. Exactly one
	// of Decode and File is set when Directives is not empty.
	File func() FileDecoder
	// TargetScopes are the frontend's directives whose declarations the
	// owner reads where their target scope makes them its own. They need a
	// decoder as Directives do.
	TargetScopes []TargetScope
	// Limits are the kinds of ptah:schema:notdescribed declaration the owner
	// reads, in lower case, such as "coordination_node". A file's
	// declarations of them reach Coverage instead of the frontend's own
	// coverage set. A kind belongs to one owner, and never to the frontend's
	// own vocabulary.
	Limits []string
	// Coverage is the knowledge a Go annotation source holds about Kinds,
	// given the not-described declarations of Limits it wrote: a source that
	// could have declared a model and did not declares its absence, except
	// where a limit leaves it unmanaged. It is required, so the claim is
	// always the owner's to make.
	Coverage func(limits []coverage.Object) (schemaext.Coverage, error)
	// Encoder writes the models of Encodes a schema holds as directives, for
	// the Go annotation writer; see [Set.Encode]. It is optional; without it
	// the writer refuses an object or a facet of Kinds it has no spelling of
	// its own for.
	Encoder Encoder
	// Retainer proves that an export preserves directives before source cleanup.
	// Without it, cleanup refuses the owner's directives.
	Retainer Retainer
	// Encodes are the models of Kinds the Encoder writes. It is required with
	// an Encoder and refused without one. A model of Kinds it leaves out, such
	// as a setting the writer spells as a platform property, is not handed
	// to the Encoder.
	Encodes []schemaext.Kind
}

// Set is the frozen collection of extensions one parse selects. The zero
// value is unselected and refused by the frontend; [None] selects no owner.
// A Set is safe for concurrent use.
type Set struct {
	selected   bool
	extensions []Extension
	owners     map[string]int
	// limits holds the extension index that reads each not-described kind.
	limits map[string]int
	// routes holds, for each frontend directive, the extension index that
	// reads its declarations scoped to each target.
	routes map[string]*targetscope.Routes
	// attributes holds, for each frontend directive, the extension index
	// that owns each attribute an owner adds to it.
	attributes map[string]map[string]int
	// vocabulary is the coverage kinds a notdescribed declaration may name
	// for the common coverage set.
	vocabulary coverage.Vocabulary
}

// None returns a selected set without owners. A parse with it reads the
// frontend's own directives only.
func None() Set {
	return Set{selected: true}
}

// NewSet validates and freezes extensions. It refuses an extension without
// an owner or a coverage claim, one that declares directives and not exactly
// one decoder, an encoder without models to encode or models without an
// encoder, a model it encodes and does not declare, a directive without a
// name, attributes on a directive without a decoder or a name, a limit kind
// that is empty, not in lower case or one of the frontend's own, and a
// directive name, an attribute of one directive, a limit kind or a model two
// extensions claim.
func NewSet(extensions ...Extension) (Set, error) {
	set := Set{selected: true, owners: make(map[string]int), limits: make(map[string]int),
		attributes: make(map[string]map[string]int), routes: make(map[string]*targetscope.Routes)}
	kinds := make(map[schemaext.Kind]string)
	for index, extension := range extensions {
		if err := checkExtension(index, extension); err != nil {
			return Set{}, err
		}
		for _, kind := range extension.Kinds {
			if owner, claimed := kinds[kind]; claimed {
				return Set{}, fmt.Errorf("%w: model %q is produced by %s and %s", schemaext.ErrDuplicate, kind, owner, extension.Owner)
			}
			kinds[kind] = extension.Owner
		}
		if err := set.claimDirectives(index, extension); err != nil {
			return Set{}, err
		}
		if err := set.claimAttributes(index, extension); err != nil {
			return Set{}, err
		}
		if err := set.claimLimits(index, extension); err != nil {
			return Set{}, err
		}
		if err := set.claimScopes(index, extension); err != nil {
			return Set{}, err
		}
		set.extensions = append(set.extensions, cloneExtension(extension))
	}
	return set, nil
}

// checkExtension refuses an extension without an owner or a coverage claim,
// and one that reads declarations without exactly one decoder.
func checkExtension(index int, extension Extension) error {
	switch {
	case strings.TrimSpace(extension.Owner) == "":
		return fmt.Errorf("annotation extension %d names no owner", index)
	case extension.Coverage == nil:
		return fmt.Errorf("annotation extension of %s makes no coverage claim", extension.Owner)
	case (len(extension.Directives) > 0 || len(extension.TargetScopes) > 0) && extension.Decode == nil && extension.File == nil:
		return fmt.Errorf("annotation extension of %s declares directives and no decoder", extension.Owner)
	case extension.Decode != nil && extension.File != nil:
		return fmt.Errorf("annotation extension of %s declares both a decoder and a file decoder", extension.Owner)
	case (extension.Encoder == nil) != (len(extension.Encodes) == 0):
		return fmt.Errorf("annotation extension of %s declares an encoder without models to encode, or models without an encoder", extension.Owner)
	}
	for _, kind := range extension.Encodes {
		if !slices.Contains(extension.Kinds, kind) {
			return fmt.Errorf("annotation extension of %s encodes model %q, which it does not declare", extension.Owner, kind)
		}
	}
	return checkDescribes(extension)
}

// checkDescribes refuses a directive that describes a part of a table where
// the frontend cannot find the part: a column directive is written on a field,
// and an index directive on a struct or a field.
func checkDescribes(extension Extension) error {
	for _, directive := range extension.Directives {
		switch directive.Describes {
		case "", SubjectTable:
		case SubjectColumn:
			if len(directive.Scopes) == 0 || slices.ContainsFunc(directive.Scopes, func(scope Scope) bool { return scope != ScopeField }) {
				return fmt.Errorf("annotation extension of %s declares column directive %q outside a field comment", extension.Owner, directive.Name)
			}
		case SubjectIndex:
			if len(directive.Scopes) == 0 || slices.ContainsFunc(directive.Scopes, func(scope Scope) bool { return scope != ScopeField && scope != ScopeStruct }) {
				return fmt.Errorf("annotation extension of %s declares index directive %q outside a struct or field comment", extension.Owner, directive.Name)
			}
		default:
			return fmt.Errorf("annotation extension of %s declares directive %q that describes unknown part %q", extension.Owner, directive.Name, directive.Describes)
		}
	}
	return nil
}

func (s *Set) claimDirectives(index int, extension Extension) error {
	for _, directive := range extension.Directives {
		if strings.TrimSpace(directive.Name) == "" || strings.HasPrefix(directive.Name, "//") {
			return fmt.Errorf("annotation extension of %s declares a directive without a name", extension.Owner)
		}
		if previous, claimed := s.owners[directive.Name]; claimed {
			return fmt.Errorf("%w: directive %q is declared by %s and %s", schemaext.ErrDuplicate, directive.Name,
				s.extensions[previous].Owner, extension.Owner)
		}
		s.owners[directive.Name] = index
	}
	return nil
}

func (s *Set) claimAttributes(index int, extension Extension) error {
	for _, group := range extension.Attributes {
		if strings.TrimSpace(group.Directive) == "" || (group.Decode == nil && group.Parameters == nil && !group.PerFile) {
			return fmt.Errorf("annotation extension of %s declares attributes without a directive or a decoder", extension.Owner)
		}
		if err := checkPerFile(extension, group); err != nil {
			return err
		}
		if group.PrimaryKey && (group.Directive != tableDirective || group.Decode == nil || group.Parameters != nil) {
			return fmt.Errorf("annotation extension of %s declares primary key attributes of %q, which needs the table directive and a decoder alone",
				extension.Owner, group.Directive)
		}
		claimed := s.attributes[group.Directive]
		if claimed == nil {
			claimed = make(map[string]int)
			s.attributes[group.Directive] = claimed
		}
		for _, attribute := range group.Attributes {
			if strings.TrimSpace(attribute.Name) == "" {
				return fmt.Errorf("annotation extension of %s declares an attribute of %q without a name", extension.Owner, group.Directive)
			}
			if previous, taken := claimed[attribute.Name]; taken {
				owner := extension.Owner
				if previous < len(s.extensions) {
					owner = s.extensions[previous].Owner
				}
				return fmt.Errorf("%w: attribute %q of %q is declared by %s and %s", schemaext.ErrDuplicate,
					attribute.Name, group.Directive, owner, extension.Owner)
			}
			claimed[attribute.Name] = index
		}
	}
	return nil
}

// checkPerFile refuses a per-file attribute group the frontend would not call
// or the owner could not answer.
func checkPerFile(extension Extension, group DirectiveAttributes) error {
	if !group.PerFile {
		return nil
	}
	if !slices.Contains([]string{tableDirective, fieldDirective, indexDirective}, group.Directive) {
		return fmt.Errorf("annotation extension of %s reads attributes of %q per file, which the frontend reads for %s, %s and %s only",
			extension.Owner, group.Directive, tableDirective, fieldDirective, indexDirective)
	}
	if extension.File == nil || group.Decode != nil || group.Parameters != nil || group.PrimaryKey {
		return fmt.Errorf("annotation extension of %s reads attributes of %q per file, which needs a file decoder and no Decode, Parameters or PrimaryKey",
			extension.Owner, group.Directive)
	}
	return nil
}

func (s *Set) claimLimits(index int, extension Extension) error {
	for _, kind := range extension.Limits {
		if kind == "" || kind != strings.ToLower(strings.TrimSpace(kind)) {
			return fmt.Errorf("annotation extension of %s reads not-described kind %q, which is not a lower-case name", extension.Owner, kind)
		}
		if _, err := coverage.ParseKind(kind); err == nil {
			return fmt.Errorf("annotation extension of %s reads not-described kind %q, which is the frontend's own", extension.Owner, kind)
		}
		if previous, claimed := s.limits[kind]; claimed {
			return fmt.Errorf("%w: not-described kind %q is read by %s and %s", schemaext.ErrDuplicate, kind,
				s.extensions[previous].Owner, extension.Owner)
		}
		s.limits[kind] = index
	}
	return nil
}

// Attributes returns the attributes the set's owners add to directive, in
// the order the owners declared them. The results are copies.
func (s Set) Attributes(directive string) []Attribute {
	var result []Attribute
	for _, extension := range s.extensions {
		for _, group := range extension.Attributes {
			if group.Directive == directive {
				result = append(result, group.Attributes...)
			}
		}
	}
	return result
}

// ReadsPerFile reports whether an owner of the set holds the attribute name of
// directive as its own, read per file (see [DirectiveAttributes.PerFile]). The
// value is then the owner's, and the frontend does not keep it in its own model
// beside the owner's.
func (s Set) ReadsPerFile(directive, name string) bool {
	return slices.ContainsFunc(s.extensions, func(extension Extension) bool {
		return slices.ContainsFunc(extension.Attributes, func(group DirectiveAttributes) bool {
			return group.PerFile && group.Directive == directive &&
				slices.ContainsFunc(group.Attributes, func(attribute Attribute) bool { return attribute.Name == name })
		})
	})
}

// AttributedDirectives returns, sorted, the directives to which the set's
// owners add attributes.
func (s Set) AttributedDirectives() []string {
	return slices.Sorted(maps.Keys(s.attributes))
}

// DecodeAttributes hands each owner the attributes it adds to directive that
// attributes holds, with the ones of its Reads, and joins the facets they
// return: those of the object the directive declares. An owner that reads
// none of them is not called. A facet of a model the owner does not declare,
// and one two owners return, are errors.
func (s Set) DecodeAttributes(directive string, attributes map[string]string) (schemaext.Facets, error) {
	return s.decodeAttributes(directive, attributes, func(group DirectiveAttributes) bool { return !group.PrimaryKey })
}

// DecodePrimaryKeyAttributes is [Set.DecodeAttributes] for the attributes of
// directive that describe the primary key of the table it declares (see
// [DirectiveAttributes.PrimaryKey]).
func (s Set) DecodePrimaryKeyAttributes(directive string, attributes map[string]string) (schemaext.Facets, error) {
	return s.decodeAttributes(directive, attributes, func(group DirectiveAttributes) bool { return group.PrimaryKey })
}

// decodeAttributes decodes the groups of directive that selects picks.
func (s Set) decodeAttributes(directive string, attributes map[string]string, selects func(DirectiveAttributes) bool) (schemaext.Facets, error) {
	var joined schemaext.Facets
	for _, extension := range s.extensions {
		for _, group := range extension.Attributes {
			written := group.written(directive, attributes)
			if len(written) == 0 || group.Decode == nil || !selects(group) {
				continue
			}
			facets, err := group.Decode(written)
			if err != nil {
				return schemaext.Facets{}, err
			}
			for _, kind := range facets.DeclaredKinds() {
				if !slices.Contains(extension.Kinds, kind) {
					return schemaext.Facets{}, fmt.Errorf("%w: attributes of %q contributed a model %s does not declare",
						schemaext.ErrInvalidValue, directive, extension.Owner)
				}
			}
			if joined, err = joined.Merge(facets); err != nil {
				return schemaext.Facets{}, err
			}
		}
	}
	return joined, nil
}

// DecodeParameters hands each owner whose attributes of directive spell
// options the same attributes [Set.DecodeAttributes] does, and joins the
// options they return. It returns nil where no owner returns one. An option
// two owners return is an error.
func (s Set) DecodeParameters(directive string, attributes map[string]string) (map[string]string, error) {
	var joined map[string]string
	for _, extension := range s.extensions {
		for _, group := range extension.Attributes {
			written := group.written(directive, attributes)
			if len(written) == 0 || group.Parameters == nil {
				continue
			}
			options, err := group.Parameters(written)
			if err != nil {
				return nil, err
			}
			for name, value := range options {
				if _, taken := joined[name]; taken {
					return nil, fmt.Errorf("%w: option %q of %q is returned by two owners", schemaext.ErrDuplicate, name, directive)
				}
				if joined == nil {
					joined = make(map[string]string, len(options))
				}
				joined[name] = value
			}
		}
	}
	return joined, nil
}

// written returns the attributes of directive a declaration wrote that the
// group reads: its own, and its Reads where it wrote at least one of either.
func (g DirectiveAttributes) written(directive string, attributes map[string]string) map[string]string {
	if g.Directive != directive {
		return nil
	}
	written := make(map[string]string)
	for _, attribute := range g.Attributes {
		if value, found := attributes[attribute.Name]; found {
			written[attribute.Name] = value
		}
	}
	for _, name := range g.Reads {
		if value, found := attributes[name]; found {
			written[name] = value
		}
	}
	return written
}

// Selected reports whether the set was built by [NewSet] or [None].
func (s Set) Selected() bool {
	return s.selected
}

// Directives returns every directive the set's owners declare, sorted by
// name. The results are copies.
func (s Set) Directives() []Directive {
	var result []Directive
	for _, extension := range s.extensions {
		for _, directive := range extension.Directives {
			result = append(result, directive.Clone())
		}
	}
	slices.SortFunc(result, func(a, b Directive) int { return strings.Compare(a.Name, b.Name) })
	return result
}

// Owner returns the owner that declares directive, or false when no
// extension of the set does.
func (s Set) Owner(directive string) (string, bool) {
	index, found := s.owners[directive]
	if !found {
		return "", false
	}
	return s.extensions[index].Owner, true
}

// WithCoverageVocabulary returns the set with the coverage kinds a
// notdescribed declaration may name for the frontend's own coverage set: the
// common kinds and those the selected owners record. The zero vocabulary
// holds the common kinds only. An owner limit the vocabulary also holds is
// refused, since one spelling would then reach two coverage sets.
func (s Set) WithCoverageVocabulary(vocabulary coverage.Vocabulary) (Set, error) {
	for kind, index := range s.limits {
		if _, err := vocabulary.ParseKind(kind); err == nil {
			return Set{}, fmt.Errorf("annotation extension of %s reads not-described kind %q, which the coverage vocabulary also holds",
				s.extensions[index].Owner, kind)
		}
	}
	s.vocabulary = vocabulary
	return s, nil
}

// CoverageVocabulary returns the coverage kinds a notdescribed declaration
// may name for the frontend's own coverage set.
func (s Set) CoverageVocabulary() coverage.Vocabulary {
	return s.vocabulary
}

// LimitOwner returns the owner that reads not-described declarations of
// kind, compared without case and surrounding space, or false when no
// extension of the set does.
func (s Set) LimitOwner(kind string) (string, bool) {
	index, found := s.limits[strings.ToLower(strings.TrimSpace(kind))]
	if !found {
		return "", false
	}
	return s.extensions[index].Owner, true
}

// checkContributions refuses a contribution that sets neither or both of an
// object and a facet, and one of a model extension does not declare.
// directive names the declaration decoded, or is empty for a contribution
// that names its source.
func checkContributions(extension Extension, directive string, contributions []Contribution) error {
	for _, contribution := range contributions {
		written := cmp.Or(directive, contribution.Source.Directive)
		if (contribution.Object == nil) == (contribution.Facet == nil) {
			return fmt.Errorf("%w: directive %q contributed neither or both of an object and a facet",
				schemaext.ErrInvalidValue, written)
		}
		value := contribution.Facet
		if contribution.Object != nil {
			value = contribution.Object.Value
		}
		if value == nil || !slices.Contains(extension.Kinds, value.Kind()) {
			return fmt.Errorf("%w: directive %q contributed a model %s does not declare",
				schemaext.ErrInvalidValue, written, extension.Owner)
		}
	}
	return nil
}

func cloneAttributes(attributes map[string]string) map[string]string {
	return maps.Clone(attributes)
}

// Coverage combines the coverage claims of every extension of the set, each
// given the limits whose kind it reads. A set without extensions has none. A
// limit of a kind no extension reads is an error.
func (s Set) Coverage(limits ...coverage.Object) (schemaext.Coverage, error) {
	read := make(map[int][]coverage.Object)
	for _, limit := range limits {
		index, found := s.limits[strings.ToLower(strings.TrimSpace(string(limit.Kind)))]
		if !found {
			return schemaext.Coverage{}, fmt.Errorf("no selected owner reads not-described kind %q", limit.Kind)
		}
		read[index] = append(read[index], limit)
	}
	var combined schemaext.Coverage
	for index, extension := range s.extensions {
		claim, err := extension.Coverage(read[index])
		if err != nil {
			return schemaext.Coverage{}, fmt.Errorf("coverage of %s: %w", extension.Owner, err)
		}
		if combined, err = combined.Combine(claim); err != nil {
			return schemaext.Coverage{}, fmt.Errorf("coverage of %s: %w", extension.Owner, err)
		}
	}
	return combined, nil
}

// Kinds returns every model the set's extensions produce, sorted.
func (s Set) Kinds() []schemaext.Kind {
	var kinds []schemaext.Kind
	for _, extension := range s.extensions {
		kinds = append(kinds, extension.Kinds...)
	}
	slices.Sort(kinds)
	return kinds
}

func cloneExtension(extension Extension) Extension {
	directives := make([]Directive, 0, len(extension.Directives))
	for _, directive := range extension.Directives {
		directives = append(directives, directive.Clone())
	}
	extension.Directives = directives
	extension.Kinds = slices.Clone(extension.Kinds)
	extension.Encodes = slices.Clone(extension.Encodes)
	extension.Limits = slices.Clone(extension.Limits)
	scopes := make([]TargetScope, 0, len(extension.TargetScopes))
	for _, scope := range extension.TargetScopes {
		scope.Targets = slices.Clone(scope.Targets)
		scopes = append(scopes, scope)
	}
	extension.TargetScopes = scopes
	groups := make([]DirectiveAttributes, 0, len(extension.Attributes))
	for _, group := range extension.Attributes {
		group.Attributes = slices.Clone(group.Attributes)
		group.Reads = slices.Clone(group.Reads)
		groups = append(groups, group)
	}
	extension.Attributes = groups
	return extension
}

// Runtime supplies the annotation extensions of one explicit provider
// selection, so a caller parses Go annotations with the owners it renders and
// compares with. *ptah.run/engine.Runtime implements it.
type Runtime interface {
	Annotations() Set
}

// Unlimited adapts the coverage claim of an owner that reads no
// not-described kind into [Extension.Coverage]: the claim is the same
// whatever the source declares unmanaged.
func Unlimited(claim func() (schemaext.Coverage, error)) func(limits []coverage.Object) (schemaext.Coverage, error) {
	return func([]coverage.Object) (schemaext.Coverage, error) { return claim() }
}
