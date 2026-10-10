package annotation

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
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
}

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
	// Decode turns one declaration of one of Directives into what it
	// declares. It is required when Directives is not empty. It must not keep
	// the declaration, and it returns an error for a value it refuses.
	Decode func(Declaration) ([]Contribution, error)
	// Coverage is the knowledge a Go annotation source holds about Kinds: a
	// source that could have declared a model and did not declares its
	// absence. It is required, so the claim is always the owner's to make.
	Coverage func() (schemaext.Coverage, error)
}

// Set is the frozen collection of extensions one parse selects. The zero
// value is unselected and refused by the frontend; [None] selects no owner.
// A Set is safe for concurrent use.
type Set struct {
	selected   bool
	extensions []Extension
	owners     map[string]int
}

// None returns a selected set without owners. A parse with it reads the
// frontend's own directives only.
func None() Set {
	return Set{selected: true}
}

// NewSet validates and freezes extensions. It refuses an extension without
// an owner or a coverage claim, one that declares directives and no decoder,
// a directive without a name, and a directive name or a kind two extensions
// claim.
func NewSet(extensions ...Extension) (Set, error) {
	set := Set{selected: true, owners: make(map[string]int)}
	kinds := make(map[schemaext.Kind]string)
	for index, extension := range extensions {
		if strings.TrimSpace(extension.Owner) == "" {
			return Set{}, fmt.Errorf("annotation extension %d names no owner", index)
		}
		if extension.Coverage == nil {
			return Set{}, fmt.Errorf("annotation extension of %s makes no coverage claim", extension.Owner)
		}
		if len(extension.Directives) > 0 && extension.Decode == nil {
			return Set{}, fmt.Errorf("annotation extension of %s declares directives and no decoder", extension.Owner)
		}
		for _, kind := range extension.Kinds {
			if owner, claimed := kinds[kind]; claimed {
				return Set{}, fmt.Errorf("%w: model %q is produced by %s and %s", schemaext.ErrDuplicate, kind, owner, extension.Owner)
			}
			kinds[kind] = extension.Owner
		}
		for _, directive := range extension.Directives {
			if strings.TrimSpace(directive.Name) == "" || strings.HasPrefix(directive.Name, "//") {
				return Set{}, fmt.Errorf("annotation extension of %s declares a directive without a name", extension.Owner)
			}
			if previous, claimed := set.owners[directive.Name]; claimed {
				return Set{}, fmt.Errorf("%w: directive %q is declared by %s and %s", schemaext.ErrDuplicate, directive.Name,
					set.extensions[previous].Owner, extension.Owner)
			}
			set.owners[directive.Name] = index
		}
		set.extensions = append(set.extensions, cloneExtension(extension))
	}
	return set, nil
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

// Decode hands declaration to the owner of its directive. A directive no
// extension declares is an error.
func (s Set) Decode(declaration Declaration) ([]Contribution, error) {
	index, found := s.owners[declaration.Directive]
	if !found {
		return nil, fmt.Errorf("no selected owner declares directive %q", declaration.Directive)
	}
	declaration.Attributes = maps.Clone(declaration.Attributes)
	contributions, err := s.extensions[index].Decode(declaration)
	if err != nil {
		return nil, err
	}
	extension := s.extensions[index]
	for _, contribution := range contributions {
		if (contribution.Object == nil) == (contribution.Facet == nil) {
			return nil, fmt.Errorf("%w: directive %q contributed neither or both of an object and a facet",
				schemaext.ErrInvalidValue, declaration.Directive)
		}
		value := contribution.Facet
		if contribution.Object != nil {
			value = contribution.Object.Value
		}
		if value == nil || !slices.Contains(extension.Kinds, value.Kind()) {
			return nil, fmt.Errorf("%w: directive %q contributed a model %s does not declare",
				schemaext.ErrInvalidValue, declaration.Directive, extension.Owner)
		}
	}
	return contributions, nil
}

// Coverage combines the coverage claims of every extension of the set. A set
// without extensions has none.
func (s Set) Coverage() (schemaext.Coverage, error) {
	var combined schemaext.Coverage
	for _, extension := range s.extensions {
		claim, err := extension.Coverage()
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
	return extension
}

// Runtime supplies the annotation extensions of one explicit provider
// selection, so a caller parses Go annotations with the owners it renders and
// compares with. *ptah.run/engine.Runtime implements it.
type Runtime interface {
	Annotations() Set
}
