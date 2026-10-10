// Package yamlext is the contract between the YAML schema frontend and the
// feature owners whose models a YAML document can declare.
//
// The frontend, ptah.run/core/yamlschema, reads the keys every target shares.
// A model only one owner understands, such as CockroachDB row-level TTL
// declared through a table's cockroachdb platform group, is the owner's: the
// owner states the knowledge a YAML document holds about it, so a document
// that could have declared the model and did not describes a database
// without it. An owner may also read top-level keys of its own, each a
// [Section], into the objects and table facets they declare. A [Set] freezes
// the owners one parse selects; the frontend imports no owner.
package yamlext

import (
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// ErrUnselected is returned for a [Set] nobody built: the zero value. A
// caller that wants no owner passes [None] instead, so a forgotten selection
// is refused rather than read as a decision to leave the owners' models
// unknown.
var ErrUnselected = errors.New("no YAML schema owners were selected")

// Extension is what one feature owner contributes to the YAML frontend.
type Extension struct {
	// Owner names the owner, as its provider ID does.
	Owner string
	// Kinds are the models the claim covers and the sections declare. The
	// provider that registers the extension owns their desired codecs.
	Kinds []schemaext.Kind
	// Sections are the top-level document keys the owner reads.
	Sections []Section
	// Coverage is the knowledge a YAML document holds about Kinds. It is
	// required, so the claim is always the owner's to make.
	Coverage func() (schemaext.Coverage, error)
}

// Set is the frozen collection of extensions one parse selects. The zero
// value is unselected and refused by the frontend; [None] selects no owner.
// A Set is safe for concurrent use.
type Set struct {
	selected   bool
	extensions []Extension
	// sections holds the extension index that reads each section key.
	sections map[string]int
}

// None returns a selected set without owners. A parse with it claims no
// knowledge of any owner's model.
func None() Set {
	return Set{selected: true}
}

// NewSet validates and freezes extensions. It refuses an extension without an
// owner or a coverage claim, a section without a key or a decoder, and a model
// or a section key two extensions claim.
func NewSet(extensions ...Extension) (Set, error) {
	set := Set{selected: true, sections: make(map[string]int)}
	kinds := make(map[schemaext.Kind]string)
	for index, extension := range extensions {
		if strings.TrimSpace(extension.Owner) == "" {
			return Set{}, fmt.Errorf("YAML extension %d names no owner", index)
		}
		if extension.Coverage == nil {
			return Set{}, fmt.Errorf("YAML extension of %s makes no coverage claim", extension.Owner)
		}
		for _, kind := range extension.Kinds {
			if owner, claimed := kinds[kind]; claimed {
				return Set{}, fmt.Errorf("%w: model %q is claimed by %s and %s", schemaext.ErrDuplicate, kind, owner, extension.Owner)
			}
			kinds[kind] = extension.Owner
		}
		for _, section := range extension.Sections {
			if strings.TrimSpace(section.Key) == "" || section.Decode == nil {
				return Set{}, fmt.Errorf("YAML extension of %s declares a section without a key or a decoder", extension.Owner)
			}
			if previous, claimed := set.sections[section.Key]; claimed {
				return Set{}, fmt.Errorf("%w: YAML key %q is read by %s and %s", schemaext.ErrDuplicate, section.Key,
					set.extensions[previous].Owner, extension.Owner)
			}
			set.sections[section.Key] = index
		}
		extension.Kinds = slices.Clone(extension.Kinds)
		extension.Sections = slices.Clone(extension.Sections)
		set.extensions = append(set.extensions, extension)
	}
	return set, nil
}

// Selected reports whether the set was built by [NewSet] or [None].
func (s Set) Selected() bool {
	return s.selected
}

// Kinds returns every model the set's extensions claim, sorted.
func (s Set) Kinds() []schemaext.Kind {
	var kinds []schemaext.Kind
	for _, extension := range s.extensions {
		kinds = append(kinds, extension.Kinds...)
	}
	slices.Sort(kinds)
	return kinds
}

// Coverage combines the claims of every extension of the set. A set without
// extensions has none.
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

// Runtime supplies the YAML extensions of one explicit provider selection, so
// a caller parses YAML with the owners it renders and compares with.
// *ptah.run/engine.Runtime implements it.
type Runtime interface {
	YAML() Set
}
