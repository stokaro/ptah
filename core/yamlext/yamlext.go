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
	"ptah.run/internal/targetscope"
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
	// EntryAttributes are the scalar keys the owner adds to the frontend's
	// table and index entries.
	EntryAttributes []EntryAttributes
	// EntrySections are the keys the owner adds to the frontend's table
	// entries whose value is not a scalar.
	EntrySections []EntrySection
	// TargetScopes are the frontend's keys whose entries the owner reads
	// where their target scope makes them its own.
	TargetScopes []TargetScope
	// Entries reads the entries TargetScopes hand the owner, every one of a
	// document at once and in the document's order, and returns what they
	// declare. It is required with TargetScopes.
	Entries func(entries []Entry, tables Tables) ([]Contribution, error)
	// Cover narrows the document's claim by what it declares of the owner's
	// models, handed the document's objects of those models, such as a
	// table whose policies leave its row-level security switches to the
	// owner's default. It is optional.
	Cover func(claim schemaext.Coverage, objects schemaext.Objects) (schemaext.Coverage, error)
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
	// routes holds, for each frontend key, the extension index that reads
	// its entries scoped to each target.
	routes map[string]*targetscope.Routes
	// entryKeys holds, for each frontend entry, the extension index that
	// reads each key an owner adds to it.
	entryKeys map[string]map[string]int
}

// None returns a selected set without owners. A parse with it claims no
// knowledge of any owner's model.
func None() Set {
	return Set{selected: true}
}

// NewSet validates and freezes extensions. It refuses an extension without an
// owner or a coverage claim, a section without a key or a decoder, target
// scopes without a key, a target or a reader, and a model, a section key or
// the entries of one key and target two extensions claim.
func NewSet(extensions ...Extension) (Set, error) {
	set := Set{selected: true, sections: make(map[string]int), routes: make(map[string]*targetscope.Routes),
		entryKeys: make(map[string]map[string]int)}
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
		if err := set.claimScopes(index, extension); err != nil {
			return Set{}, err
		}
		if err := set.claimEntryKeys(index, extension); err != nil {
			return Set{}, err
		}
		extension.Kinds = slices.Clone(extension.Kinds)
		extension.Sections = slices.Clone(extension.Sections)
		extension.TargetScopes = slices.Clone(extension.TargetScopes)
		extension.EntrySections = slices.Clone(extension.EntrySections)
		attributes := make([]EntryAttributes, 0, len(extension.EntryAttributes))
		for _, group := range extension.EntryAttributes {
			group.Attributes = slices.Clone(group.Attributes)
			group.Reads = slices.Clone(group.Reads)
			attributes = append(attributes, group)
		}
		extension.EntryAttributes = attributes
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
