// Package schemastate is the canonical schema state from ADR 0001, built for
// the one object family stokaro/ptah#1350 selects: foreign-key constraints.
//
// # Why it exists
//
// The tree describes a schema two ways. `core/goschema.Database` is what an
// authoring source produces and `dbschema/types.DBSchema` is what a catalog
// read produces; they carry different field sets, spell two families
// differently, and four packages under internal/convert exist to move between
// them. Every conversion is a place a fact can be lost with no gate that
// notices, and the diff those two states produce is a list of NAMES -- which
// is why the planner takes the desired description as a second parameter, to
// recover what the diff dropped.
//
// This package is the shape ADR 0001 decides instead: one state, produced
// directly by every adapter, keyed by the identity model from
// stokaro/ptah#1345.
//
// # Scope
//
// It models tables, their columns, and foreign-key constraints. That is the
// slice, not a limitation to be fixed later by the same code: a prototype whose
// scope is implicit is one whose gaps read as answers. A [State] therefore
// carries [State.Scope], the families the adapter that built it actually
// looked at, and a comparison refuses to plan for a family outside it rather
// than reading its absence as a removal.
//
// Scope is deliberately NOT core/coverage.Set. That type answers a different
// question -- which objects a description declines to describe, for the
// families where absence is ambiguous -- and its kind list is closed and does
// not contain tables or constraints, because their absence is never ambiguous.
// Forcing one concept into the other would widen a closed list built for
// another purpose. See ADR 0001, decision 10, and the revision this prototype
// records against it.
package schemastate

import (
	"fmt"
	"slices"
	"strings"

	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/internal/objectidentity"
)

// ReferentialAction is what a foreign key does to the referencing row when the
// referenced one changes.
//
// It is a typed value rather than the raw string both sources carry, because
// the two sources spell the same action differently: an authoring document
// writes `CASCADE` and a catalog reports `CASCADE`, but a catalog also reports
// `NO ACTION` where a document wrote nothing at all. A comparison over the raw
// strings plans a modification for a foreign key nobody changed.
type ReferentialAction string

// The actions the SQL standard defines. ActionUnspecified is what a source that
// said nothing carries, and it is NOT the same value as ActionNoAction: the
// difference is what lets normalization decide, per target, whether writing
// nothing and writing NO ACTION are the same foreign key.
const (
	ActionUnspecified ReferentialAction = ""
	ActionNoAction    ReferentialAction = "NO ACTION"
	ActionRestrict    ReferentialAction = "RESTRICT"
	ActionCascade     ReferentialAction = "CASCADE"
	ActionSetNull     ReferentialAction = "SET NULL"
	ActionSetDefault  ReferentialAction = "SET DEFAULT"
)

var referentialActions = []ReferentialAction{
	ActionNoAction, ActionRestrict, ActionCascade, ActionSetNull, ActionSetDefault,
}

// Action is one referential action: what the source wrote, and what the target
// applies.
//
// The two are separate for the reason ADR 0001 invariant 2 gives for identifier
// components, and the prototype found that the invariant is not about
// identifiers -- it is about any value where a comparison folds and a renderer
// emits. A source that wrote no ON DELETE and a catalog that reports NO ACTION
// describe one foreign key, so comparison has to see one value; but rendering
// NO ACTION into DDL the author wrote without it changes the text of their
// schema for nothing.
//
// Carrying one folded value for both jobs is how the differential test against
// the existing path first went red.
type Action struct {
	// Source is the spelling the source wrote, empty when it wrote nothing.
	Source string
	// Normalized is what the target applies, which is what comparison reads.
	Normalized ReferentialAction
}

// Empty reports that the source wrote no action at all.
func (a Action) Empty() bool {
	return strings.TrimSpace(a.Source) == ""
}

// ParseReferentialAction resolves a spelling from either source.
//
// It refuses an unknown action rather than passing it through. A referential
// action Ptah does not understand is one it cannot reason about: planning a
// foreign key with it would render a clause whose behavior on delete is
// unknown, and that is the fail-closed rule of ADR 0001 invariant 10.
func ParseReferentialAction(value string) (ReferentialAction, error) {
	folded := ReferentialAction(strings.ToUpper(strings.Join(strings.Fields(value), " ")))
	if folded == ActionUnspecified {
		return ActionUnspecified, nil
	}
	if slices.Contains(referentialActions, folded) {
		return folded, nil
	}
	return "", fmt.Errorf("unknown referential action %q", value)
}

// Column is one column of a table.
//
// The prototype carries the type and nullability because a foreign key depends
// on them: MySQL and MariaDB refuse ALTER TABLE MODIFY on a column that
// participates in one, and the referencing and referenced types have to match
// for the constraint to be creatable at all.
type Column struct {
	ID       objectidentity.ID
	Type     string
	Nullable bool
	// Unique records that this column alone is a key. PostgreSQL, MySQL and
	// MariaDB all refuse a foreign key whose referenced columns are not, so it
	// is a fact the plan needs rather than an attribute of the column nobody
	// reads.
	//
	// The prototype reads single-column uniqueness only: a column that is
	// unique as part of a composite constraint reads as false here, which is
	// conservative in the safe direction -- it blocks a foreign key the target
	// might have accepted, rather than planning one the target refuses.
	Unique bool
}

// Table is a table and the columns a foreign key can reference.
type Table struct {
	Columns []Column
}

// Column returns the named column, folding through the identity model so the
// two sources' spellings resolve to one column.
func (t Table) Column(id objectidentity.ID) (Column, bool) {
	for _, column := range t.Columns {
		if column.ID.Key() == id.Key() {
			return column, true
		}
	}
	return Column{}, false
}

// ForeignKey is a foreign-key constraint.
//
// The referenced table is an identity rather than the string each source wrote,
// which is the point of ADR 0001 invariant 3: the reference is resolved once,
// by the adapter that knows its source's quoting and defaulting rules, and no
// later stage re-parses a name.
type ForeignKey struct {
	Columns           []string
	ReferencedTable   objectidentity.ID
	ReferencedColumns []string
	OnDelete          Action
	OnUpdate          Action
}

// Provenance is where an object came from. It never joins an identity: two
// objects that differ only in which file declared them are one object (ADR 0001
// invariant 5).
type Provenance struct {
	// Source names the adapter, so a diagnostic can say which reader is
	// responsible for a fact an operator disputes.
	Source string
	// Location is a file position or the catalog relation a row came from.
	Location string
}

// Object is one schema object in the canonical state.
//
// Exactly one payload pointer is set, decided by ID.Kind. It is a struct with
// typed payloads rather than an interface because ADR 0001 decision 1 rules out
// the type switch: a stage that walks every object must not be able to miss a
// family by forgetting a case.
type Object struct {
	ID         objectidentity.ID
	Table      *Table
	ForeignKey *ForeignKey
	Provenance Provenance
}

// State is a schema description: objects keyed by identity, the families the
// reader that built it looked at, and what it declines to describe.
type State struct {
	objects  map[objectidentity.Key]Object
	order    []objectidentity.Key
	scope    []objectidentity.Kind
	coverage coverage.Set
	dialect  string

	// normalized and profile record that the target's rules have been applied
	// and which target's they were. They live on the state rather than beside
	// it so a stage that requires normalization can refuse a state that has not
	// had it, and so applying it twice is detectable.
	normalized bool
	profile    Profile
}

// New returns an empty state for a dialect, declaring the families its builder
// describes.
//
// The scope is required rather than defaulted. An adapter that forgets to say
// what it read is one whose silence about a family reads as "there are none of
// those", and that is the failure mode ADR 0001 invariant 4 exists to stop.
func New(dialect string, scope ...objectidentity.Kind) *State {
	return &State{
		objects: map[objectidentity.Key]Object{},
		scope:   slices.Clone(scope),
		dialect: dialect,
	}
}

// Dialect returns the target the state was read for or written against.
func (s *State) Dialect() string {
	return s.dialect
}

// Scope returns the families the reader that built this state looked at.
func (s *State) Scope() []objectidentity.Kind {
	return slices.Clone(s.scope)
}

// Describes reports whether this state's reader looked at a family at all.
func (s *State) Describes(kind objectidentity.Kind) bool {
	return slices.Contains(s.scope, kind)
}

// Coverage returns what the description declines to describe, for the families
// where absence is ambiguous.
func (s *State) Coverage() coverage.Set {
	return s.coverage
}

// WithCoverage records what the description declines to describe.
func (s *State) WithCoverage(set coverage.Set) *State {
	s.coverage = set
	return s
}

// Add records an object, reporting the one already present under the same
// identity.
//
// A second Add under one identity is a collision rather than an overwrite:
// silently replacing is how one of two objects the target cannot hold both of
// disappears before anything can report it.
func (s *State) Add(object Object) (existing Object, collided bool) {
	key := object.ID.Key()
	if previous, ok := s.objects[key]; ok {
		return previous, true
	}
	s.objects[key] = object
	s.order = append(s.order, key)
	return Object{}, false
}

// Get returns the object with an identity.
func (s *State) Get(id objectidentity.ID) (Object, bool) {
	object, ok := s.objects[id.Key()]
	return object, ok
}

// Objects returns every object in a deterministic order.
//
// The order is the order objects were added, not map iteration order. Go
// randomizes the latter per run, and a plan whose statement order changes
// between two runs over one input is a plan nobody can review.
func (s *State) Objects() []Object {
	out := make([]Object, 0, len(s.order))
	for _, key := range s.order {
		out = append(out, s.objects[key])
	}
	return out
}

// OfKind returns every object of one family, in the same deterministic order.
func (s *State) OfKind(kind objectidentity.Kind) []Object {
	out := make([]Object, 0)
	for _, object := range s.Objects() {
		if object.ID.Kind == kind {
			out = append(out, object)
		}
	}
	return out
}

// Len returns the number of objects.
func (s *State) Len() int {
	return len(s.objects)
}
