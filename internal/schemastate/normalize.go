package schemastate

import (
	"errors"
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/internal/objectidentity"
)

// Profile is the target a state is normalized and planned against: what the
// engine can do, how it folds identifiers, and which dialect it is.
//
// It is one value rather than three parameters threaded separately because
// ADR 0001 decision 7 makes normalization a phase with one input. The tree
// currently passes identifier semantics as a nullable field on the diff and
// capabilities separately to the planner, and every consumer carries a fallback
// for the nil case.
type Profile struct {
	Dialect      string
	Semantics    identifier.Semantics
	Capabilities capability.Capabilities
}

// ErrOutsideScope reports a comparison asked about a family the reader that
// built a state never looked at.
var ErrOutsideScope = errors.New("object family outside the reader's scope")

// ErrUnnormalized reports a state reaching a stage that requires normalization
// without having been through it.
var ErrUnnormalized = errors.New("state has not been normalized against a target profile")

// Normalize applies the target's rules to a state, once.
//
// Two things happen here that used to happen in comparison, or not at all:
//
//   - An unspecified referential action becomes the action the target actually
//     applies. Both engines and documents leave ON DELETE unwritten; a catalog
//     then reports NO ACTION, and a comparison over the raw values plans a
//     modification for a foreign key nobody changed.
//   - Every foreign key's reference is checked against the tables the state
//     holds. A dangling one is an error here, where the source that wrote it is
//     still nameable, rather than a silently dropped clause later.
//
// It refuses to run twice. ADR 0001 invariant 8 says a value is folded once,
// and the verbatim constructors stokaro/ptah#1345 added to objectidentity exist
// because a second fold is what the tree does today.
func Normalize(state *State, profile Profile) (*State, error) {
	if state.normalized {
		return nil, fmt.Errorf(
			"%w: normalizing twice applies the target's rules to values that already carry them",
			errAlreadyNormalized)
	}
	out := New(state.dialect, state.scope...).WithCoverage(state.coverage)
	out.normalized = true
	out.profile = profile

	for _, object := range state.Objects() {
		normalized := object
		if object.ForeignKey != nil {
			key := *object.ForeignKey
			key.OnDelete = defaultAction(key.OnDelete)
			key.OnUpdate = defaultAction(key.OnUpdate)
			normalized.ForeignKey = &key
		}
		out.Add(normalized)
	}
	if err := resolveReferences(out); err != nil {
		return nil, err
	}
	return out, nil
}

var errAlreadyNormalized = errors.New("state is already normalized")

// defaultAction resolves what the target does when a source wrote nothing.
//
// Every engine Ptah targets applies NO ACTION in that case, which is why this
// takes no dialect: writing the rule down once, in the phase that owns it,
// is what stops each comparator from having its own idea. A target that
// differs gets a branch here and nowhere else.
func defaultAction(action Action) Action {
	if action.Normalized == ActionUnspecified {
		// The SOURCE stays empty. Comparison reads Normalized and now sees the
		// action the target applies; the renderer reads Source and emits what
		// the author wrote, which for an unwritten clause is nothing.
		action.Normalized = ActionNoAction
	}
	return action
}

// resolveReferences checks every foreign key against the tables in the state,
// and refuses the two ways a reference fails to name one column.
func resolveReferences(state *State) error {
	problems := make([]error, 0)
	for _, object := range state.OfKind(objectidentity.KindConstraint) {
		if object.ForeignKey == nil {
			continue
		}
		referenced, ok := state.Get(object.ForeignKey.ReferencedTable)
		if !ok || referenced.Table == nil {
			problems = append(problems, fmt.Errorf("%w: %s references %s, which no table in this schema is",
				objectidentity.ErrDanglingReference, object.ID, object.ForeignKey.ReferencedTable))
			continue
		}
		problems = append(problems, referencedColumnProblems(object, referenced)...)
	}
	return errors.Join(problems...)
}

// referencedColumnProblems reports referenced columns the referenced table does
// not carry, and a column-count mismatch between the two sides.
func referencedColumnProblems(object, referenced Object) []error {
	key := object.ForeignKey
	problems := make([]error, 0)
	if len(key.Columns) != len(key.ReferencedColumns) {
		problems = append(problems, fmt.Errorf(
			"%s references %d columns with %d of its own, so no pairing of them is the one the author meant",
			object.ID, len(key.ReferencedColumns), len(key.Columns)))
	}
	for _, column := range key.ReferencedColumns {
		wanted := columnOf(referenced, column)
		if _, ok := referenced.Table.Column(wanted); !ok {
			problems = append(problems, fmt.Errorf("%w: %s references column %q of %s, which does not exist",
				objectidentity.ErrDanglingReference, object.ID, column, referenced.ID))
		}
	}
	return problems
}

// columnOf builds the identity a referenced column name has on its table,
// folding through the same rule the adapter used.
func columnOf(table Object, column string) objectidentity.ID {
	return objectidentity.ID{
		Kind:   objectidentity.KindColumn,
		Schema: table.ID.Schema,
		Parent: table.ID.Name,
		Name: objectidentity.Part{
			Source:     column,
			Normalized: normalizedColumn(table, column),
		},
	}
}

// normalizedColumn folds a column name the way the table's own columns were
// folded, by finding the rule from a column the table already carries.
//
// It reads the fold off the state rather than taking a Semantics parameter,
// because the caller that has a reference does not necessarily have the profile
// the state was built under, and asking it to would be the second source of
// truth this package exists to remove.
func normalizedColumn(table Object, column string) string {
	for _, existing := range table.Table.Columns {
		if strings.EqualFold(existing.ID.Name.Source, column) {
			return existing.ID.Name.Normalized
		}
	}
	return column
}

// RequireScope refuses a stage that would read a family the state's reader
// never looked at.
//
// This is invariant 4 made mechanical. A reader that did not inspect foreign
// keys produces a state with none, and a comparison that treats that as "the
// schema has no foreign keys" plans to drop every one the database holds.
func RequireScope(state *State, kind objectidentity.Kind) error {
	if state.Describes(kind) {
		return nil
	}
	return fmt.Errorf("%w: this description does not cover %s, so its silence about one is not a removal",
		ErrOutsideScope, kind)
}

// Normalized reports whether a state has been through [Normalize].
func (s *State) Normalized() bool {
	return s.normalized
}

// Profile returns the target this state was normalized against.
func (s *State) Profile() Profile {
	return s.profile
}

// RequiredFacts are the capability keys a foreign-key change cannot be planned
// without.
//
// Naming them on the change rather than checking them inside a renderer is
// ADR 0001 decision 5: an operator whose plan is blocked learns which
// measurement is missing, instead of learning that something failed. The list
// is one key today because the slice is one family; it is a slice so a change
// that needs two says so.
func RequiredFacts() []capability.Capability {
	return []capability.Capability{capability.ForeignKeys}
}

// UniqueReferenceRequired reports whether the profile's target refuses a
// foreign key whose referenced columns are not declared unique.
//
// It is a capability rather than a dialect test because the answer is measured
// per target: the probe decides foreign_keys_require_unique_reference on every
// matrix cell rather than this package deciding it from a name.
func (p Profile) UniqueReferenceRequired() bool {
	return p.Supports(capability.ForeignKeysRequireUniqueReference)
}

// ReferencedColumnsAreUnique reports whether every column a foreign key
// references is known to be a key on its own.
//
// An unknown answer is a false: the plan cannot see a composite constraint that
// might make the reference legal, and blocking a foreign key the target might
// have accepted is the safe direction. Planning one the target refuses fails at
// apply time, on the operator's database.
func ReferencedColumnsAreUnique(referenced Object, key ForeignKey) bool {
	if referenced.Table == nil {
		return false
	}
	for _, name := range key.ReferencedColumns {
		column, ok := referenced.Table.Column(columnOf(referenced, name))
		if !ok || !column.Unique {
			return false
		}
	}
	return true
}

// Supports reports whether the profile's target has a fact.
func (p Profile) Supports(fact capability.Capability) bool {
	return p.Capabilities.Has(fact)
}

// MissingFacts returns the required facts this profile's target does not have,
// in a deterministic order.
func (p Profile) MissingFacts(facts []capability.Capability) []capability.Capability {
	missing := make([]capability.Capability, 0)
	for _, fact := range facts {
		if !p.Supports(fact) {
			missing = append(missing, fact)
		}
	}
	return missing
}

// NormalizedDialect returns the profile's dialect in the canonical spelling
// every other package compares against.
func (p Profile) NormalizedDialect() string {
	return string(platform.NormalizeDialect(p.Dialect))
}
