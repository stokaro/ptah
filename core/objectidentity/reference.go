package objectidentity

import (
	"errors"
	"fmt"
	"slices"
	"strings"
)

// The four ways a reference fails to name exactly one object. They are separate
// errors because they ask the author for different things: an ambiguous
// reference needs qualifying, a dangling one needs the object created or the
// name corrected, a collision needs one of two objects renamed, and an invalid
// scope transition needs the reference moved.
var (
	// ErrAmbiguousReference reports a reference matching more than one object.
	ErrAmbiguousReference = errors.New("ambiguous reference")
	// ErrDanglingReference reports a reference matching no object.
	ErrDanglingReference = errors.New("dangling reference")
	// ErrNormalizedCollision reports two objects whose spellings differ and
	// whose comparison identities do not, so the target cannot hold both.
	ErrNormalizedCollision = errors.New("normalized identity collision")
	// ErrInvalidScope reports a reference crossing a boundary the object model
	// does not permit, such as a column reference naming another schema's table
	// where the reference is defined to be schema-local.
	ErrInvalidScope = errors.New("invalid reference scope")
	// ErrMissingComponent reports a reference that cannot be resolved because
	// the source did not supply a component safe resolution needs. Resolving it
	// anyway is what the fail-closed rule exists to prevent: a guess targets
	// another object, and the plan acts on that one.
	ErrMissingComponent = errors.New("reference is missing a component required to resolve it")
)

// Reference is a name to resolve against a set of candidates, with the position
// that wrote it so a refusal can quote the source.
type Reference struct {
	// Kind is the object family the reference names.
	Kind Kind
	// ID is the identity as written. A component the source did not supply is
	// empty, which is what makes an unresolvable reference detectable rather
	// than silently defaulted.
	ID ID
	// Origin describes what wrote the reference, for the diagnostic.
	Origin string
}

// Resolve returns the one candidate a reference names, or the reason it names
// zero or several.
//
// Candidates are matched on comparison identity. A candidate list carrying two
// members with the same comparison identity and different spellings is a
// collision in the CANDIDATES rather than in the reference, and it is reported
// as one: the target cannot hold both, so no answer about the reference would
// be meaningful.
func Resolve(reference Reference, candidates []ID) (ID, error) {
	if err := requireResolvableComponents(reference); err != nil {
		return ID{}, err
	}
	if err := requireDistinctCandidates(candidates); err != nil {
		return ID{}, err
	}

	matches := make([]ID, 0, 1)
	for _, candidate := range candidates {
		if candidate.Kind == reference.ID.Kind && candidate.Equal(reference.ID) {
			matches = append(matches, candidate)
		}
	}
	switch len(matches) {
	case 1:
		return matches[0], nil
	case 0:
		return ID{}, fmt.Errorf("%w: %s%s", ErrDanglingReference, reference.ID, originSuffix(reference))
	default:
		return ID{}, fmt.Errorf("%w: %s matches %d objects%s",
			ErrAmbiguousReference, reference.ID, len(matches), originSuffix(reference))
	}
}

// requireResolvableComponents refuses a reference whose source did not supply
// what resolution needs.
//
// A name is always required. A parent is required for the families whose name
// is scoped to one -- a column or a policy without its table names nothing in
// particular, and picking the first match would be the guess this refuses.
func requireResolvableComponents(reference Reference) error {
	if reference.ID.Name.Empty() {
		return fmt.Errorf("%w: %s reference carries no name%s",
			ErrMissingComponent, reference.ID.Kind, originSuffix(reference))
	}
	if parentScopedKinds[reference.ID.Kind] && reference.ID.Parent.Empty() {
		return fmt.Errorf("%w: %s %q carries no owning table%s",
			ErrMissingComponent, reference.ID.Kind, reference.ID.Name.Source, originSuffix(reference))
	}
	return nil
}

// parentScopedKinds are the families whose name is unique only within an owning
// object, so an identity without the parent is not injective.
var parentScopedKinds = map[Kind]bool{
	KindColumn:     true,
	KindPolicy:     true,
	KindConstraint: true,
}

// requireDistinctCandidates refuses a candidate list that cannot exist on the
// target: two objects whose comparison identities are equal and whose source
// spellings differ.
func requireDistinctCandidates(candidates []ID) error {
	seen := make(map[Key]ID, len(candidates))
	for _, candidate := range candidates {
		previous, ok := seen[candidate.Key()]
		if ok && previous.String() != candidate.String() {
			return fmt.Errorf("%w: %s and %s fold to one identity",
				ErrNormalizedCollision, previous, candidate)
		}
		seen[candidate.Key()] = candidate
	}
	return nil
}

// RequireScope refuses a reference that leaves the schema it is defined to stay
// within, which is an invalid scope transition rather than a dangling name.
func RequireScope(reference Reference, scope Part) error {
	if scope.Empty() || reference.ID.Schema.Empty() {
		return nil
	}
	if reference.ID.Schema.Normalized == scope.Normalized {
		return nil
	}
	return fmt.Errorf("%w: %s refers to schema %q from schema %q%s",
		ErrInvalidScope, reference.ID, reference.ID.Schema.Source, scope.Source, originSuffix(reference))
}

// Collisions reports every pair of identities in a list that fold together
// while spelling differently, ordered so a diagnostic reads the same each run.
//
// It is separate from Resolve because a caller validating a whole schema wants
// every collision at once rather than the first one a lookup happened to hit.
func Collisions(ids []ID) []string {
	seen := make(map[Key][]ID, len(ids))
	for _, id := range ids {
		seen[id.Key()] = append(seen[id.Key()], id)
	}
	reports := make([]string, 0)
	for _, group := range seen {
		spellings := make([]string, 0, len(group))
		for _, id := range group {
			if !slices.Contains(spellings, id.String()) {
				spellings = append(spellings, id.String())
			}
		}
		if len(spellings) > 1 {
			slices.Sort(spellings)
			reports = append(reports, strings.Join(spellings, " and ")+" fold to one identity")
		}
	}
	slices.Sort(reports)
	return reports
}

func originSuffix(reference Reference) string {
	if reference.Origin == "" {
		return ""
	}
	return " (" + reference.Origin + ")"
}
