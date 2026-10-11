// Package typecompare compares PostgreSQL domains, composite types and range
// types for the owner of package pgtype. It performs no database access.
package typecompare

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/pgtypealias"
)

// Service compares user types. Its zero value is ready for concurrent use.
//
// A type is identified by its kind, schema and name, so a declaration that
// names no schema and a type the catalog reports in the default schema agree.
// Only what a declaration states is compared: an attribute the catalog
// resolves on its own, such as a range type's operator class, is no
// difference from a declaration that leaves it out. Type spellings are
// compared in the form format_type reports, so `VARCHAR(20)` and
// `character varying(20)` agree.
//
// A domain's CHECK and default are compared only in the server's spelling,
// which a live comparison attaches as [pgtype.DesiredDomain.Normalized]: the
// server stores a parse tree and prints it back, so the declaration and the
// catalog never agree as written (stokaro/ptah#1717). Without that spelling
// they are written when the domain is created and never compared. A
// composite type's attribute list is compared in order, by name without
// regard to case and by type. A comment is compared only where the target
// stores the kind's comment.
//
// The comparison does not create a type whose presence the read could not
// establish, and reports it undecided instead: CREATE DOMAIN and CREATE TYPE
// have no conditional form. It does not drop a type the desired source did
// not describe or asked to default.
type Service struct{}

// pair is one type's declaration and observation, either missing. Only the
// fields of the model its identity names are set.
type pair struct {
	ref               objectidentity.ID
	desiredDomain     *pgtype.DesiredDomain
	observedDomain    *pgtype.ObservedDomain
	desiredComposite  *pgtype.DesiredComposite
	observedComposite *pgtype.ObservedComposite
	desiredRange      *pgtype.DesiredRange
	observedRange     *pgtype.ObservedRange
}

// declared reports whether the desired side holds the type.
func (p pair) declared() bool {
	return p.desiredDomain != nil || p.desiredComposite != nil || p.desiredRange != nil
}

// held reports whether the current side holds the type.
func (p pair) held() bool {
	return p.observedDomain != nil || p.observedComposite != nil || p.observedRange != nil
}

// CompareObjects returns one change per type that differs, in identity order.
// Inputs remain unchanged.
func (Service) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: PostgreSQL user type comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if len(request.Kinds) == 0 || slices.ContainsFunc(request.Kinds, func(kind schemaext.Kind) bool { return !slices.Contains(pgtype.Kinds(), kind) }) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported user type comparison kinds", schemaext.ErrInvalidValue)
	}
	if len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: user type comparison accepts no change requests", schemaext.ErrInvalidValue)
	}
	pairs, err := typePairs(request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: request.Desired}
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		compare(request, pair, &result)
	}
	return result, ctx.Err()
}

func compare(request schemaext.ObjectComparisonRequest, pair pair, result *schemaext.ObjectComparisonResult) {
	kind := schemaext.Kind(pair.ref.Kind)
	switch {
	case !pair.held():
		current := request.Current.Coverage.Lookup(kind, pair.ref)
		if unknown(current) {
			result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: kind, Subject: pair.ref,
				Reason: fmt.Sprintf("whether the %s exists was not established: %s", pgtype.Label(pair.ref), current.Reason)})
			return
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref, Value: creation(pair)})
	case !pair.declared():
		desired := request.Desired.Coverage.Lookup(kind, pair.ref)
		if unknown(desired) || desired.State == schemaext.Defaulted {
			return
		}
		result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref, Value: removal(pair)})
	default:
		if change := difference(request.Capabilities, pair); change != nil {
			result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.ref, Value: change})
		}
	}
}

// creation is the change that creates a declared type.
func creation(pair pair) schemaext.ChangeValue {
	switch schemaext.Kind(pair.ref.Kind) {
	case pgtype.DomainKind:
		return &pgtype.DomainChange{After: pair.desiredDomain}
	case pgtype.CompositeKind:
		return &pgtype.CompositeChange{After: pair.desiredComposite}
	default:
		return &pgtype.RangeChange{After: pair.desiredRange}
	}
}

// removal is the change that drops a type nothing declares.
func removal(pair pair) schemaext.ChangeValue {
	switch schemaext.Kind(pair.ref.Kind) {
	case pgtype.DomainKind:
		return &pgtype.DomainChange{Before: pair.observedDomain}
	case pgtype.CompositeKind:
		return &pgtype.CompositeChange{Before: pair.observedComposite}
	default:
		return &pgtype.RangeChange{Before: pair.observedRange}
	}
}

// difference is the change of a type both sides hold, or nil where they
// agree.
func difference(caps capability.Capabilities, pair pair) schemaext.ChangeValue {
	switch schemaext.Kind(pair.ref.Kind) {
	case pgtype.DomainKind:
		if differs := domainDifferences(caps, pair.desiredDomain, pair.observedDomain); len(differs) > 0 {
			return &pgtype.DomainChange{Before: pair.observedDomain, After: pair.desiredDomain, Differs: differs}
		}
	case pgtype.CompositeKind:
		declared, held := pair.desiredComposite, pair.observedComposite
		var differs []string
		var added []pgtype.Attribute
		var removed []string
		if attributeList(declared.Attributes) != attributeList(held.Attributes) {
			differs = append(differs, pgtype.DiffAttributes)
			added, removed = pgtype.AttributeDelta(declared.Attributes, held.Attributes)
		}
		differs = commentDifference(differs, caps, capability.TypeComments, declared.Comment, held.Comment)
		if len(differs) > 0 {
			return &pgtype.CompositeChange{Before: held, After: declared, Differs: differs, Added: added, Removed: removed}
		}
	default:
		if differs := rangeDifferences(caps, pair.desiredRange, pair.observedRange); len(differs) > 0 {
			return &pgtype.RangeChange{Before: pair.observedRange, After: pair.desiredRange, Differs: differs}
		}
	}
	return nil
}

// domainDifferences compares a domain's base type, its NOT NULL, its CHECK
// and its default in the server's spelling where the comparison has it, and
// its comment, sorted.
//
// The comparison is deliberately one-directional: a declaration that states
// no CHECK and no default is not read as a request to remove the ones the
// catalog holds. Adopting Ptah over an existing database would otherwise drop
// constraints nobody asked about.
func domainDifferences(caps capability.Capabilities, declared *pgtype.DesiredDomain, held *pgtype.ObservedDomain) []string {
	var differs []string
	if pgtypealias.Canonical(declared.BaseType) != pgtypealias.Canonical(held.BaseType) {
		differs = append(differs, pgtype.DiffBaseType)
	}
	if declared.NotNull != held.NotNull {
		differs = append(differs, pgtype.DiffNotNull)
	}
	if normalized := declared.Normalized; normalized != nil {
		if normalized.Check != "" && !checkMatches(normalized.Check, held) {
			differs = append(differs, pgtype.DiffCheck)
		}
		if normalized.Default != "" && normalized.Default != held.Default {
			differs = append(differs, pgtype.DiffDefault)
		}
	}
	differs = commentDifference(differs, caps, capability.DomainComments, declared.Comment, held.Comment)
	slices.Sort(differs)
	return differs
}

// checkMatches reports whether the catalog enforces exactly the declared
// constraint and nothing else.
//
// It compares against the constraints one by one rather than against the
// joined expression. A domain holding two constraints joins as `(a) AND (b)`,
// and a declaration of `a AND b` normalizes to `((a) AND (b))`: equal in
// meaning, different by one pair of parentheses, so comparing the joined forms
// would plan a replacement on every run. A read that could not enumerate the
// constraints falls back to the joined form, where a false difference is
// better than silence.
func checkMatches(declared string, held *pgtype.ObservedDomain) bool {
	if len(held.Checks) == 0 {
		return declared == held.Check
	}
	return len(held.Checks) == 1 && held.Checks[0].Expression == declared
}

// rangeDifferences compares a range type's subtype, each optional attribute
// the declaration states or clears, and its comment, sorted.
//
// An optional attribute is compared without regard to case on trimmed text,
// because PostgreSQL reports each as a bare identifier. A declaration that
// leaves one out says nothing about it: the catalog always resolves an
// operator class, and a range carrying a SUBTYPE_DIFF its author never heard
// of is not rebuilt without it. One the declaration clears differs from a
// catalog that holds one (stokaro/ptah#2223).
func rangeDifferences(caps capability.Capabilities, declared *pgtype.DesiredRange, held *pgtype.ObservedRange) []string {
	var differs []string
	if pgtypealias.Canonical(declared.Subtype) != pgtypealias.Canonical(held.Subtype) {
		differs = append(differs, pgtype.DiffSubtype)
	}
	for _, attribute := range pgtype.RangeAttributes() {
		stated := strings.TrimSpace(declared.Attribute(attribute))
		if stated == "" && !declared.Clears(attribute) {
			continue
		}
		if !strings.EqualFold(stated, strings.TrimSpace(held.Attribute(attribute))) {
			differs = append(differs, attribute)
		}
	}
	differs = commentDifference(differs, caps, capability.TypeComments, declared.Comment, held.Comment)
	slices.Sort(differs)
	return differs
}

// commentDifference adds the comment to differs where the target stores the
// kind's comment and the two differ. Surrounding whitespace is not part of a
// comment. Everywhere else the reader reports no comment whatever was
// written, so a declared one would be a difference no plan can close
// (stokaro/ptah#3627).
func commentDifference(differs []string, caps capability.Capabilities, key capability.Capability, declared, held string) []string {
	if caps.Has(key) && strings.TrimSpace(declared) != strings.TrimSpace(held) {
		return append(differs, pgtype.DiffComment)
	}
	return differs
}

// attributeList is a composite type's attributes as compared: each name in
// lower case beside its canonical type, in order.
func attributeList(attributes []pgtype.Attribute) string {
	parts := make([]string, len(attributes))
	for i, attribute := range attributes {
		parts[i] = strings.ToLower(attribute.Name) + " " + pgtypealias.Canonical(attribute.Type)
	}
	return strings.Join(parts, ", ")
}

// unknown reports knowledge that establishes neither presence nor absence.
func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

// typePairs pairs declarations with observations by identity, ordered by
// identity. A declaration among the observations, or the reverse, is refused.
func typePairs(request schemaext.ObjectComparisonRequest) ([]pair, error) {
	pairs := make(map[objectidentity.Key]*pair)
	for _, side := range []struct {
		state          schemaext.ObjectState
		representation schemaext.Representation
	}{{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed}} {
		objects, err := side.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if err := pgtype.ValidateRef(object.Ref); err != nil {
				return nil, err
			}
			entry := pairs[object.Ref.Key()]
			if entry == nil {
				entry = &pair{ref: object.Ref}
				pairs[object.Ref.Key()] = entry
			}
			if err := capture(entry, object.Value, side.representation); err != nil {
				return nil, err
			}
		}
	}
	result := make([]pair, 0, len(pairs))
	for _, key := range slices.SortedFunc(maps.Keys(pairs), func(a, b objectidentity.Key) int {
		return schemaext.CompareRefs(pairs[a].ref, pairs[b].ref)
	}) {
		result = append(result, *pairs[key])
	}
	return result, nil
}

// capture records one operand of entry, refusing a value of another model
// than its identity names, or of the other representation.
func capture(entry *pair, value schemaext.Value, representation schemaext.Representation) error {
	if value == nil || value.Kind() != schemaext.Kind(entry.ref.Kind) {
		return fmt.Errorf("%w: unexpected user type operand %T for %s", schemaext.ErrInvalidValue, value, entry.ref)
	}
	declaredSide := representation == schemaext.Desired
	switch typed := value.(type) {
	case *pgtype.DesiredDomain:
		entry.desiredDomain = typed
		return placed(entry, declaredSide, true, pgtype.ValidateDesiredDomain(typed))
	case *pgtype.DesiredComposite:
		entry.desiredComposite = typed
		return placed(entry, declaredSide, true, pgtype.ValidateDesiredComposite(typed))
	case *pgtype.DesiredRange:
		entry.desiredRange = typed
		return placed(entry, declaredSide, true, pgtype.ValidateDesiredRange(typed))
	case *pgtype.ObservedDomain:
		entry.observedDomain = typed
		return placed(entry, declaredSide, false, pgtype.ValidateObservedDomain(typed))
	case *pgtype.ObservedComposite:
		entry.observedComposite = typed
		return placed(entry, declaredSide, false, pgtype.ValidateObservedComposite(typed))
	case *pgtype.ObservedRange:
		entry.observedRange = typed
		return placed(entry, declaredSide, false, pgtype.ValidateObservedRange(typed))
	default:
		return fmt.Errorf("%w: unexpected user type operand %T for %s", schemaext.ErrInvalidValue, value, entry.ref)
	}
}

// placed refuses a declaration among the observations and the reverse, and
// otherwise answers the operand's own validation.
func placed(entry *pair, declaredSide, declaration bool, valid error) error {
	switch {
	case declaration && !declaredSide:
		return fmt.Errorf("%w: a %s declaration among the observations: %s", schemaext.ErrInvalidValue, pgtype.Label(entry.ref), entry.ref)
	case !declaration && declaredSide:
		return fmt.Errorf("%w: a %s observation among the declarations: %s", schemaext.ErrInvalidValue, pgtype.Label(entry.ref), entry.ref)
	default:
		return valid
	}
}
