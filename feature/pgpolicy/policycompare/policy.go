// Package policycompare compares PostgreSQL row-security state for the owner
// of package pgpolicy: policies as feature objects of their tables, and a
// table's ENABLE and FORCE switches as a facet of it. It performs no database
// access. A server's spelling of a declared policy arrives attached to the
// declaration, put there by a normalization probe.
package policycompare

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/normalize"
)

// PolicyService compares the policies of surviving tables. Its zero value is
// ready for concurrent use.
//
// A policy is identified by its schema, its table and its name, so two tables
// may each hold a policy of one name, and a declaration and an observation
// pair by that identity rather than by spelling: a schema the declaration left
// to the default and the one the catalog names are one schema. A source builds
// the identity under the target's identifier rules, as it builds the identity
// of the table the policy is on.
//
// The command, the role list and the composition are compared with
// PostgreSQL's defaults resolved, so an omitted FOR is ALL, an omitted TO is
// PUBLIC and an omitted AS is permissive, and the role list is a set. The
// clauses are compared through the server's spelling where a probe attached
// one, and through a textual fold otherwise: PostgreSQL stores a parse tree,
// and the cast it inserts depends on the type of the column a clause names.
// A role keyword the server resolves when the policy is created never equals
// the role name the catalog reports, unless a probe resolved it.
//
// A table's creation and removal carry its policies, so a policy on a table
// the plan creates or drops is not a change here.
type PolicyService struct{}

type policyPair struct {
	key                    objectidentity.ID
	desiredRef, currentRef objectidentity.ID
	desired                *pgpolicy.DesiredPolicy
	current                *pgpolicy.ObservedPolicy
}

// CompareObjects never treats an unread namespace as empty and never removes
// a policy the desired source could not describe. A declared policy whose
// presence was not established is undecided. An observed policy the desired
// source could not describe is adopted into the effective declaration, and
// its coverage records that it is known. Inputs remain unchanged.
func (PolicyService) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if !platform.IsPostgresFamily(request.Target) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: PostgreSQL row-security comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{pgpolicy.PolicyKind}) {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: unsupported row-security comparison kinds", schemaext.ErrInvalidValue)
	}
	if len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: row-security comparison accepts no change requests", schemaext.ErrInvalidValue)
	}
	parents, err := policyParents(request.Parents)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	pairs, err := policyPairs(ctx, request, parents)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: schemaext.ObjectState{Objects: request.Desired.Objects}}
	var adopted []schemaext.SubjectCoverage
	for _, pair := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		parent := parents[pgpolicy.Table(pair.key).Key()]
		if !parent.Desired || !parent.Current {
			continue
		}
		record, err := comparePolicy(request, pair, &result)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		if record != nil {
			adopted = append(adopted, *record)
		}
	}
	undecidedNamespaces(request, &result)
	result.Desired.Coverage, err = policyCoverage(request, adopted)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, nil
}

func comparePolicy(request schemaext.ObjectComparisonRequest, pair policyPair, result *schemaext.ObjectComparisonResult) (*schemaext.SubjectCoverage, error) {
	if pair.desired != nil && limited(request.Desired.Coverage, pair.desiredRef) {
		undecided(result, pair.key, "the desired source cannot describe the complete policy")
		return nil, nil
	}
	currentRef := pair.currentRef
	if pair.current == nil {
		currentRef = pair.key
	}
	currentKnowledge := request.Current.Coverage.Lookup(pgpolicy.PolicyKind, currentRef)
	if pair.current != nil && limited(request.Current.Coverage, pair.currentRef) || pair.current == nil && unknown(currentKnowledge) {
		if pair.desired != nil {
			undecided(result, pair.key, "the current policy or its absence was not established: "+currentKnowledge.Reason)
		}
		return nil, nil
	}
	if pair.desired == nil {
		return dropOrAdopt(request, pair, result)
	}
	change := &pgpolicy.PolicyChange{After: pair.desired, Access: pgpolicy.PolicyAccess(nil, pair.desired, pgpolicy.ExpressionsDiffer)}
	if pair.current != nil {
		found := difference(pair.desired, pair.current)
		if !found.definition && !found.comment {
			return nil, nil
		}
		change = &pgpolicy.PolicyChange{Before: pair.current, After: pair.desired, CommentOnly: !found.definition,
			Access: pgpolicy.PolicyAccess(pair.current, pair.desired, found.expressions)}
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.desiredRef, Value: change})
	return nil, nil
}

// dropOrAdopt answers an observed policy nothing declares. A source that could
// not describe it has not asked for it to be dropped, so it is kept as a
// declaration of what the server holds; otherwise its absence is a request to
// drop it.
func dropOrAdopt(request schemaext.ObjectComparisonRequest, pair policyPair, result *schemaext.ObjectComparisonResult) (*schemaext.SubjectCoverage, error) {
	desiredKnowledge := request.Desired.Coverage.Lookup(pgpolicy.PolicyKind, pair.currentRef)
	if desiredKnowledge.State == schemaext.Defaulted {
		return nil, fmt.Errorf("%w: policy %s requires a definition; a table has no default policy", schemaext.ErrInvalidValue, pair.currentRef)
	}
	if unknown(desiredKnowledge) {
		declared, err := pair.current.Desired()
		if err != nil {
			return nil, err
		}
		result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: pair.currentRef, Value: declared})
		if err != nil {
			return nil, err
		}
		return &schemaext.SubjectCoverage{Kind: pgpolicy.PolicyKind, Subject: pair.currentRef,
			Knowledge: schemaext.Knowledge{State: schemaext.Complete}}, nil
	}
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: pair.currentRef,
		Value: &pgpolicy.PolicyChange{Before: pair.current, Access: pgpolicy.PolicyAccess(pair.current, nil, pgpolicy.ExpressionsDiffer)}})
	return nil, nil
}

// found is what a comparison of a declaration with an observation established.
type found struct {
	definition  bool
	comment     bool
	expressions pgpolicy.ExpressionFinding
}

// difference compares a declaration with an observation with PostgreSQL's
// defaults resolved.
func difference(declared *pgpolicy.DesiredPolicy, observed *pgpolicy.ObservedPolicy) found {
	using, withCheck := declared.Using, declared.WithCheck
	if declared.Normalized != nil {
		using, withCheck = declared.Normalized.Using, declared.Normalized.WithCheck
	}
	result := found{comment: declared.Comment != observed.Comment, expressions: pgpolicy.ExpressionsDiffer}
	if sameClause(using, observed.Using) && sameClause(withCheck, observed.WithCheck) {
		result.expressions = pgpolicy.ExpressionsSame
	}
	result.definition = result.expressions != pgpolicy.ExpressionsSame ||
		declared.EffectiveCommand() != observed.Command ||
		declared.EffectiveComposition() != observed.Composition ||
		!sameRoleSet(declared.ComparedRoles(), observed.Roles)
	return result
}

// sameClause compares one clause: present on both sides and equal after the
// textual fold, or absent on both. The fold is all a declaration without a
// server's spelling has; with one, both sides are the server's and the fold
// only evens out spacing.
func sameClause(declared, observed *string) bool {
	if declared == nil || observed == nil {
		return declared == nil && observed == nil
	}
	return normalize.Expression(*declared) == normalize.Expression(*observed)
}

// sameRoleSet compares two validated role lists, which hold no selector twice,
// as sets.
func sameRoleSet(left, right []pgpolicy.RoleSelector) bool {
	return len(left) == len(right) && !slices.ContainsFunc(left, func(role pgpolicy.RoleSelector) bool {
		return !slices.Contains(right, role)
	})
}

// policyParents indexes the table lifecycles by identity.
func policyParents(states []schemaext.ParentState) (map[objectidentity.Key]schemaext.ParentState, error) {
	parents := make(map[objectidentity.Key]schemaext.ParentState, len(states))
	for _, parent := range states {
		if parent.Subject.Kind != objectidentity.KindTable || parent.Subject.Name.Empty() || (!parent.Desired && !parent.Current) {
			return nil, fmt.Errorf("%w: invalid row-security policy parent %s", schemaext.ErrInvalidValue, parent.Subject)
		}
		if _, found := parents[parent.Subject.Key()]; found {
			return nil, fmt.Errorf("%w: duplicate row-security policy parent %s", schemaext.ErrDuplicate, parent.Subject)
		}
		parents[parent.Subject.Key()] = parent
	}
	return parents, nil
}

// policyPairs pairs declarations with observations by identity, ordered by
// identity. A policy whose table its own side does
// not hold is refused: a policy cannot exist without its table.
func policyPairs(ctx context.Context, request schemaext.ObjectComparisonRequest, parents map[objectidentity.Key]schemaext.ParentState) ([]policyPair, error) {
	pairs := make(map[objectidentity.Key]*policyPair)
	for _, source := range []struct {
		state     schemaext.ObjectState
		direction schemaext.Representation
	}{{request.Desired, schemaext.Desired}, {request.Current, schemaext.Observed}} {
		objects, err := source.state.Objects.All()
		if err != nil {
			return nil, err
		}
		for _, object := range objects {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			if err := pgpolicy.ValidatePolicyRef(object.Ref); err != nil {
				return nil, err
			}
			key := object.Ref
			parent := parents[pgpolicy.Table(key).Key()]
			if source.direction == schemaext.Desired && !parent.Desired || source.direction == schemaext.Observed && !parent.Current {
				return nil, fmt.Errorf("%w: policy %s is on a table its %s schema does not hold", ptaherr.ErrInvalidSchemaDiff, object.Ref, source.direction)
			}
			pair := pairs[key.Key()]
			if pair == nil {
				pair = &policyPair{key: key}
				pairs[key.Key()] = pair
			}
			if err := capturePair(pair, object, source.direction); err != nil {
				return nil, err
			}
		}
	}
	result := make([]policyPair, 0, len(pairs))
	for _, key := range slices.SortedFunc(maps.Keys(pairs), func(a, b objectidentity.Key) int {
		return schemaext.CompareRefs(pairs[a].key, pairs[b].key)
	}) {
		result = append(result, *pairs[key])
	}
	return result, nil
}

func capturePair(pair *policyPair, object schemaext.Object, direction schemaext.Representation) error {
	switch value := object.Value.(type) {
	case *pgpolicy.DesiredPolicy:
		if direction != schemaext.Desired {
			return fmt.Errorf("%w: a policy declaration among the observations: %s", schemaext.ErrInvalidValue, object.Ref)
		}
		if pair.desired != nil {
			return fmt.Errorf("%w: policies %s and %s are one policy on this target", ptaherr.ErrInvalidSchemaDiff, pair.desiredRef, object.Ref)
		}
		if err := pgpolicy.ValidateDesiredPolicy(value); err != nil {
			return err
		}
		pair.desiredRef, pair.desired = object.Ref, value
	case *pgpolicy.ObservedPolicy:
		if direction != schemaext.Observed {
			return fmt.Errorf("%w: a policy observation among the declarations: %s", schemaext.ErrInvalidValue, object.Ref)
		}
		if pair.current != nil {
			return fmt.Errorf("%w: observed policies %s and %s are one policy on this target", schemaext.ErrInvalidValue, pair.currentRef, object.Ref)
		}
		if err := pgpolicy.ValidateObservedPolicy(value); err != nil {
			return err
		}
		pair.currentRef, pair.current = object.Ref, value
	default:
		return fmt.Errorf("%w: unexpected row-security policy operand %T", schemaext.ErrInvalidValue, object.Value)
	}
	return nil
}

// undecidedNamespaces reports each surviving table whose policies the desired
// source describes and the read did not enumerate: an observed policy there
// may be one the description drops, and nobody saw it.
func undecidedNamespaces(request schemaext.ObjectComparisonRequest, result *schemaext.ObjectComparisonResult) {
	for _, parent := range request.Parents {
		if !parent.Desired || !parent.Current {
			continue
		}
		// A table subject reads the table's namespace claim, and the kind's
		// claim where the table has none.
		current := request.Current.Coverage.Lookup(pgpolicy.PolicyKind, parent.Subject)
		if unknown(current) && !unknown(request.Desired.Coverage.Lookup(pgpolicy.PolicyKind, parent.Subject)) {
			undecided(result, parent.Subject, "the table's policies were not enumerated: "+current.Reason)
		}
	}
}

// policyCoverage keeps the source's claims and records the observed policies
// the comparison adopted, so a later stage knows they are known rather than
// declared.
func policyCoverage(request schemaext.ObjectComparisonRequest, adopted []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	kinds := request.Desired.Coverage.KindRecords()
	if len(kinds) == 0 {
		enrolled, err := pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Desired,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the desired source did not describe row-security policies"}, nil)
		if err != nil {
			return schemaext.Coverage{}, err
		}
		kinds = enrolled.KindRecords()
	}
	records := make(map[objectidentity.Key]schemaext.SubjectCoverage)
	for _, record := range request.Desired.Coverage.SubjectRecords() {
		records[record.Subject.Key()] = record
	}
	for _, record := range adopted {
		records[record.Subject.Key()] = record
	}
	return schemaext.NewCoverage(schemaext.Desired, kinds, slices.Collect(maps.Values(records)))
}

func limited(coverage schemaext.Coverage, ref objectidentity.ID) bool {
	knowledge, found := coverage.SubjectKnowledge(pgpolicy.PolicyKind, ref)
	return found && unknown(knowledge)
}

func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

func undecided(result *schemaext.ObjectComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: pgpolicy.PolicyKind, Subject: subject, Reason: reason})
}
