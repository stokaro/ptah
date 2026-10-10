// Package mssqlcompare compares SQL Server security policies, declared against
// observed, into changes that carry their access assessment. It reads only
// the captured states it is handed: no database, no source file.
package mssqlcompare

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqldiff"
	"ptah.run/dialect/mssql/mssqlschema"
)

// Service compares security policies by schema and name. Its zero value is
// ready for concurrent use.
//
// A policy belongs to its schema, so it pairs by its identity under the
// request's identifier rules: an unqualified declaration takes the target's
// default schema. A pair whose predicate arguments SQL Server may have
// rewritten, and that differs in nothing else, is undecided rather than
// changed: the catalog stores `tenant_id` as `[tenant_id]` and `CAST(t AS int)`
// as `CONVERT([int],[t])`, and an offline comparison cannot tell a rewrite
// from another expression (see [mssqlschema.CompareArgument]).
//
// SQL Server enables at most one policy with a predicate on a table: enabling
// a second is refused with Msg 33264. The comparison refuses a desired schema
// in which two enabled policies bind one table, counting the observed
// policies the description keeps, so the refusal names both policies before
// any statement runs. Tables are matched by [mssqlschema.ObjectName.ConflictKey],
// since a case-insensitive database refuses spellings that differ only in case
// or trailing spaces.
type Service struct{}

// pair holds a declaration and an observation of one policy, each under the
// identity its source captured it with, which is what that source's coverage
// is keyed by.
type pair struct {
	desiredRef, currentRef objectidentity.ID
	desired                *mssqlschema.DesiredSecurityPolicy
	current                *mssqlschema.ObservedSecurityPolicy
}

// subject is the identity a change or a diagnostic names: the declaration's
// when there is one.
func (p pair) subject() objectidentity.ID {
	if p.desired != nil {
		return p.desiredRef
	}
	return p.currentRef
}

// kept is a policy the server holds once the plan has run.
type kept struct {
	ref    objectidentity.ID
	policy *mssqlschema.DesiredSecurityPolicy
}

// CompareObjects never treats an unread namespace as empty: a declared policy
// whose presence was not established is undecided, and an observed policy a
// description could not express is kept rather than dropped. Inputs remain
// unchanged.
func (Service) CompareObjects(ctx context.Context, request schemaext.ObjectComparisonRequest) (schemaext.ObjectComparisonResult, error) {
	if ctx == nil {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: comparison requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	if platform.NormalizeDialect(request.Target) != platform.SQLServer {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: SQL Server security policy comparison on %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	if !slices.Equal(request.Kinds, []schemaext.Kind{mssqlschema.SecurityPolicyKind}) || len(request.Requests) != 0 {
		return schemaext.ObjectComparisonResult{}, fmt.Errorf("%w: security policy comparison takes only its own kind and no change request", schemaext.ErrInvalidValue)
	}
	pairs, err := pairPolicies(ctx, request)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result := schemaext.ObjectComparisonResult{Complete: true, Desired: schemaext.ObjectState{Objects: request.Desired.Objects}}
	var adopted []schemaext.SubjectCoverage
	remaining := make([]kept, 0, len(pairs))
	for _, candidate := range pairs {
		if err := ctx.Err(); err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		outcome, err := comparePair(request, candidate, &result)
		if err != nil {
			return schemaext.ObjectComparisonResult{}, err
		}
		if outcome.adopted {
			adopted = append(adopted, schemaext.SubjectCoverage{Kind: mssqlschema.SecurityPolicyKind, Subject: candidate.currentRef,
				Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
		}
		if outcome.remains != nil {
			remaining = append(remaining, kept{ref: candidate.subject(), policy: outcome.remains})
		}
	}
	if err := refuseSharedTables(remaining); err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	result.Desired.Coverage, err = desiredCoverage(request, adopted)
	if err != nil {
		return schemaext.ObjectComparisonResult{}, err
	}
	return result, ctx.Err()
}

// outcome is what one pair leaves behind: the policy the server holds once
// the plan has run, if any, and whether an observed policy was adopted.
type outcome struct {
	remains *mssqlschema.DesiredSecurityPolicy
	adopted bool
}

func comparePair(request schemaext.ObjectComparisonRequest, candidate pair, result *schemaext.ObjectComparisonResult) (outcome, error) {
	subject := candidate.subject()
	if candidate.desired != nil && limited(request.Desired.Coverage, candidate.desiredRef) {
		knowledge := request.Desired.Coverage.Lookup(mssqlschema.SecurityPolicyKind, candidate.desiredRef)
		undecided(result, subject, "the desired source cannot describe the complete security policy: "+knowledge.Reason)
		return outcome{remains: candidate.desired}, nil
	}
	currentKnowledge := request.Current.Coverage.Lookup(mssqlschema.SecurityPolicyKind, subject)
	if candidate.current != nil && limited(request.Current.Coverage, candidate.currentRef) || candidate.current == nil && unknown(currentKnowledge) {
		if candidate.desired != nil {
			undecided(result, subject, "the current security policy or its absence was not established: "+currentKnowledge.Reason)
			return outcome{remains: candidate.desired}, nil
		}
		return outcome{remains: observedAsDeclared(candidate.current)}, nil
	}
	desiredKnowledge := request.Desired.Coverage.Lookup(mssqlschema.SecurityPolicyKind, subject)
	switch {
	case candidate.desired == nil && unknown(desiredKnowledge):
		// A description that could not express a security policy has not asked
		// for one to be dropped; dropping it would take away the rows' guard.
		declared, err := candidate.current.Desired()
		if err != nil {
			return outcome{}, err
		}
		result.Desired.Objects, err = result.Desired.Objects.With(schemaext.Object{Ref: candidate.currentRef, Value: declared})
		if err != nil {
			return outcome{}, err
		}
		return outcome{remains: declared, adopted: true}, nil
	case candidate.desired == nil:
		change(request.Identifiers, result, subject, candidate.current, nil)
		return outcome{}, nil
	case candidate.current == nil:
		change(request.Identifiers, result, subject, nil, candidate.desired)
		return outcome{remains: candidate.desired}, nil
	}
	switch agreement, reason := mssqlschema.ComparePolicy(request.Identifiers, candidate.desired, candidate.current); agreement {
	case mssqlschema.Differ:
		change(request.Identifiers, result, subject, candidate.current, candidate.desired)
	case mssqlschema.Undecided:
		undecided(result, subject, reason)
		return outcome{remains: observedAsDeclared(candidate.current)}, nil
	}
	return outcome{remains: candidate.desired}, nil
}

// observedAsDeclared is the policy an observation keeps on the server when
// the plan leaves it alone. A nil or invalid observation keeps nothing.
func observedAsDeclared(observed *mssqlschema.ObservedSecurityPolicy) *mssqlschema.DesiredSecurityPolicy {
	if observed == nil {
		return nil
	}
	declared, err := observed.Desired()
	if err != nil {
		return nil
	}
	return declared
}

func change(semantics identifier.Semantics, result *schemaext.ObjectComparisonResult, subject objectidentity.ID,
	before *mssqlschema.ObservedSecurityPolicy, after *mssqlschema.DesiredSecurityPolicy,
) {
	result.Changes = append(result.Changes, schemaext.ChangeRecord{Subject: subject, Value: &mssqldiff.SecurityPolicy{
		Before: before.Copy(), After: after.Copy(), Access: mssqldiff.Assess(semantics, before, after),
	}})
}

// refuseSharedTables refuses two enabled policies binding one table in the
// state the plan leaves, which SQL Server refuses with Msg 33264 when the
// second is created or enabled.
func refuseSharedTables(policies []kept) error {
	named := make([]mssqlschema.NamedPolicy, len(policies))
	for i, policy := range policies {
		named[i] = mssqlschema.NamedPolicy{Name: mssqlschema.ObjectName{Schema: policy.ref.Schema.Source, Name: policy.ref.Name.Source}, Policy: policy.policy}
	}
	var problems []error
	for _, conflict := range mssqlschema.EnabledTableConflicts(named) {
		problems = append(problems, fmt.Errorf("%w: enabled security policies %s and %s both bind table %s; "+
			"SQL Server enables one policy per table and refuses the other with Msg 33264: disable one of them, "+
			"or move the table's predicates into one policy", ptaherr.ErrInvalidSchemaDiff, conflict.First, conflict.Second, conflict.Table))
	}
	return errors.Join(problems...)
}

func pairPolicies(ctx context.Context, request schemaext.ObjectComparisonRequest) ([]pair, error) {
	pairs := make(map[objectidentity.Key]*pair)
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
			if err := mssqlschema.ValidateSecurityPolicyRef(object.Ref); err != nil {
				return nil, err
			}
			key := rekey(request.Identifiers, object.Ref).Key()
			found := pairs[key]
			if found == nil {
				found = &pair{}
				pairs[key] = found
			}
			if err := capture(found, object, source.direction); err != nil {
				return nil, err
			}
		}
	}
	result := make([]pair, 0, len(pairs))
	for _, found := range pairs {
		result = append(result, *found)
	}
	slices.SortFunc(result, func(a, b pair) int { return schemaext.CompareRefs(a.subject(), b.subject()) })
	return result, nil
}

func capture(found *pair, object schemaext.Object, direction schemaext.Representation) error {
	switch value := object.Value.(type) {
	case *mssqlschema.DesiredSecurityPolicy:
		if direction != schemaext.Desired || found.desired != nil {
			return fmt.Errorf("%w: duplicate or misplaced security policy declaration %s", schemaext.ErrInvalidValue, name(object.Ref))
		}
		if err := mssqlschema.ValidateDesiredSecurityPolicy(value); err != nil {
			return err
		}
		found.desiredRef, found.desired = object.Ref, value
	case *mssqlschema.ObservedSecurityPolicy:
		if direction != schemaext.Observed || found.current != nil {
			return fmt.Errorf("%w: duplicate or misplaced security policy observation %s", schemaext.ErrInvalidValue, name(object.Ref))
		}
		if err := mssqlschema.ValidateObservedSecurityPolicy(value); err != nil {
			return err
		}
		found.currentRef, found.current = object.Ref, value
	default:
		return fmt.Errorf("%w: unexpected security policy operand %T", schemaext.ErrInvalidValue, object.Value)
	}
	return nil
}

// rekey resolves a captured identity under the comparison's identifier rules.
// A defaulted schema takes the target's own default rather than the one the
// source assumed, which joins an unqualified declaration to the policy a read
// reported in the connection's default schema.
func rekey(semantics identifier.Semantics, ref objectidentity.ID) objectidentity.ID {
	return mssqlschema.SecurityPolicyRefWith(semantics, ref.Schema.Authored(), ref.Name.Source)
}

func name(ref objectidentity.ID) string {
	return mssqlschema.ObjectName{Schema: ref.Schema.Source, Name: ref.Name.Source}.String()
}

// desiredCoverage keeps the source's claims and records the observed
// policies it adopted, so a later stage knows they are known rather than
// declared.
func desiredCoverage(request schemaext.ObjectComparisonRequest, adopted []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	kinds := request.Desired.Coverage.KindRecords()
	if len(kinds) == 0 {
		enrolled, err := mssqlschema.Coverage(schemaext.Desired,
			schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the desired source did not describe security policies"}, nil)
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
	knowledge, found := coverage.SubjectKnowledge(mssqlschema.SecurityPolicyKind, ref)
	return found && unknown(knowledge)
}

func unknown(knowledge schemaext.Knowledge) bool {
	return knowledge.State == schemaext.Uninspected || knowledge.State == schemaext.Unrepresentable
}

func undecided(result *schemaext.ObjectComparisonResult, subject objectidentity.ID, reason string) {
	result.Undecided = append(result.Undecided, schemaext.UndecidedChange{Kind: mssqlschema.SecurityPolicyKind, Subject: subject, Reason: reason})
}
