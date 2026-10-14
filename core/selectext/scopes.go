package selectext

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

// ScopeSubject identifies one feature object or coverage-only scope. Kind is
// the value model, which can differ from the identity kind of a coverage scope.
type ScopeSubject struct {
	Kind    schemaext.Kind
	Subject objectidentity.ID
}

// ScopeRequest supplies a captured batch in one representation. Objects and
// Coverage are independent snapshots. A service performs no external I/O.
type ScopeRequest struct {
	Objects  schemaext.Objects
	Coverage schemaext.Coverage
}

// SubjectScope describes the explicit schema and named roles a subject refers
// to. An empty Schema means database-wide state, never the connection default.
// Roles contains decoded names, without SQL quoting or a pseudo-role such as
// PUBLIC. Excluding a named role excludes an object that references it; keeping
// an object retains its named roles when the source declares them.
type SubjectScope struct {
	ScopeSubject
	Schema string
	Roles  []string
}

// ScopeResult describes every requested subject exactly once. Complete confirms
// that the service finishes the batch; an empty zero result is not success.
type ScopeResult struct {
	Complete bool
	Subjects []SubjectScope
}

// ScopeService describes schema-wide objects and knowledge scopes as one pure
// contextual batch. In-process and executable providers share this data contract.
// Failure or cancellation returns no partial result.
type ScopeService interface {
	DescribeScopes(context.Context, ScopeRequest) (ScopeResult, error)
}

type scopeKey struct {
	kind schemaext.Kind
	ref  objectidentity.Key
}

// Subjects returns the complete ordered set the service must describe. A held
// object's source identity takes precedence over a coverage alias of the same
// comparison identity; coverage-only scopes retain their own identity.
func (r ScopeRequest) Subjects() []ScopeSubject {
	seen := make(map[scopeKey]bool)
	var subjects []ScopeSubject
	for _, ref := range r.Objects.Refs() {
		subject := ScopeSubject{Kind: schemaext.Kind(ref.Kind), Subject: ref}
		seen[subject.key()] = true
		subjects = append(subjects, subject)
	}
	for _, record := range r.Coverage.SubjectRecords() {
		subject := ScopeSubject{Kind: record.Kind, Subject: record.Subject}
		if !seen[subject.key()] {
			subjects = append(subjects, subject)
			seen[subject.key()] = true
		}
	}
	slices.SortFunc(subjects, compareScopeSubjects)
	return subjects
}

func (s ScopeSubject) key() scopeKey { return scopeKey{kind: s.Kind, ref: s.Subject.Key()} }

func compareScopeSubjects(a, b ScopeSubject) int {
	if order := strings.Compare(string(a.Kind), string(b.Kind)); order != 0 {
		return order
	}
	return schemaext.CompareRefs(a.Subject, b.Subject)
}

// DescribeScopes snapshots and routes schema-wide subjects to their owners.
// Each owner receives only its models, including unknown scopes without values.
// Replies must account for the entire batch with the exact supplied identities.
// The result is sorted and independent of service-owned slices.
func (s Set) DescribeScopes(ctx context.Context, request ScopeRequest) (ScopeResult, error) {
	if !s.selected {
		return ScopeResult{}, ErrUnselected
	}
	if ctx == nil {
		return ScopeResult{}, fmt.Errorf("%w: scope description requires a context", schemaext.ErrInvalidValue)
	}
	if err := ctx.Err(); err != nil {
		return ScopeResult{}, err
	}
	combined := ScopeResult{Complete: true}
	for _, extension := range s.extensions {
		kinds := extension.schemaWideKinds()
		if len(kinds) == 0 {
			continue
		}
		owned, err := request.selectKinds(kinds)
		if err != nil {
			return ScopeResult{}, err
		}
		if len(owned.Subjects()) == 0 {
			continue
		}
		result, err := extension.Scopes.DescribeScopes(ctx, owned)
		if err != nil {
			return ScopeResult{}, err
		}
		if err := ctx.Err(); err != nil {
			return ScopeResult{}, err
		}
		subjects, err := checkedScopes(owned.Subjects(), result)
		if err != nil {
			return ScopeResult{}, err
		}
		combined.Subjects = append(combined.Subjects, subjects...)
	}
	slices.SortFunc(combined.Subjects, func(a, b SubjectScope) int { return compareScopeSubjects(a.ScopeSubject, b.ScopeSubject) })
	if err := ctx.Err(); err != nil {
		return ScopeResult{}, err
	}
	return combined, nil
}

func (e Extension) schemaWideKinds() []schemaext.Kind {
	var kinds []schemaext.Kind
	for _, selection := range e.Selections {
		if selection.Naming == SchemaWide {
			kinds = append(kinds, selection.Kind)
		}
	}
	return kinds
}

func (r ScopeRequest) selectKinds(kinds []schemaext.Kind) (ScopeRequest, error) {
	objects, err := r.Objects.Select(func(ref objectidentity.ID) bool { return slices.Contains(kinds, schemaext.Kind(ref.Kind)) }).All()
	if err != nil {
		return ScopeRequest{}, err
	}
	snapshot, err := schemaext.NewObjects(objects...)
	if err != nil {
		return ScopeRequest{}, err
	}
	return ScopeRequest{Objects: snapshot, Coverage: r.Coverage.SelectKinds(kinds)}, nil
}

func checkedScopes(expected []ScopeSubject, result ScopeResult) ([]SubjectScope, error) {
	if !result.Complete || len(result.Subjects) != len(expected) {
		return nil, fmt.Errorf("%w: a scope description must account for every subject", schemaext.ErrInvalidValue)
	}
	pending := make(map[scopeKey]ScopeSubject, len(expected))
	for _, subject := range expected {
		pending[subject.key()] = subject
	}
	output := make([]SubjectScope, 0, len(expected))
	for _, scope := range result.Subjects {
		expected, found := pending[scope.key()]
		if !found || expected != scope.ScopeSubject {
			return nil, fmt.Errorf("%w: scope description returned an unrequested, changed or duplicate subject", schemaext.ErrInvalidValue)
		}
		delete(pending, scope.key())
		if err := schemaext.ValidText("scope schema", scope.Schema); err != nil {
			return nil, err
		}
		scope.Roles = slices.Clone(scope.Roles)
		slices.Sort(scope.Roles)
		for i, role := range scope.Roles {
			if role == "" || (i > 0 && scope.Roles[i-1] == role) {
				return nil, fmt.Errorf("%w: scope description has an empty or duplicate role", schemaext.ErrInvalidValue)
			}
			if err := schemaext.ValidText("scope role", role); err != nil {
				return nil, err
			}
		}
		output = append(output, scope)
	}
	return output, nil
}
