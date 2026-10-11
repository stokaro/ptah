package engine

import (
	"fmt"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemavalidation"
)

func (r *Runtime) registerOffTarget(owner string, policy OffTargetPolicy) error {
	if !policy.Action.Valid() {
		return fmt.Errorf("%w: provider %q states an unknown off-target answer %q", ErrInvalidRegistration, owner, policy.Action)
	}
	if strings.TrimSpace(policy.Label) == "" {
		return fmt.Errorf("%w: provider %q states an off-target answer without a label", ErrInvalidRegistration, owner)
	}
	if len(policy.Kinds) == 0 {
		return fmt.Errorf("%w: provider %q states an off-target answer for no model", ErrInvalidRegistration, owner)
	}
	for _, kind := range policy.Kinds {
		if !kind.Valid() || !r.ownsCodec(owner, kind, schemaext.Desired) {
			return fmt.Errorf("%w: provider %q states an off-target answer for model %q it does not own", ErrInvalidRegistration, owner, kind)
		}
		if _, found := r.offTarget[kind]; found {
			return fmt.Errorf("%w: model %q has two off-target answers", ErrInvalidRegistration, kind)
		}
		r.offTarget[kind] = policy
	}
	return nil
}

// serves reports whether target compares or plans declarations of kind.
func (r *Runtime) serves(target string, kind schemaext.Kind) bool {
	key := conversionKey{target: target, kind: kind}
	_, compares := r.comparisons[key]
	_, declares := r.declarations[key]
	return compares || declares
}

// hosts lists, sorted, the targets that serve kind.
func (r *Runtime) hosts(kind schemaext.Kind) []string {
	var hosts []string
	for _, name := range r.Targets() {
		if r.serves(name, kind) {
			hosts = append(hosts, name)
		}
	}
	return hosts
}

// ScopeOffTarget returns objects without the ones target leaves out because
// it does not serve their model and their owner answers
// [schemaext.OffTargetSkip], and the ones it left out, in identity order. An
// object whose owner answers [schemaext.OffTargetRefuse], or one a target
// that refuses skips would skip, is refused, naming the targets that host its
// model. An object whose model the target serves, or whose owner states
// nothing, stays. objects is not changed.
func (r *Runtime) ScopeOffTarget(target string, objects schemaext.Objects) (schemaext.Objects, []schemaext.SkippedObject, error) {
	scope, err := r.scopeOffTarget(target, objects)
	if err != nil {
		return schemaext.Objects{}, nil, err
	}
	if scope.refusal != nil {
		return schemaext.Objects{}, nil, fmt.Errorf("%w: %s", ptaherr.ErrUnsupportedFeature, scope.refusal.Message)
	}
	return scope.objects, scope.skipped, nil
}

// offTargetScope is what a target keeps of a collection: the objects, the
// ones it skipped, or the completed refusal of one it refuses.
type offTargetScope struct {
	objects schemaext.Objects
	skipped []schemaext.SkippedObject
	refusal *schemavalidation.Diagnostic
}

// scopeOffTarget is [Runtime.ScopeOffTarget] with the refusal as the
// completed diagnostic a validation or a render reports. The error is a
// target the runtime does not know.
func (r *Runtime) scopeOffTarget(target string, objects schemaext.Objects) (offTargetScope, error) {
	selected, found := r.lookup(target)
	if !found {
		return offTargetScope{}, fmt.Errorf("%w: %q", ptaherr.ErrUnsupportedDialect, target)
	}
	var skipped []schemaext.SkippedObject
	left := make(map[objectidentity.Key]bool)
	for _, ref := range objects.Refs() {
		kind := schemaext.Kind(ref.Kind)
		policy, stated := r.offTarget[kind]
		if !stated || r.serves(selected.name, kind) {
			continue
		}
		if policy.Action == schemaext.OffTargetRefuse || selected.refusesSkips {
			name := schemaext.DisplayName(ref)
			return offTargetScope{refusal: &schemavalidation.Diagnostic{Code: schemavalidation.UnsupportedFeature, Kind: string(kind),
				Object: name, Feature: policy.Label, Message: fmt.Sprintf("%s %q cannot be planned on %s; scope its declaration to the "+
					`targets that host it, as dialects=%q does in a Go annotation`, policy.Label, name, selected.name,
					strings.Join(r.hosts(kind), ","))}}, nil
		}
		skipped = append(skipped, schemaext.SkippedObject{Subject: ref, Label: policy.Label, Target: selected.name})
		left[ref.Key()] = true
	}
	if len(skipped) == 0 {
		return offTargetScope{objects: objects}, nil
	}
	return offTargetScope{objects: objects.Select(func(ref objectidentity.ID) bool { return !left[ref.Key()] }), skipped: skipped}, nil
}
