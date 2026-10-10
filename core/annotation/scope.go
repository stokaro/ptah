package annotation

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/internal/targetscope"
)

func (s *Set) claimScopes(index int, extension Extension) error {
	for _, scope := range extension.TargetScopes {
		if strings.TrimSpace(scope.Directive) == "" || (len(scope.Targets) == 0 && !scope.Unscoped) {
			return fmt.Errorf("annotation extension of %s declares a scope without a directive or a target", extension.Owner)
		}
		routes := s.routes[scope.Directive]
		if routes == nil {
			routes = targetscope.New()
			s.routes[scope.Directive] = routes
		}
		conflict, taken := routes.Claim(index, targetscope.Scope{Targets: scope.Targets, Unscoped: scope.Unscoped, Label: scope.Label})
		if !taken {
			continue
		}
		if conflict.Target == "" {
			return fmt.Errorf("%w: unscoped declarations of %q are read by %s and %s", schemaext.ErrDuplicate,
				scope.Directive, s.extensions[conflict.Owner].Owner, extension.Owner)
		}
		return fmt.Errorf("%w: declarations of %q scoped to %s are read by %s and %s", schemaext.ErrDuplicate,
			scope.Directive, conflict.Target, s.extensions[conflict.Owner].Owner, extension.Owner)
	}
	return nil
}

// TargetScopedDirectives returns, sorted, the frontend directives whose
// declarations an owner of the set reads by their target scope.
func (s Set) TargetScopedDirectives() []string {
	return slices.Sorted(maps.Keys(s.routes))
}

// TargetOwner returns the owner that reads a declaration of directive scoped
// to targets, or false when no owner does and the frontend keeps it. A
// declaration naming no target goes to the owner that reads unscoped ones.
// A scope that names targets of an owner beside targets the owner does not
// read is refused, since the owner's object and the others' are different
// objects, and the refusal says how to split the declaration.
func (s Set) TargetOwner(directive string, targets []string) (string, bool, error) {
	index, found, err := s.scopeIndex(directive, targets)
	if err != nil || !found {
		return "", false, err
	}
	return s.extensions[index].Owner, true, nil
}

func (s Set) scopeIndex(directive string, targets []string) (int, bool, error) {
	return s.routes[directive].Owner(targets)
}

// FileCoverage is implemented by a [FileDecoder] whose declarations narrow
// the owner's coverage claim for their file, such as PostgreSQL row-level
// security, which leaves the switches of a table whose policies the file
// declares to the owner's default.
type FileCoverage interface {
	// Cover returns claim, the coverage of the whole file, narrowed by what
	// the file's declarations of the owner declare. It is called after
	// Finish.
	Cover(claim schemaext.Coverage) (schemaext.Coverage, error)
}

// Cover hands claim to each file decoder of the file that narrows it, in the
// order the owners first appeared in the file, and returns the result.
func (r *Reader) Cover(claim schemaext.Coverage) (schemaext.Coverage, error) {
	for _, index := range r.order {
		narrowing, ok := r.files[index].(FileCoverage)
		if !ok {
			continue
		}
		var err error
		if claim, err = narrowing.Cover(claim); err != nil {
			return schemaext.Coverage{}, fmt.Errorf("coverage of %s: %w", r.set.extensions[index].Owner, err)
		}
	}
	return claim, nil
}
