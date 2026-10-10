package annotation

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
)

func (s *Set) claimScopes(index int, extension Extension) error {
	for _, scope := range extension.TargetScopes {
		if strings.TrimSpace(scope.Directive) == "" || (len(scope.Targets) == 0 && !scope.Unscoped) {
			return fmt.Errorf("annotation extension of %s declares a scope without a directive or a target", extension.Owner)
		}
		claimed := s.scopes[scope.Directive]
		if claimed == nil {
			claimed = make(map[string]int)
			s.scopes[scope.Directive] = claimed
		}
		for _, target := range scope.Targets {
			target = platform.NormalizeDialect(target)
			if previous, taken := claimed[target]; taken {
				return fmt.Errorf("%w: declarations of %q scoped to %s are read by %s and %s", schemaext.ErrDuplicate,
					scope.Directive, target, s.extensionOwner(previous, extension), extension.Owner)
			}
			claimed[target] = index
		}
		if !scope.Unscoped {
			continue
		}
		if previous, taken := s.unscoped[scope.Directive]; taken {
			return fmt.Errorf("%w: unscoped declarations of %q are read by %s and %s", schemaext.ErrDuplicate,
				scope.Directive, s.extensionOwner(previous, extension), extension.Owner)
		}
		s.unscoped[scope.Directive] = index
	}
	return nil
}

// extensionOwner names the owner of the extension at index, which may be the
// one being claimed and not yet frozen.
func (s *Set) extensionOwner(index int, claiming Extension) string {
	if index < len(s.extensions) {
		return s.extensions[index].Owner
	}
	return claiming.Owner
}

// TargetScopedDirectives returns, sorted, the frontend directives whose
// declarations an owner of the set reads by their target scope.
func (s Set) TargetScopedDirectives() []string {
	directives := make(map[string]bool)
	for directive := range s.scopes {
		directives[directive] = true
	}
	for directive := range s.unscoped {
		directives[directive] = true
	}
	return slices.Sorted(maps.Keys(directives))
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
	if len(targets) == 0 {
		index, found := s.unscoped[directive]
		return index, found, nil
	}
	claimed := s.scopes[directive]
	owner := -1
	var owned, others []string
	for _, target := range targets {
		if index, taken := claimed[platform.NormalizeDialect(target)]; taken && (owner < 0 || owner == index) {
			owner = index
			owned = append(owned, target)
			continue
		}
		others = append(others, target)
	}
	if owner < 0 {
		return 0, false, nil
	}
	if len(others) > 0 {
		return 0, false, fmt.Errorf("%w: a declaration scoped to %s mixes %s with others; they declare different objects, "+
			"so declare one scoped to %s and another scoped to %s", ptaherr.ErrInvalidAttributeValue, strings.Join(targets, ","),
			s.scopeLabel(owner, directive), strings.Join(owned, ","), strings.Join(others, ","))
	}
	return owner, true, nil
}

// scopeLabel names the targets the owner at index reads for directive.
func (s Set) scopeLabel(index int, directive string) string {
	for _, scope := range s.extensions[index].TargetScopes {
		if scope.Directive == directive && scope.Label != "" {
			return scope.Label
		}
	}
	return "the targets of " + s.extensions[index].Owner
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
