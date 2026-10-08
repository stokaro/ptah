package schemaext

import (
	"fmt"
	"slices"
	"strings"
)

// TargetSelection is one resolved target's immutable naming metadata. It makes
// no capability claim and owns no service registry. The zero value is invalid.
type TargetSelection struct {
	name  string
	names []string
}

// NewTargetSelection captures a canonical name and its accepted aliases. Names
// use lowercase ASCII letters, digits, '-', '_' and '+', beginning with a letter.
// Duplicate names are refused. The returned value does not alias the input.
func NewTargetSelection(name string, aliases ...string) (TargetSelection, error) {
	names := append([]string{name}, aliases...)
	seen := make(map[string]bool, len(names))
	for _, spelling := range names {
		if !validTargetName(spelling) {
			return TargetSelection{}, fmt.Errorf("%w: invalid target name %q", ErrInvalidValue, spelling)
		}
		if seen[spelling] {
			return TargetSelection{}, fmt.Errorf("%w: duplicate target name %q", ErrInvalidValue, spelling)
		}
		seen[spelling] = true
	}
	slices.Sort(names)
	return TargetSelection{name: name, names: names}, nil
}

// Name returns the selected canonical target name.
func (s TargetSelection) Name() string { return s.name }

// Names returns an independent sorted list of accepted names, including Name.
func (s TargetSelection) Names() []string { return slices.Clone(s.names) }

// Validate rejects an unresolved selection before it can filter a declaration.
func (s TargetSelection) Validate() error {
	if s.name == "" {
		return fmt.Errorf("%w: target selection is unresolved", ErrInvalidValue)
	}
	return nil
}

// Includes reports whether this selection belongs to a declaration's scope.
// An empty scope includes every resolved target. Matching ignores surrounding
// whitespace and ASCII case, using only the names explicitly selected here.
// An unresolved selection matches no scope, including an empty one.
func (s TargetSelection) Includes(scope []string) bool {
	if s.name == "" {
		return false
	}
	return len(scope) == 0 || slices.ContainsFunc(scope, func(name string) bool {
		spelling, err := NormalizeTargetSpelling(name)
		return err == nil && slices.Contains(s.names, spelling)
	})
}

// NormalizeTargetSpelling trims surrounding whitespace and folds ASCII case.
// It checks target-name syntax but does not resolve aliases or grant selection.
func NormalizeTargetSpelling(name string) (string, error) {
	spelling := []byte(strings.TrimSpace(name))
	for index, char := range spelling {
		if char >= 'A' && char <= 'Z' {
			spelling[index] = char + ('a' - 'A')
		}
	}
	normalized := string(spelling)
	if !validTargetName(normalized) {
		return "", fmt.Errorf("%w: invalid target spelling %q", ErrInvalidValue, name)
	}
	return normalized, nil
}

// TargetResolver supplies naming metadata from an explicitly selected registry.
// An unregistered target is an error, never an identity projection or a fallback.
type TargetResolver interface {
	ResolveTarget(string) (TargetSelection, error)
}

func validTargetName(name string) bool {
	if name == "" || name[0] < 'a' || name[0] > 'z' {
		return false
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '-' && char != '_' && char != '+' {
			return false
		}
	}
	return true
}
