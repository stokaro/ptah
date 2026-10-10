// Package targetscope routes a declaration of a source frontend's own grammar
// to the feature owner its target scope names: one owner per target, at most
// one owner for the declarations that name no target, and a scope that names
// one owner's targets beside others refused. The Go annotation contract,
// ptah.run/core/annotation, and the YAML one, ptah.run/core/yamlext, both
// route through it, so a declaration routes alike from either source.
package targetscope

import (
	"fmt"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
)

// Routes records which owner reads the declarations of one directive or key
// scoped to each target. Owners are numbered by the contract that holds them.
// The zero value is not ready; use [New].
type Routes struct {
	targets  map[string]int
	unscoped int
	labels   map[int]string
}

// New returns routes no owner has claimed.
func New() *Routes {
	return &Routes{targets: make(map[string]int), unscoped: -1, labels: make(map[int]string)}
}

// Conflict is a claim another owner holds already: the owner, and the target
// it reads, which is empty for the declarations that name none.
type Conflict struct {
	Owner  int
	Target string
}

// Scope is what one owner claims: the targets whose declarations it reads,
// compared as [platform.NormalizeDialect] names them, whether it reads the
// declarations that name no target, and the label that names its targets in
// a refusal.
type Scope struct {
	Targets  []string
	Unscoped bool
	Label    string
}

// Claim records that owner reads what scope names. It returns the first
// claim another owner holds already, and records nothing then.
func (r *Routes) Claim(owner int, scope Scope) (Conflict, bool) {
	for _, target := range scope.Targets {
		if previous, taken := r.targets[platform.NormalizeDialect(target)]; taken && previous != owner {
			return Conflict{Owner: previous, Target: platform.NormalizeDialect(target)}, true
		}
	}
	if scope.Unscoped && r.unscoped >= 0 && r.unscoped != owner {
		return Conflict{Owner: r.unscoped}, true
	}
	for _, target := range scope.Targets {
		r.targets[platform.NormalizeDialect(target)] = owner
	}
	if scope.Unscoped {
		r.unscoped = owner
	}
	if scope.Label != "" {
		r.labels[owner] = scope.Label
	}
	return Conflict{}, false
}

// Owner returns the owner that reads a declaration scoped to targets, or
// false when none does and the frontend keeps it. A declaration naming no
// target goes to the owner that reads unscoped ones. A scope that names one
// owner's targets beside targets that owner does not read is refused, since
// the owner's object and the others' are different objects, and the refusal
// says how to split the declaration.
func (r *Routes) Owner(targets []string) (int, bool, error) {
	if r == nil {
		return 0, false, nil
	}
	if len(targets) == 0 {
		return r.unscoped, r.unscoped >= 0, nil
	}
	owner := -1
	var owned, others []string
	for _, target := range targets {
		if index, taken := r.targets[platform.NormalizeDialect(target)]; taken && (owner < 0 || owner == index) {
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
		label := r.labels[owner]
		if label == "" {
			label = "one owner's targets"
		}
		return 0, false, fmt.Errorf("%w: a declaration scoped to %s mixes %s with others; they declare different objects, "+
			"so declare one scoped to %s and another scoped to %s", ptaherr.ErrInvalidAttributeValue, strings.Join(targets, ","),
			label, strings.Join(owned, ","), strings.Join(others, ","))
	}
	return owner, true, nil
}
