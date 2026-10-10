package yamlext

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/internal/targetscope"
)

// TargetScope hands an owner the entries of one of the frontend's own keys
// whose target scope makes them the owner's, such as a row-level security
// policy scoped to PostgreSQL: the owner reads them in place of the
// frontend.
type TargetScope struct {
	// Key names the frontend's key, such as "rls_policies".
	Key string
	// Targets are the dialects whose entries the owner reads. A target
	// belongs to one owner for a key.
	Targets []string
	// Unscoped hands the owner the entries that name no target too. One owner
	// of a key may set it.
	Unscoped bool
	// Label names Targets in a refusal, such as "PostgreSQL-family targets".
	Label string
}

// Entry is one entry of a frontend key that an owner reads by its target
// scope.
type Entry struct {
	// Key is the frontend key the entry is routed by, such as
	// "rls_policies".
	Key string
	// Origin names the entry in a refusal, such as "rls_policies.docs_read".
	Origin string
	// Attributes are the entry's fields as the frontend read them, keyed by
	// the name the Go annotation of the same declaration gives them, with the
	// scope left out. A field the entry leaves out is absent.
	Attributes map[string]string
	// Targets are the targets the entry's dialects scope it to. Empty means
	// it names none.
	Targets []string
}

func (s *Set) claimScopes(index int, extension Extension) error {
	if len(extension.TargetScopes) > 0 && extension.Entries == nil {
		return fmt.Errorf("YAML extension of %s reads scoped entries and declares no reader", extension.Owner)
	}
	for _, scope := range extension.TargetScopes {
		if strings.TrimSpace(scope.Key) == "" || (len(scope.Targets) == 0 && !scope.Unscoped) {
			return fmt.Errorf("YAML extension of %s declares a scope without a key or a target", extension.Owner)
		}
		routes := s.routes[scope.Key]
		if routes == nil {
			routes = targetscope.New()
			s.routes[scope.Key] = routes
		}
		conflict, taken := routes.Claim(index, targetscope.Scope{Targets: scope.Targets, Unscoped: scope.Unscoped, Label: scope.Label})
		if !taken {
			continue
		}
		if conflict.Target == "" {
			return fmt.Errorf("%w: unscoped entries of YAML key %q are read by %s and %s", schemaext.ErrDuplicate,
				scope.Key, s.extensions[conflict.Owner].Owner, extension.Owner)
		}
		return fmt.Errorf("%w: entries of YAML key %q scoped to %s are read by %s and %s", schemaext.ErrDuplicate,
			scope.Key, conflict.Target, s.extensions[conflict.Owner].Owner, extension.Owner)
	}
	return nil
}

// TargetScopedKeys returns, sorted, the frontend keys whose entries an owner
// of the set reads by their target scope.
func (s Set) TargetScopedKeys() []string {
	return slices.Sorted(maps.Keys(s.routes))
}

// TargetOwner returns the owner that reads an entry of key scoped to
// targets, or false when no owner does and the frontend keeps it. An entry
// naming no target goes to the owner that reads unscoped ones. A scope that
// names an owner's targets beside targets the owner does not read is refused,
// as it is for a Go annotation.
func (s Set) TargetOwner(key string, targets []string) (string, bool, error) {
	index, found, err := s.routes[key].Owner(targets)
	if err != nil || !found {
		return "", false, err
	}
	return s.extensions[index].Owner, true, nil
}

// ReadEntries hands each owner the entries routed to it, in their order, and
// joins what the owners declare. tables are the tables the document declares.
// An entry no owner reads is an error, and so is a contribution
// [Set.Decode] would refuse.
func (s Set) ReadEntries(entries []Entry, tables Tables) ([]Contribution, error) {
	routed := make(map[int][]Entry)
	var order []int
	for _, entry := range entries {
		index, found, err := s.routes[entry.Key].Owner(entry.Targets)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", entry.Origin, err)
		}
		if !found {
			return nil, fmt.Errorf("%s: no selected owner reads it", entry.Origin)
		}
		if _, seen := routed[index]; !seen {
			order = append(order, index)
		}
		entry.Attributes = maps.Clone(entry.Attributes)
		entry.Targets = slices.Clone(entry.Targets)
		routed[index] = append(routed[index], entry)
	}
	var joined []Contribution
	for _, index := range order {
		extension := s.extensions[index]
		contributions, err := extension.Entries(routed[index], slices.Clone(tables))
		if err != nil {
			return nil, err
		}
		if err := checkContributions(extension, "scoped entries", contributions, len(tables)); err != nil {
			return nil, err
		}
		joined = append(joined, contributions...)
	}
	return joined, nil
}

// Cover hands claim to each owner that narrows it by what the document
// declares, with the document's objects of the owner's models, and returns
// the result.
func (s Set) Cover(claim schemaext.Coverage, objects schemaext.Objects) (schemaext.Coverage, error) {
	for _, extension := range s.extensions {
		if extension.Cover == nil {
			continue
		}
		owned := objects.Select(func(ref objectidentity.ID) bool {
			return slices.Contains(extension.Kinds, schemaext.Kind(ref.Kind))
		})
		var err error
		if claim, err = extension.Cover(claim, owned); err != nil {
			return schemaext.Coverage{}, fmt.Errorf("coverage of %s: %w", extension.Owner, err)
		}
	}
	return claim, nil
}
