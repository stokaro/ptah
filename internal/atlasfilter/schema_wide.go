package atlasfilter

import (
	"slices"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

// excludeSchemaWideFeatures applies exclusions to captured objects. It asks
// every role selector even when another role or the schema removes the object,
// so a matched selector is not reported as unmatched. Coverage without a value
// stays diagnostic: an exclusion cannot claim the source read an unknown ACL.
func (s *exclusionState) excludeSchemaWideFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (schemaext.Objects, schemaext.Coverage, error) {
	scopes, err := s.vocabulary.selections.DescribeScopes(ownerContext(), selectext.ScopeRequest{Objects: objects, Coverage: coverage})
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	held := featureObjectKeys(objects)
	dropped := make(map[objectidentity.Key]bool)
	for _, scope := range scopes.Subjects {
		excluded := false
		for _, role := range scope.Roles {
			if s.matches("role", role) {
				excluded = true
			}
		}
		if _, excludedSchema := s.excludedSchemas[scope.Schema]; scope.Schema != "" && excludedSchema {
			excluded = true
		}
		if held[scope.Subject.Key()] && excluded {
			dropped[scope.Subject.Key()] = true
		}
	}
	keep := func(ref objectidentity.ID) bool { return !dropped[ref.Key()] }
	return objects.Select(keep), coverage.SelectSubjects(keep), nil
}

// selectSchemaWideFeatures keeps schema-wide state under the schema universe.
// Include selectors retain it only when --schema names its schema explicitly.
// Only a held, kept value retains named roles; unknown coverage does not invent
// declarations or grant authority to remove objects belonging to those roles.
func (s *scopeSelection) selectSchemaWideFeatures(objects schemaext.Objects, coverage schemaext.Coverage) (featureSelection, error) {
	scopes, err := s.vocabulary.selections.DescribeScopes(ownerContext(), selectext.ScopeRequest{Objects: objects, Coverage: coverage})
	if err != nil {
		return featureSelection{}, err
	}
	held := featureObjectKeys(objects)
	dropped := make(map[objectidentity.Key]bool)
	var roles, schemas []string
	for _, scope := range scopes.Subjects {
		if !held[scope.Subject.Key()] {
			continue
		}
		if !s.schemaWideSelected(scope.Schema) {
			dropped[scope.Subject.Key()] = true
			continue
		}
		roles = append(roles, scope.Roles...)
		if scope.Schema != "" {
			schemas = append(schemas, scope.Schema)
		}
	}
	slices.Sort(roles)
	slices.Sort(schemas)
	keep := func(ref objectidentity.ID) bool { return !dropped[ref.Key()] }
	return featureSelection{objects: objects.Select(keep), coverage: coverage.SelectSubjects(keep), roles: slices.Compact(roles), schemas: slices.Compact(schemas)}, nil
}

func featureObjectKeys(objects schemaext.Objects) map[objectidentity.Key]bool {
	held := make(map[objectidentity.Key]bool)
	for _, ref := range objects.Refs() {
		held[ref.Key()] = true
	}
	return held
}

func (s *scopeSelection) schemaWideSelected(schema string) bool {
	if schema == "" {
		return len(s.selectors) == 0
	}
	if len(s.allowed) > 0 {
		if _, allowed := s.allowed[schema]; !allowed {
			return false
		}
	}
	if len(s.selectors) == 0 {
		return true
	}
	_, named := s.allowed[schema]
	return named
}

// featureSelection carries one comparison side and the roles and schemas its
// kept objects require. It does not share mutable selection state with the other side.
type featureSelection struct {
	objects  schemaext.Objects
	coverage schemaext.Coverage
	roles    []string
	schemas  []string
}
