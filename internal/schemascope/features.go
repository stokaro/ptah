package schemascope

import (
	"context"
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/selectext"
)

func validateFilter(ctx context.Context, selections selectext.Set) error {
	if !selections.Selected() {
		return selectext.ErrUnselected
	}
	if ctx == nil {
		return fmt.Errorf("%w: schema filtering requires a context", schemaext.ErrInvalidValue)
	}
	return ctx.Err()
}

// filterFeatureScopes retains table children with their table and schema-wide
// objects with their explicit schema. Database-wide support objects remain
// available. Coverage without a held value stays diagnostic, including unknown
// classes whose identity kind differs from the owner's value model.
func filterFeatureScopes(ctx context.Context, selections selectext.Set, objects schemaext.Objects, coverage schemaext.Coverage,
	schemas []string, keepChild func(objectidentity.ID) bool,
) (schemaext.Objects, schemaext.Coverage, error) {
	// Owner placements and this selection carry decoded identifiers. Reusing
	// the CLI comma-list parser here changes the objects the caller selected.
	allowed := make(map[string]struct{}, len(schemas))
	for _, schema := range schemas {
		allowed[schema] = struct{}{}
	}
	scopes, err := selections.DescribeScopes(ctx, selectext.ScopeRequest{Objects: objects, Coverage: coverage})
	if err != nil {
		return schemaext.Objects{}, schemaext.Coverage{}, err
	}
	held := make(map[objectidentity.Key]bool)
	dropped := make(map[objectidentity.Key]bool)
	for _, ref := range objects.Refs() {
		held[ref.Key()] = true
		if !keepChild(ref) {
			dropped[ref.Key()] = true
		}
	}
	for _, scope := range scopes.Subjects {
		if scope.Schema == "" || !held[scope.Subject.Key()] {
			continue
		}
		if _, included := allowed[scope.Schema]; !included {
			dropped[scope.Subject.Key()] = true
		}
	}
	keep := func(ref objectidentity.ID) bool { return !dropped[ref.Key()] }
	return objects.Select(keep), coverage.SelectSubjects(keep), nil
}
