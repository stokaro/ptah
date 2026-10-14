package atlasfilter_test

import (
	"context"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/selectext"
	"ptah.run/internal/atlasfilter"
)

// ambientScopes uses a declared placement, independent of the object's name.
// The filter must ask the service instead of inferring scope from an identity.
type ambientScopes struct {
	calls *[]selectext.ScopeRequest
	err   error
}

func (a ambientScopes) DescribeScopes(_ context.Context, request selectext.ScopeRequest) (selectext.ScopeResult, error) {
	if a.calls != nil {
		*a.calls = append(*a.calls, request)
	}
	if a.err != nil {
		return selectext.ScopeResult{}, a.err
	}
	result := selectext.ScopeResult{Complete: true}
	for _, subject := range request.Subjects() {
		result.Subjects = append(result.Subjects, selectext.SubjectScope{ScopeSubject: subject, Schema: "public", Roles: []string{"writer", "reader"}})
	}
	return result, nil
}

func ambientSelections(service ambientScopes) selectext.Set {
	return must.Must(selectext.NewSet(selectext.Extension{Owner: "example.org/custom", Scopes: service,
		Selections: []selectext.Selection{{Kind: gadgetKind, Naming: selectext.SchemaWide}},
	}))
}

func ambientObjects() schemaext.Objects {
	return must.Must(schemaext.NewObjects(schemaext.Object{
		Ref:   objectidentity.ID{Kind: objectidentity.Kind(gadgetKind), Name: objectidentity.Part{Source: "ambient", Normalized: "ambient"}},
		Value: &gadget{Label: "opaque"},
	}))
}

func TestScopeUsesSchemaWideOwnerDescriptions(t *testing.T) {
	c := qt.New(t)
	var calls []selectext.ScopeRequest
	selections := ambientSelections(ambientScopes{calls: &calls})
	objects := ambientObjects()
	scope := atlasfilter.Scope{Selections: selections, Schemas: []string{"public"}, Include: []string{"users"}, DefaultSchema: "public"}
	database, err := atlasfilter.ScopeDatabase(&catalog.Database{FeatureObjects: objects,
		Tables: []catalog.Table{{Name: "users"}}, Roles: []catalog.Role{{Name: "reader"}, {Name: "writer"}, {Name: "other"}},
	}, scope)
	c.Assert(err, qt.IsNil)
	generated, err := atlasfilter.ScopeGenerated(&schemamodel.Database{FeatureObjects: objects,
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}}, Roles: []schemamodel.Role{{Name: "reader"}, {Name: "writer"}, {Name: "other"}},
	}, scope)
	c.Assert(err, qt.IsNil)
	c.Assert(database.FeatureObjects.Refs(), qt.DeepEquals, objects.Refs())
	c.Assert(generated.FeatureObjects.Refs(), qt.DeepEquals, objects.Refs())
	c.Assert(databaseRoleNames(database.Roles), qt.DeepEquals, []string{"reader", "writer"})
	c.Assert(generatedRoleNames(generated.Roles), qt.DeepEquals, []string{"reader", "writer"})
	c.Assert(calls, qt.HasLen, 2)
}

func TestExcludeUsesSchemaWideRolesWithoutDeclaredRoles(t *testing.T) {
	c := qt.New(t)
	var calls []selectext.ScopeRequest
	selections := ambientSelections(ambientScopes{calls: &calls})
	objects := ambientObjects()
	patterns := []string{"reader[type=role]", "writer[type=role]"}
	database, reportDatabase, err := atlasfilter.ExcludeDatabaseReport(selections, &catalog.Database{FeatureObjects: objects}, patterns, "public")
	c.Assert(err, qt.IsNil)
	generated, reportGenerated, err := atlasfilter.ExcludeGeneratedReport(selections, &schemamodel.Database{FeatureObjects: objects}, patterns, "public")
	c.Assert(err, qt.IsNil)
	c.Assert(database.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(generated.FeatureObjects.Len(), qt.Equals, 0)
	c.Assert(reportDatabase.Unmatched, qt.HasLen, 0)
	c.Assert(reportGenerated.Unmatched, qt.HasLen, 0)
	c.Assert(calls, qt.HasLen, 2)
}

func TestSchemaWideOwnerFailureReturnsNoProjection(t *testing.T) {
	c := qt.New(t)
	refused := errors.New("cannot describe captured state")
	selections := ambientSelections(ambientScopes{err: refused})
	scope := atlasfilter.Scope{Selections: selections, Schemas: []string{"public"}}
	observed, desired := &catalog.Database{FeatureObjects: ambientObjects()}, &schemamodel.Database{FeatureObjects: ambientObjects()}
	database, err := atlasfilter.ScopeDatabase(observed, scope)
	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(database, qt.IsNil)
	generated, err := atlasfilter.ScopeGenerated(desired, scope)
	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(generated, qt.IsNil)
	database, err = atlasfilter.ExcludeDatabase(selections, observed, []string{"reader[type=role]"})
	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(database, qt.IsNil)
	generated, err = atlasfilter.ExcludeGenerated(selections, desired, []string{"reader[type=role]"})
	c.Assert(err, qt.ErrorIs, refused)
	c.Assert(generated, qt.IsNil)
}
