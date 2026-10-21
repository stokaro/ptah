package engine_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
)

func TestRelationsSeparateDatabaseAndNamespaceParents(t *testing.T) {
	c := qt.New(t)
	first := relationObject(conversionFirst, "policy")
	first.Subject.Subject.Database = objectidentity.Part{Source: "tenant", Normalized: "tenant"}
	first.Subject.Subject.Namespace = objectidentity.NewParts(objectidentity.Part{Source: "a/b", Normalized: "a/b"})
	first.Subject.Subject.Parent = objectidentity.Part{Source: "orders", Normalized: "orders"}
	second, third := first, first
	second.Subject.Subject.Namespace = objectidentity.NewParts(objectidentity.Part{Source: "a", Normalized: "a"}, objectidentity.Part{Source: "b", Normalized: "b"})
	third.Subject.Subject.Database = objectidentity.Part{Source: "elsewhere", Normalized: "elsewhere"}
	runtime := mustRuntime(c, relationProvider(relationsFunc(func(_ context.Context, request schemaext.RelationRequest) (schemaext.RelationResult, error) {
		return relationReply(request), nil
	})))
	snapshot, err := runtime.CaptureRelations(c.Context(), relationRequest(runtime, first, second, third))
	c.Assert(err, qt.IsNil)
	ref := first.Subject.Subject
	parent := objectidentity.ID{Kind: objectidentity.KindTable, Database: ref.Database, Schema: ref.Schema, Namespace: ref.Namespace, Name: ref.Parent}
	related, err := snapshot.CaptureRelated(c.Context(), []objectidentity.ID{parent})
	c.Assert(err, qt.IsNil)
	c.Assert(related.Values, qt.DeepEquals, []schemaext.RelationValue{first})
	c.Assert(related.Required, qt.DeepEquals, []objectidentity.ID{parent})
	// The schema contains both paths, but not the same schema in another database.
	schema := objectidentity.ID{Kind: objectidentity.KindSchema, Database: ref.Database, Name: ref.Schema}
	related, err = snapshot.CaptureRelated(c.Context(), []objectidentity.ID{schema})
	c.Assert(err, qt.IsNil)
	c.Assert(related.Values, qt.DeepEquals, []schemaext.RelationValue{first, second})
}
