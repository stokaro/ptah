package providerhost

// White-box testing required: the projection is the engine service a
// provider target registers, which the host reaches only from a reverse plan
// over a running provider. The subprocess test in integration/providercli
// drives that path; this one pins the projection's answers for each kind of
// constraint without a process.

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaprojection"
)

// projectedPosts is main.posts holding the constraints, with the index
// posts_by_owner.
func projectedPosts(constraints ...catalog.Constraint) schemaprojection.TableState {
	return schemaprojection.TableState{
		Table:       catalog.Table{Name: "posts", Schema: "main", Columns: []catalog.Column{{Name: "id"}, {Name: "owner_id"}}},
		Indexes:     []catalog.Index{{Name: "posts_by_owner", TableName: "posts", Schema: "main", Columns: []string{"owner_id"}}},
		Constraints: constraints,
	}
}

func postsForeignKey() catalog.Constraint {
	return catalog.Constraint{Name: "posts_owner_fk", TableName: "posts", Schema: "main", Type: "FOREIGN KEY", ColumnNames: []string{"owner_id"},
		ForeignTable: new("owners"), ForeignColumns: []string{"id"}}
}

// A foreign key added to a provider table changes the table's constraints and
// nothing else: the read leaves the index an engine builds for it out of the
// table's indexes. The key takes the spelling a read reports.
func TestConstraintProjection_ForeignKey(t *testing.T) {
	c := qt.New(t)
	key := postsForeignKey()
	request := schemaprojection.ConstraintRequest{Target: "fixture", Identifiers: identifier.Semantics{DefaultSchema: "main"},
		Before: projectedPosts(), After: projectedPosts(key), Changes: []schemaprojection.ConstraintChange{{After: &key}}}

	result, err := constraintProjection{}.ProjectConstraints(context.Background(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Unavailable, qt.Equals, "")
	want := postsForeignKey()
	want.ForeignSchema, want.ForeignColumn, want.DeleteRule, want.UpdateRule = "main", new("id"), new("NO ACTION"), new("NO ACTION")
	c.Assert(result.State, qt.DeepEquals, new(projectedPosts(want)))
}

// A removed foreign key leaves the table without it.
func TestConstraintProjection_RemovedForeignKey(t *testing.T) {
	c := qt.New(t)
	key := postsForeignKey()
	request := schemaprojection.ConstraintRequest{Target: "fixture", Identifiers: identifier.Semantics{DefaultSchema: "main"},
		Before: projectedPosts(key), After: projectedPosts(), Changes: []schemaprojection.ConstraintChange{{Before: &key}}}

	result, err := constraintProjection{}.ProjectConstraints(context.Background(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.State, qt.DeepEquals, new(projectedPosts()))
}

// A key whose index a provider read reports, a primary key or a UNIQUE, is
// not predicted: the host does not know the index the engine builds.
func TestConstraintProjection_FailurePath(t *testing.T) {
	c := qt.New(t)
	unique := catalog.Constraint{Name: "posts_owner_key", TableName: "posts", Schema: "main", Type: "UNIQUE", ColumnNames: []string{"owner_id"}}
	request := schemaprojection.ConstraintRequest{Target: "fixture", Identifiers: identifier.Semantics{DefaultSchema: "main"},
		Before: projectedPosts(), After: projectedPosts(unique), Changes: []schemaprojection.ConstraintChange{{After: &unique}}}

	result, err := constraintProjection{}.ProjectConstraints(context.Background(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.State, qt.IsNil)
	c.Assert(result.Unavailable, qt.Equals,
		"a provider read reports the effect of a UNIQUE constraint posts_owner_key on its table's indexes, which the host cannot predict")
}
