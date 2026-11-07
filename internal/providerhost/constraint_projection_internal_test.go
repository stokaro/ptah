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
	"ptah.run/core/schemaext"
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

// normalizedKey is a foreign key of main.posts in the spelling a provider read
// reports: referenced schema, first referenced column and both rules stated.
func normalizedKey(name string, columns []string, schema, table string, foreign []string, deleteRule, updateRule string) catalog.Constraint {
	return catalog.Constraint{Name: name, TableName: "posts", Schema: "main", Type: "FOREIGN KEY", ColumnNames: columns,
		ForeignTable: new(table), ForeignSchema: schema, ForeignColumns: foreign, ForeignColumn: new(foreign[0]),
		DeleteRule: new(deleteRule), UpdateRule: new(updateRule)}
}

// declaredKey is a foreign key of main.posts as a declaration states it: no
// referenced schema, no first column and no rules unless the row sets them.
func declaredKey(name string, columns []string, table string, foreign []string) catalog.Constraint {
	return catalog.Constraint{Name: name, TableName: "posts", Schema: "main", Type: "FOREIGN KEY", ColumnNames: columns,
		ForeignTable: new(table), ForeignColumns: foreign}
}

// Each row is a change of main.posts' constraints. The projection writes
// every foreign key a change adds or replaces in the spelling a read reports,
// and leaves every other constraint as the request holds it, so the reverse
// plan starts from the table a read after the forward plan would return.
func TestConstraintProjection_ForeignKeyMatrix(t *testing.T) {
	composite := declaredKey("posts_pair_fk", []string{"owner_id", "id"}, "owners", []string{"id", "post_id"})
	elsewhere := declaredKey("posts_owner_fk", []string{"owner_id"}, "owners", []string{"id"})
	elsewhere.ForeignSchema = "audit"
	cascadeBefore := normalizedKey("posts_owner_fk", []string{"owner_id"}, "main", "owners", []string{"id"}, "NO ACTION", "NO ACTION")
	cascadeAfter := declaredKey("posts_owner_fk", []string{"owner_id"}, "owners", []string{"id"})
	cascadeAfter.DeleteRule = new("CASCADE")
	movedBefore := normalizedKey("posts_owner_fk", []string{"owner_id"}, "main", "owners", []string{"id"}, "NO ACTION", "NO ACTION")
	movedAfter := declaredKey("posts_owner_fk", []string{"id"}, "users", []string{"id"})
	// untouched keeps a captured spelling the projection would rewrite, so a
	// row shows it was left alone.
	untouched := declaredKey("posts_author_fk", []string{"id"}, "authors", []string{"id"})
	addedBeside := declaredKey("posts_owner_fk", []string{"owner_id"}, "owners", []string{"id"})
	deferred := declaredKey("posts_owner_fk", []string{"owner_id"}, "owners", []string{"id"})
	deferred.Deferrable, deferred.Initially = true, "DEFERRED"
	deferredWant := normalizedKey("posts_owner_fk", []string{"owner_id"}, "main", "owners", []string{"id"}, "NO ACTION", "NO ACTION")
	deferredWant.Deferrable, deferredWant.Initially = true, "DEFERRED"
	firstColumn := declaredKey("posts_owner_fk", []string{"owner_id"}, "owners", []string{"id"})
	firstColumn.ForeignColumn = new("id")
	check := catalog.Constraint{Name: "posts_id_positive", TableName: "posts", Schema: "main", Type: "CHECK", CheckClause: new("id > 0")}
	checkBefore := catalog.Constraint{Name: "posts_id_positive", TableName: "posts", Schema: "main", Type: "CHECK", CheckClause: new("(id > '0'::bigint)")}

	tests := []struct {
		name    string
		before  []catalog.Constraint
		after   []catalog.Constraint
		changes []schemaprojection.ConstraintChange
		want    []catalog.Constraint
	}{
		{
			name:    "a composite key states its first referenced column",
			after:   []catalog.Constraint{composite},
			changes: []schemaprojection.ConstraintChange{{After: &composite}},
			want:    []catalog.Constraint{normalizedKey("posts_pair_fk", []string{"owner_id", "id"}, "main", "owners", []string{"id", "post_id"}, "NO ACTION", "NO ACTION")},
		},
		{
			name:    "a key to another schema keeps that schema",
			after:   []catalog.Constraint{elsewhere},
			changes: []schemaprojection.ConstraintChange{{After: &elsewhere}},
			want:    []catalog.Constraint{normalizedKey("posts_owner_fk", []string{"owner_id"}, "audit", "owners", []string{"id"}, "NO ACTION", "NO ACTION")},
		},
		{
			name:    "an ON DELETE change keeps the declared rule and states the other",
			before:  []catalog.Constraint{cascadeBefore},
			after:   []catalog.Constraint{cascadeAfter},
			changes: []schemaprojection.ConstraintChange{{Before: &cascadeBefore, After: &cascadeAfter}},
			want:    []catalog.Constraint{normalizedKey("posts_owner_fk", []string{"owner_id"}, "main", "owners", []string{"id"}, "CASCADE", "NO ACTION")},
		},
		{
			name:    "a same-name replacement over other columns takes the new definition",
			before:  []catalog.Constraint{movedBefore},
			after:   []catalog.Constraint{movedAfter},
			changes: []schemaprojection.ConstraintChange{{Before: &movedBefore, After: &movedAfter}},
			want:    []catalog.Constraint{normalizedKey("posts_owner_fk", []string{"id"}, "main", "users", []string{"id"}, "NO ACTION", "NO ACTION")},
		},
		{
			name:    "a key the change leaves alone keeps the captured spelling",
			before:  []catalog.Constraint{untouched},
			after:   []catalog.Constraint{untouched, addedBeside},
			changes: []schemaprojection.ConstraintChange{{After: &addedBeside}},
			want:    []catalog.Constraint{untouched, normalizedKey("posts_owner_fk", []string{"owner_id"}, "main", "owners", []string{"id"}, "NO ACTION", "NO ACTION")},
		},
		{
			name:    "DEFERRABLE INITIALLY DEFERRED stays as declared",
			after:   []catalog.Constraint{deferred},
			changes: []schemaprojection.ConstraintChange{{After: &deferred}},
			want:    []catalog.Constraint{deferredWant},
		},
		{
			name:    "a first referenced column the declaration states is kept",
			after:   []catalog.Constraint{firstColumn},
			changes: []schemaprojection.ConstraintChange{{After: &firstColumn}},
			want:    []catalog.Constraint{normalizedKey("posts_owner_fk", []string{"owner_id"}, "main", "owners", []string{"id"}, "NO ACTION", "NO ACTION")},
		},
		{
			name:    "a written CHECK keeps the declared clause",
			before:  []catalog.Constraint{checkBefore},
			after:   []catalog.Constraint{check},
			changes: []schemaprojection.ConstraintChange{{Before: &checkBefore, After: &check}},
			want:    []catalog.Constraint{check},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaprojection.ConstraintRequest{Target: "fixture", Identifiers: identifier.Semantics{DefaultSchema: "main"},
				Before: projectedPosts(test.before...), After: projectedPosts(test.after...), Changes: test.changes}

			result, err := constraintProjection{}.ProjectConstraints(context.Background(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Unavailable, qt.Equals, "")
			c.Assert(result.State, qt.DeepEquals, new(projectedPosts(test.want...)))
		})
	}
}

// The projection writes into a copy: the request's table after the change
// keeps the declaration's spelling.
func TestConstraintProjection_LeavesTheRequestAlone(t *testing.T) {
	c := qt.New(t)
	key := postsForeignKey()
	request := schemaprojection.ConstraintRequest{Target: "fixture", Identifiers: identifier.Semantics{DefaultSchema: "main"},
		Before: projectedPosts(), After: projectedPosts(key), Changes: []schemaprojection.ConstraintChange{{After: &key}}}

	_, err := constraintProjection{}.ProjectConstraints(context.Background(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(request.After, qt.DeepEquals, projectedPosts(postsForeignKey()))
}

// Each row is a change the projection cannot predict: a key whose index a read
// reports, added or removed, and a constraint that carries facets.
func TestConstraintProjection_UnpredictedFailurePath(t *testing.T) {
	primary := catalog.Constraint{Name: "posts_pkey", TableName: "posts", Schema: "main", Type: "PRIMARY KEY", ColumnNames: []string{"id"}}
	unique := catalog.Constraint{Name: "posts_owner_key", TableName: "posts", Schema: "main", Type: "UNIQUE", ColumnNames: []string{"owner_id"}}

	tests := []struct {
		name    string
		before  []catalog.Constraint
		after   []catalog.Constraint
		changes []schemaprojection.ConstraintChange
		want    string
	}{
		{
			name:    "an added primary key",
			after:   []catalog.Constraint{primary},
			changes: []schemaprojection.ConstraintChange{{After: &primary}},
			want:    "a provider read reports the effect of a PRIMARY KEY constraint posts_pkey on its table's indexes, which the host cannot predict",
		},
		{
			name:    "a removed UNIQUE",
			before:  []catalog.Constraint{unique},
			changes: []schemaprojection.ConstraintChange{{Before: &unique}},
			want:    "a provider read reports the effect of a UNIQUE constraint posts_owner_key on its table's indexes, which the host cannot predict",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaprojection.ConstraintRequest{Target: "fixture", Identifiers: identifier.Semantics{DefaultSchema: "main"},
				Before: projectedPosts(test.before...), After: projectedPosts(test.after...), Changes: test.changes}

			result, err := constraintProjection{}.ProjectConstraints(context.Background(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.State, qt.IsNil)
			c.Assert(result.Unavailable, qt.Equals, test.want)
		})
	}
}

// A foreign key that carries facets is not predicted: the host cannot know
// what an engine's own facet does to the table a read reports.
func TestConstraintProjection_FacetedKeyFailurePath(t *testing.T) {
	c := qt.New(t)
	_, value := planningFixture(c)
	facets, err := schemaext.NewFacets(value)
	c.Assert(err, qt.IsNil)
	key := postsForeignKey()
	key.Facets = facets
	request := schemaprojection.ConstraintRequest{Target: "fixture", Identifiers: identifier.Semantics{DefaultSchema: "main"},
		Before: projectedPosts(), After: projectedPosts(key), Changes: []schemaprojection.ConstraintChange{{After: &key}}}

	result, err := constraintProjection{}.ProjectConstraints(context.Background(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.State, qt.IsNil)
	c.Assert(result.Unavailable, qt.Equals,
		"the FOREIGN KEY constraint posts_owner_fk carries facets its engine owns, whose effect on the table a provider read reports the host cannot predict")
}
