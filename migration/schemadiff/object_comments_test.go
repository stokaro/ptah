package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// commentedObjects declares one object of every kind whose comment
// ObjectCommentsChanged carries, each with comment, in the schema app.
func commentedObjects(comment string) *schemamodel.Database {
	return &schemamodel.Database{
		Views:           []schemamodel.View{{Name: "app.v", Body: "SELECT 1", Comment: comment}},
		Sequences:       []schemamodel.Sequence{{Name: "s", Schema: "app", Comment: comment}},
		FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Desired)),
		FeatureObjects: must.Must(schemaext.NewObjects(
			schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "app", "d"), Value: &pgtype.DesiredDomain{BaseType: "integer", Comment: comment}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "app", "c"),
				Value: &pgtype.DesiredComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "integer"}}, Comment: comment}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "app", "r"), Value: &pgtype.DesiredRange{Subtype: "integer", Comment: comment}})),
	}
}

// reportedObjects is the database side of commentedObjects: the same objects,
// with comment on each, as a reader reports them.
func reportedObjects(comment string) *catalog.Database {
	return &catalog.Database{
		Views:           []catalog.View{{Name: "v", Schema: "app", Body: "SELECT 1", Comment: comment}},
		Sequences:       []catalog.Sequence{{Name: "s", Schema: "app", Comment: comment}},
		FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Observed)),
		FeatureObjects: must.Must(schemaext.NewObjects(
			schemaext.Object{Ref: pgtype.Ref(pgtype.DomainKind, "app", "d"), Value: &pgtype.ObservedDomain{BaseType: "integer", Comment: comment}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.CompositeKind, "app", "c"),
				Value: &pgtype.ObservedComposite{Attributes: []pgtype.Attribute{{Name: "n", Type: "integer"}}, Comment: comment}},
			schemaext.Object{Ref: pgtype.Ref(pgtype.RangeKind, "app", "r"), Value: &pgtype.ObservedRange{Subtype: "integer", Comment: comment}})),
	}
}

// everyObjectComment is the transition of every common object
// commentedObjects declares from current to desired, in the order the
// comparison sorts them. The type owner carries the comment of its types in
// its own changes.
func everyObjectComment(current, desired string) []difftypes.ObjectCommentChange {
	return []difftypes.ObjectCommentChange{
		{Kind: difftypes.CommentedSequence, Name: "app.s", Current: current, Desired: desired},
		{Kind: difftypes.CommentedView, Name: "app.v", Current: current, Desired: desired},
	}
}

// A comment that differs on a view, a sequence, a domain, a composite or a
// range type is a transition of its own, and nothing else about the object
// is reported as changed: a comment is the one change these objects take in
// place (stokaro/ptah#3627).
func TestCompareWithDialect_ObjectCommentDifferenceIsAChange(t *testing.T) {
	tests := []struct {
		name       string
		declared   string
		inDatabase string
		want       []difftypes.ObjectCommentChange
		wantTypes  int
	}{
		{name: "a comment rewritten", declared: "new", inDatabase: "old", want: everyObjectComment("old", "new"), wantTypes: 1},
		{name: "a comment added", declared: "new", inDatabase: "", want: everyObjectComment("", "new"), wantTypes: 1},
		{name: "a comment removed from the declaration", declared: "", inDatabase: "old", want: everyObjectComment("old", ""), wantTypes: 1},
		{name: "the same comment on both sides", declared: "same", inDatabase: "same", want: nil},
		{name: "a difference in surrounding space only", declared: "same", inDatabase: " same\n", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := must.Must(schemadiff.CompareWithDialect(
				t.Context(), commentedObjects(test.declared),
				reportedObjects(test.inDatabase),
				platform.Postgres, must.Must(builtin.New()),
			))

			c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, test.want)
			c.Assert(diff.ViewsModified, qt.HasLen, 0)
			c.Assert(diff.SequencesModified, qt.HasLen, 0)
			c.Assert(typeChangesOf(diff, pgtype.DomainChangeKind), qt.HasLen, test.wantTypes)
			for _, change := range typeChangesOf(diff, pgtype.DomainChangeKind) {
				c.Assert(change.Value.(*pgtype.DomainChange).Differs, qt.DeepEquals, []string{pgtype.DiffComment})
			}
			c.Assert(typeChangesOf(diff, pgtype.CompositeChangeKind), qt.HasLen, test.wantTypes)
			for _, change := range typeChangesOf(diff, pgtype.CompositeChangeKind) {
				c.Assert(change.Value.(*pgtype.CompositeChange).Differs, qt.DeepEquals, []string{pgtype.DiffComment})
			}
			c.Assert(rangeChangesOf(diff), qt.HasLen, test.wantTypes)
			for _, change := range rangeChangesOf(diff) {
				c.Assert(change.Value.(*pgtype.RangeChange).Differs, qt.DeepEquals, []string{pgtype.DiffComment})
			}
		})
	}
}

// A kind is compared only where the target stores its comment and reads it
// back. CockroachDB 25.4 answers `syntax error` to COMMENT ON VIEW, so a
// declared view comment there is a difference nothing could close, planned
// again on every run; 26.3 takes it. The capabilities a live server resolved
// to decide, not the dialect's default preset.
func TestCompareWithDatabaseInfo_ObjectCommentsFollowTheTargetsCapabilities(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		caps    capability.Capabilities
		want    []difftypes.ObjectCommentChange
	}{
		{
			name:    "CockroachDB 25.4 stores none of them",
			dialect: platform.CockroachDB,
			caps:    capability.CockroachDB25(),
		},
		{
			name:    "CockroachDB 26.3 stores the view's and the sequence's",
			dialect: platform.CockroachDB,
			caps:    capability.CockroachDB263(),
			want: []difftypes.ObjectCommentChange{
				{Kind: difftypes.CommentedSequence, Name: "app.s", Current: "old", Desired: "new"},
				{Kind: difftypes.CommentedView, Name: "app.v", Current: "old", Desired: "new"},
			},
		},
		{
			name:    "PostgreSQL stores every one",
			dialect: platform.Postgres,
			caps:    capability.Postgres18(),
			want:    everyObjectComment("old", "new"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := commentedObjects("new")
			// CockroachDB has neither domains nor ranges, and a declaration
			// it cannot host is refused before any comparison.
			declared.FeatureObjects = schemaext.Objects{}

			diff, err := schemadiff.CompareWithDatabaseInfo(
				t.Context(), declared,
				reportedObjects("old"),
				catalog.ServerInfo{Dialect: test.dialect, Capabilities: test.caps},
				nil, must.Must(builtin.New()),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, test.want)
		})
	}
}

// An object only one side holds carries no comment transition: a created
// object takes its comment from the statement that creates it.
func TestCompareWithDialect_ObjectCommentOnlyForObjectsBothSidesHold(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), commentedObjects("new"), &catalog.Database{FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Observed))}, platform.Postgres, must.Must(builtin.New())))

	c.Assert(diff.ObjectCommentsChanged, qt.HasLen, 0)
	c.Assert(diff.ViewsAdded, qt.HasLen, 1)
}

// A comparison that names no dialect is a PostgreSQL comparison, as every
// other comparator here reads one, and compares every object comment.
func TestCompareSchemas_UnnamedDialectComparesObjectCommentsAsPostgreSQL(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), commentedObjects("new"), reportedObjects("old"), "postgres", must.Must(builtin.New())))

	c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, everyObjectComment("old", "new"))
}

// Two declarations compared with each other go through the conversion that
// turns the current one into a catalog, and a comment has to survive it: a
// comparison of a document with a changed comment against its own previous
// version reports the change.
func TestCompareSchemas_ObjectCommentsSurviveTheConversion(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareSchemas(t.Context(), commentedObjects("new"), commentedObjects("old"), platform.Postgres, must.Must(builtin.New())))

	c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, everyObjectComment("old", "new"))
}

// A removed domain, composite or range carries its comment, so the rollback
// that creates it again writes the comment back.
func TestCompareWithDialect_RemovedUserTypesCarryTheirComments(t *testing.T) {
	c := qt.New(t)

	diff := must.Must(schemadiff.CompareWithDialect(t.Context(), &schemamodel.Database{FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Desired))}, reportedObjects("kept"), platform.Postgres, must.Must(builtin.New())))

	c.Assert(typeChangesOf(diff, pgtype.DomainChangeKind), qt.HasLen, 1)
	c.Assert(typeChangesOf(diff, pgtype.DomainChangeKind)[0].Value.(*pgtype.DomainChange).Before.Comment, qt.Equals, "kept")
	c.Assert(typeChangesOf(diff, pgtype.CompositeChangeKind), qt.HasLen, 1)
	c.Assert(typeChangesOf(diff, pgtype.CompositeChangeKind)[0].Value.(*pgtype.CompositeChange).Before.Comment, qt.Equals, "kept")
	c.Assert(rangeChangesOf(diff), qt.HasLen, 1)
	c.Assert(rangeChangesOf(diff)[0].Value.(*pgtype.RangeChange).Before.Comment, qt.Equals, "kept")
}

// withoutKinds drops the transitions of the given kinds.
func withoutKinds(
	changes []difftypes.ObjectCommentChange, kinds ...difftypes.CommentedObjectKind,
) []difftypes.ObjectCommentChange {
	var kept []difftypes.ObjectCommentChange
	for _, change := range changes {
		drop := false
		for _, kind := range kinds {
			drop = drop || change.Kind == kind
		}
		if !drop {
			kept = append(kept, change)
		}
	}
	return kept
}
