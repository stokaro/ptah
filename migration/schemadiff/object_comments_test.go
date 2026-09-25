package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// commentedObjects declares one object of every kind whose comment
// ObjectCommentsChanged carries, each with comment, in the schema app. The
// extension carries extension.
func commentedObjects(comment, extension string) *schemamodel.Database {
	return &schemamodel.Database{
		Views:          []schemamodel.View{{Name: "app.v", Body: "SELECT 1", Comment: comment}},
		Sequences:      []schemamodel.Sequence{{Name: "s", Schema: "app", Comment: comment}},
		Domains:        []schemamodel.Domain{{Name: "d", Schema: "app", BaseType: "integer", Comment: comment}},
		CompositeTypes: []schemamodel.CompositeType{{Name: "c", Schema: "app", Fields: []schemamodel.CompositeField{{Name: "n", Type: "integer"}}, Comment: comment}},
		Ranges:         []schemamodel.Range{{Name: "r", Schema: "app", Subtype: "integer", Comment: comment}},
		Extensions:     []schemamodel.Extension{{Name: "hstore", Comment: extension}},
	}
}

// reportedObjects is the database side of commentedObjects: the same objects,
// with comment on each and extension on the extension, as a reader reports
// them.
func reportedObjects(comment, extension string) *catalog.Database {
	var extensionComment *string
	if extension != "" {
		extensionComment = &extension
	}
	return &catalog.Database{
		Views:      []catalog.View{{Name: "v", Schema: "app", Body: "SELECT 1", Comment: comment}},
		Sequences:  []catalog.Sequence{{Name: "s", Schema: "app", Comment: comment}},
		Domains:    []catalog.Domain{{Name: "d", Schema: "app", BaseType: "integer", Comment: comment}},
		Composites: []catalog.CompositeType{{Name: "c", Schema: "app", Fields: []catalog.CompositeField{{Name: "n", Type: "integer"}}, Comment: comment}},
		Ranges:     []catalog.Range{{Name: "r", Schema: "app", Subtype: "integer", Comment: comment}},
		Extensions: []catalog.Extension{{Name: "hstore", Schema: "public", Comment: extensionComment}},
	}
}

// everyObjectComment is the transition of every object commentedObjects
// declares from current to desired, in the order the comparison sorts them.
func everyObjectComment(current, desired string) []difftypes.ObjectCommentChange {
	return []difftypes.ObjectCommentChange{
		{Kind: difftypes.CommentedCompositeType, Name: "app.c", Current: current, Desired: desired},
		{Kind: difftypes.CommentedDomain, Name: "app.d", Current: current, Desired: desired},
		{Kind: difftypes.CommentedRangeType, Name: "app.r", Current: current, Desired: desired},
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
	}{
		{name: "a comment rewritten", declared: "new", inDatabase: "old", want: everyObjectComment("old", "new")},
		{name: "a comment added", declared: "new", inDatabase: "", want: everyObjectComment("", "new")},
		{name: "a comment removed from the declaration", declared: "", inDatabase: "old", want: everyObjectComment("old", "")},
		{name: "the same comment on both sides", declared: "same", inDatabase: "same", want: nil},
		{name: "a difference in surrounding space only", declared: "same", inDatabase: " same\n", want: nil},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff := schemadiff.CompareWithDialect(
				commentedObjects(test.declared, ""),
				reportedObjects(test.inDatabase, ""),
				platform.Postgres,
			)

			c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, test.want)
			c.Assert(diff.ViewsModified, qt.HasLen, 0)
			c.Assert(diff.SequencesModified, qt.HasLen, 0)
			c.Assert(diff.DomainsModified, qt.HasLen, 0)
			c.Assert(diff.CompositeTypesModified, qt.HasLen, 0)
			c.Assert(diff.RangesModified, qt.HasLen, 0)
		})
	}
}

// An extension's comment is compared only when the declaration states one,
// because CREATE EXTENSION gives every extension the comment its control file
// carries.
func TestCompareWithDialect_ExtensionCommentOnlyWhenDeclared(t *testing.T) {
	tests := []struct {
		name       string
		declared   string
		inDatabase string
		ignored    []string
		want       []difftypes.ObjectCommentChange
	}{
		{
			name: "a declared comment that differs", declared: "mine", inDatabase: "data type for storing sets",
			want: []difftypes.ObjectCommentChange{{
				Kind: difftypes.CommentedExtension, Name: "hstore", Current: "data type for storing sets", Desired: "mine",
			}},
		},
		{
			name: "a declared comment on an extension with none", declared: "mine", inDatabase: "",
			want: []difftypes.ObjectCommentChange{{Kind: difftypes.CommentedExtension, Name: "hstore", Desired: "mine"}},
		},
		{name: "no declared comment beside the control file's", declared: "", inDatabase: "data type for storing sets"},
		{name: "the same comment", declared: "mine", inDatabase: "mine"},
		{name: "an ignored extension", declared: "mine", inDatabase: "data type for storing sets", ignored: []string{"hstore"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			opts := config.DefaultCompareOptions()
			opts.Dialect = platform.Postgres
			opts.IgnoredExtensions = test.ignored

			diff := schemadiff.CompareWithOptions(
				commentedObjects("same", test.declared),
				reportedObjects("same", test.inDatabase),
				opts,
			)

			c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, test.want)
			c.Assert(diff.ExtensionsModified, qt.HasLen, 0)
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
			declared := commentedObjects("new", "")
			// CockroachDB has neither domains nor ranges before 26.3, and a
			// declaration it cannot host is refused before any comparison.
			declared.Domains, declared.Ranges = nil, nil

			diff, err := schemadiff.CompareWithDatabaseInfo(
				declared,
				reportedObjects("old", ""),
				catalog.ServerInfo{Dialect: test.dialect, Capabilities: test.caps},
				nil,
			)

			c.Assert(err, qt.IsNil)
			c.Assert(withoutKinds(diff.ObjectCommentsChanged, difftypes.CommentedDomain, difftypes.CommentedRangeType),
				qt.DeepEquals, withoutKinds(test.want, difftypes.CommentedDomain, difftypes.CommentedRangeType))
		})
	}
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

// An object only one side holds carries no comment transition: a created
// object takes its comment from the statement that creates it.
func TestCompareWithDialect_ObjectCommentOnlyForObjectsBothSidesHold(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(commentedObjects("new", "mine"), &catalog.Database{}, platform.Postgres)

	c.Assert(diff.ObjectCommentsChanged, qt.HasLen, 0)
	c.Assert(diff.ViewsAdded, qt.HasLen, 1)
}

// A comparison that names no dialect is a PostgreSQL comparison, as every
// other comparator here reads one, and compares every object comment.
func TestCompareSchemas_UnnamedDialectComparesObjectCommentsAsPostgreSQL(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.Compare(commentedObjects("new", ""), reportedObjects("old", ""))

	c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, everyObjectComment("old", "new"))
}

// Two declarations compared with each other go through the conversion that
// turns the current one into a catalog, and a comment has to survive it: a
// comparison of a document with a changed comment against its own previous
// version reports the change.
func TestCompareSchemas_ObjectCommentsSurviveTheConversion(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareSchemas(commentedObjects("new", "mine"), commentedObjects("old", "theirs"), platform.Postgres)

	want := everyObjectComment("old", "new")
	// The kinds sort by name, so the extension falls between the domain and
	// the range.
	want = append(want[:2:2], append([]difftypes.ObjectCommentChange{
		{Kind: difftypes.CommentedExtension, Name: "hstore", Current: "theirs", Desired: "mine"},
	}, want[2:]...)...)
	c.Assert(diff.ObjectCommentsChanged, qt.DeepEquals, want)
}

// A removed domain, composite or range carries its comment, so the rollback
// that creates it again writes the comment back.
func TestCompareWithDialect_RemovedUserTypesCarryTheirComments(t *testing.T) {
	c := qt.New(t)

	diff := schemadiff.CompareWithDialect(&schemamodel.Database{}, reportedObjects("kept", ""), platform.Postgres)

	c.Assert(diff.DomainsRemoved, qt.HasLen, 1)
	c.Assert(diff.DomainsRemoved[0].Comment, qt.Equals, "kept")
	c.Assert(diff.CompositeTypesRemoved, qt.HasLen, 1)
	c.Assert(diff.CompositeTypesRemoved[0].Comment, qt.Equals, "kept")
	c.Assert(diff.RangesRemoved, qt.HasLen, 1)
	c.Assert(diff.RangesRemoved[0].Comment, qt.Equals, "kept")
}
