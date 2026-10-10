package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// changefeedDeclaration declares table events with the given changefeeds.
func changefeedDeclaration(changefeeds ...ydbschema.ChangefeedSpec) *schemamodel.Database {
	var objects []schemaext.Object
	for _, stream := range changefeeds {
		objects = append(objects, ydbschema.DesiredObject("", "events", stream))
	}
	return &schemamodel.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil)),
		Tables:          []schemamodel.Table{{StructName: "Event", Name: "events"}},
		Fields:          []schemamodel.Field{{StructName: "Event", Name: "id", Type: "BIGINT", Primary: true}},
	}
}

// changefeedCatalog is table events as the YDB reader reports it, with the
// given changefeeds.
func changefeedCatalog(changefeeds ...ydbschema.ChangefeedSpec) *catalog.Database {
	var objects []schemaext.Object
	for _, stream := range changefeeds {
		objects = append(objects, ydbschema.ObservedObject("", "events", stream))
	}
	return &catalog.Database{
		FeatureObjects:  must.Must(schemaext.NewObjects(objects...)),
		FeatureCoverage: completeYDBFixtureCoverage(),
		Tables: []catalog.Table{{Name: "events", Type: "TABLE", Columns: []catalog.Column{
			{Name: "id", DataType: "Int64", ColumnType: "Int64", IsNullable: "NO", IsPrimaryKey: true, OrdinalPosition: 1},
		}}},
		Constraints: []catalog.Constraint{{Name: "events_pkey", TableName: "events", Type: "PRIMARY KEY",
			ColumnName: "id", ColumnNames: []string{"id"}}},
	}
}

// declared is a changefeed as a declaration states it, and read the same one
// as the reader reports it: the retention in its own spelling, and the
// consumer's start as the epoch YDB reports for none.
var (
	declared = ydbschema.ChangefeedSpec{Name: "updates", Mode: "updates", Format: "json", RetentionPeriod: "PT720M",
		Consumers: []ast.TopicConsumerSpec{{Name: "audit", SupportedCodecs: []string{"GZIP", "raw"}}}}
	read = ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", RetentionPeriod: "PT12H",
		Consumers: []ast.TopicConsumerSpec{{Name: "audit", ReadFrom: "1970-01-01T00:00:00Z", SupportedCodecs: []string{"raw", "gzip"}}}}
)

// TestCompare_YDBChangefeedReadsBackAsDeclared holds a changefeed the database
// holds as declared equal to its declaration, against the database and
// against the same document alike: neither plans anything.
func TestCompare_YDBChangefeedReadsBackAsDeclared(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "against the database", diff: must.Must(schemadiff.CompareWithDialect(
			t.Context(), changefeedDeclaration(declared), changefeedCatalog(read), platform.YDB, must.Must(builtin.New())))},
		{name: "against the same document", diff: must.Must(schemadiff.CompareSchemas(
			t.Context(), changefeedDeclaration(declared), changefeedDeclaration(declared), platform.YDB, must.Must(builtin.New())))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.diff.HasChanges(), qt.IsFalse)
		})
	}
}

// TestCompare_YDBChangefeedChange carries both sides whole where they differ,
// so a planner can pair them by name.
func TestCompare_YDBChangefeedChange(t *testing.T) {
	other := ydbschema.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	tests := []struct {
		name     string
		desired  *schemamodel.Database
		current  *catalog.Database
		wantDiff []schemaext.ChangeRecord
	}{
		{name: "one added", desired: changefeedDeclaration(declared, other), current: changefeedCatalog(read),
			wantDiff: []schemaext.ChangeRecord{{Subject: ydbschema.ChangefeedRef("", "events", "keys"), Value: &ydbdiff.Changefeed{After: &ydbschema.DesiredChangefeed{Spec: other}}}}},
		{name: "one removed", desired: changefeedDeclaration(), current: changefeedCatalog(read),
			wantDiff: []schemaext.ChangeRecord{{Subject: ydbschema.ChangefeedRef("", "events", "updates"), Value: &ydbdiff.Changefeed{Before: &ydbschema.ObservedChangefeed{Spec: read}}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), test.desired, test.current, platform.YDB, must.Must(builtin.New())))
			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(diff.TablesModified[0].FeatureChanges, qt.DeepEquals, test.wantDiff)
		})
	}
}

// TestCompare_YDBChangefeedCoverage reads each side's silence through what it
// said it does not describe: a changefeed the read recorded as not described
// is neither added nor dropped, and one the declaration does not describe is
// kept as it is.
func TestCompare_YDBChangefeedCoverage(t *testing.T) {
	c := qt.New(t)
	unread := changefeedCatalog()
	unread.FeatureCoverage = must.Must(ydbschema.ChangefeedCoverage(schemaext.Observed, []schemaext.SubjectCoverage{{
		Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("", "events", "updates"), Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unsupported stream settings"},
	}}))
	undeclared := changefeedDeclaration()
	undeclared.FeatureCoverage = schemaext.Coverage{}

	opts := config.DefaultCompareOptions()
	opts.Dialect = platform.YDB

	withheld, undecided, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), changefeedDeclaration(declared), unread, opts, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	kept := must.Must(schemadiff.CompareWithDialect(t.Context(), undeclared, changefeedCatalog(read), platform.YDB, must.Must(builtin.New())))

	c.Assert(withheld.HasChanges(), qt.IsFalse)
	c.Assert(undecided.Features, qt.DeepEquals, []schemaext.UndecidedChange{{Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("", "events", "updates"), Reason: "the current changefeed or its absence was not established: unsupported stream settings"}})
	c.Assert(kept.HasChanges(), qt.IsFalse)
}

// TestCompare_YDBChangefeedsADeclarationDoesNotDescribeAreTheDatabases takes
// the changefeeds a declaration does not describe from the database, onto the
// declaration of the table the diff carries, so a rebuild adds them back after
// the swap rather than dropping them with the old table. A record naming one
// changefeed takes only that one, and a declaration that describes every
// changefeed and declares none drops them, which is the control on the
// first row. The declaration passed in is not changed.
func TestCompare_YDBChangefeedsADeclarationDoesNotDescribeAreTheDatabases(t *testing.T) {
	other := ydbschema.ChangefeedSpec{Name: "keys", Mode: "KEYS_ONLY", Format: "JSON"}
	unknown := schemaext.Knowledge{State: schemaext.Uninspected, Reason: "source does not declare this stream"}
	tests := []struct {
		name     string
		coverage schemaext.Coverage
		want     []ydbschema.ChangefeedSpec
		removed  []string
	}{
		{name: "the whole kind", want: []ydbschema.ChangefeedSpec{other, read}},
		{name: "one changefeed by name", coverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, []schemaext.SubjectCoverage{{Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("", "events", "updates"), Knowledge: unknown}})), want: []ydbschema.ChangefeedSpec{read}, removed: []string{"keys"}},
		{name: "none", coverage: must.Must(ydbschema.ChangefeedCoverage(schemaext.Desired, nil)), removed: []string{"keys", "updates"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := changefeedDeclaration()
			desired.Fields = append(desired.Fields, schemamodel.Field{StructName: "Event", Name: "n", Type: "BIGINT"})
			desired.FeatureCoverage = test.coverage
			current := changefeedCatalog(read, other)
			current.Tables[0].Columns = append(current.Tables[0].Columns, catalog.Column{Name: "n", DataType: "Int32",
				ColumnType: "Int32", IsNullable: "YES", OrdinalPosition: 2})

			diff := must.Must(schemadiff.CompareWithDialect(t.Context(), desired, current, platform.YDB, must.Must(builtin.New())))

			c.Assert(diff.TablesModified, qt.HasLen, 1)
			c.Assert(must.Must(ydbschema.DesiredChangefeeds(diff.TablesModified[0].Desired.OwnedObjects, "", "events")), qt.DeepEquals, test.want)
			var removed []string
			for _, change := range diff.TablesModified[0].FeatureChanges {
				c.Assert(change.Value.(*ydbdiff.Changefeed).After, qt.IsNil)
				removed = append(removed, change.Subject.Name.Source)
			}
			c.Assert(removed, qt.DeepEquals, test.removed)
			c.Assert(desired.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

func completeYDBFixtureCoverage() schemaext.Coverage {
	feeds := must.Must(ydbschema.ChangefeedCoverage(schemaext.Observed, nil))
	nodes := must.Must(ydbcoordination.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
	queries := must.Must(ydbstreaming.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
	secrets := must.Must(ydbsecret.Coverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
	combined := must.Must(must.Must(must.Must(feeds.Combine(nodes)).Combine(queries)).Combine(secrets))
	return must.Must(combined.Combine(workloadCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete})))
}
