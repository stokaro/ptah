package ydbcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/engine"
)

func externalRuntime(c *qt.C) *engine.Runtime {
	c.Helper()
	runtime, err := engine.New(engine.Provider{ID: "ptah.run/ydb", Targets: []engine.Target{{Name: "ydb"}},
		Codecs: append(ydbexternal.Codecs(), ydbdiff.ExternalDataSourceCodec(), ydbdiff.ExternalTableCodec()),
		Comparisons: []engine.ObjectComparison{{Target: "ydb", Kinds: []schemaext.Kind{ydbexternal.SourceKind, ydbexternal.TableKind},
			ChangeKinds: []schemaext.Kind{ydbdiff.ExternalDataSourceKind, ydbdiff.ExternalTableKind}, Service: ydbcompare.ExternalService{}}},
	})
	c.Assert(err, qt.IsNil)
	return runtime
}

// externalState is objects in representation, from a source that claims both
// namespaces, with limits on the table namespace.
func externalState(representation schemaext.Representation, objects []schemaext.Object, limits ...schemaext.SubjectCoverage) schemaext.ObjectState {
	complete := schemaext.Knowledge{State: schemaext.Complete}
	sources := must.Must(ydbexternal.SourceCoverage(representation, complete, nil))
	tables := must.Must(ydbexternal.TableCoverage(representation, complete, limits))
	return schemaext.ObjectState{Objects: must.Must(schemaext.NewObjects(objects...)), Coverage: must.Must(sources.Combine(tables))}
}

func externalComparison(root string, desired, current schemaext.ObjectState) schemaext.ObjectComparisonRequest {
	return schemaext.ObjectComparisonRequest{Target: "ydb", Identifiers: identifier.ForDialect("ydb"),
		Capabilities: capability.YDB262().With(capability.ExternalDataSources, true), DatabasePath: root,
		Kinds: []schemaext.Kind{ydbexternal.SourceKind, ydbexternal.TableKind}, Desired: desired, Current: current}
}

var comparedBucket = ydbexternal.DataSource{SourceType: "ObjectStorage", AuthMethod: "NONE"}

// TestExternalComparison_ReadsAbsolutePathsAgainstTheRoot compares a table's
// data source and a source's secret written absolute equal to the relative
// path the read reports only where the request names the database: with no
// root, an absolute path names no object of this database.
func TestExternalComparison_ReadsAbsolutePathsAgainstTheRoot(t *testing.T) {
	declaredTable := ydbexternal.Table{DataSource: "/local/ext/bucket", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	heldTable := declaredTable.Clone()
	heldTable.DataSource = "ext/bucket"
	tests := []struct {
		name string
		root string
		want int
	}{
		{name: "the database named", root: "/local", want: 0},
		{name: "no root", want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			desired := externalState(schemaext.Desired, []schemaext.Object{
				ydbexternal.DesiredSourceObject("ext", "bucket", "", comparedBucket), ydbexternal.DesiredTableObject("ext", "events", "", declaredTable)})
			current := externalState(schemaext.Observed, []schemaext.Object{
				ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket), ydbexternal.ObservedTableObject("ext", "events", heldTable)})

			result, err := externalRuntime(c).CompareObjects(t.Context(), externalComparison(test.root, desired, current))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, test.want)
		})
	}
}

// TestExternalComparison_FailurePath refuses a dotted limit that reads like a
// path to a held object the plan would drop, and a plan that drops a data
// source a declared external table reads.
func TestExternalComparison_FailurePath(t *testing.T) {
	events := ydbexternal.Table{DataSource: "ext/bucket", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	tests := []struct {
		name             string
		desired, current schemaext.ObjectState
		wantErr          string
	}{
		{name: "a dotted limit",
			desired: externalState(schemaext.Desired, nil, schemaext.SubjectCoverage{Kind: ydbexternal.TableKind,
				Subject:   must.Must(ydbexternal.ParsePath(ydbexternal.TableKind, "ext.events")),
				Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not described"}}),
			current: externalState(schemaext.Observed, []schemaext.Object{ydbexternal.ObservedTableObject("ext", "events", events)}),
			wantErr: `.*the external table limit "ext\.events" names ext\.events at the database root, .*`},
		{name: "a declared table over a dropped source",
			desired: externalState(schemaext.Desired, []schemaext.Object{ydbexternal.DesiredTableObject("ext", "events", "", events)}),
			current: externalState(schemaext.Observed, []schemaext.Object{
				ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket), ydbexternal.ObservedTableObject("ext", "events", events)}),
			wantErr: ".*external table ext/events reads data source ext/bucket, which the plan drops; declare the data source or " +
				"move the table to one the plan keeps"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			result, err := externalRuntime(c).CompareObjects(t.Context(), externalComparison("/local", test.desired, test.current))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Changes, qt.HasLen, 0)
		})
	}
}

// TestExternalComparison_LeavesAnUndecidedTableOverARecreatedSource compares
// a data source the plan drops and creates again, with a table over it that
// both sides hold and one side cannot describe.
//
// The table is reported undecided, and a recreated source takes along every
// declared table over it that the comparison names nowhere yet. Taken along
// as well, the table is named by a change and a diagnostic at once, and the
// engine refuses the whole reply as `invalid comparison diagnostic for
// ext.events` instead of reporting why the table could not be compared.
func TestExternalComparison_LeavesAnUndecidedTableOverARecreatedSource(t *testing.T) {
	events := ydbexternal.Table{DataSource: "ext/bucket", Location: "e/", Columns: []ydbexternal.Column{{Name: "id", Type: "Int64"}}}
	relocated := comparedBucket
	relocated.Location = "https://storage.example/other/"
	limited := schemaext.SubjectCoverage{Kind: ydbexternal.TableKind, Subject: ydbexternal.TableRef("ext", "events"),
		Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "an unreadable setting"}}
	declared := []schemaext.Object{
		ydbexternal.DesiredSourceObject("ext", "bucket", "", relocated), ydbexternal.DesiredTableObject("ext", "events", "", events)}
	held := []schemaext.Object{
		ydbexternal.ObservedSourceObject("ext", "bucket", comparedBucket), ydbexternal.ObservedTableObject("ext", "events", events)}
	tests := []struct {
		name             string
		desired, current schemaext.ObjectState
	}{
		{name: "the declaration cannot describe the table",
			desired: externalState(schemaext.Desired, declared, limited), current: externalState(schemaext.Observed, held)},
		{name: "the read cannot describe the table",
			desired: externalState(schemaext.Desired, declared), current: externalState(schemaext.Observed, held, limited)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := externalComparison("/local", test.desired, test.current)
			request.Capabilities = request.Capabilities.With(capability.ExternalObjectReplace, false)

			result, err := externalRuntime(c).CompareObjects(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 1)
			c.Assert(result.Changes[0].Subject, qt.DeepEquals, ydbexternal.SourceRef("ext", "bucket"))
			c.Assert(result.Undecided, qt.HasLen, 1)
			c.Assert(result.Undecided[0].Subject, qt.DeepEquals, ydbexternal.TableRef("ext", "events"))
		})
	}
}
