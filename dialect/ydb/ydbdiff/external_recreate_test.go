package ydbdiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbexternal"
)

// TestExternalDataSource_Recreates says which source change the plan makes by
// dropping the source and creating it again, which takes its permission
// entries: every change of a source both sides hold on a target without
// CREATE OR REPLACE, and none with it, which keeps them.
func TestExternalDataSource_Recreates(t *testing.T) {
	moved := diffBucket.Clone()
	moved.Location = "https://s3.example.test/other/"
	replace := capability.YDB262().With(capability.ExternalObjectReplace, true)
	tests := []struct {
		name      string
		change    *ydbdiff.ExternalDataSource
		caps      capability.Capabilities
		recreates bool
	}{
		{name: "moved, without replace", change: change(&diffBucket, &moved), caps: capability.YDB262(), recreates: true},
		{name: "moved, with replace", change: change(&diffBucket, &moved), caps: replace},
		{name: "removed", change: change(&diffBucket, nil), caps: capability.YDB262()},
		{name: "created", change: change(nil, &diffBucket), caps: capability.YDB262()},
		{name: "no change", caps: capability.YDB262()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Recreates(test.caps), qt.Equals, test.recreates)
		})
	}
}

// TestExternalTable_Recreates says which external table change the plan makes
// by dropping the table and creating it again: every change of a table both
// sides hold on a target without CREATE OR REPLACE, and with it one that reads
// or read a data source the plan drops or displaces, whether the table names
// it relative to the database or by its absolute path.
func TestExternalTable_Recreates(t *testing.T) {
	moved := diffEvents.Clone()
	moved.Location = "f/"
	absolute := diffEvents.Clone()
	absolute.DataSource = "/local/s3"
	replace := capability.YDB262().With(capability.ExternalObjectReplace, true)
	displaced := map[objectidentity.Key]bool{ydbexternal.SourceRef("", "s3").Key(): true}
	tests := []struct {
		name      string
		change    *ydbdiff.ExternalTable
		caps      capability.Capabilities
		displaced map[objectidentity.Key]bool
		recreates bool
	}{
		{name: "changed, without replace", change: tableDiff(&diffEvents, &moved), caps: capability.YDB262(), recreates: true},
		{name: "changed, with replace", change: tableDiff(&diffEvents, &moved), caps: replace},
		{name: "over a displaced source, with replace", change: tableDiff(&diffEvents, &diffEvents), caps: replace,
			displaced: displaced, recreates: true},
		{name: "over a displaced source named by its path", change: tableDiff(&absolute, &absolute), caps: replace,
			displaced: displaced, recreates: true},
		{name: "removed", change: tableDiff(&diffEvents, nil), caps: capability.YDB262()},
		{name: "created", change: tableDiff(nil, &diffEvents), caps: capability.YDB262()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.change.Recreates(test.caps, "/local", test.displaced), qt.Equals, test.recreates)
		})
	}
}

// TestDisplacedSources names the sources whose tables a change set displaces:
// a dropped source, and one the target replaces only by dropping it. A created
// source, a source CREATE OR REPLACE keeps the tables over, and a change of
// another kind name none.
func TestDisplacedSources(t *testing.T) {
	moved := diffBucket.Clone()
	moved.Location = "https://s3.example.test/other/"
	changes := []schemaext.ChangeRecord{
		{Subject: ydbexternal.SourceRef("", "gone"), Value: change(&diffBucket, nil)},
		{Subject: ydbexternal.SourceRef("", "moved"), Value: change(&diffBucket, &moved)},
		{Subject: ydbexternal.SourceRef("", "new"), Value: change(nil, &diffBucket)},
		{Subject: ydbexternal.TableRef("", "events"), Value: tableDiff(&diffEvents, nil)},
	}
	tests := []struct {
		name string
		caps capability.Capabilities
		want map[objectidentity.Key]bool
	}{
		{name: "without replace", caps: capability.YDB262(), want: map[objectidentity.Key]bool{
			ydbexternal.SourceRef("", "gone").Key(): true, ydbexternal.SourceRef("", "moved").Key(): true}},
		{name: "with replace", caps: capability.YDB262().With(capability.ExternalObjectReplace, true),
			want: map[objectidentity.Key]bool{ydbexternal.SourceRef("", "gone").Key(): true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbdiff.DisplacedSources(changes, test.caps), qt.DeepEquals, test.want)
		})
	}
}

// tableDiff is a table change from before to after; a nil side is absence.
func tableDiff(before, after *ydbexternal.Table) *ydbdiff.ExternalTable {
	result := &ydbdiff.ExternalTable{}
	if before != nil {
		result.Before = &ydbexternal.ObservedTable{Spec: *before}
	}
	if after != nil {
		result.After = &ydbexternal.DesiredTable{Spec: *after}
	}
	return result
}
