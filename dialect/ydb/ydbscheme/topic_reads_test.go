package ydbscheme_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/plangraph"
	"ptah.run/dialect/ydb/ydbscheme"
	"ptah.run/dialect/ydb/ydbtopic"
)

// TestTopicPathReads reads the topic of this database a source names. A path
// written absolute is read against the database root; one outside it, or
// written absolute where the root is not known, names no topic the plan
// manages and reads nothing, and so does an empty source.
func TestTopicPathReads(t *testing.T) {
	tests := []struct {
		name   string
		root   string
		source string
		want   []plangraph.Effect
	}{
		{name: "a relative path", source: "app/events.v1", want: []plangraph.Effect{{Subject: ydbtopic.Ref("app", "events.v1"), Action: plangraph.Read}}},
		{name: "an absolute path in the database", root: "/local", source: "/local/app/events",
			want: []plangraph.Effect{{Subject: ydbtopic.Ref("app", "events"), Action: plangraph.Read}}},
		{name: "an absolute path in another database", root: "/local", source: "/other/app/events"},
		{name: "an absolute path, the root unknown", source: "/local/app/events"},
		{name: "no source", source: " "},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbscheme.TopicPathReads(test.root, test.source), qt.DeepEquals, test.want)
		})
	}
}
