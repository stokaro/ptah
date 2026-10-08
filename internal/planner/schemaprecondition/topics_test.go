package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestRefuseTopics_FailurePath refuses a diff that creates, drops or changes
// a topic, naming the first one, for a planner that plans none.
func TestRefuseTopics_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want string
	}{
		{name: "a created topic", diff: &difftypes.SchemaDiff{TopicsAdded: difftypes.TopicChanges{{Name: "events", Schema: "app"}}},
			want: "the diff creates topic app.events, which requires target capability topics, unavailable on this postgres " +
				"target; only a YDB plan creates, drops or changes a topic"},
		{name: "a dropped topic", diff: &difftypes.SchemaDiff{TopicsRemoved: difftypes.TopicChanges{{Name: "events"}}},
			want: "the diff drops topic events, which requires target capability topics, unavailable on this postgres " +
				"target; only a YDB plan creates, drops or changes a topic"},
		{name: "a changed topic", diff: &difftypes.SchemaDiff{TopicsModified: []difftypes.TopicDiff{{Name: "events"}}},
			want: "the diff changes topic events, which requires target capability topics, unavailable on this postgres " +
				"target; only a YDB plan creates, drops or changes a topic"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			err := schemaprecondition.RefuseTopics("postgres", test.diff)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
		})
	}
}

// TestRefuseTopics_HappyPath passes a diff that carries no topic, and no diff.
func TestRefuseTopics_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
	}{
		{name: "no diff"},
		{name: "a diff without topics", diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "t"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprecondition.RefuseTopics("postgres", test.diff), qt.IsNil)
		})
	}
}
