package viewcapture_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/viewcapture"
)

// A replacement and a removal's rollback both need every setting the read
// could not capture. A known setting or a limit on another view is not one.
func TestUnreadSettings(t *testing.T) {
	const kind schemaext.Kind = "test.example/views/storage"
	identities := objectidentity.NewBuilder(identifier.ForDialect("clickhouse"))
	view := identities.SchemaScopedParts(objectidentity.KindMatView, "analytics", "daily")
	for _, test := range []struct {
		name, schema, view string
		state              schemaext.KnowledgeState
		want               []schemaext.KnowledgeState
	}{
		{name: "unrepresented", schema: "analytics", view: "daily", state: schemaext.Unrepresentable, want: []schemaext.KnowledgeState{schemaext.Unrepresentable}},
		{name: "uninspected", schema: "analytics", view: "daily", state: schemaext.Uninspected, want: []schemaext.KnowledgeState{schemaext.Uninspected}},
		{name: "complete", schema: "analytics", view: "daily", state: schemaext.Complete},
		{name: "absent", schema: "analytics", view: "daily", state: schemaext.Absent},
		{name: "another schema", schema: "archive", view: "daily", state: schemaext.Unrepresentable},
		{name: "another view", schema: "analytics", view: "hourly", state: schemaext.Unrepresentable},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			subject := identities.SchemaScopedParts(objectidentity.KindMatView, test.schema, test.view)
			coverage, err := schemaext.NewCoverage(schemaext.Observed,
				[]schemaext.KindCoverage{{
					Model:     schemaext.CodecIdentity{Owner: "test.example/views", Kind: kind, Version: 1, Representation: schemaext.Observed, Definition: "storage-v1"},
					Knowledge: schemaext.GlobalKnowledge(schemaext.Knowledge{State: schemaext.Complete}),
				}},
				[]schemaext.SubjectCoverage{{Kind: kind, Subject: subject, Knowledge: schemaext.Knowledge{State: test.state, Reason: "stored engine"}}},
			)
			c.Assert(err, qt.IsNil)

			unread := viewcapture.UnreadSettings(coverage, view)

			var states []schemaext.KnowledgeState
			for _, record := range unread {
				states = append(states, record.Knowledge.State)
				c.Assert(record.Subject, qt.Equals, view)
				c.Assert(record.Knowledge.Reason, qt.Equals, "stored engine")
			}
			c.Assert(states, qt.DeepEquals, test.want)
		})
	}
}
