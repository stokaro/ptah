package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/internal/atlashcl"
)

// TestParseCoordinationNode reads the YDB coordination node block: the schema
// label or attribute, and the settings as the annotation spells them.
func TestParseCoordinationNode(t *testing.T) {
	c := qt.New(t)

	db, err := atlashcl.Parse([]byte(`
schema "app" {
}

coordination_node "app" "limits" {
  self_check_period       = "PT2S"
  read_consistency_mode   = "strict"
}

coordination_node "locks" {
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	objects, err := db.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.ContentEquals, []schemaext.Object{
		ydbcoordination.DesiredObject("app", "limits", "", ydbcoordination.Spec{SelfCheckPeriodMillis: 2000, ReadConsistencyMode: "strict"}),
		ydbcoordination.DesiredObject("", "locks", "", ydbcoordination.Spec{}),
	})
}

// TestParseCoordinationNode_FailurePath refuses what the annotation refuses,
// and an attribute the block does not have.
func TestParseCoordinationNode_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		block   string
		wantErr string
	}{
		{name: "Ptah's lock node", block: `coordination_node "ptah_locks" {}`,
			wantErr: `(?s).*coordination node ptah_locks at the database root holds Ptah's own locks.*`},
		{name: "a setting the node would not run with", block: `coordination_node "locks" { self_check_period = "PT0.1S" }`,
			wantErr: `(?s).*coordination_node "locks": self_check_period PT0.1S: .*`},
		{name: "an unknown attribute", block: `coordination_node "locks" { session_timeout = "PT1S" }`,
			wantErr: `(?s).*session_timeout.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := atlashcl.Parse([]byte(test.block), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
