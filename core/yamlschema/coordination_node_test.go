package yamlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
)

// TestParse_CoordinationNodes_HappyPath reads YDB coordination nodes from the
// coordination_nodes key, with the settings the annotation reads, in key
// order. A node with no name of its own takes its key.
func TestParse_CoordinationNodes_HappyPath(t *testing.T) {
	c := qt.New(t)

	db, err := yamlschema.Parse([]byte(`
coordination_nodes:
  locks: {}
  limits:
    name: rate_limits
    schema: app
    self_check_period: PT0.5S
    session_grace_period: PT30S
    read_consistency_mode: strict
    attach_consistency_mode: relaxed
    rate_limiter_counters_mode: detailed
`))

	c.Assert(err, qt.IsNil)
	c.Assert(db.CoordinationNodes, qt.DeepEquals, []schemamodel.CoordinationNode{
		{Schema: "app", Name: "rate_limits", Spec: ast.CoordinationNodeSpec{
			SelfCheckPeriodMillis: 500, SessionGracePeriodMillis: 30000,
			ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
		}},
		{Name: "locks"},
	})
}

// TestParse_CoordinationNodes_FailurePath refuses what the annotation refuses,
// with the key named, and an empty setting, which would otherwise read as no
// setting at all.
func TestParse_CoordinationNodes_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "Ptah's lock node", document: "coordination_nodes:\n  ptah_locks: {}\n",
			wantErr: `coordination node "ptah_locks": coordination node ptah_locks at the database root holds Ptah's own locks, .*`},
		{name: "an empty setting", document: "coordination_nodes:\n  locks:\n    read_consistency_mode: \"\"\n",
			wantErr: `coordination node "locks": read_consistency_mode is empty: leave the setting out to keep the node's default`},
		{name: "a period out of range", document: "coordination_nodes:\n  locks:\n    session_grace_period: PT31S\n",
			wantErr: `coordination node "locks": session_grace_period PT31S: .*`},
		{name: "an unknown key", document: "coordination_nodes:\n  locks:\n    session_timeout: PT1S\n",
			wantErr: `(?s)parse YAML schema: .*field session_timeout not found.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := yamlschema.Parse([]byte(test.document))
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
