package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// TestParseCoordinationNode reads the YDB coordination node block: the schema
// label or attribute, and the settings as the annotation spells them.
func TestParseCoordinationNode(t *testing.T) {
	c := qt.New(t)

	db, err := parseWithOwners([]byte(`
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
			db, err := parseWithOwners([]byte(test.block), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}

// TestParsePermissionOnACoordinationNode reads a `permission` and a `revoke`
// whose `for` names a coordination node block, by its directory and name or
// by its name at the database root, as grants on the node.
func TestParsePermissionOnACoordinationNode(t *testing.T) {
	c := qt.New(t)

	db, err := parseWithOwners([]byte(`
schema "app" {
}

coordination_node "app" "limits" {
}

coordination_node "locks" {
}

permission {
  to         = "reader"
  for        = coordination_node.app.limits
  privileges = ["ydb.generic.use"]
}

revoke {
  from       = "reader"
  for        = coordination_node.locks
  privileges = ["ydb.generic.full"]
}
`), "schema.hcl")

	c.Assert(err, qt.IsNil)
	c.Assert(db.Grants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "reader", Privileges: []string{"YDB.GENERIC.USE"}, OnObject: "app.limits", ObjectKind: "COORDINATION NODE"},
	})
	c.Assert(db.RevokedGrants, qt.DeepEquals, []schemamodel.Grant{
		{Role: "reader", Privileges: []string{"YDB.GENERIC.FULL"}, OnObject: "locks", ObjectKind: "COORDINATION NODE"},
	})
}

// TestParsePermissionOnAnUnownedBlock refuses a `for` that names a block no
// owner's grant kind covers, rather than reading it as a grant on nothing.
func TestParsePermissionOnAnUnownedBlock(t *testing.T) {
	c := qt.New(t)

	db, err := parseWithOwners([]byte(`
permission {
  to         = "reader"
  for        = topic.app.events
  privileges = ["ydb.generic.read"]
}
`), "schema.hcl")

	c.Assert(err, qt.ErrorMatches, `(?s).*permission requires table, view, schema, sequence, function or procedure target, `+
		`or an object an owner's block declares.*`)
	c.Assert(db, qt.IsNil)
}
