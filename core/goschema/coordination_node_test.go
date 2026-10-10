package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbcoordination"
)

// coordinationNodeSource is an entity declaring a coordination node with
// attributes.
func coordinationNodeSource(attributes string) string {
	return `package entities

//ptah:schema:coordinationnode ` + attributes + `
type Locks struct{}
`
}

// TestParseSource_CoordinationNode_HappyPath reads a YDB coordination node:
// its directory, its name, and each setting it sets, the periods as ISO 8601
// durations and the modes in either case. A setting left out stays unset.
func TestParseSource_CoordinationNode_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		want       schemaext.Object
	}{
		{name: "a node with the defaults", attributes: `name="locks"`,
			want: ydbcoordination.DesiredObject("", "locks", "Locks", ydbcoordination.Spec{})},
		{
			name: "every setting",
			attributes: `name="limits" schema="app" self_check_period="PT2S" session_grace_period="PT15S" ` +
				`read_consistency_mode="STRICT" attach_consistency_mode="relaxed" rate_limiter_counters_mode="detailed"`,
			want: ydbcoordination.DesiredObject("app", "limits", "Locks", ydbcoordination.Spec{
				SelfCheckPeriodMillis: 2000, SessionGracePeriodMillis: 15000,
				ReadConsistencyMode: "strict", AttachConsistencyMode: "relaxed", RateLimiterCountersMode: "detailed",
			}),
		},
		{name: "Ptah's lock node name in a directory", attributes: `name="ptah_locks" schema="app"`,
			want: ydbcoordination.DesiredObject("app", "ptah_locks", "Locks", ydbcoordination.Spec{})},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(noOwners, "locks.go", coordinationNodeSource(test.attributes))
			c.Assert(err, qt.IsNil)
			objects, err := db.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.DeepEquals, []schemaext.Object{test.want})
			c.Assert(db.FeatureCoverage.Lookup(ydbcoordination.Kind, test.want.Ref).State, qt.Equals, schemaext.Complete)
		})
	}
}

// TestParseSource_CoordinationNode_FailurePath refuses a node where it is
// written: no name, an attribute the directive does not have, Ptah's own lock
// node, a server path, and a setting the node would not run with, each naming
// the attribute it is about.
func TestParseSource_CoordinationNode_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		attributes string
		wantErr    string
		wantIs     error
	}{
		{name: "no name", attributes: `schema="app"`,
			wantErr: `.*missing required annotation attribute "name" on //ptah:schema:coordinationnode at Locks.*`,
			wantIs:  ptaherr.ErrMissingRequiredAttribute},
		{name: "an unknown attribute", attributes: `name="locks" session_timeout="PT1S"`,
			wantErr: `.*unknown annotation attribute "session_timeout" on //ptah:schema:coordinationnode at Locks.*`,
			wantIs:  ptaherr.ErrUnknownAttribute},
		{name: "Ptah's lock node", attributes: `name="ptah_locks"`,
			wantErr: `.*coordination node ptah_locks at the database root holds Ptah's own locks, .* on //ptah:schema:coordinationnode at Locks`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a server path", attributes: `name="locks" schema=".sys"`,
			wantErr: `.*coordination node .sys/locks has the path segment ".sys"; .*`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a period that is not a duration", attributes: `name="locks" self_check_period="2s"`,
			wantErr: `.*self_check_period "2s": a period is an ISO 8601 duration such as PT1S or PT0.5S on //ptah:schema:coordinationnode at Locks`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a mode the node does not have", attributes: `name="locks" attach_consistency_mode="eventual"`,
			wantErr: `.*attach_consistency_mode "eventual": the mode is "strict" or "relaxed" on //ptah:schema:coordinationnode at Locks`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
		{name: "a combination the node would not run", attributes: `name="locks" self_check_period="PT10S"`,
			wantErr: `.*session_grace_period PT10S: .* on //ptah:schema:coordinationnode at Locks`,
			wantIs:  ptaherr.ErrInvalidAttributeValue},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := goschema.ParseSource(noOwners, "locks.go", coordinationNodeSource(test.attributes))
			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
