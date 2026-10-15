package migrator

// White-box testing required: an observation's update when a statement
// attaches an index to a constraint must preserve identifier spelling and
// schema isolation, and the observation is unexported migrator state.
// The public migration API requires a live PostgreSQL catalog; its behavior is
// covered in integration/gonative/index_constraint_attachment_live_test.go.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migratorext"
)

func TestPostgresIndexObservationConstraintAttachment(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   []migratorext.IndexState
	}{
		{
			name:   "qualified target preserves other identities",
			schema: "app",
			want: []migratorext.IndexState{
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "members_key"},
				{TargetFound: true, TargetSchema: "audit", TargetTable: "members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "other_members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "other_key"},
			},
		},
		{
			name: "unknown ambient path preserves every candidate",
			want: []migratorext.IndexState{
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "members_key"},
				{TargetFound: true, TargetSchema: "audit", TargetTable: "members", Name: "members_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "other_members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "other_key"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			observation := &postgresIndexObservation{identities: []migratorext.IndexState{
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "audit", TargetTable: "members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "other_members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "other_key"},
			}}
			observation.attachConstraint(migratorext.IndexAttachment{
				Index: migratorext.IndexRef{Schema: test.schema, Table: "members", Name: "temporary_key"}, Name: "members_key",
			})
			c.Assert(observation.identities, qt.DeepEquals, test.want)
		})
	}
}
