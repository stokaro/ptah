package migrator

// White-box testing required: attachment recognition and observation updates
// must preserve identifier spelling, action boundaries, and schema isolation.
// The public migration API requires a live PostgreSQL catalog; its behavior is
// covered in integration/gonative/index_constraint_attachment_live_test.go.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestPostgresIndexConstraintAttachments(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []postgresIndexConstraintAttachment
	}{
		{
			name: "unique constraint",
			sql:  `ALTER TABLE members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key;`,
			want: []postgresIndexConstraintAttachment{{Index: postgresIndexRef{Table: "members", Name: "temporary_key"}, Name: "members_key"}},
		},
		{
			name: "qualified primary key",
			sql:  `ALTER TABLE IF EXISTS ONLY App.Members ADD CONSTRAINT Members_Key PRIMARY KEY USING INDEX Temporary_Key DEFERRABLE;`,
			want: []postgresIndexConstraintAttachment{{Index: postgresIndexRef{Schema: "app", Table: "members", Name: "temporary_key"}, Name: "members_key"}},
		},
		{
			name: "quoted identifiers",
			sql:  `ALTER TABLE "App"."Members" ADD CONSTRAINT "Members""Key" UNIQUE USING INDEX "TemporaryKey";`,
			want: []postgresIndexConstraintAttachment{{Index: postgresIndexRef{Schema: "App", Table: "Members", Name: "TemporaryKey"}, Name: `Members"Key`}},
		},
		{
			name: "multiple alter actions",
			sql:  `ALTER TABLE members ADD COLUMN note text CHECK (note IN ('a', 'b')), ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key, ADD CONSTRAINT other_key PRIMARY KEY USING INDEX other_index;`,
			want: []postgresIndexConstraintAttachment{
				{Index: postgresIndexRef{Table: "members", Name: "temporary_key"}, Name: "members_key"},
				{Index: postgresIndexRef{Table: "members", Name: "other_index"}, Name: "other_key"},
			},
		},
		{
			name: "comments and inheritance marker",
			sql:  `/* attach */ ALTER TABLE members * ADD CONSTRAINT members_key UNIQUE /* catalog name */ USING INDEX temporary_key;`,
			want: []postgresIndexConstraintAttachment{{Index: postgresIndexRef{Table: "members", Name: "temporary_key"}, Name: "members_key"}},
		},
		{name: "unnamed attachment keeps its name", sql: `ALTER TABLE members ADD UNIQUE USING INDEX temporary_key;`},
		{name: "column constraint creates its own index", sql: `ALTER TABLE members ADD CONSTRAINT members_key UNIQUE (id);`},
		{name: "check constraint", sql: `ALTER TABLE members ADD CONSTRAINT positive CHECK (id > 0);`},
		{name: "attachment text in a literal", sql: `ALTER TABLE members ADD COLUMN note text DEFAULT 'ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key';`},
		{name: "attachment text in a comment", sql: `-- ALTER TABLE members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key;`},
		{name: "procedural block", sql: `DO $$ BEGIN ALTER TABLE members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key; END $$;`},
		{name: "different statement", sql: `SELECT 'ALTER TABLE members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key';`},
		{name: "incomplete primary key", sql: `ALTER TABLE members ADD CONSTRAINT members_key PRIMARY KEY USING INDEX;`},
		{name: "empty statement"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(postgresIndexConstraintAttachments(test.sql), qt.DeepEquals, test.want)
		})
	}
}

func TestPostgresIndexObservationConstraintAttachment(t *testing.T) {
	tests := []struct {
		name   string
		schema string
		want   []postgresIndexState
	}{
		{
			name:   "qualified target preserves other identities",
			schema: "app",
			want: []postgresIndexState{
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "members_key"},
				{TargetFound: true, TargetSchema: "audit", TargetTable: "members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "other_members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "other_key"},
			},
		},
		{
			name: "unknown ambient path preserves every candidate",
			want: []postgresIndexState{
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
			observation := &postgresIndexObservation{identities: []postgresIndexState{
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "audit", TargetTable: "members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "other_members", Name: "temporary_key"},
				{TargetFound: true, TargetSchema: "app", TargetTable: "members", Name: "other_key"},
			}}
			observation.attachConstraint(postgresIndexConstraintAttachment{
				Index: postgresIndexRef{Schema: test.schema, Table: "members", Name: "temporary_key"}, Name: "members_key",
			})
			c.Assert(observation.identities, qt.DeepEquals, test.want)
		})
	}
}
