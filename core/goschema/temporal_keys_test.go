package goschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/goschema"
)

// TestParseSource_TemporalKeys_HappyPath reads without_overlaps, period and
// primary_key_without_overlaps off the annotations (stokaro/ptah#6330).
func TestParseSource_TemporalKeys_HappyPath(t *testing.T) {
	c := qt.New(t)

	db := mustParseSource(c, "rooms.go", `package test

//ptah:schema:table name="rooms" primary_key="id,valid_at" primary_key_without_overlaps="true"
//ptah:schema:constraint name="rooms_uq" type="UNIQUE" columns="id,valid_at" without_overlaps="true"
//ptah:schema:constraint name="rooms_pk2" type="PRIMARY KEY" columns="id,valid_at" without_overlaps="true"
//ptah:schema:constraint name="rooms_fk" type="FOREIGN KEY" columns="room_id,valid_at" foreign_table="rooms" foreign_columns="id,valid_at" period="true"
//ptah:schema:constraint name="rooms_plain" type="UNIQUE" columns="id,valid_at"
type Room struct{}
`)

	c.Assert(db.Tables, qt.HasLen, 1)
	c.Assert(db.Tables[0].PrimaryKeyWithoutOverlaps, qt.IsTrue)
	c.Assert(db.Constraints, qt.HasLen, 4)
	c.Assert(db.Constraints[0].WithoutOverlaps, qt.IsTrue)
	c.Assert(db.Constraints[1].WithoutOverlaps, qt.IsTrue)
	c.Assert(db.Constraints[2].Period, qt.IsTrue)
	c.Assert(db.Constraints[2].WithoutOverlaps, qt.IsFalse)
	c.Assert(db.Constraints[3].WithoutOverlaps, qt.IsFalse)
	c.Assert(db.Constraints[3].Period, qt.IsFalse)
}

// TestParseSource_TemporalKeys_FailurePath refuses a clause on the wrong kind of
// constraint, a key too short to carry it, and a value that is not a boolean.
func TestParseSource_TemporalKeys_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		comment string
		want    string
	}{
		{
			name:    "without_overlaps on a foreign key",
			comment: `//ptah:schema:constraint name="f" type="FOREIGN KEY" columns="a,b" foreign_table="p" foreign_columns="a,b" without_overlaps="true"`,
			want:    `(?s).*constraint "f" is a FOREIGN KEY, and without_overlaps belongs to a PRIMARY KEY or UNIQUE constraint.*`,
		},
		{
			name:    "period on a unique constraint",
			comment: `//ptah:schema:constraint name="u" type="UNIQUE" columns="a,b" period="true"`,
			want:    `(?s).*constraint "u" is a UNIQUE, and period belongs to a FOREIGN KEY constraint.*`,
		},
		{
			name:    "without_overlaps on one column",
			comment: `//ptah:schema:constraint name="u" type="UNIQUE" columns="valid_at" without_overlaps="true"`,
			want:    `(?s).*constraint "u" has without_overlaps and fewer than two columns.*`,
		},
		{
			name:    "primary_key_without_overlaps on one column",
			comment: `//ptah:schema:table name="t" primary_key="valid_at" primary_key_without_overlaps="true"`,
			want:    `(?s).*table "t" has primary_key_without_overlaps and a primary_key of fewer than two columns.*`,
		},
		{
			name:    "without_overlaps that is not a boolean",
			comment: `//ptah:schema:constraint name="u" type="UNIQUE" columns="id,valid_at" without_overlaps="yes"`,
			want:    `(?s).*without_overlaps="yes" is not a boolean; write true or false.*`,
		},
		{
			name:    "an empty without_overlaps",
			comment: `//ptah:schema:constraint name="u" type="UNIQUE" columns="id,valid_at" without_overlaps=""`,
			want:    `(?s).*without_overlaps="" is not a boolean; write true or false.*`,
		},
		{
			name:    "without_overlaps in another case",
			comment: `//ptah:schema:constraint name="u" type="UNIQUE" columns="id,valid_at" without_overlaps="TRUE"`,
			want:    `(?s).*without_overlaps="TRUE" is not a boolean; write true or false.*`,
		},
		{
			name: "period with an action other than NO ACTION",
			comment: `//ptah:schema:constraint name="f" type="FOREIGN KEY" columns="a,b" foreign_table="p" ` +
				`foreign_columns="a,b" on_delete="CASCADE" period="true"`,
			want: `(?s).*constraint "f" has period and ON DELETE CASCADE; a PERIOD foreign key takes only NO ACTION.*`,
		},
		{
			name:    "period that is not a boolean",
			comment: `//ptah:schema:constraint name="f" type="FOREIGN KEY" columns="a,b" foreign_table="p" foreign_columns="a,b" period="1"`,
			want:    `(?s).*period="1" is not a boolean; write true or false.*`,
		},
		{
			name:    "primary_key_without_overlaps that is not a boolean",
			comment: `//ptah:schema:table name="t" primary_key="id,valid_at" primary_key_without_overlaps="valid_at"`,
			want:    `(?s).*primary_key_without_overlaps="valid_at" is not a boolean; write true or false.*`,
		},
		{
			name:    "primary_key_without_overlaps with no primary_key",
			comment: `//ptah:schema:table name="t" primary_key_without_overlaps="true"`,
			want:    `(?s).*table "t" has primary_key_without_overlaps and a primary_key of fewer than two columns.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, err := goschema.ParseSource(rowSecurityOwners, "bad.go", "package test\n\n"+test.comment+"\ntype T struct{}\n")

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
