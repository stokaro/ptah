package compare

// White-box testing required: notNullConstraintNameChange is unexported, and
// the case that matters -- an omitted name producing no change against a
// populated catalog name -- is invisible from outside because it produces
// exactly the same diff as two columns that agree.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestNotNullConstraintNameChange_OnlyAnExplicitNameIsCompared pins the
// omitted-attribute rule on this attribute.
//
// The empty-desired row is the load-bearing one. PostgreSQL 18 names EVERY NOT
// NULL and offers no catalog flag separating an author-supplied name from a
// generated one, so on that target the current side is populated for every
// non-nullable column in the database. Comparing an omitted declaration against
// it would report a rename on every column of every table nobody touched, and
// no apply could settle it: the next read returns the new generated name and
// reports the difference again (stokaro/ptah#2161).
func TestNotNullConstraintNameChange_OnlyAnExplicitNameIsCompared(t *testing.T) {
	tests := []struct {
		name       string
		desired    string
		current    string
		generated  string
		wantChange bool
		why        string
	}{
		{
			name:       "an omitted name leaves the actual one unmanaged",
			desired:    "",
			current:    "widget_a_not_null",
			wantChange: false,
			why:        "the declaration did not ask about the name, so nothing about it changed",
		},
		{
			name:       "an explicit name that differs is a change",
			desired:    "c_new",
			current:    "c_old",
			wantChange: true,
			why:        "the declaration manages the name and the catalog holds another",
		},
		{
			name:       "an explicit name that agrees is not",
			desired:    "c_keep",
			current:    "c_keep",
			wantChange: false,
			why:        "nothing to do",
		},
		{
			name:       "an explicit name with nothing to rename from is still a change",
			desired:    "c_new",
			current:    "",
			wantChange: true,
			why: "the target reported no name; the constraint has to be established under " +
				"the declared one, which the planner decides rather than the comparator",
		},
		{
			name:       "the generated name beside no name is not a change",
			desired:    "widget_a_not_null",
			current:    "",
			generated:  "widget_a_not_null",
			wantChange: false,
			why: "the current side left the name to the server, and the server gives it " +
				"the desired one",
		},
		{
			name:       "a numbered name beside no name is a change",
			desired:    "widget_a_not_null1",
			current:    "",
			generated:  "widget_a_not_null",
			wantChange: true,
			why:        "the server numbers the name only when another constraint holds the plain one",
		},
		{
			name:       "the generated name beside another name is a change",
			desired:    "widget_a_not_null",
			current:    "c_old",
			generated:  "widget_a_not_null",
			wantChange: true,
			why:        "the current side named the constraint, so a rename is planned",
		},
		{
			name:       "a name beside no name on a target that generates none is a change",
			desired:    "widget_a_not_null",
			current:    "",
			generated:  "",
			wantChange: true,
			why:        "no server would give the constraint that name",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			change := notNullConstraintNameChange(test.desired, test.current, test.generated)

			c.Assert(change != nil, qt.Equals, test.wantChange, qt.Commentf("%s", test.why))
		})
	}
}

// TestNotNullConstraintNameChange_CarriesBothSides is what the planner needs:
// a rename statement names the old constraint as well as the new one.
func TestNotNullConstraintNameChange_CarriesBothSides(t *testing.T) {
	c := qt.New(t)

	change := notNullConstraintNameChange("c_new", "c_old", "widget_a_not_null")

	c.Assert(change, qt.IsNotNil)
	c.Assert(change.Current, qt.Equals, "c_old")
	c.Assert(change.Desired, qt.Equals, "c_new")
}
