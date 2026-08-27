package sqlutil_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/sqlutil"
)

// TestCheckOptionRequestsCheck names every word a reader in this tree puts on a
// catalog view, and the shape of an unknown one.
//
// The empty string is not a placeholder: Oracle's view query selects no check
// option at all, because `all_views` carries it as a constraint rather than a
// column, so every Oracle view arrives with one. It has to mean the same as
// NONE, and a rule that read it as "something other than NONE" would put
// WITH CHECK OPTION on every Oracle view.
func TestCheckOptionRequestsCheck(t *testing.T) {
	tests := []struct {
		name        string
		checkOption string
		want        bool
		why         string
	}{
		{
			name:        "an absent option is no option",
			checkOption: "",
			want:        false,
			why:         "the Oracle reader selects no check option, so its views carry none",
		},
		{
			name:        "NONE is no option",
			checkOption: "NONE",
			want:        false,
			why:         "what the MySQL, ClickHouse, SQLite and SQL Server readers write for a view without the clause",
		},
		{
			name:        "LOCAL asks for one",
			checkOption: "LOCAL",
			want:        true,
			why:         "information_schema.VIEWS reports it for a view declared WITH LOCAL CHECK OPTION",
		},
		{
			name:        "CASCADED asks for one",
			checkOption: "CASCADED",
			want:        true,
			why:         "the same, for the cascaded form",
		},
		{
			name:        "an unknown word asks for one",
			checkOption: "CASCADE",
			want:        true,
			why:         "a dialect equivalent is a clause the view declares, and defaulting it to false would drop it silently",
		},
		{
			name:        "the two ways of saying no are matched without regard to case",
			checkOption: "none",
			want:        false,
			why:         "a reader is free to report the catalog's own casing",
		},
		{
			name:        "surrounding space does not make an option",
			checkOption: "  ",
			want:        false,
			why:         "a padded empty column is still an absent one",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(sqlutil.CheckOptionRequestsCheck(test.checkOption), qt.Equals, test.want,
				qt.Commentf("%s", test.why))
		})
	}
}
