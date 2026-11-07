package goschematodb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/goschematodb"
)

// TestToDBSchema_ReportsTheCheckOptionLevel pins that a document converted to
// stand for a database reports each view's check option as the server would:
// CASCADED for WITH CHECK OPTION and LOCAL for WITH LOCAL CHECK OPTION. A
// file-to-file comparison reads this side as the database, so reporting LOCAL
// for every check option made a CASCADED declaration compare as a change.
func TestToDBSchema_ReportsTheCheckOptionLevel(t *testing.T) {
	tests := []struct {
		name      string
		withCheck bool
		local     bool
		want      string
	}{
		{name: "CASCADED", withCheck: true, want: "CASCADED"},
		{name: "LOCAL", withCheck: true, local: true, want: "LOCAL"},
		{name: "no check option", want: "NONE"},
		{name: "LOCAL without a check option", local: true, want: "NONE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db := &schemamodel.Database{Views: []schemamodel.View{{Name: "own_rows", Body: "SELECT 1", WithCheck: test.withCheck, LocalCheck: test.local}}}

			got := must.Must(goschematodb.ToDBSchema(t.Context(), db, platform.Postgres, must.Must(builtin.New())))

			c.Assert(got.Views, qt.HasLen, 1)
			c.Assert(got.Views[0].CheckOption, qt.Equals, test.want)
		})
	}
}
