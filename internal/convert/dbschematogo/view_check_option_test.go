package dbschematogo_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/engine/builtin"
	"ptah.run/internal/convert/dbschematogo"
)

// TestConvertViews_CarriesTheCheckOptionLevel pins that a view read from a
// database keeps the level of its check option. Without it a LOCAL view was
// exported, inspected and compared as one with a check option, which every
// writer spelled as the stricter CASCADED check (stokaro/ptah#5647).
func TestConvertViews_CarriesTheCheckOptionLevel(t *testing.T) {
	tests := []struct {
		name          string
		checkOption   string
		wantWithCheck bool
		wantLocal     bool
	}{
		{name: "LOCAL", checkOption: "LOCAL", wantWithCheck: true, wantLocal: true},
		{name: "CASCADED", checkOption: "CASCADED", wantWithCheck: true},
		{name: "NONE", checkOption: "NONE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted := must.Must(dbschematogo.ConvertDBSchemaToGoSchema(t.Context(), &catalog.Database{
				Views: []catalog.View{{Schema: "public", Name: "own_rows", Body: "SELECT 1", CheckOption: test.checkOption}},
			}, "postgres", must.Must(builtin.New())))

			c.Assert(converted.Views, qt.HasLen, 1)
			c.Assert(converted.Views[0].WithCheck, qt.Equals, test.wantWithCheck)
			c.Assert(converted.Views[0].LocalCheck, qt.Equals, test.wantLocal)
		})
	}
}
