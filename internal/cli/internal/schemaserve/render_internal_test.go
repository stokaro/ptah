package schemaserve

// White-box testing required: the page reaches an undecided object only when
// the database it compares against refuses a catalog read, which the e2e suite
// measures for the verbs that report one. observationOf grades the comparison
// and render draws it, so what the page says about a comparison is pinned here.

import (
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/internal/cli/internal/schemaops"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// TestRender_NamesWhatTheComparisonCouldNotCheck holds the page to the rule the
// failure banner keeps: a view that could not look does not say the database
// matches. A withheld role is counted as a finding and listed with the reason
// the read gave (stokaro/ptah#3844).
func TestRender_NamesWhatTheComparisonCouldNotCheck(t *testing.T) {
	c := qt.New(t)
	withheld := coverage.Refused(coverage.Role)
	withheld.Name = "reporter"
	result := &schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}, Undecided: schemadiff.Diagnostics{Common: []coverage.Object{withheld}}}
	s := &server{opts: Options{DatabaseURL: "mysql://app@db/app", Now: time.Now}}

	page := s.render(observationOf(result, time.Unix(0, 0)))

	c.Assert(page, qt.Not(qt.Contains), "The database matches the declared schema.")
	c.Assert(page, qt.Contains, `<td class="name">undecided</td><td>1</td>`)
	c.Assert(page, qt.Contains, `<h3>Undecided</h3>`)
	c.Assert(page, qt.Contains, `<td>role</td><td class="name">reporter</td>`+
		`<td>the database does not describe role objects because the read was refused`+
		` the catalog that would have listed them</td>`)
}

// TestRender_SaysTheDatabaseMatchesWhenNothingWasWithheld is the control.
func TestRender_SaysTheDatabaseMatchesWhenNothingWasWithheld(t *testing.T) {
	c := qt.New(t)
	s := &server{opts: Options{DatabaseURL: "mysql://app@db/app", Now: time.Now}}

	page := s.render(observationOf(&schemaops.CompareResult{Diff: &difftypes.SchemaDiff{}}, time.Unix(0, 0)))

	c.Assert(page, qt.Contains, "The database matches the declared schema.")
	c.Assert(page, qt.Not(qt.Contains), "<h3>Undecided</h3>")
}
