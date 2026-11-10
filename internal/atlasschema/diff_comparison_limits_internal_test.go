package atlasschema

// White-box testing required: source kinds and whole-server scope are private
// to diff resolution, before the public CLI renders comparison limits.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/internal/atlasreport"
	"ptah.run/internal/atlassource"
)

func TestDiffComparisonLimitsNamesUnreadRolesAndVisibleDatabases(t *testing.T) {
	c := qt.New(t)
	from := atlassource.State{Kind: atlassource.KindDatabase, DB: &catalog.Database{}, WholeServer: true}
	to := atlassource.State{Kind: atlassource.KindDatabase, DB: &catalog.Database{}}
	fromRead := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Refused(coverage.Role))}
	toRead := &catalog.Database{NotDescribed: coverage.Set{}.With(coverage.Refused(coverage.Role))}

	c.Assert(diffComparisonLimits(from, to, fromRead, toRead), qt.DeepEquals, []atlasreport.SchemaDiffLimit{
		{Side: "--from", Kind: "roles and grants", Reason: "the source did not describe this kind"},
		{Side: "--from", Kind: "database list", Reason: "only databases visible to the connected account were read"},
		{Side: "--to", Kind: "roles and grants", Reason: "the source did not describe this kind"},
	})
	c.Assert(diffComparisonLimits(atlassource.State{}, atlassource.State{}, fromRead, toRead), qt.HasLen, 0)
}
