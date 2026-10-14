package atlasreport_test

import (
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgtype"
	"ptah.run/internal/atlasreport"
)

// typeLimitLine matches a not-described line of a PostgreSQL user type kind.
var typeLimitLine = regexp.MustCompile(`(?m)^// ptah:not-described (?:domain|composite|range)\b`)

// TestSchemaInspect_ASQLiteReadWritesNoTypeLimit pins that inspecting a target
// without domains, composite types or range types writes no not-described line
// for them, on the native and the Atlas-compatible framing. Such a line would
// declare a gap in a family SQLite does not have, and the pinned binary's
// output carries none.
func TestSchemaInspect_ASQLiteReadWritesNoTypeLimit(t *testing.T) {
	for _, test := range []struct {
		name    string
		framing bool
	}{
		{name: "native"},
		{name: "compatibility", framing: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			format := must.Must(atlasreport.NormalizeSchemaInspectFormat("hcl"))

			output, err := atlasreport.RenderSchemaInspect(format, emptySQLiteInspectReport(c, test.framing))

			c.Assert(err, qt.IsNil)
			c.Assert(typeLimitLine.FindAllString(output.Text, -1), qt.HasLen, 0, qt.Commentf("output:\n%s", output.Text))
		})
	}
}

// TestSchemaInspect_APostgreSQLReadKeepsItsTypeLimits is the control the test
// above needs: a PostgreSQL read that could not reach its user types writes a
// line for each kind, so a writer that never wrote one would not pass both.
func TestSchemaInspect_APostgreSQLReadKeepsItsTypeLimits(t *testing.T) {
	c := qt.New(t)
	unread := coverage.Set{}
	for _, kind := range []coverage.Kind{coverage.Domain, coverage.Composite} {
		unread = unread.With(coverage.Object{Kind: kind, Reason: coverage.Unsupported, Provenance: coverage.DerivedFromTarget})
	}
	ranges := must.Must(pgtype.Coverage(pgtype.RangeKind, schemaext.Desired,
		schemaext.Knowledge{State: schemaext.Uninspected, Reason: "range type dependencies are unavailable"}, nil))
	report := newInspectReport(c,
		&schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "public"}}, NotDescribed: unread, FeatureCoverage: ranges},
		&catalog.Database{Schemas: []catalog.Schema{{Name: "public"}}},
		catalog.ServerInfo{Dialect: platform.Postgres, Schema: "public"},
		nil,
		atlasreport.SchemaInspectReportOptions{DescribeSchemas: true},
	)
	format := must.Must(atlasreport.NormalizeSchemaInspectFormat("hcl"))

	output, err := atlasreport.RenderSchemaInspect(format, report)

	c.Assert(err, qt.IsNil)
	c.Assert(typeLimitLine.FindAllString(output.Text, -1), qt.DeepEquals, []string{
		"// ptah:not-described composite", "// ptah:not-described domain", "// ptah:not-described range",
	}, qt.Commentf("output:\n%s", output.Text))
}
