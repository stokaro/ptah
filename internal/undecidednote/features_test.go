package undecidednote_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/coverage"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/internal/undecidednote"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff"
)

func TestFeatureLimitsRetainNamespaceAndSubjectInEveryReport(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect("ydb"))
	limits := schemadiff.Diagnostics{Features: []schemaext.UndecidedChange{
		{Kind: "test/changefeed", Subject: builder.TableParts("", "audit.events"), Reason: "current children were not inspected"},
		{Kind: "test/changefeed", Subject: builder.TableParts("audit", "events"), Reason: "desired children were not described"},
	}}
	entries := undecidednote.Entries(limits, "the database")
	c.Assert(entries, qt.HasLen, 2)
	c.Assert(entries[0].Name, qt.Not(qt.Equals), entries[1].Name)
	var output strings.Builder
	undecidednote.Report(&output, limits, "--from", "--to")
	c.Assert(output.String(), qt.Contains, `Warning: test/changefeed "audit.events" could not be compared`)
	c.Assert(output.String(), qt.Contains, `Warning: test/changefeed audit.events could not be compared`)
	c.Assert(output.String(), qt.Contains, "current children were not inspected")
	c.Assert(output.String(), qt.Contains, "desired children were not described")
	c.Assert(output.String(), qt.Contains, "between --from and --to")
	c.Assert(undecidednote.Summary(limits), qt.Equals, "2 comparison limits remain unresolved")
	c.Assert(undecidednote.Findings(limits), qt.DeepEquals, []safety.Finding{
		{Category: undecidednote.FindingCategory, Count: 2, Severity: safety.Warning},
	})

	// Presentation order must not depend on provider order, nor mutate it.
	reversed := limits.Clone()
	reversed.Features[0], reversed.Features[1] = reversed.Features[1], reversed.Features[0]
	c.Assert(undecidednote.Entries(reversed, "the database"), qt.DeepEquals, entries)
	c.Assert(limits.Features[0].Subject.Name.Source, qt.Equals, "audit.events")
}

func TestMixedKnowledgeLimitsCannotReadAsAgreement(t *testing.T) {
	c := qt.New(t)
	limits := schemadiff.Diagnostics{
		Common: []coverage.Object{withheldExtension(coverage.Refused(coverage.Extension))},
		Features: []schemaext.UndecidedChange{{
			Kind: "test/namespace", Subject: objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("", "events"),
			Reason: "the current namespace is unknown",
		}},
	}
	var output strings.Builder
	undecidednote.Report(&output, limits, "the database", "the declaration")
	c.Assert(output.String(), qt.Contains, `extension "citext"`)
	c.Assert(output.String(), qt.Contains, "test/namespace")
	c.Assert(undecidednote.Findings(limits)[0].Count, qt.Equals, 2)
	c.Assert(undecidednote.Summary(limits), qt.Equals, "2 comparison limits remain unresolved")
	c.Assert(undecidednote.Summary(schemadiff.Diagnostics{Features: limits.Features}), qt.Equals, "1 comparison limit remains unresolved")
	undecidednote.Report(nil, limits, "the database", "the declaration")
}

func TestStandaloneNamespaceLimitHasNoInventedObject(t *testing.T) {
	c := qt.New(t)
	limits := schemadiff.Diagnostics{Features: []schemaext.UndecidedChange{{Kind: "test/standalone", Reason: "namespace was not inspected"}}}
	c.Assert(limits.Err(), qt.ErrorMatches, "schema comparison is incomplete: test/standalone namespace: namespace was not inspected")
	c.Assert(undecidednote.Entries(limits, "database"), qt.DeepEquals, []undecidednote.Entry{{Kind: "test/standalone", Name: "namespace", Reason: "namespace was not inspected"}})
	var output strings.Builder
	undecidednote.Report(&output, limits, "database", "declaration")
	c.Assert(output.String(), qt.Contains, "namespace was not inspected")
	c.Assert(undecidednote.Summary(limits), qt.Equals, "1 comparison limit remains unresolved")
}

// TestFeatureLimitsNameTheModelByItsLabel pins how a report names an owner's
// object: by the label its owner states for the model, where there is one,
// and otherwise by the model's kind, then by its identity with a dotted
// component quoted.
func TestFeatureLimitsNameTheModelByItsLabel(t *testing.T) {
	builder := objectidentity.NewBuilder(identifier.ForDialect("postgres"))
	tests := []struct {
		name       string
		diagnostic schemaext.UndecidedChange
		want       string
	}{
		{name: "a labeled model", diagnostic: schemaext.UndecidedChange{Kind: "test/extension", Label: "PostgreSQL extension",
			Subject: objectidentity.ID{Kind: "test/extension", Name: objectidentity.Part{Source: "citext", Normalized: "citext"}},
			Reason:  "it was not read"},
			want: "Warning: PostgreSQL extension citext could not be compared between --from and --to: it was not read.\n"},
		{name: "a model without a label", diagnostic: schemaext.UndecidedChange{Kind: "test/changefeed",
			Subject: builder.TableParts("audit", "events"), Reason: "it was not read"},
			want: "Warning: test/changefeed audit.events could not be compared between --from and --to: it was not read.\n"},
		{name: "a dotted name", diagnostic: schemaext.UndecidedChange{Kind: "test/changefeed", Label: "changefeed",
			Subject: builder.TableParts("", "audit.events"), Reason: "it was not read"},
			want: "Warning: changefeed public.\"audit.events\" could not be compared between --from and --to: it was not read.\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var output strings.Builder

			undecidednote.Report(&output, schemadiff.Diagnostics{Features: []schemaext.UndecidedChange{test.diagnostic}}, "--from", "--to")

			c.Assert(output.String(), qt.Equals, test.want)
		})
	}
}
