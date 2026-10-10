package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/goschema"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/yamlschema"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/migration/schemadiff"
)

// A Go source states every materialized view's refresh schedule, so a view it
// declares without one asks for a plain view. A YAML document cannot state a
// schedule, and a comparison of the two plain views, the Go source desired and
// the document current, has nothing to decide: the document states no
// schedule it could not read. Undecided, it failed the whole comparison with
// "schema comparison is incomplete" (stokaro/ptah#4278).
func TestCompareSchemas_APlainViewAgainstADocumentThatStatesNoSchedule(t *testing.T) {
	c := qt.New(t)
	desired := must.Must(goschema.ParseSource(builtintest.Annotations(), "views.go", `package models

//ptah:schema:matview name="daily" body="SELECT 1 AS c"
type Daily struct{}
`))
	current := must.Must(yamlschema.Parse([]byte("matviews:\n  daily:\n    body: SELECT 1 AS c\n")))

	diff, err := schemadiff.CompareSchemas(t.Context(), &desired, current, "clickhouse", must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// Two declarations of one view, `daily` and `analytics.daily` where analytics
// is the default schema, are one view whose last declaration stands, as the
// common comparison already reads its body. Refused as a duplicate owner,
// every comparison of such a source failed (stokaro/ptah#4278).
func TestCompareWithOptions_TheLastDeclarationOfAViewStands(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForDialect("clickhouse")
	semantics.DefaultSchema = "analytics"
	desired := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{
		{Name: "daily", Body: "SELECT 1 AS c"},
		{Name: "analytics.daily", Body: "SELECT 2 AS c"},
	}}
	current := &catalog.Database{MatViews: []catalog.MaterializedView{{Schema: "analytics", Name: "daily", Body: "SELECT 2 AS c"}}}

	diff, err := schemadiff.CompareWithOptions(t.Context(), desired, current,
		&config.CompareOptions{Dialect: "clickhouse", IdentifierSemantics: &semantics}, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 0)
	c.Assert(diff.MaterializedViewsAdded, qt.HasLen, 0)
}

// A view a dialect scope takes out of the comparison takes the knowledge of
// its attached settings with it. Kept, an unreadable schedule the read
// recorded for it named an owner the comparison no longer had, and the
// comparison was refused, so scoping the view away could not work around it
// (stokaro/ptah#4278).
func TestCompareWithOptions_AScopedAwayViewTakesItsKnowledgeWithIt(t *testing.T) {
	c := qt.New(t)
	view := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "daily")
	desired := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{
		{Name: "daily", Body: "SELECT 1 AS c", Dialects: []string{"postgres"}},
	}}
	current := &catalog.Database{
		MatViews: []catalog.MaterializedView{{Schema: "analytics", Name: "daily", Body: "SELECT 1 AS c"}},
		FeatureCoverage: must.Must(chschema.RefreshCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: chschema.RefreshKind, Subject: view, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unreadable"}}})),
	}

	diff, err := schemadiff.CompareWithOptions(t.Context(), desired, current, &config.CompareOptions{Dialect: "clickhouse"}, must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

// liveOptions compare against a ClickHouse read of database analytics, whose
// unqualified names the connection resolves there.
func liveOptions() *config.CompareOptions {
	semantics := identifier.ForDialect("clickhouse")
	semantics.DefaultSchema = "analytics"
	return &config.CompareOptions{Dialect: "clickhouse", IdentifierSemantics: &semantics}
}

// unreadableView is a current view whose stored refresh clause the read could
// not capture.
func unreadableView() *catalog.Database {
	view := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "analytics", "daily")
	return &catalog.Database{
		MatViews: []catalog.MaterializedView{{Schema: "analytics", Name: "daily", Body: "SELECT 1 AS c"}},
		FeatureCoverage: must.Must(chschema.RefreshCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete},
			[]schemaext.SubjectCoverage{{Kind: chschema.RefreshKind, Subject: view, Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unreadable"}}})),
	}
}

// A changed body replaces the view, and the replacement is created from the
// declaration. A source that cannot state a schedule keeps the server's by
// adopting it, which needs a schedule the read could capture; one it could
// not would be dropped by the replacement with no change saying so, so the
// replacement is refused (stokaro/ptah#4278).
func TestCompareWithOptions_AReplacementWouldLoseAnUnreadSchedule_FailurePath(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "daily", Body: "SELECT 2 AS c"}}}

	diff, err := schemadiff.CompareWithOptions(t.Context(), desired, unreadableView(), liveOptions(), must.Must(builtin.New()))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err, qt.ErrorMatches, `.*materialized view daily is replaced, and its "ptah.run/clickhouse/refresh" could not be read \(unreadable\).*`)
	c.Assert(diff, qt.IsNil)
}

// The control: the same replacement of a view whose schedule the read
// captured, here as none, is planned. The refusal is about the setting the
// read could not capture, not about replacing a view.
func TestCompareWithOptions_AReplacementOfAViewWithAKnownSchedule(t *testing.T) {
	c := qt.New(t)
	desired := &schemamodel.Database{MaterializedViews: []schemamodel.MaterializedView{{Name: "daily", Body: "SELECT 2 AS c"}}}
	current := unreadableView()
	current.FeatureCoverage = must.Must(chschema.RefreshCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))

	diff, err := schemadiff.CompareWithOptions(t.Context(), desired, current, liveOptions(), must.Must(builtin.New()))

	c.Assert(err, qt.IsNil)
	c.Assert(diff.MaterializedViewsModified, qt.HasLen, 1)
	c.Assert(diff.MaterializedViewsModified[0].Replaces(), qt.IsTrue)
}
