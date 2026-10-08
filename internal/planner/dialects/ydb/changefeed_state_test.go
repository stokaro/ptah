package ydb_test

import (
	"context"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/engine/builtin"
	"ptah.run/internal/planner/dialects/ydb"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestGenerateMigrationAST_Changefeeds_RebuildRequiresCapturedKnowledge(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*testing.T, *difftypes.TableDiff)
		want   string
	}{
		{name: "missing observed operand", mutate: func(_ *testing.T, table *difftypes.TableDiff) {
			table.Current = schemacapture.TableObservation{}
		}, want: "no observed table state"},
		{name: "unknown observed namespace", mutate: func(_ *testing.T, table *difftypes.TableDiff) {
			table.Current.FeatureCoverage = schemaext.Coverage{}
		}, want: "no matching source coverage"},
		{name: "unknown desired namespace", mutate: func(_ *testing.T, table *difftypes.TableDiff) {
			table.Desired.FeatureCoverage = schemaext.Coverage{}
		}, want: "no matching source coverage"},
		{name: "format cannot inspect namespace", mutate: func(t *testing.T, table *difftypes.TableDiff) {
			parent := objectidentity.NewBuilder(identifier.ForDialect("ydb")).TableParts("app", "items")
			table.Current.FeatureCoverage = feedCoverage(t, schemaext.Observed, schemaext.SubjectCoverage{
				Kind: ydbschema.ChangefeedKind, Subject: parent,
				Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "source format has no streams"}})
		}, want: "namespace is not fully described: source format has no streams"},
		{name: "one unread stream", mutate: func(t *testing.T, table *difftypes.TableDiff) {
			table.Current.FeatureCoverage = feedCoverage(t, schemaext.Observed, schemaext.SubjectCoverage{
				Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("app", "items", "updates"),
				Knowledge: schemaext.Knowledge{State: schemaext.Unrepresentable, Reason: "unsupported settings"}})
		}, want: "a changefeed is not fully described: unsupported settings"},
		{name: "disabled stream cannot be recreated", mutate: func(t *testing.T, table *difftypes.TableDiff) {
			stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Disabled: true}
			table.Current = observedFeeds(t, "app", "items", stream)
			table.Desired = declaredFeeds(t, table.Desired, stream)
		}, want: "no statement that disables"},
		{name: "different captured parent", mutate: func(t *testing.T, table *difftypes.TableDiff) {
			table.Current = observedFeeds(t, "app", "other")
		}, want: "observed planning table disagrees with its subject"},
		{name: "unchanged attached feature", mutate: func(t *testing.T, table *difftypes.TableDiff) {
			c := qt.New(t)
			facets, err := schemaext.NewFacets(&ydbschema.DesiredChangefeed{Spec: ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}})
			c.Assert(err, qt.IsNil)
			table.Desired.Table.Facets = facets
		}, want: "changefeeds require named objects"},
		{name: "captured child of another table", mutate: func(t *testing.T, table *difftypes.TableDiff) {
			other := observedFeeds(t, "other", "items", ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"})
			table.Current.OwnedObjects = other.OwnedObjects
		}, want: "no rebuild handler for the captured feature object"},
		{name: "present stream marked absent", mutate: func(t *testing.T, table *difftypes.TableDiff) {
			table.Current.FeatureCoverage = feedCoverage(t, schemaext.Observed, schemaext.SubjectCoverage{
				Kind: ydbschema.ChangefeedKind, Subject: ydbschema.ChangefeedRef("app", "items", "updates"),
				Knowledge: schemaext.Knowledge{State: schemaext.Absent}})
		}, want: "operand is not known"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			stream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
			diff := modified(t, difftypes.TableDiff{TableName: "app.items",
				Desired:         declaredFeeds(t, appItems(field("n", "BIGINT", true)), stream),
				Current:         observedFeeds(t, "app", "items", stream),
				ColumnsModified: []difftypes.ColumnDiff{{ColumnName: "n", Changes: map[string]string{"type": "Int32 -> Int64"}}}})
			test.mutate(t, &diff.TablesModified[0])
			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).WithTableRebuild(true).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				diff,
			)
			c.Assert(err, qt.ErrorMatches, "(?s).*"+regexp.QuoteMeta(test.want)+".*")
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateMigrationAST_Changefeeds_RejectInconsistentRecords(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(*difftypes.TableDiff)
		want   string
	}{
		{name: "other parent", mutate: func(table *difftypes.TableDiff) {
			table.FeatureChanges[0].Subject = ydbschema.ChangefeedRef("", "other", "updates")
		}, want: "feature parent disagrees with the changed table"},
		{name: "other table name", mutate: func(table *difftypes.TableDiff) {
			table.TableName = "other"
		}, want: "feature parent disagrees with the changed table"},
		{name: "other payload name", mutate: func(table *difftypes.TableDiff) {
			table.FeatureChanges[0].Value.(*ydbdiff.Changefeed).After.Spec.Name = "other"
		}, want: "disagrees with its subject or table"},
		{name: "other desired value", mutate: func(table *difftypes.TableDiff) {
			table.FeatureChanges[0].Value.(*ydbdiff.Changefeed).After.Spec.Mode = "KEYS_ONLY"
		}, want: "disagrees with its captured table state"},
		{name: "duplicate subject", mutate: func(table *difftypes.TableDiff) {
			table.FeatureChanges = append(table.FeatureChanges, table.FeatureChanges[0])
		}, want: "duplicate planning input"},
		{name: "unknown absence", mutate: func(table *difftypes.TableDiff) {
			table.Current.FeatureCoverage = schemaext.Coverage{}
		}, want: "operand is not known"},
		{name: "missing capture", mutate: func(table *difftypes.TableDiff) {
			table.Current = schemacapture.TableObservation{}
		}, want: "require captured desired and observed tables"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := changefeedsChanged(t, []ydbschema.ChangefeedSpec{{Name: "updates", Mode: "UPDATES", Format: "JSON"}}, nil)
			test.mutate(&diff.TablesModified[0])
			nodes, err := ydb.NewWithCapabilities(capability.YDB262()).GenerateMigrationAST(
				context.Background(), must.Must(builtin.New()),
				diff,
			)
			c.Assert(err, qt.ErrorMatches, "(?s).*"+regexp.QuoteMeta(test.want)+".*")
			c.Assert(nodes, qt.IsNil)
		})
	}
}

func TestGenerateMigrationAST_Changefeeds_ConstraintOnlyRebuild(t *testing.T) {
	c := qt.New(t)
	old := ydbschema.ChangefeedSpec{Name: "old", Mode: "KEYS_ONLY", Format: "JSON"}
	newStream := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON"}
	diff := &difftypes.SchemaDiff{
		DeclaredConstraintHosts: []schemacapture.TableDeclaration{declaredFeeds(t, appItems(field("label", "TEXT", true)), newStream)},
		ObservedConstraintHosts: []schemacapture.TableObservation{observedFeeds(t, "app", "items", old)},
		ConstraintsAdded:        difftypes.ConstraintAdditions{{Name: "items_pk", TableName: "app.items", Type: "PRIMARY KEY"}},
	}
	sql := renderRebuild(c, capability.YDB262(), diff)
	c.Assert(sql, qt.Contains, "DROP CHANGEFEED `old`;")
	c.Assert(sql, qt.Contains, "ADD CHANGEFEED `updates`")
	c.Assert(sql, qt.Not(qt.Contains), "DROP CHANGEFEED `updates`")
	c.Assert(sql, qt.Not(qt.Contains), "ADD CHANGEFEED `old`")
	c.Assert(sql, qt.Not(qt.Contains), "__ptah_rebuild_items` ADD CHANGEFEED")
}

func TestGenerateMigrationAST_Changefeeds_PreservesDisabledStateInPlace(t *testing.T) {
	held := ydbschema.ChangefeedSpec{Name: "held", Mode: "UPDATES", Format: "JSON", Disabled: true}
	changedTopic := held.Clone()
	changedTopic.RetentionPeriod = "PT6H"
	fresh := ydbschema.ChangefeedSpec{Name: "fresh", Mode: "UPDATES", Format: "JSON"}
	cases := []struct {
		name    string
		desired []ydbschema.ChangefeedSpec
		want    string
	}{
		{name: "unrelated addition", desired: []ydbschema.ChangefeedSpec{held, fresh},
			want: "ALTER TABLE `items` ADD CHANGEFEED `fresh` WITH (MODE = 'UPDATES', FORMAT = 'JSON');\n"},
		{name: "topic retention in place", desired: []ydbschema.ChangefeedSpec{changedTopic},
			want: "ALTER TOPIC `items/held` SET (retention_period = Interval('PT6H'));\n"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := changefeedsChanged(t, test.desired, []ydbschema.ChangefeedSpec{held})
			c.Assert(render(c, capability.YDB262(), diff), qt.Equals, test.want)
		})
	}
}
