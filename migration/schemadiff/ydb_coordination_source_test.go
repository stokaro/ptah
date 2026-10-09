package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// The annotation's limits reach the owner comparator without recreating a
// common coordination collection or treating an unmanaged node as absent.
func TestGoSourceStandaloneLimitsPreventRemoval(t *testing.T) {
	for _, family := range []string{"coordination_node", "streaming_query"} {
		t.Run(family, func(t *testing.T) {
			for _, test := range []struct {
				name         string
				directive    string
				removed      []string
				commonLimits int
			}{
				{name: "managed omission", removed: []string{"dropped", "kept"}},
				{name: "unmanaged namespace", directive: `//ptah:schema:notdescribed kind="coordination_node"`},
				{name: "kind case", directive: `//ptah:schema:notdescribed kind="COORDINATION_NODE"`},
				{name: "unmanaged node", directive: `//ptah:schema:notdescribed kind="coordination_node" name="app.kept"`, removed: []string{"dropped"}},
				{name: "path spelling", directive: `//ptah:schema:notdescribed kind="coordination_node" name="app/kept"`, removed: []string{"dropped"}},
				{name: "unmanaged schema", directive: `//ptah:schema:notdescribed kind="schema" name="app"`, commonLimits: 1},
				{name: "other schema", directive: `//ptah:schema:notdescribed kind="schema" name="other"`, removed: []string{"dropped", "kept"}, commonLimits: 1},
			} {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					desired, err := goschema.ParseSource("schema.go", "package entities\n"+standaloneLimitDirective(test.directive, family)+"\ntype _ struct{}")
					c.Assert(err, qt.IsNil)
					current := &catalog.Database{
						FeatureCoverage: completeYDBFixtureCoverage(),
						FeatureObjects:  standaloneLimitObjects(family),
					}
					diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), &desired, current, catalog.ServerInfo{Dialect: "ydb", Capabilities: capability.YDB262().With(capability.StreamingQueries, true)}, nil, must.Must(builtin.New()))
					c.Assert(err, qt.IsNil)
					var removed []string
					for _, change := range diff.FeatureChanges {
						removed = append(removed, change.Subject.Name.Source)
					}
					c.Assert(removed, qt.DeepEquals, test.removed)
					c.Assert(desired.NotDescribed.Objects, qt.HasLen, test.commonLimits)
				})
			}
		})
	}
}

func TestCoordinationCreationRequiresItsSchemaToBeInspected(t *testing.T) {
	c := qt.New(t)
	desired, err := goschema.ParseSource("schema.go", "package entities\n//ptah:schema:coordinationnode name=\"locks\" schema=\"app\"\ntype Locks struct{}")
	c.Assert(err, qt.IsNil)
	current := &catalog.Database{
		FeatureCoverage: completeYDBFixtureCoverage(),
		NotDescribed:    coverage.Set{}.WithObject(coverage.Schema, "app"),
	}
	diff, undecided, err := schemadiff.CompareReportingUndecidedAdditions(t.Context(), &desired, current, &config.CompareOptions{Dialect: "ydb"}, must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	c.Assert(undecided.Features, qt.HasLen, 1)
	c.Assert(undecided.Features[0].Subject, qt.Equals, ydbcoordination.Ref("app", "locks"))
	c.Assert(current.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
}

func standaloneLimitDirective(directive, family string) string {
	directive = strings.ReplaceAll(directive, "coordination_node", family)
	return strings.ReplaceAll(directive, "COORDINATION_NODE", strings.ToUpper(family))
}

func standaloneLimitObjects(family string) schemaext.Objects {
	if family == "streaming_query" {
		return must.Must(schemaext.NewObjects(
			ydbstreaming.ObservedObject("app", "kept", ydbstreaming.Spec{Text: "SELECT 1;"}),
			ydbstreaming.ObservedObject("app", "dropped", ydbstreaming.Spec{Text: "SELECT 2;"}),
		))
	}
	return must.Must(schemaext.NewObjects(
		ydbcoordination.ObservedObject("app", "kept", ydbcoordination.Spec{SelfCheckPeriodMillis: 2000}),
		ydbcoordination.ObservedObject("app", "dropped", ydbcoordination.Spec{}),
	))
}
