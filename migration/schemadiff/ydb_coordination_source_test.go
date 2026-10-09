package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/config"
	"ptah.run/core/coverage"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

// The annotation's limits reach the owner comparator without recreating a
// common coordination collection or treating an unmanaged node as absent.
func TestGoSourceCoordinationLimitsPreventRemoval(t *testing.T) {
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
			desired, err := goschema.ParseSource("schema.go", "package entities\n"+test.directive+"\ntype _ struct{}")
			c.Assert(err, qt.IsNil)
			current := &catalog.Database{
				FeatureCoverage: completeYDBFixtureCoverage(),
				FeatureObjects: must.Must(schemaext.NewObjects(
					ydbcoordination.ObservedObject("app", "kept", ydbcoordination.Spec{SelfCheckPeriodMillis: 2000}),
					ydbcoordination.ObservedObject("app", "dropped", ydbcoordination.Spec{}),
				)),
			}
			diff, err := schemadiff.CompareWithDialect(t.Context(), &desired, current, "ydb", must.Must(builtin.New()))
			c.Assert(err, qt.IsNil)
			var removed []string
			for _, change := range diff.FeatureChanges {
				removed = append(removed, change.Subject.Name.Source)
			}
			c.Assert(removed, qt.DeepEquals, test.removed)
			c.Assert(desired.NotDescribed.Objects, qt.HasLen, test.commonLimits)
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
