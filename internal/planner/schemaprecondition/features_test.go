package schemaprecondition_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestPlannerWithoutFeatureHandlersRefusesEveryExtensionLocation(t *testing.T) {
	change := schemaext.ChangeRecord{Subject: objectidentity.ID{
		Kind: "external/object", Name: objectidentity.Part{Source: "events", Normalized: "events"},
	}}
	tests := []struct {
		name string
		diff *difftypes.SchemaDiff
		want error
	}{
		{name: "absent diff"},
		{name: "relational change", diff: &difftypes.SchemaDiff{TablesRemoved: difftypes.TableRemovals{{Name: "obsolete"}}}},
		{name: "standalone feature", diff: &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{change}}, want: ptaherr.ErrUnsupportedFeature},
		{name: "table-owned feature", diff: &difftypes.SchemaDiff{TablesModified: []difftypes.TableDiff{{
			TableName: "orders", FeatureChanges: []schemaext.ChangeRecord{change},
		}}}, want: ptaherr.ErrUnsupportedFeature},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(schemaprecondition.RefuseFeatureChanges("sqlite", test.diff), qt.ErrorIs, test.want)
		})
	}
}
