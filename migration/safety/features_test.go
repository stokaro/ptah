package safety_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/migration/safety"
	"ptah.run/migration/schemadiff/difftypes"
)

type unknownChange struct{}

func (*unknownChange) Kind() schemaext.Kind               { return "example.org/unknown-change" }
func (*unknownChange) CloneChange() schemaext.ChangeValue { return &unknownChange{} }

type classifiedChange struct{ effect schemaext.Effect }

func (*classifiedChange) Kind() schemaext.Kind { return "example.org/classified-change" }
func (v *classifiedChange) CloneChange() schemaext.ChangeValue {
	return &classifiedChange{effect: v.effect}
}
func (v *classifiedChange) Effect() schemaext.Effect { return v.effect }

// Standalone, table-bound and view-bound changes must appear in pre-plan reports.
// Unknown effects cannot make a nonempty diff read as safe.
func TestClassifySchemaDiffKeepsUnknownFeatureRisk(t *testing.T) {
	values := []schemaext.ChangeValue{&unknownChange{}, &classifiedChange{},
		&classifiedChange{effect: schemaext.Effect{Impact: schemaext.Additive}},
		&classifiedChange{effect: schemaext.Effect{Impact: "future", Reason: "not understood"}},
	}
	for _, value := range values {
		c := qt.New(t)
		ref := objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts("example.org/subject", "app", "name")
		record := schemaext.ChangeRecord{Subject: ref, Value: value}
		diff := &difftypes.SchemaDiff{
			FeatureChanges:            []schemaext.ChangeRecord{record},
			TablesModified:            []difftypes.TableDiff{{FeatureChanges: []schemaext.ChangeRecord{record}}},
			MaterializedViewsModified: []difftypes.MaterializedViewDiff{{FeatureChanges: []schemaext.ChangeRecord{record}}},
		}
		findings := safety.ClassifySchemaDiff(diff)
		c.Assert(findings, qt.DeepEquals, []safety.Finding{{Category: "feature_changes:" + string(value.Kind()), Count: 3, Severity: safety.Destructive}})
	}
}

// A view the plan replaces loses its rows, so a setting change on it is as
// destructive as the replacement whatever its own effect says: a schedule
// changed in place keeps the rows, and changed beside the view's body it does
// not (stokaro/ptah#4278). The control is the same change on a view the plan
// keeps, which reports its own effect.
func TestClassifySchemaDiffReportsAChangeOnAReplacedViewAsDestructive(t *testing.T) {
	ref := objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).SchemaScopedParts(objectidentity.KindMatView, "", "daily")
	record := schemaext.ChangeRecord{Subject: ref, Value: &classifiedChange{effect: schemaext.Effect{Impact: schemaext.Behavioral, Reason: "rows are kept"}}}
	for _, test := range []struct {
		name    string
		changes map[string]string
		want    safety.Severity
	}{
		{"a view the plan keeps", make(map[string]string), safety.Warning},
		{"a view the plan replaces", map[string]string{"body": "SELECT 1 -> SELECT 2"}, safety.Destructive},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{MaterializedViewsModified: []difftypes.MaterializedViewDiff{
				{ViewName: "daily", Changes: test.changes, FeatureChanges: []schemaext.ChangeRecord{record}},
			}}

			findings := safety.ClassifySchemaDiff(diff)

			c.Assert(findings, qt.DeepEquals, []safety.Finding{{Category: "feature_changes:example.org/classified-change", Count: 1, Severity: test.want}})
		})
	}
}
