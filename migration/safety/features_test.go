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
