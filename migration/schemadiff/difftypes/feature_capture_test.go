package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/migration/schemadiff/difftypes"
)

func TestTableCaptures_OwnedFeaturesKeepIdentityAndUnknownState(t *testing.T) {
	cases := []struct {
		name    string
		capture func(*schemamodel.Database, schemamodel.Table, identifier.Semantics) (schemaext.Objects, schemaext.Coverage)
	}{
		{name: "creation", capture: func(db *schemamodel.Database, table schemamodel.Table, semantics identifier.Semantics) (schemaext.Objects, schemaext.Coverage) {
			c := difftypes.TableCreationFor(db, table, table.Name, semantics)
			return c.OwnedObjects, c.FeatureCoverage
		}},
		{name: "rebuild", capture: func(db *schemamodel.Database, table schemamodel.Table, semantics identifier.Semantics) (schemaext.Objects, schemaext.Coverage) {
			c := difftypes.TableDeclarationFor(db, table, semantics)
			return c.OwnedObjects, c.FeatureCoverage
		}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			semantics := identifier.ForDialect("ydb")
			literal := schemamodel.Table{Name: "tenant.Orders", StructName: "Literal"}
			nested := schemamodel.Table{Schema: "tenant", Name: "Orders", StructName: "Nested"}
			feed := ydbschema.ChangefeedSpec{Name: "updates", Mode: "UPDATES", Format: "JSON", Disabled: true}
			objects, err := schemaext.NewObjects(ydbschema.DesiredObject("", literal.Name, feed), ydbschema.DesiredObject(nested.Schema, nested.Name, feed))
			c.Assert(err, qt.IsNil)
			parent := objectidentity.NewBuilder(semantics).TableParts(literal.Schema, literal.Name)
			coverage, err := ydbschema.ChangefeedCoverage(schemaext.Desired, []schemaext.SubjectCoverage{{Kind: ydbschema.ChangefeedKind, Subject: parent, Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "topic settings were not read"}}})
			c.Assert(err, qt.IsNil)
			desired := &schemamodel.Database{Tables: []schemamodel.Table{literal, nested}, FeatureObjects: objects, FeatureCoverage: coverage}
			captured, knowledge := test.capture(desired, literal, semantics)
			c.Assert(captured.Refs(), qt.DeepEquals, []objectidentity.ID{ydbschema.ChangefeedRef("", literal.Name, "updates")})
			c.Assert(knowledge.Lookup(ydbschema.ChangefeedKind, parent).State, qt.Equals, schemaext.Uninspected)
			desired.FeatureObjects = schemaext.Objects{}
			retained, _, err := captured.Get(ydbschema.ChangefeedRef("", literal.Name, "updates"))
			c.Assert(err, qt.IsNil)
			c.Assert(retained.Value.(*ydbschema.DesiredChangefeed).Spec.Disabled, qt.IsTrue)
		})
	}
}
