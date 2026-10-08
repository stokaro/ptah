package ydb_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/capability"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbcompare"
	"ptah.run/dialect/ydb/ydbschema"
)

func feedCoverage(t *testing.T, representation schemaext.Representation, limits ...schemaext.SubjectCoverage) schemaext.Coverage {
	t.Helper()
	c := qt.New(t)
	coverage, err := ydbschema.ChangefeedCoverage(representation, limits)
	c.Assert(err, qt.IsNil)
	return coverage
}

func declaredFeeds(t *testing.T, declaration schemacapture.TableDeclaration, specs ...ydbschema.ChangefeedSpec) schemacapture.TableDeclaration {
	t.Helper()
	c := qt.New(t)
	var objects []schemaext.Object
	for _, spec := range specs {
		objects = append(objects, ydbschema.DesiredObject(declaration.Table.Schema, declaration.Table.Name, spec))
	}
	var err error
	declaration.OwnedObjects, err = schemaext.NewObjects(objects...)
	c.Assert(err, qt.IsNil)
	declaration.FeatureCoverage = feedCoverage(t, schemaext.Desired)
	return declaration
}

func observedFeeds(t *testing.T, schema, table string, specs ...ydbschema.ChangefeedSpec) schemacapture.TableObservation {
	t.Helper()
	c := qt.New(t)
	var values []schemaext.Object
	for _, spec := range specs {
		values = append(values, ydbschema.ObservedObject(schema, table, spec))
	}
	objects, err := schemaext.NewObjects(values...)
	c.Assert(err, qt.IsNil)
	return schemacapture.TableObservation{Table: catalog.Table{Schema: schema, Name: table},
		OwnedObjects: objects, FeatureCoverage: feedCoverage(t, schemaext.Observed)}
}

func feedChanges(t *testing.T, desired schemacapture.TableDeclaration, current schemacapture.TableObservation) []schemaext.ChangeRecord {
	t.Helper()
	c := qt.New(t)
	semantics := identifier.ForDialect("ydb")
	parent := objectidentity.NewBuilder(semantics).TableParts(desired.Table.Schema, desired.Table.Name)
	result, err := (ydbcompare.Service{}).CompareObjects(t.Context(), schemaext.ObjectComparisonRequest{
		Target: "ydb", Identifiers: semantics, Capabilities: capability.YDB262(), Kinds: []schemaext.Kind{ydbschema.ChangefeedKind},
		Desired: schemaext.ObjectState{Objects: desired.OwnedObjects, Coverage: desired.FeatureCoverage},
		Current: schemaext.ObjectState{Objects: current.OwnedObjects, Coverage: current.FeatureCoverage},
		Parents: []schemaext.ParentState{{Subject: parent, Desired: true, Current: true}},
	})
	c.Assert(err, qt.IsNil)
	return result.Changes
}
