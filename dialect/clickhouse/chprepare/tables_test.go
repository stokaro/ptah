package chprepare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/clickhouse/chprepare"
	"ptah.run/dialect/clickhouse/chresolve"
	"ptah.run/dialect/clickhouse/chschema"
)

func observedTableCoverage() schemaext.Coverage {
	var codecs []schemaext.OwnedCodec
	for _, codec := range chschema.Codecs() {
		codecs = append(codecs, schemaext.OwnedCodec{Owner: "ptah.run/clickhouse", Codec: codec})
	}
	models := make(map[schemaext.Representation]schemaext.CodecIdentity)
	for _, model := range must.Must(schemaext.NewRegistry(codecs...)).Definitions() {
		models[model.Representation] = model
	}
	return must.Must(schemaext.NewCoverage(schemaext.Observed, []schemaext.KindCoverage{{
		Model: models[schemaext.Observed], Knowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}, nil))
}

func typedKeyRequest(primary chschema.Setting, observedKey string) schemapreparation.Request {
	return schemapreparation.Request{Target: "clickhouse", Tables: []schemapreparation.Table{{
		Subject: objectidentity.NewBuilder(identifier.ForDialect("clickhouse")).TableParts("", "events"),
		Desired: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "events", Facets: must.Must(schemaext.NewFacets(&chschema.DesiredTable{PrimaryKey: primary}))},
			Fields: []schemamodel.Field{{Name: "id", Type: "UInt64"}},
		},
		Current: schemacapture.TableObservation{
			Table: catalog.Table{Name: "events", Facets: must.Must(schemaext.NewFacets(&chschema.ObservedTable{
				Engine: "MergeTree", OrderBy: "id", PrimaryKey: observedKey,
			}))},
			FeatureCoverage: observedTableCoverage(),
		},
		CurrentKnowledge: schemaext.Knowledge{State: schemaext.Complete},
	}}}
}

func TestPrepareTables_PreservesKeyIntent(t *testing.T) {
	for _, test := range []struct {
		name     string
		primary  chschema.Setting
		observed string
		want     bool
	}{
		{name: "omitted retains empty sparse key", observed: "", want: false},
		{name: "default inherits sorting key", primary: chschema.Setting{State: chschema.Default}, observed: "", want: true},
		{name: "explicit empty removes membership", primary: chschema.Setting{State: chschema.Explicit}, observed: "id", want: false},
		{name: "omitted retains inspected membership", observed: "id", want: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := typedKeyRequest(test.primary, test.observed)
			result, err := (chprepare.Service{}).PrepareTables(t.Context(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result.Tables[0].Desired.Fields[0].Primary, qt.Equals, test.want)
			c.Assert(result.Tables[0].Desired.Table.Facets, qt.DeepEquals, request.Tables[0].Desired.Table.Facets)
			c.Assert(result.Tables[0].Current, qt.DeepEquals, request.Tables[0].Current)
			c.Assert(result.Tables[0].ColumnPrimaryKeysPrepared, qt.IsTrue)
		})
	}
}

func TestPrepareTables_RefusesUninspectedFeatureState(t *testing.T) {
	c := qt.New(t)
	request := typedKeyRequest(chschema.Setting{}, "id")
	request.Tables[0].Current.FeatureCoverage = schemaext.Coverage{}
	result, err := (chprepare.Service{}).PrepareTables(t.Context(), request)
	c.Assert(err, qt.ErrorIs, chresolve.ErrUnknownCurrent)
	c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
}

func TestPrepareTables_RefusesDuplicateRepresentations(t *testing.T) {
	c := qt.New(t)
	request := typedKeyRequest(chschema.Setting{}, "id")
	request.Tables[0].Desired.Table.Overrides = map[string]map[string]string{"clickhouse": {"order_by": ""}}
	result, err := (chprepare.Service{}).PrepareTables(t.Context(), request)
	c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
	c.Assert(result, qt.DeepEquals, schemapreparation.Result{})
}
