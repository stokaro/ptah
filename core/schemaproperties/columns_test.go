package schemaproperties_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaproperties"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
	"ptah.run/engine"
)

func columnPropertyRuntime() *engine.Runtime {
	return must.Must(engine.New(engine.Provider{
		ID: mysqlschema.Owner, Targets: []engine.Target{{Name: "mysql"}, {Name: "mariadb"}, {Name: "postgres"}},
		Codecs: mysqlschema.ColumnSettingsCodecs(), Properties: []engine.PropertySource{{
			Target: "mysql", Format: schemaext.ColumnPlatformProperties, Definitions: mysqlsource.ColumnDefinitions(), Service: mysqlsource.ColumnService{},
		}},
	}))
}

func TestDecodeColumns_HappyPath(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{Fields: []schemamodel.Field{
		{StructName: "User", Name: "seen", Type: "TIMESTAMP", Overrides: map[string]map[string]string{
			"mysql": {"charset": "latin1", "on_update": "CURRENT_TIMESTAMP", "type": "DATETIME"}, "mariadb": {"charset": "utf8mb3"},
		}},
		{StructName: "User", Name: "plain", Type: "TEXT"},
	}}

	decoded, err := schemaproperties.DecodeColumns(t.Context(), source, "mysql", columnPropertyRuntime())

	c.Assert(err, qt.IsNil)
	value, found, err := schemaext.FacetAs[*mysqlschema.DesiredColumnSettings](decoded.Fields[0].Facets, mysqlschema.ColumnSettingsKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(*value, qt.Equals, mysqlschema.DesiredColumnSettings{Charset: "latin1", OnUpdate: "CURRENT_TIMESTAMP"})
	c.Assert(decoded.Fields[0].Facets.TargetScope(mysqlschema.ColumnSettingsKind), qt.DeepEquals, []string{"mysql"})
	// The common override stays for its own reader, and another target's
	// group is not this target's to decode.
	c.Assert(decoded.Fields[0].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"type": "DATETIME"}, "mariadb": {"charset": "utf8mb3"}})
	c.Assert(decoded.Fields[1].Facets.IsZero(), qt.IsTrue)
	c.Assert(source.Fields[0].Overrides["mysql"]["charset"], qt.Equals, "latin1")
	c.Assert(source.Fields[0].Facets.IsZero(), qt.IsTrue)

	exported, err := schemaproperties.EncodeColumns(t.Context(), decoded, "mysql", columnPropertyRuntime())

	c.Assert(err, qt.IsNil)
	c.Assert(exported.Fields[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(exported.Fields[0].Overrides["mysql"], qt.DeepEquals, map[string]string{"charset": "latin1", "on_update": "CURRENT_TIMESTAMP", "type": "DATETIME"})
}

func TestDecodeColumns_LeavesAnotherTargetsGroups(t *testing.T) {
	c := qt.New(t)
	source := &schemamodel.Database{Fields: []schemamodel.Field{
		{StructName: "User", Name: "seen", Type: "TIMESTAMP", Overrides: map[string]map[string]string{"mysql": {"charset": "latin1"}}},
	}}

	decoded, err := schemaproperties.Decode(t.Context(), source, "postgres", columnPropertyRuntime())

	c.Assert(err, qt.IsNil)
	c.Assert(decoded.Fields[0].Facets.IsZero(), qt.IsTrue)
	c.Assert(decoded.Fields[0].Overrides, qt.DeepEquals, map[string]map[string]string{"mysql": {"charset": "latin1"}})
}

func TestDecodeColumns_FailurePath(t *testing.T) {
	declared := must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{Charset: "utf8mb4"}))
	tests := []struct {
		name    string
		field   schemamodel.Field
		wantErr string
	}{
		{name: "an empty value", field: schemamodel.Field{StructName: "User", Name: "seen",
			Overrides: map[string]map[string]string{"mysql": {"charset": " "}}},
			wantErr: `.*MySQL column property "charset" is empty; leave it out instead`},
		{name: "a property beside a declared facet", field: schemamodel.Field{StructName: "User", Name: "seen", Facets: declared,
			Overrides: map[string]map[string]string{"mysql": {"on_update": "NOW()"}}},
			wantErr: `.*column "User.seen" declares both properties and facet "ptah.run/mysql/column-settings"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			decoded, err := schemaproperties.DecodeColumns(t.Context(), &schemamodel.Database{Fields: []schemamodel.Field{test.field}}, "mysql", columnPropertyRuntime())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(decoded, qt.IsNil)
		})
	}
}
