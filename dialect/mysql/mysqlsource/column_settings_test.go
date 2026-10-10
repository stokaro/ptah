package mysqlsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
)

func TestColumnService_HappyPath(t *testing.T) {
	c := qt.New(t)
	fragment := schemaext.PropertyFragment{Kind: mysqlschema.ColumnSettingsKind,
		Properties: map[string]string{"charset": " latin1 ", "on_update": "CURRENT_TIMESTAMP(3)"}}

	values, err := mysqlsource.ColumnService{}.DecodeProperties(t.Context(), schemaext.PropertyDecodeRequest{
		Target: "mariadb", Format: schemaext.ColumnPlatformProperties, Fragments: []schemaext.PropertyFragment{fragment}})

	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []schemaext.Value{&mysqlschema.DesiredColumnSettings{Charset: "latin1", OnUpdate: "CURRENT_TIMESTAMP(3)"}})

	fragments, err := mysqlsource.ColumnService{}.EncodeProperties(t.Context(), schemaext.PropertyEncodeRequest{
		Target: "mysql", Format: schemaext.ColumnPlatformProperties, Values: values})

	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.DeepEquals, []schemaext.PropertyFragment{{Kind: mysqlschema.ColumnSettingsKind,
		Properties: map[string]string{"charset": "latin1", "on_update": "CURRENT_TIMESTAMP(3)"}}})
}

func TestColumnService_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.PropertyDecodeRequest
		wantIs  error
		wantErr string
	}{
		{name: "another target", request: schemaext.PropertyDecodeRequest{Target: "postgres", Format: schemaext.ColumnPlatformProperties},
			wantIs: ptaherr.ErrUnsupportedDialect, wantErr: `.*MySQL source target "postgres"`},
		{name: "another format", request: schemaext.PropertyDecodeRequest{Target: "mysql", Format: schemaext.TablePlatformProperties},
			wantIs: ptaherr.ErrUnsupportedFeature, wantErr: `.*MySQL column source format .*`},
		{name: "an empty value", request: schemaext.PropertyDecodeRequest{Target: "mysql", Format: schemaext.ColumnPlatformProperties,
			Fragments: []schemaext.PropertyFragment{{Kind: mysqlschema.ColumnSettingsKind, Properties: map[string]string{"on_update": ""}}}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*MySQL column property "on_update" is empty; leave it out instead`},
		{name: "another kind", request: schemaext.PropertyDecodeRequest{Target: "mysql", Format: schemaext.ColumnPlatformProperties,
			Fragments: []schemaext.PropertyFragment{{Kind: "example.org/other", Properties: map[string]string{"charset": "x"}}}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*MySQL column source kind "example.org/other"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := mysqlsource.ColumnService{}.DecodeProperties(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}
