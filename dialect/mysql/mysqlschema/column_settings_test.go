package mysqlschema_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
)

// Both representations encode the settings they state and nothing else, and
// decode back to an equal value.
func TestColumnSettingsCodecs_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		codec schemaext.Codec
		value schemaext.Value
		want  string
	}{
		{name: "a declared character set", codec: mysqlschema.ColumnSettingsCodecs()[0],
			value: &mysqlschema.DesiredColumnSettings{Charset: "utf8mb4"}, want: `{"charset":"utf8mb4"}`},
		{name: "a declared ON UPDATE clause", codec: mysqlschema.ColumnSettingsCodecs()[0],
			value: &mysqlschema.DesiredColumnSettings{OnUpdate: "CURRENT_TIMESTAMP(6)"}, want: `{"on_update":"CURRENT_TIMESTAMP(6)"}`},
		{name: "both observed", codec: mysqlschema.ColumnSettingsCodecs()[1],
			value: &mysqlschema.ObservedColumnSettings{Charset: "latin1", OnUpdate: "CURRENT_TIMESTAMP"}, want: `{"charset":"latin1","on_update":"CURRENT_TIMESTAMP"}`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			encoded, err := test.codec.Encode(test.value)

			c.Assert(err, qt.IsNil)
			c.Assert(string(encoded), qt.Equals, test.want)
			decoded := must.Must(test.codec.Decode(encoded))
			c.Assert(decoded.(schemaext.Value).Equal(test.value), qt.IsTrue)
		})
	}
}

// A decoder accepts only what the encoder writes, and a validator refuses a
// value that states nothing or that holds untrimmed text.
func TestColumnSettingsCodecs_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		data    string
		wantErr string
	}{
		{name: "nothing stated", data: `{}`, wantErr: `.*MySQL column settings state nothing; omit the value instead`},
		{name: "an empty string", data: `{"charset":""}`, wantErr: `.*charset.*`},
		{name: "a null", data: `{"on_update":null}`, wantErr: `.*on_update.*`},
		{name: "a key in another case", data: `{"Charset":"utf8"}`, wantErr: `.*Charset.*`},
		{name: "an unknown key", data: `{"charset":"utf8","collate":"utf8_bin"}`, wantErr: `.*collate.*`},
		{name: "untrimmed text", data: `{"charset":" utf8"}`, wantErr: `.*MySQL column setting charset " utf8" must be trimmed text`},
	}
	for _, test := range tests {
		for i, codec := range mysqlschema.ColumnSettingsCodecs() {
			t.Run(test.name+"/"+string(codec.Representation), func(t *testing.T) {
				c := qt.New(t)

				decoded, err := mysqlschema.ColumnSettingsCodecs()[i].Decode(json.RawMessage(test.data))

				c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
				c.Assert(err, qt.ErrorMatches, test.wantErr)
				c.Assert(decoded, qt.IsNil)
			})
		}
	}
}

// Settings reads either representation, so a rollback that restores an
// observation renders it like a declaration.
func TestSettings_ReadsEitherRepresentation(t *testing.T) {
	tests := []struct {
		name   string
		facets schemaext.Facets
	}{
		{name: "a declaration", facets: must.Must(mysqlschema.WithColumnSettings(schemaext.Facets{},
			mysqlschema.ColumnSettings{Charset: "latin1", OnUpdate: "NOW()"}))},
		{name: "an observation", facets: must.Must(mysqlschema.WithObservedColumnSettings(schemaext.Facets{},
			mysqlschema.ColumnSettings{Charset: "latin1", OnUpdate: "NOW()"}))},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			settings, found, err := mysqlschema.Settings(test.facets)

			c.Assert(err, qt.IsNil)
			c.Assert(found, qt.IsTrue)
			c.Assert(settings, qt.Equals, mysqlschema.ColumnSettings{Charset: "latin1", OnUpdate: "NOW()"})
			c.Assert(test.facets.TargetScope(mysqlschema.ColumnSettingsKind), qt.DeepEquals, []string{"mariadb", "mysql"})
		})
	}
}

// Nothing stated adds nothing, and an empty collection holds no settings.
func TestWithColumnSettings_AnEmptyValueAddsNothing(t *testing.T) {
	c := qt.New(t)

	facets, err := mysqlschema.WithColumnSettings(schemaext.Facets{}, mysqlschema.ColumnSettings{})

	c.Assert(err, qt.IsNil)
	c.Assert(facets.IsZero(), qt.IsTrue)
	settings, found, err := mysqlschema.Settings(facets)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsFalse)
	c.Assert(settings, qt.Equals, mysqlschema.ColumnSettings{})
}

// A declaration and an observation of the same settings convert into each
// other, and a value of the other representation is not equal.
func TestColumnSettings_ConvertBetweenRepresentations(t *testing.T) {
	c := qt.New(t)
	observed := &mysqlschema.ObservedColumnSettings{Charset: "utf8mb4", OnUpdate: "CURRENT_TIMESTAMP"}

	declared := observed.Desired()

	c.Assert(*declared, qt.Equals, mysqlschema.DesiredColumnSettings{Charset: "utf8mb4", OnUpdate: "CURRENT_TIMESTAMP"})
	c.Assert(declared.Observed().Equal(observed), qt.IsTrue)
	c.Assert(declared.Equal(observed), qt.IsFalse)
	c.Assert(observed.Equal(&mysqlschema.ObservedColumnSettings{Charset: "utf8mb4"}), qt.IsFalse)
}
