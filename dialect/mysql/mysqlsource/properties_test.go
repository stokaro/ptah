package mysqlsource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
)

func decode(ctx context.Context, target string, properties map[string]string) ([]schemaext.Value, error) {
	return mysqlsource.Service{}.DecodeProperties(ctx, schemaext.PropertyDecodeRequest{
		Target: target, Format: schemaext.TablePlatformProperties,
		Fragments: []schemaext.PropertyFragment{{Kind: mysqlschema.TableKind, Properties: properties}},
	})
}

// TestDefinitions_ClaimTheOptions pins the vocabulary: the three option keys,
// and no common attribute taken over.
func TestDefinitions_ClaimTheOptions(t *testing.T) {
	c := qt.New(t)

	definitions := mysqlsource.Definitions()

	c.Assert(definitions, qt.HasLen, 1)
	c.Assert(definitions[0].Kind, qt.Equals, mysqlschema.TableKind)
	c.Assert(definitions[0].Keys, qt.DeepEquals, []string{"auto_increment", "charset", "engine"})
	c.Assert(definitions[0].Absorbs, qt.HasLen, 0)
}

// TestDecodeProperties_HappyPath reads each option on both targets, and reads
// an empty value as an option left out, as a declaration that names no
// engine has always been read.
func TestDecodeProperties_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		properties map[string]string
		want       *mysqlschema.DesiredTable
	}{
		{name: "every option on mysql", target: "mysql",
			properties: map[string]string{"engine": "InnoDB", "auto_increment": "1000", "charset": "utf8mb4"},
			want:       &mysqlschema.DesiredTable{Engine: "InnoDB", AutoIncrement: "1000", Charset: "utf8mb4"}},
		{name: "a charset on mariadb", target: "mariadb", properties: map[string]string{"charset": "latin1"},
			want: &mysqlschema.DesiredTable{Charset: "latin1"}},
		{name: "an empty engine", target: "mysql", properties: map[string]string{"engine": ""}, want: &mysqlschema.DesiredTable{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := decode(c.Context(), test.target, test.properties)

			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

// TestDecodeProperties_FailurePath refuses what no statement can write, where
// it is written.
func TestDecodeProperties_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		target     string
		properties map[string]string
		wantErr    string
	}{
		{name: "an unknown key", target: "mysql", properties: map[string]string{"row_format": "COMPRESSED"},
			wantErr: `invalid feature value: unknown MySQL table property "row_format"`},
		{name: "an auto-increment that is no number", target: "mysql", properties: map[string]string{"auto_increment": "ten"},
			wantErr: `desired model "ptah.run/mysql/table": invalid feature value: MySQL table auto_increment "ten" is not a whole decimal number`},
		{name: "an engine that is no name", target: "mariadb", properties: map[string]string{"engine": "MergeTree()"},
			wantErr: `desired model "ptah.run/mysql/table": invalid feature value: MySQL table engine "MergeTree\(\)" is not a name .*`},
		{name: "another target", target: "postgres", properties: map[string]string{"charset": "utf8"},
			wantErr: `unsupported database dialect: MySQL source target "postgres"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := decode(c.Context(), test.target, test.properties)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}

// TestEncodeProperties_RoundTrip writes the options a declaration states and
// leaves the others out, so decoding the fragment gives the declaration back.
func TestEncodeProperties_RoundTrip(t *testing.T) {
	c := qt.New(t)
	declared := &mysqlschema.DesiredTable{Engine: "InnoDB", Charset: "utf8mb4"}

	fragments, err := mysqlsource.Service{}.EncodeProperties(c.Context(), schemaext.PropertyEncodeRequest{
		Target: "mysql", Format: schemaext.TablePlatformProperties, Values: []schemaext.Value{declared},
	})
	c.Assert(err, qt.IsNil)
	c.Assert(fragments, qt.HasLen, 1)
	c.Assert(fragments[0].Properties, qt.DeepEquals, map[string]string{"engine": "InnoDB", "charset": "utf8mb4"})
	values, err := decode(c.Context(), "mysql", fragments[0].Properties)

	c.Assert(err, qt.IsNil)
	c.Assert(values, qt.DeepEquals, []schemaext.Value{declared})
}
