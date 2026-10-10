package mysqlsource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
)

func decodeIndex(c *qt.C, target string, properties map[string]string) ([]schemaext.Value, error) {
	c.Helper()
	return mysqlsource.IndexService{}.DecodeProperties(c.Context(), schemaext.PropertyDecodeRequest{
		Target: target, Format: schemaext.IndexPlatformProperties,
		Fragments: []schemaext.PropertyFragment{{Kind: mysqlschema.IndexKind, Properties: properties}},
	})
}

// TestIndexService_DecodesTheParser reads `platform.mysql.parser` on both
// targets, and writes it back as the same property.
func TestIndexService_DecodesTheParser(t *testing.T) {
	for _, target := range []string{"mysql", "mariadb"} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)

			values, err := decodeIndex(c, target, map[string]string{"parser": "ngram"})
			c.Assert(err, qt.IsNil)
			c.Assert(values, qt.DeepEquals, []schemaext.Value{&mysqlschema.DesiredIndex{Parser: "ngram"}})
			fragments, err := mysqlsource.IndexService{}.EncodeProperties(c.Context(), schemaext.PropertyEncodeRequest{
				Target: target, Format: schemaext.IndexPlatformProperties, Values: values,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(fragments, qt.DeepEquals, []schemaext.PropertyFragment{{Kind: mysqlschema.IndexKind, Properties: map[string]string{"parser": "ngram"}}})
		})
	}
}

// TestIndexService_FailurePath refuses an unknown key, a parser that is no
// name, and the table property format.
func TestIndexService_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		properties map[string]string
		wantErr    string
	}{
		{name: "an unknown key", properties: map[string]string{"key_block_size": "8"},
			wantErr: `invalid feature value: unknown MySQL index property "key_block_size"`},
		{name: "a parser that is no name", properties: map[string]string{"parser": "ng ram"},
			wantErr: `desired model "ptah.run/mysql/index": invalid feature value: MySQL parser "ng ram" is not a name .*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			values, err := decodeIndex(c, "mysql", test.properties)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(values, qt.IsNil)
		})
	}
}
