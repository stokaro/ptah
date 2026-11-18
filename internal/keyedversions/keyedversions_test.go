package keyedversions_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/keyedversions"
)

func TestStrip_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		doc  string
		want string
	}{
		{
			name: "a list beside its keys, with the member order kept",
			doc:  `{"planned":[1,2],"planned_keys":["00","1"],"outcome":"applied"}`,
			want: `{"planned_keys":["00","1"],"outcome":"applied"}`,
		},
		{
			name: "a number beside its key, at depth",
			doc:  `{"status":{"current_version":2,"current_version_key":"1","migrations":[{"version":1,"version_key":"00","state":"applied"}]}}`,
			want: `{"status":{"current_version_key":"1","migrations":[{"version_key":"00","state":"applied"}]}}`,
		},
		{
			name: "a revision row names its migration in atlas_version",
			doc:  `{"dirty_revision":{"version":3,"atlas_version":"v1","applied":1,"total":2}}`,
			want: `{"dirty_revision":{"atlas_version":"v1","applied":1,"total":2}}`,
		},
		{
			name: "an empty list names nothing in either spelling",
			doc:  `{"pending_migrations":[],"out_of_order_migrations":null,"total_migrations":3}`,
			want: `{"total_migrations":3}`,
		},
		{
			name: "a number with no key stays",
			doc:  `{"current_version":0,"applied":1}`,
			want: `{"current_version":0,"applied":1}`,
		},
		{
			name: "a member that is not numeric stays",
			doc:  `{"version":"v1","version_key":"v1"}`,
			want: `{"version":"v1","version_key":"v1"}`,
		},
		{
			name: "a string keeps its bytes",
			doc:  `{"message":"a < b","version":1,"version_key":"1"}`,
			want: `{"message":"a < b","version_key":"1"}`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			got, err := keyedversions.Strip([]byte(tt.doc))

			c.Assert(err, qt.IsNil)
			c.Assert(string(got), qt.Equals, tt.want)
		})
	}
}

func TestStrip_FailurePath(t *testing.T) {
	c := qt.New(t)

	got, err := keyedversions.Strip([]byte(`{"planned":[1,`))

	c.Assert(err, qt.ErrorMatches, `strip the numeric versions of a migration document: .*`)
	c.Assert(got, qt.IsNil)
}

func TestWriteIndented_HappyPath(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := keyedversions.WriteIndented(&out, []byte(`{"planned":[1],"planned_keys":["00"]}`))

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "{\n  \"planned_keys\": [\n    \"00\"\n  ]\n}\n")
}

func TestWriteIndented_FailurePath(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := keyedversions.WriteIndented(&out, []byte(`[1,`))

	c.Assert(err, qt.ErrorMatches, `strip the numeric versions of a migration document: .*`)
	c.Assert(out.String(), qt.Equals, "")
}
