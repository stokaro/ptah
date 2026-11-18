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
			name: "applied beside applied_keys",
			doc:  `{"applied":[1],"applied_keys":["00"]}`,
			want: `{"applied_keys":["00"]}`,
		},
		{
			name: "rolled_back beside rolled_back_keys",
			doc:  `{"rolled_back":[2],"rolled_back_keys":["1"]}`,
			want: `{"rolled_back_keys":["1"]}`,
		},
		{
			name: "replayed beside replayed_keys in the drift check",
			doc:  `{"drift_check":{"outcome":"clean","replayed":[1],"replayed_keys":["00"]}}`,
			want: `{"drift_check":{"outcome":"clean","replayed_keys":["00"]}}`,
		},
		{
			name: "the failure and the inputs",
			doc:  `{"failure":{"version":2,"version_key":"1","statement":1},"inputs":[{"version":2,"version_key":"1"}]}`,
			want: `{"failure":{"version_key":"1","statement":1},"inputs":[{"version_key":"1"}]}`,
		},
		{
			name: "the status of a run, and its entries",
			doc:  `{"status":{"current_version":2,"current_version_key":"1","migrations":[{"version":1,"version_key":"00","state":"applied"}]}}`,
			want: `{"status":{"current_version_key":"1","migrations":[{"version_key":"00","state":"applied"}]}}`,
		},
		{
			name: "a status document, and its revision row named in atlas_version",
			doc:  `{"current_version":3,"current_version_key":"v1","dirty_revision":{"version":3,"atlas_version":"v1","applied":1,"total":2}}`,
			want: `{"current_version_key":"v1","dirty_revision":{"atlas_version":"v1","applied":1,"total":2}}`,
		},
		{
			name: "an empty list names nothing in either spelling",
			doc:  `{"pending_migrations":[],"out_of_order_migrations":null,"total_migrations":3}`,
			want: `{"total_migrations":3}`,
		},
		{
			name: "a checkpoint version, which has no key member",
			doc:  `{"checkpoint_version":1,"total_migrations":3}`,
			want: `{"total_migrations":3}`,
		},
		{
			name: "a number with no key stays",
			doc:  `{"current_version":0,"applied":[1]}`,
			want: `{"current_version":0,"applied":[1]}`,
		},
		{
			name: "a member that is not numeric stays",
			doc:  `{"failure":{"version":"v1","version_key":"v1"}}`,
			want: `{"failure":{"version":"v1","version_key":"v1"}}`,
		},
		{
			name: "a version at a path no document gives one stays",
			doc:  `{"status":{"repeatables":[{"version":1,"version_key":"R"}]},"server_details":{"version":16,"version_key":"x"}}`,
			want: `{"status":{"repeatables":[{"version":1,"version_key":"R"}]},"server_details":{"version":16,"version_key":"x"}}`,
		},
		{
			name: "a member no document carries stays",
			doc:  `{"target_version":4,"target_version_key":"v1"}`,
			want: `{"target_version":4,"target_version_key":"v1"}`,
		},
		{
			name: "a string keeps its bytes",
			doc:  `{"message":"a \u003c b","planned":[1],"planned_keys":["1"]}`,
			want: `{"message":"a \u003c b","planned_keys":["1"]}`,
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
