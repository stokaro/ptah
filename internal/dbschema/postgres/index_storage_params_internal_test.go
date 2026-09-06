package postgres

// White-box testing required: the reloptions decoder is package-local and the
// set it records is not reachable through an exported API.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/pgindexstorage"
)

// reloptions is what pg_class.reloptions looks like once the query has encoded
// it as the JSON array the reader fetches.
const hnswReloptions = `["m=32","ef_construction=128"]`

// By default the reader records only what every surface carries.
//
// The set is decided by the weakest surface rather than by the catalog: a
// parameter one surface records and another drops makes every such index differ
// from its own inspected document forever (stokaro/ptah#2183).
func TestPostgresIndexStorageParams_TheDefaultSetIsTheCompatibleOne(t *testing.T) {
	tests := []struct {
		name    string
		options string
		want    map[string]string
	}{
		{
			name:    "an HNSW pair is not recorded",
			options: hnswReloptions,
			want:    nil,
		},
		{
			name:    "fillfactor is not recorded",
			options: `["fillfactor=70"]`,
			want:    nil,
		},
		{
			name:    "pages_per_range is",
			options: `["pages_per_range=32"]`,
			want:    map[string]string{"pages_per_range": "32"},
		},
		{
			name:    "and it is kept out of a mixed set",
			options: `["pages_per_range=32","fillfactor=70"]`,
			want:    map[string]string{"pages_per_range": "32"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(pgindexstorage.EnvVar, "false")

			params, err := postgresIndexStorageParams(test.options)

			c.Assert(err, qt.IsNil)
			c.Assert(params, qt.DeepEquals, test.want)
		})
	}
}

// Under the switch the reader records everything the catalog holds.
//
// This is the half a mutant that ignored the switch would leave green: the
// default table above passes whether or not the setting is consulted.
func TestPostgresIndexStorageParams_TheSwitchRecordsEverything(t *testing.T) {
	tests := []struct {
		name    string
		options string
		want    map[string]string
	}{
		{
			name:    "an HNSW pair",
			options: hnswReloptions,
			want:    map[string]string{"m": "32", "ef_construction": "128"},
		},
		{
			name:    "fillfactor",
			options: `["fillfactor=70"]`,
			want:    map[string]string{"fillfactor": "70"},
		},
		{
			name:    "an IVFFlat list count",
			options: `["lists=250"]`,
			want:    map[string]string{"lists": "250"},
		},
		{
			name:    "the compatible one is still recorded",
			options: `["pages_per_range=32","fillfactor=70"]`,
			want:    map[string]string{"pages_per_range": "32", "fillfactor": "70"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(pgindexstorage.EnvVar, "true")

			params, err := postgresIndexStorageParams(test.options)

			c.Assert(err, qt.IsNil)
			c.Assert(params, qt.DeepEquals, test.want)
		})
	}
}

// A reloption with no value is skipped whatever the setting says, because a
// valueless entry is not something CREATE INDEX can be handed back.
func TestPostgresIndexStorageParams_AValuelessEntryIsNotRecorded(t *testing.T) {
	tests := []struct {
		name    string
		setting string
	}{
		{name: "by default", setting: "false"},
		{name: "and under the switch", setting: "true"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv(pgindexstorage.EnvVar, test.setting)

			params, err := postgresIndexStorageParams(`["deduplicate_items"]`)

			c.Assert(err, qt.IsNil)
			c.Assert(params, qt.IsNil)
		})
	}
}
