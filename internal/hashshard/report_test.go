package hashshard_test

import (
	"bytes"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/internal/hashshard"
)

// TestReportUndescribed_HappyPath pins the note. The key row names a primary
// key once although the catalog reports it as a constraint and as the index
// behind it; the several row arrives out of order, in two schemas.
func TestReportUndescribed_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		schema *catalog.Database
		want   string
	}{
		{
			name: "a primary key, reported as a constraint and as its index",
			schema: &catalog.Database{
				Constraints: []catalog.Constraint{{
					TableName: "hs", Name: "hs_pkey", Type: "PRIMARY KEY", HashShardBuckets: 16,
				}},
				Indexes: []catalog.Index{{
					TableName: "hs", Name: "hs_pkey", IsPrimary: true, HashShardBuckets: 16,
				}},
			},
			want: "note: 1 CockroachDB key or index built USING HASH is described without it, because no" +
				" schema source can declare hash sharding; a description applied to another database" +
				" builds it unsharded, and a diff between the two reports no difference: hs.hs_pkey (16 buckets).\n",
		},
		{
			name: "several",
			schema: &catalog.Database{
				Constraints: []catalog.Constraint{
					{TableName: "hs8", Name: "hs8_pkey", Type: "PRIMARY KEY", HashShardBuckets: 8},
					{TableName: "plain", Name: "plain_pkey", Type: "PRIMARY KEY"},
				},
				Indexes: []catalog.Index{
					{Schema: "app", TableName: "events", Name: "events_at_idx", HashShardBuckets: 16},
					{TableName: "plain", Name: "plain_v_idx", HashShardBuckets: 16},
					{TableName: "plain", Name: "plain_w_idx"},
				},
			},
			want: "note: 3 CockroachDB keys and indexes built USING HASH are described without it, because" +
				" no schema source can declare hash sharding; a description applied to another database" +
				" builds them unsharded, and a diff between the two reports no difference:" +
				" app.events.events_at_idx (16 buckets), hs8.hs8_pkey (8 buckets), plain.plain_v_idx (16 buckets).\n",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			hashshard.ReportUndescribed(&out, test.schema)

			c.Assert(out.String(), qt.Equals, test.want)
		})
	}
}

// TestReportUndescribed_StaysSilentWithNothingSharded keeps the note off every
// read that met no hash-sharded key, which is every read outside CockroachDB.
func TestReportUndescribed_StaysSilentWithNothingSharded(t *testing.T) {
	tests := []struct {
		name   string
		schema *catalog.Database
	}{
		{name: "ordinary keys", schema: &catalog.Database{
			Constraints: []catalog.Constraint{{TableName: "t", Name: "t_pkey", Type: "PRIMARY KEY"}},
			Indexes:     []catalog.Index{{TableName: "t", Name: "t_v_idx"}},
		}},
		{name: "no description", schema: nil},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var out bytes.Buffer

			hashshard.ReportUndescribed(&out, test.schema)

			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

// TestReportUndescribed_AcceptsNoDiagnosticsStream is the inspect surfaces'
// spelling of "nowhere to write": a nil writer drops the note rather than
// failing the read that produced it.
func TestReportUndescribed_AcceptsNoDiagnosticsStream(_ *testing.T) {
	hashshard.ReportUndescribed(nil, &catalog.Database{
		Indexes: []catalog.Index{{TableName: "plain", Name: "plain_v_idx", HashShardBuckets: 16}},
	})
}
