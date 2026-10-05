package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlashcl"
)

func TestYDBIndexPartitioning_RejectsInvalidSettings(t *testing.T) {
	for _, test := range []struct{ name, setting, value string }{
		{"zero count", "auto_partitioning_min_partitions_count", "0"},
		{"invalid switch", "auto_partitioning_by_load", `"sometimes"`},
		{"invalid replicas", "read_replicas_settings", `"nowhere"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := atlashcl.Parse([]byte("table \"items\" {\n column \"id\" { type = Int64 }\n index \"by_id\" {\n columns = [column.id]\n "+test.setting+" = "+test.value+"\n }\n}"), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, `(?s).*index "by_id": invalid `+test.setting+`.*`)
		})
	}
}
