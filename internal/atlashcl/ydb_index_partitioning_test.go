package atlashcl_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestYDBIndexPartitioning_RejectsInvalidSettings(t *testing.T) {
	for _, test := range []struct{ name, setting, value string }{
		{"zero count", "auto_partitioning_min_partitions_count", "0"},
		{"invalid switch", "auto_partitioning_by_load", `"sometimes"`},
		{"invalid replicas", "read_replicas_settings", `"nowhere"`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, err := parseWithOwners(t.Context(), []byte("table \"items\" {\n column \"id\" { type = Int64 }\n index \"by_id\" {\n columns = [column.id]\n "+test.setting+" = "+test.value+"\n }\n}"), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, `(?s).*index "by_id": invalid `+test.setting+`.*`)
		})
	}
}

// TestYDBIndexPartitioning_ReportsTheEarlierRefusal reports a document's first
// mistake by position, whether the YDB owner or the reader refuses it. The
// owner decides after the reader has walked the whole document, so without
// the position rule a wrong index setting would yield to a reader error that
// comes after it in the file.
func TestYDBIndexPartitioning_ReportsTheEarlierRefusal(t *testing.T) {
	wrongIndex := "table \"items\" {\n  column \"id\" {\n    type = Int64\n  }\n  index \"by_id\" {\n    columns = [column.id]\n    auto_partitioning_min_partitions_count = 0\n  }\n}\n"
	laterTable := "table \"later\" {\n  column \"id\" {\n    type = Int64\n    null = \"x\"\n  }\n}\n"
	tests := []struct {
		name     string
		document string
		wantErr  string
	}{
		{name: "the index setting comes first", document: wrongIndex + laterTable,
			wantErr: `parse HCL schema at schema\.hcl:5,3-8: index "by_id": invalid auto_partitioning_min_partitions_count.*`},
		{name: "the reader's error comes first", document: laterTable + wrongIndex,
			wantErr: `parse HCL schema at schema\.hcl:4,5-9: column attribute "null" must be a bool`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := parseWithOwners(t.Context(), []byte(test.document), "schema.hcl")
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db, qt.IsNil)
		})
	}
}
