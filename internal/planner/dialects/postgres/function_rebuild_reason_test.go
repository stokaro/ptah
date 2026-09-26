package postgres_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/schemadiff/difftypes"
)

// The comment on the drop names what changed. A fixed reason would report a
// routine whose parameters changed as one whose return type did
// (stokaro/ptah#3673).
func TestPlanner_FunctionRebuild_TheDropNamesWhatChanged(t *testing.T) {
	tests := []struct {
		name    string
		changes map[string]string
		want    string
	}{
		{name: "the return type", changes: map[string]string{"returns": "integer -> bigint"}, want: "its return type changed"},
		{name: "the parameters", changes: map[string]string{"parameters": "n integer -> n bigint"}, want: "its parameters changed"},
		{
			name:    "both",
			changes: map[string]string{"parameters": "n integer -> n bigint", "returns": "integer -> bigint"},
			want:    "its return type and parameters changed",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			sql := renderFunctionModification(c, difftypes.FunctionDiff{
				FunctionName:     "billing.total",
				Changes:          test.changes,
				CurrentSignature: new("n integer"),
				Desired:          totalFunction("n bigint", "bigint"),
			})

			c.Assert(sql, qt.Contains, "-- Drop function billing.total to recreate it: "+test.want+"\n")
		})
	}
}
