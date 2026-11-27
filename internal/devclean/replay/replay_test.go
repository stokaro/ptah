package replay_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/devclean/replay"
)

// TestRefusal_Error pins the wording of a refusal: a migration replay's or a
// rehearsal baseline's, with the reason the guard gives or the default one,
// and with the remedy after it.
func TestRefusal_Error(t *testing.T) {
	tests := []struct {
		name    string
		refusal replay.Refusal
		want    string
	}{
		{
			name:    "a migration replay",
			refusal: replay.Refusal{Dialect: "sqlserver", Operation: "USE"},
			want:    "sqlserver migration replay rejects USE because its effects cannot be confined to the disposable database realm",
		},
		{
			name:    "a rehearsal baseline",
			refusal: replay.Refusal{Dialect: "sqlserver", Operation: "USE", Baseline: true},
			want:    "sqlserver rehearsal baseline refuses USE because its effects cannot be confined to the dev database realm",
		},
		{
			name:    "a migration replay with a reason and a remedy",
			refusal: replay.Refusal{Dialect: "sqlserver", Operation: "T-SQL at offset 9", Reason: "it is unreadable", Remedy: "rewrite it"},
			want:    "sqlserver migration replay rejects T-SQL at offset 9 because it is unreadable; rewrite it",
		},
		{
			name:    "a rehearsal baseline with a reason",
			refusal: replay.Refusal{Dialect: "sqlserver", Operation: "T-SQL at offset 9", Reason: "it is unreadable", Baseline: true},
			want:    "sqlserver rehearsal baseline refuses T-SQL at offset 9 because it is unreadable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(test.refusal.Error(), qt.Equals, test.want)
		})
	}
}
