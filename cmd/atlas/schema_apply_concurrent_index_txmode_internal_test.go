package atlas

// White-box testing required: concurrentIndexPolicySetting is the internal
// classifier behind the schema-apply transaction-mode guard, and its whole
// value is in the spellings it recognizes.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestConcurrentIndexPolicySetting pins which planned statements force
// --tx-mode none on schema apply. PostgreSQL rejects every CONCURRENTLY index
// form inside a transaction block, so a statement the classifier misses is one
// Ptah would wrap and then watch fail at execution.
//
// Reverting the classifier to a bare "CREATE INDEX CONCURRENTLY" substring test
// prints `values are not equal: "" != "diff.concurrent_index.create"` for the
// unique row and `"" != "diff.concurrent_index.drop"` for the drop row. The
// blocking rows keep returning "" either way, which is what makes the pair
// discriminating rather than a rule that fires on everything.
func TestConcurrentIndexPolicySetting(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		want      string
	}{
		{
			name:      "concurrent build",
			statement: `CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_members_email" ON "members" ("email");`,
			want:      "diff.concurrent_index.create",
		},
		{
			name:      "concurrent unique build",
			statement: `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_members_email" ON "members" ("email");`,
			want:      "diff.concurrent_index.create",
		},
		{
			name:      "concurrent drop",
			statement: `DROP INDEX CONCURRENTLY IF EXISTS "idx_members_email";`,
			want:      "diff.concurrent_index.drop",
		},
		{
			name:      "blocking build",
			statement: `CREATE INDEX IF NOT EXISTS "idx_members_email" ON "members" ("email");`,
			want:      "",
		},
		{
			name:      "blocking unique build",
			statement: `CREATE UNIQUE INDEX IF NOT EXISTS "idx_members_email" ON "members" ("email");`,
			want:      "",
		},
		{
			name:      "blocking drop",
			statement: `DROP INDEX IF EXISTS "idx_members_email";`,
			want:      "",
		},
		{
			name:      "lowercase concurrent drop",
			statement: `drop index concurrently if exists "idx_members_email";`,
			want:      "diff.concurrent_index.drop",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(concurrentIndexPolicySetting(tt.statement), qt.Equals, tt.want)
		})
	}
}
