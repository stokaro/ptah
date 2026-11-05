package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
)

// TestAnalyzeWithPolicy_TransactionOwnerDecidesTX201 holds that the gate
// judges a file's transaction control by who opens the transaction it runs
// in. With TX201 gated at error, a file that opens and commits its own
// transaction refuses an apply whose migrator wraps it, and passes one that
// runs it as written, as golang-migrate does.
func TestAnalyzeWithPolicy_TransactionOwnerDecidesTX201(t *testing.T) {
	fsys := fstest.MapFS{
		lint.ConfigFileName:       {Data: []byte("gate:\n  families: [TX]\nrules:\n  TX201:\n    severity: error\n")},
		"0000000001_own.up.sql":   {Data: []byte("BEGIN;\nCREATE TABLE t1 (id int);\nCOMMIT;\n")},
		"0000000001_own.down.sql": {Data: []byte("DROP TABLE t1;\n")},
	}
	tests := []struct {
		name  string
		owner lintext.TransactionOwner
		want  []string
	}{
		{name: "a migrator that wraps each file", owner: lintext.TransactionOwnerMigrator, want: []string{"TX201"}},
		{name: "files that open their own transactions", owner: lintext.TransactionOwnerFile, want: make([]string, 0)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			policy, err := migrationlintgate.LoadPolicy(builtintest.Runtime(), fsys, "postgres")
			c.Assert(err, qt.IsNil)

			findings, err := migrationlintgate.AnalyzeWithPolicy(c.Context(), builtintest.Runtime(), fsys, []int64{1},
				policy.WithTransactionOwner(test.owner), "")

			c.Assert(err, qt.IsNil)
			c.Assert(gateRules(findings), qt.DeepEquals, test.want)
		})
	}
}
