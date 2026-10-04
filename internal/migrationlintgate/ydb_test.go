package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
)

// ydbGateFS is a YDB directory whose pending version 2 drops a table and adds
// a unique index the newest line refuses on an existing table.
func ydbGateFS(policy string) fstest.MapFS {
	return fstest.MapFS{
		lint.ConfigFileName: {Data: []byte(policy)},
		"0000000001_users.up.sql": {Data: []byte(
			"CREATE TABLE `shop/users` (id Uint64 NOT NULL, email Utf8, PRIMARY KEY (id));\n" +
				"CREATE TABLE `shop/legacy` (id Uint64 NOT NULL, PRIMARY KEY (id));\n")},
		"0000000001_users.down.sql": {Data: []byte("DROP TABLE `shop/users`;\nDROP TABLE `shop/legacy`;\n")},
		"0000000002_change.up.sql": {Data: []byte(
			"ALTER TABLE `shop/users` ADD INDEX users_email GLOBAL UNIQUE SYNC ON (email);\n" +
				"DROP TABLE `shop/legacy`;\n")},
		"0000000002_change.down.sql": {Data: []byte("ALTER TABLE `shop/users` DROP INDEX users_email;\n")},
	}
}

// On a YDB connection the gate reads the migrations as YQL and plans against
// the YDB line the policy names, and blocks on the data-safety family as it
// does everywhere. The YD findings are reported by `ptah migrations lint` and
// refuse an apply only where the policy gates on the family.
func TestAnalyze_YDB(t *testing.T) {
	tests := []struct {
		name   string
		policy string
		want   []string
	}{
		{name: "the default gate", policy: "dialect: ydb\n", want: []string{"DS101"}},
		{name: "a policy that gates on the YD family", policy: "dialect: ydbs\nserver-version: \"25.1.4.7\"\ngate:\n  families: [YD]\n",
			want: []string{"YD101", "DS101"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			findings, err := migrationlintgate.Analyze(ydbGateFS(test.policy), []int64{2}, "ydb", "")

			c.Assert(err, qt.IsNil)
			c.Assert(gateRules(findings), qt.DeepEquals, test.want)
		})
	}
}

// A policy for YDB names its own family: the YDB lines share no rule set with
// another engine.
func TestLoadPolicy_YDBStandsAlone(t *testing.T) {
	c := qt.New(t)

	_, err := migrationlintgate.LoadPolicy(ydbGateFS("dialect: postgres\n"), "ydb")

	c.Assert(err, qt.ErrorMatches, `lint dialect "postgres" does not match database dialect "ydb"`)
}
