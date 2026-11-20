package pglint_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/lintext"
	"ptah.run/dialect/postgres/pglint"
	"ptah.run/internal/lintscan"
)

// file is a migration file of the given direction holding sql, split the way
// the bundled PostgreSQL owner splits it.
func file(name string, up bool, sql string) lintext.File {
	return lintext.File{Name: name, Up: up, Down: !up, Statements: lintscan.Split(sql, lintscan.ForDialect("postgres"))}
}

// check runs the owner's rules over files and returns the findings.
func check(c *qt.C, files ...lintext.File) []lintext.Finding {
	c.Helper()
	return checkOwned(c, lintext.TransactionOwnerMigrator, files...)
}

// checkOwned is [check] for a run whose files run in transactions owner
// opens.
func checkOwned(c *qt.C, owner lintext.TransactionOwner, files ...lintext.File) []lintext.Finding {
	c.Helper()
	request := lintext.CheckRequest{Target: lintext.Target{Name: "postgres"}, Names: []string{"transaction/mixed-statements"}, Files: files,
		TransactionOwner: owner}
	result, err := pglint.Service{}.CheckMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(lintext.ValidateCheck(request, result), qt.IsNil)
	return result.Findings
}

// Rules publishes the owner's families in code order, each rule valid as
// data, and a call's rules share nothing with the next call's.
func TestRules(t *testing.T) {
	c := qt.New(t)
	rules := pglint.Rules()
	rules[0].Targets[0] = "mysql"

	again := pglint.Rules()
	codes := make([]string, 0, len(again))
	for _, rule := range again {
		c.Assert(rule.Validate(), qt.IsNil)
		codes = append(codes, rule.Name)
	}
	c.Assert(codes, qt.DeepEquals, []string{
		"online/unproven", "online/lock-held-across-scan",
		"locking/create-index", "transaction/add-enum-value", "transaction/concurrent-index", "transaction/per-chunk-index", "locking/add-primary-key", "locking/add-unique-constraint", "locking/drop-index", "locking/partitioned-index", "locking/add-exclusion-constraint", "design/column-alignment",
		"rewrite/alter-column-type", "rewrite/in-place-column-change", "rewrite/volatile-default", "locking/set-not-null", "locking/add-primary-key-on-nullable", "locking/add-check", "locking/add-foreign-key", "rewrite/set-persistence", "locking/create-trigger", "rewrite/add-stored-generated-column", "rewrite/add-identity-column", "rewrite/set-access-method",
		"locking/replace-primary-key", "security/definer-without-search-path", "rollout/weaken-replica-identity", "design/disable-autovacuum",
		"naming/identifier-too-long", "naming/identifier-collision",
		"transaction/mixed-statements",
	})
	c.Assert(again[0].Targets, qt.DeepEquals, []string{"postgres"})
	c.Assert(again[len(again)-1:], qt.DeepEquals, []lintext.Rule{
		{Name: "transaction/mixed-statements", Title: "transactional and non-transactional statements mixed", Severity: lintext.Warning, Targets: []string{"postgres"}},
	})
}

// transaction/mixed-statements reports the first statement that needs autocommit in a file that also
// holds transactional DDL, in either direction, and nothing in a file that
// runs outside a transaction or holds nothing transactional. BEGIN and COMMIT
// are neither side of the mix; transaction/explicit-transaction judges them in migration/lint.
func TestService_CheckMigrations_TransactionMix(t *testing.T) {
	const mixed = "CREATE TABLE t (id int);\nCREATE INDEX CONCURRENTLY idx ON t (id);\n"
	noTransaction := file("1.up.sql", true, mixed)
	noTransaction.NoTransaction = true
	for _, test := range []struct {
		name string
		file lintext.File
		want []lintext.Finding
	}{
		{"an up file", file("1.up.sql", true, mixed), []lintext.Finding{{Name: "transaction/mixed-statements", File: 0, Statement: 1,
			Message: "this migration mixes PostgreSQL statements that require autocommit with transactional DDL: " +
				"CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block; split them into separate migrations"}}},
		{"a down file", file("1.down.sql", false, "BEGIN;\n"+mixed+"COMMIT;\n"), []lintext.Finding{{Name: "transaction/mixed-statements", File: 0, Statement: 2,
			Message: "this migration mixes PostgreSQL statements that require autocommit with transactional DDL: " +
				"CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block; split them into separate migrations"}}},
		{"a file outside a transaction", noTransaction, nil},
		{"nothing transactional", file("1.up.sql", true, "CREATE INDEX CONCURRENTLY idx ON t (id);\n"), nil},
		{"transaction control alone around it", file("1.up.sql", true, "BEGIN;\nCREATE INDEX CONCURRENTLY idx ON t (id);\nCOMMIT;\n"), nil},
		{"nothing that needs autocommit", file("1.up.sql", true, "CREATE TABLE t (id int);\n"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(check(c, test.file), qt.DeepEquals, test.want)
		})
	}
}

// transaction/mixed-statements does not ask who opens the transaction a file runs in: the server
// refuses a concurrent index beside another statement in one query just as it
// refuses it in the migrator's transaction.
func TestService_CheckMigrations_TransactionMixIgnoresTheTransactionOwner(t *testing.T) {
	c := qt.New(t)

	got := checkOwned(c, lintext.TransactionOwnerFile,
		file("1.up.sql", true, "CREATE TABLE t (id int);\nCREATE INDEX CONCURRENTLY idx ON t (id);\n"))

	c.Assert(got, qt.DeepEquals, []lintext.Finding{{Name: "transaction/mixed-statements", File: 0, Statement: 1,
		Message: "this migration mixes PostgreSQL statements that require autocommit with transactional DDL: " +
			"CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block; split them into separate migrations"}})
}

func TestService_CheckMigrations_FailurePath(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	for _, test := range []struct {
		name    string
		ctx     context.Context
		codes   []string
		wantErr string
	}{
		{"a code the owner does not publish", context.Background(), []string{"locking/blocking-alter-table"}, `.*no rule of code locking/blocking-alter-table`},
		{"cancellation", canceled, []string{"transaction/mixed-statements"}, `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := pglint.Service{}.CheckMigrations(test.ctx, lintext.CheckRequest{Names: test.codes,
				Files: []lintext.File{file("1.up.sql", true, "BEGIN;\n")}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
		})
	}
}
