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
	request := lintext.CheckRequest{Target: lintext.Target{Name: "postgres"}, Codes: []string{"TX101", "TX201"}, Files: files}
	result, err := pglint.Service{}.CheckMigrations(c.Context(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(lintext.ValidateCheck(request, result), qt.IsNil)
	return result.Findings
}

func TestRules(t *testing.T) {
	c := qt.New(t)
	rules := pglint.Rules()
	rules[0].Targets[0] = "mysql"

	c.Assert(pglint.Rules(), qt.DeepEquals, []lintext.Rule{
		{Code: "TX101", Title: "transactional and non-transactional statements mixed", Severity: lintext.Warning, Targets: []string{"postgres"}},
		{Code: "TX201", Title: "transaction block embedded in migration", Severity: lintext.Warning, Targets: []string{"postgres"}},
	})
}

// TX101 reports the first statement that needs autocommit in a file that also
// holds transactional DDL, in either direction, and nothing in a file that
// runs outside a transaction or holds nothing transactional; BEGIN and COMMIT
// are neither side of the mix, which leaves TX201 to report them.
func TestService_CheckMigrations_TransactionMix(t *testing.T) {
	const mixed = "CREATE TABLE t (id int);\nCREATE INDEX CONCURRENTLY idx ON t (id);\n"
	noTransaction := file("1.up.sql", true, mixed)
	noTransaction.NoTransaction = true
	for _, test := range []struct {
		name string
		file lintext.File
		want []lintext.Finding
	}{
		{"an up file", file("1.up.sql", true, mixed), []lintext.Finding{{Code: "TX101", File: 0, Statement: 1,
			Message: "this migration mixes PostgreSQL statements that require autocommit with transactional DDL: " +
				"CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block; split them into separate migrations"}}},
		{"a down file", file("1.down.sql", false, "BEGIN;\n"+mixed+"COMMIT;\n"), []lintext.Finding{{Code: "TX101", File: 0, Statement: 2,
			Message: "this migration mixes PostgreSQL statements that require autocommit with transactional DDL: " +
				"CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block; split them into separate migrations"}}},
		{"a file outside a transaction", noTransaction, nil},
		{"nothing transactional", file("1.up.sql", true, "CREATE INDEX CONCURRENTLY idx ON t (id);\n"), nil},
		{"transaction control alone around it", file("1.up.sql", true, "BEGIN;\nCREATE INDEX CONCURRENTLY idx ON t (id);\nCOMMIT;\n"),
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: "explicit transaction blocks conflict with the migrator's " +
				"transaction management; remove BEGIN/COMMIT or mark the migration non-transactional"}}},
		{"nothing that needs autocommit", file("1.up.sql", true, "CREATE TABLE t (id int);\n"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(check(c, test.file), qt.DeepEquals, test.want)
		})
	}
}

// TX201 reports an explicit transaction block in an up file, and leaves a
// rollback alone.
func TestService_CheckMigrations_EmbeddedTransaction(t *testing.T) {
	const message = "explicit transaction blocks conflict with the migrator's transaction management; " +
		"remove BEGIN/COMMIT or mark the migration non-transactional"
	for _, test := range []struct {
		name string
		file lintext.File
		want []lintext.Finding
	}{
		{"BEGIN", file("1.up.sql", true, "BEGIN;\nALTER TABLE t ADD COLUMN c int;\nCOMMIT;\n"),
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: message}}},
		{"START TRANSACTION", file("1.up.sql", true, "START TRANSACTION;\nALTER TABLE t ADD COLUMN c int;\nCOMMIT;\n"),
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: message}}},
		{"a down file", file("1.down.sql", false, "BEGIN;\nALTER TABLE t DROP COLUMN c;\nCOMMIT;\n"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(check(c, test.file), qt.DeepEquals, test.want)
		})
	}
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
		{"a code the owner does not publish", context.Background(), []string{"MY101"}, `.*no check of rule MY101`},
		{"cancellation", canceled, []string{"TX201"}, `context canceled`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := pglint.Service{}.CheckMigrations(test.ctx, lintext.CheckRequest{Codes: test.codes,
				Files: []lintext.File{file("1.up.sql", true, "BEGIN;\n")}})

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result, qt.DeepEquals, lintext.CheckResult{})
		})
	}
}
