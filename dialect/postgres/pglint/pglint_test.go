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
	request := lintext.CheckRequest{Target: lintext.Target{Name: "postgres"}, Codes: []string{"TX101", "TX201"}, Files: files,
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
		codes = append(codes, rule.Code)
	}
	c.Assert(codes, qt.DeepEquals, []string{
		"ON101", "ON103",
		"PG101", "PG102", "PG103", "PG103P", "PG104", "PG105", "PG106", "PG108", "PG109", "PG110",
		"PG301", "PG301P", "PG302", "PG303", "PG304", "PG305", "PG306", "PG307", "PG308", "PG309", "PG310", "PG311",
		"PG312", "PG312P", "PG314", "PG320",
		"TX101", "TX201",
	})
	c.Assert(again[0].Targets, qt.DeepEquals, []string{"postgres"})
	c.Assert(again[len(again)-2:], qt.DeepEquals, []lintext.Rule{
		{Code: "TX101", Title: "transactional and non-transactional statements mixed", Severity: lintext.Warning, Targets: []string{"postgres"}},
		{Code: "TX201", Title: "transaction block embedded in migration", Severity: lintext.Warning, Targets: []string{"postgres"}},
	})
}

// TX101 reports the first statement that needs autocommit in a file that also
// holds transactional DDL, in either direction, and nothing in a file that
// runs outside a transaction or holds nothing transactional. BEGIN and COMMIT
// are neither side of the mix; TX201 judges them, and reports them here
// because the migrator wraps the file.
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
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: wrappedBlockMessage}}},
		{"nothing that needs autocommit", file("1.up.sql", true, "CREATE TABLE t (id int);\n"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(check(c, test.file), qt.DeepEquals, test.want)
		})
	}
}

// wrappedBlockMessage is TX201 where the migrator wraps the file. It names a
// remedy that works there: a file the migrator wraps runs in one transaction
// already, while marking it non-transactional makes the migrator refuse the
// block outright.
const wrappedBlockMessage = "the migrator already runs this file in one transaction, so this BEGIN nests in it and " +
	"the file's COMMIT ends the migrator's transaction early; remove BEGIN/COMMIT"

// TX201 reports every explicit transaction block in an up file the migrator
// wraps, with a remedy of its own for one that runs without a transaction,
// and leaves a rollback alone.
func TestService_CheckMigrations_EmbeddedTransaction(t *testing.T) {
	noTransaction := file("1.up.sql", true, "BEGIN;\nALTER TABLE t ADD COLUMN c int;\nCOMMIT;\n")
	noTransaction.NoTransaction = true
	for _, test := range []struct {
		name string
		file lintext.File
		want []lintext.Finding
	}{
		{"BEGIN", file("1.up.sql", true, "BEGIN;\nALTER TABLE t ADD COLUMN c int;\nCOMMIT;\n"),
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: wrappedBlockMessage}}},
		{"START TRANSACTION", file("1.up.sql", true, "START TRANSACTION;\nALTER TABLE t ADD COLUMN c int;\nCOMMIT;\n"),
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: wrappedBlockMessage}}},
		{"two blocks", file("1.up.sql", true, "BEGIN;\nSELECT 1;\nCOMMIT;\nBEGIN;\nSELECT 2;\nCOMMIT;\n"),
			[]lintext.Finding{
				{Code: "TX201", File: 0, Statement: 0, Message: wrappedBlockMessage},
				{Code: "TX201", File: 0, Statement: 3, Message: wrappedBlockMessage},
			}},
		{"a migration that runs without a transaction", noTransaction,
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: "the migrator refuses transaction control in a " +
				"migration that runs without a transaction and commits each statement on its own; remove BEGIN/COMMIT"}}},
		{"a down file", file("1.down.sql", false, "BEGIN;\nALTER TABLE t DROP COLUMN c;\nCOMMIT;\n"), nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(check(c, test.file), qt.DeepEquals, test.want)
		})
	}
}

// TX201 asks who opens the transaction a file runs in. A migrator that opens
// none, as golang-migrate does, leaves the block as the file's only
// transaction, so nothing is reported; a value the owner does not know is
// judged as the migrator's. TX101 does not ask: the server refuses a
// concurrent index beside another statement in one query just as it refuses
// it in the migrator's transaction.
func TestService_CheckMigrations_EmbeddedTransactionFollowsTheTransactionOwner(t *testing.T) {
	const message = wrappedBlockMessage
	const block = "BEGIN;\nCREATE TABLE t (id int);\nINSERT INTO t VALUES (1);\nCOMMIT;\n"
	// The apply runs such a file's block as written under tx-mode none too.
	noTransaction := file("1.up.sql", true, block)
	noTransaction.NoTransaction = true
	for _, test := range []struct {
		name  string
		owner lintext.TransactionOwner
		file  lintext.File
		want  []lintext.Finding
	}{
		{"the migrator wraps the file", lintext.TransactionOwnerMigrator, file("1.up.sql", true, block),
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: message}}},
		{"the file opens its own", lintext.TransactionOwnerFile, file("1.up.sql", true, block), nil},
		{"the file opens its own and asks for no transaction", lintext.TransactionOwnerFile, noTransaction, nil},
		{"START TRANSACTION in a file that opens its own", lintext.TransactionOwnerFile,
			file("1.up.sql", true, "START TRANSACTION;\nCREATE TABLE t (id int);\nCOMMIT;\n"), nil},
		{"an owner the rule does not know", lintext.TransactionOwner("savepoint"), file("1.up.sql", true, block),
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 0, Message: message}}},
		{"a mix in a file that opens its own", lintext.TransactionOwnerFile,
			file("1.up.sql", true, "CREATE TABLE t (id int);\nCREATE INDEX CONCURRENTLY idx ON t (id);\n"),
			[]lintext.Finding{{Code: "TX101", File: 0, Statement: 1,
				Message: "this migration mixes PostgreSQL statements that require autocommit with transactional DDL: " +
					"CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block; split them into separate migrations"}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkOwned(c, test.owner, test.file), qt.DeepEquals, test.want)
		})
	}
}

// Under a runner that wraps nothing, TX201 reports only a block opened inside
// another the file has not closed, which draws "there is already a transaction
// in progress" there too. A block closed by COMMIT, END, ROLLBACK or ABORT
// makes room for the next; a savepoint rollback and AND CHAIN keep the block
// open.
func TestService_CheckMigrations_NestedTransactionBlock(t *testing.T) {
	const nested = "a transaction block is already open since the BEGIN on line 1; PostgreSQL ignores this " +
		"BEGIN with a warning, and the first COMMIT ends both blocks"
	for _, test := range []struct {
		name string
		sql  string
		want []lintext.Finding
	}{
		{"a BEGIN inside an open block", "BEGIN;\nBEGIN;\nCREATE TABLE t (id int);\nCOMMIT;\n",
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 1, Message: nested}}},
		{"START TRANSACTION inside an open block", "BEGIN;\nSTART TRANSACTION;\nCOMMIT;\n",
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 1, Message: nested}}},
		{"a block that rolls back to a savepoint stays open", "BEGIN;\nSAVEPOINT s;\nROLLBACK TO SAVEPOINT s;\nBEGIN;\nCOMMIT;\n",
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 3, Message: nested}}},
		{"a chained commit stays open", "BEGIN;\nCOMMIT AND CHAIN;\nBEGIN;\nCOMMIT;\n",
			[]lintext.Finding{{Code: "TX201", File: 0, Statement: 2, Message: nested}}},
		{"two blocks one after the other", "BEGIN;\nSELECT 1;\nCOMMIT;\nBEGIN;\nSELECT 2;\nEND;\n", nil},
		{"a block ended by ROLLBACK, then another", "BEGIN;\nROLLBACK;\nBEGIN;\nCOMMIT;\n", nil},
		{"a block ended by ABORT, then another", "START TRANSACTION;\nABORT;\nSTART TRANSACTION;\nCOMMIT;\n", nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkOwned(c, lintext.TransactionOwnerFile, file("1.up.sql", true, test.sql)), qt.DeepEquals, test.want)
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
		{"a code the owner does not publish", context.Background(), []string{"MY101"}, `.*no rule of code MY101`},
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
