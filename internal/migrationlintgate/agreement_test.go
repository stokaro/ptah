package migrationlintgate_test

import (
	"context"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/migrationlintgate"
	"go.5x5.cz/ptah/internal/migrationlintreport"
	"go.5x5.cz/ptah/migration/lint"
)

// This file is the completion criterion for stokaro/ptah#270: `ptah migrations
// lint` and `ptah migrations up` must accept exactly the same policy files.
//
// The two commands ask the same question through different code. `up` resolves
// the policy against the dialect the connection reports
// (migrationlintgate.LoadPolicy); `lint` resolves it against the dialect its
// --dev-url names (migrationlintreport.Build). They were written as two
// comparisons and drifted: after #1317 the gate compared raw strings while lint
// compared its own, so a directory `lint` reported clean at exit 0 was refused
// by `up` at exit 2. Testing them side by side is what keeps one predicate
// answering for both.

// unreachableDevPort is a port nothing listens on, so an accepted policy fails
// at the connection instead of running a replay. The refusal path never gets
// this far, which is what makes the two outcomes distinguishable without a
// database.
const unreachableDevPort = "127.0.0.1:1"

// devURLs are directly connectable URLs whose scheme names each database
// dialect the agreement rows exercise.
var devURLs = map[string]string{
	"postgres":    "postgres://ptah:ptah@" + unreachableDevPort + "/ptah?sslmode=disable",
	"mariadb":     "mariadb://ptah:ptah@" + unreachableDevPort + "/ptah",
	"mysql":       "mysql://ptah:ptah@" + unreachableDevPort + "/ptah",
	"cockroachdb": "cockroachdb://ptah:ptah@" + unreachableDevPort + "/ptah?sslmode=disable",
	"sqlserver":   "sqlserver://ptah:ptah@" + unreachableDevPort + "?database=ptah",
}

// policyDirectory is a minimal well-formed migration directory carrying the
// policy under test. The SQL is deliberately unremarkable: this file measures
// which policies are accepted, not which findings they produce.
func policyDirectory(policy string) fstest.MapFS {
	return fstest.MapFS{
		lint.ConfigFileName:                {Data: []byte(policy)},
		"0000000001_create_users.up.sql":   {Data: []byte("CREATE TABLE users (id INT PRIMARY KEY);\n")},
		"0000000001_create_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
	}
}

// refusedForDialect classifies an error as "this command rejected the policy's
// dialect". Every other outcome -- success, or the connection failure an
// accepted policy reaches on an unreachable dev URL -- counts as acceptance.
func refusedForDialect(err error) bool {
	messages := []string{
		"does not match database dialect",
		"does not match --dev-url dialect",
		"unsupported lint dialect",
		"invalid dialect",
		"invalid --dialect value",
	}
	for _, message := range messages {
		if err != nil && strings.Contains(err.Error(), message) {
			return true
		}
	}
	return false
}

// upRefuses reports whether `ptah migrations up` would refuse this policy
// against a database whose wire dialect is databaseDialect.
func upRefuses(fsys fstest.MapFS, databaseDialect string) bool {
	_, err := migrationlintgate.LoadPolicy(fsys, databaseDialect)
	return refusedForDialect(err)
}

// lintRefuses reports whether `ptah migrations lint` would refuse this policy
// against a dev database of the same dialect.
func lintRefuses(c *qt.C, fsys fstest.MapFS, databaseDialect string) bool {
	devURL, known := devURLs[databaseDialect]
	c.Assert(known, qt.IsTrue, qt.Commentf("no dev URL for dialect %q", databaseDialect))

	_, err := migrationlintreport.Build(context.Background(), migrationlintreport.Options{
		FS:        fsys,
		Dir:       "migrations",
		DirFormat: "auto",
		DevURL:    devURL,
		FailOn:    migrationlintreport.FailOnNone,
	}, projectconfig.Config{})
	return refusedForDialect(err)
}

// TestLintAndUpAgree_OnEveryPolicyShape asserts the two commands reach the same
// verdict on each policy file, and -- separately -- that the verdict is the
// expected one. Agreement alone would be satisfied by both commands refusing
// everything, so each row also pins which way they agree.
func TestLintAndUpAgree_OnEveryPolicyShape(t *testing.T) {
	tests := []struct {
		name     string
		policy   string
		database string
		assert   func(c *qt.C, refused bool)
	}{
		{
			name:     "no policy file dialect",
			policy:   "disabled-rules:\n  - DS102\n",
			database: "mariadb",
			assert:   assertAccepted,
		},
		{
			name:     "canonical dialect naming the database exactly",
			policy:   "dialect: mariadb\n",
			database: "mariadb",
			assert:   assertAccepted,
		},
		{
			name:     "documented alias of the database dialect",
			policy:   "dialect: pgx\n",
			database: "postgres",
			assert:   assertAccepted,
		},
		{
			name:     "second documented alias of the database dialect",
			policy:   "dialect: postgresql\n",
			database: "postgres",
			assert:   assertAccepted,
		},
		{
			name:     "MySQL policy on a MariaDB database",
			policy:   "dialect: mysql\n",
			database: "mariadb",
			assert:   assertAccepted,
		},
		{
			name:     "MariaDB policy on a MySQL database",
			policy:   "dialect: mariadb\n",
			database: "mysql",
			assert:   assertAccepted,
		},
		{
			name:     "PostgreSQL policy on a CockroachDB database",
			policy:   "dialect: postgres\n",
			database: "cockroachdb",
			assert:   assertAccepted,
		},
		{
			name:     "CockroachDB alias on a CockroachDB database",
			policy:   "dialect: crdb\n",
			database: "cockroachdb",
			assert:   assertAccepted,
		},
		{
			name:     "SQL Server policy on a SQL Server database",
			policy:   "dialect: sqlserver\n",
			database: "sqlserver",
			assert:   assertAccepted,
		},
		{
			name:     "SQL Server alias on a SQL Server database",
			policy:   "dialect: mssql\n",
			database: "sqlserver",
			assert:   assertAccepted,
		},
		{
			name:     "cross-family SQL Server policy on a PostgreSQL database",
			policy:   "dialect: sqlserver\n",
			database: "postgres",
			assert:   assertRefused,
		},
		{
			name:     "cross-family PostgreSQL policy on a MariaDB database",
			policy:   "dialect: postgres\n",
			database: "mariadb",
			assert:   assertRefused,
		},
		{
			name:     "cross-family MySQL policy on a PostgreSQL database",
			policy:   "dialect: mysql\n",
			database: "postgres",
			assert:   assertRefused,
		},
		{
			name:     "cross-family SQLite policy on a MariaDB database",
			policy:   "dialect: sqlite\n",
			database: "mariadb",
			assert:   assertRefused,
		},
		{
			name:     "unsupported dialect spelling",
			policy:   "dialect: oracle\n",
			database: "postgres",
			assert:   assertRefused,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := policyDirectory(test.policy)

			up := upRefuses(fsys, test.database)
			lintVerdict := lintRefuses(c, fsys, test.database)

			c.Assert(up, qt.Equals, lintVerdict, qt.Commentf(
				"migrations up refused=%v but migrations lint refused=%v for policy %q against %s",
				up, lintVerdict, test.policy, test.database,
			))
			test.assert(c, up)
		})
	}
}

func assertAccepted(c *qt.C, refused bool) {
	c.Assert(refused, qt.IsFalse)
}

func assertRefused(c *qt.C, refused bool) {
	c.Assert(refused, qt.IsTrue)
}
