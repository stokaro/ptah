package atlasschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/atlasschema"
	"ptah.run/migration/migrator"
)

// enumValueUsedPlan is the plan `ptah schema apply` computes for a desired
// state that adds a value to an existing enum and a column defaulting to it,
// comment headers included. PostgreSQL 18.6 refuses its second statement
// inside one transaction with `ERROR: unsafe use of new value "archived" of
// enum type probe_status (SQLSTATE 55P04)` (stokaro/ptah#3283).
var enumValueUsedPlan = []string{
	`ALTER TYPE "probe_status" ADD VALUE 'archived'`,
	"-- Add/modify columns for table: messages --\n-- ALTER statements: --\n" +
		`ALTER TABLE "messages" ADD COLUMN "archive_state" probe_status NOT NULL DEFAULT 'archived'`,
}

// TestPreflightApplyTransaction_FailurePath pins the refusals schema apply
// makes before it sends a plan the engine would refuse inside a transaction.
//
// The enum rows are the defect: a preflight that searches the plan for the
// text `CREATE INDEX CONCURRENTLY` lets the enum plan reach the server, which
// answers with a bare 55P04. The unique and drop rows are the concurrent-index
// spellings the same text search does not see. The first row also
// pins that the quoted statement is the one the executor runs, without the
// planner's comment header, and that a comment-only entry is not counted.
func TestPreflightApplyTransaction_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		txMode     migrator.MigrationTxMode
		statements []string
		want       string
	}{
		{
			name:       "an enum value added to an existing type and used, tx-mode file",
			txMode:     migrator.MigrationTxModeFile,
			statements: enumValueUsedPlan,
			want: `(?s)the planned changes cannot run inside a transaction: statement 2 of 2: ` +
				`it uses 'archived', a value an earlier statement in the same transaction adds to ` +
				`the pre-existing enum type "probe_status", .*; ` +
				`rerun with --tx-mode none, which commits each statement as it runs\n` +
				`SQL: ALTER TABLE "messages" ADD COLUMN "archive_state" .*`,
		},
		{
			name:   "a comment-only entry is not a statement",
			txMode: migrator.MigrationTxModeFile,
			statements: []string{
				"-- POSTGRES TABLE: messages --",
				enumValueUsedPlan[0],
				enumValueUsedPlan[1],
			},
			want: `(?s)the planned changes cannot run inside a transaction: statement 2 of 2: it uses 'archived'.*`,
		},
		{
			name:       "an enum value added to an existing type and used, tx-mode all",
			txMode:     migrator.MigrationTxModeAll,
			statements: enumValueUsedPlan,
			want:       `(?s)the planned changes cannot run inside a transaction: statement 2 of 2: it uses 'archived'.*--tx-mode none.*`,
		},
		{
			name:       "a concurrent index build",
			txMode:     migrator.MigrationTxModeFile,
			statements: []string{`CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_widgets_a" ON "widgets" ("a")`},
			want: `(?s)the planned changes cannot run inside a transaction: statement 1 of 1: ` +
				`CREATE or DROP INDEX CONCURRENTLY is refused inside a transaction block; ` +
				`rerun with --tx-mode none.*`,
		},
		{
			name:       "a concurrent unique index build",
			txMode:     migrator.MigrationTxModeFile,
			statements: []string{`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_widgets_a" ON "widgets" ("a")`},
			want:       `(?s)the planned changes cannot run inside a transaction: statement 1 of 1: CREATE or DROP INDEX CONCURRENTLY.*`,
		},
		{
			name:       "a concurrent index drop",
			txMode:     migrator.MigrationTxModeFile,
			statements: []string{`DROP INDEX CONCURRENTLY IF EXISTS "idx_widgets_a"`},
			want:       `(?s)the planned changes cannot run inside a transaction: statement 1 of 1: CREATE or DROP INDEX CONCURRENTLY.*`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := atlasschema.PreflightApplyTransaction(
				platform.Postgres, capability.Postgres16(), test.txMode, test.statements)

			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}

// TestPreflightApplyTransaction_HappyPath holds the preflight to the engine's
// rule rather than to a keyword.
//
// The tx-mode none rows are the way through the refusal names, so refusing
// them would leave the operator nothing to run. The created-type row is the
// one a keyword check fails: PostgreSQL accepts a value added to an enum type
// created in the same transaction, and a saved or edited plan can hold exactly
// that.
func TestPreflightApplyTransaction_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		txMode     migrator.MigrationTxMode
		statements []string
	}{
		{
			name:       "the enum plan under tx-mode none",
			txMode:     migrator.MigrationTxModeNone,
			statements: enumValueUsedPlan,
		},
		{
			name:       "a concurrent index build under tx-mode none",
			txMode:     migrator.MigrationTxModeNone,
			statements: []string{`CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_widgets_a" ON "widgets" ("a")`},
		},
		{
			name:   "a value added to a type created in the same plan and used",
			txMode: migrator.MigrationTxModeFile,
			statements: []string{
				`CREATE TYPE "fresh_status" AS ENUM ('draft')`,
				`ALTER TYPE "fresh_status" ADD VALUE 'archived'`,
				`CREATE TABLE "fresh_messages" ("state" fresh_status NOT NULL DEFAULT 'archived')`,
			},
		},
		{
			name:       "a value added to an existing type and not used",
			txMode:     migrator.MigrationTxModeFile,
			statements: enumValueUsedPlan[:1],
		},
		{
			name:       "an ordinary index build",
			txMode:     migrator.MigrationTxModeFile,
			statements: []string{`CREATE INDEX IF NOT EXISTS "idx_widgets_a" ON "widgets" ("a")`},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := atlasschema.PreflightApplyTransaction(
				platform.Postgres, capability.Postgres16(), test.txMode, test.statements)

			c.Assert(err, qt.IsNil)
		})
	}
}
