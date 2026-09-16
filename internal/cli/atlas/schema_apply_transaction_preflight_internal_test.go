package atlas

// White-box testing required: validateAtlasSchemaApplyDiffPolicy is the
// compatibility surface's own refusal, and what it answers for each finding is
// the whole of this change. It is unexported and takes the connection's info,
// so no exported entry point reaches it without a live server.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/migration/migrator"
)

// postgresInfo is a PostgreSQL connection's own description of itself, which is
// all the refusal reads.
func postgresInfo() catalog.ServerInfo {
	return catalog.ServerInfo{Dialect: platform.Postgres, Capabilities: capability.Postgres16()}
}

// enumValueUsedPlan is the plan a desired state produces when it adds a value
// to an existing enum and then uses it. PostgreSQL refuses the second statement
// inside one transaction with SQLSTATE 55P04.
var enumValueUsedPlan = []string{
	`ALTER TYPE "probe_status" ADD VALUE 'archived'`,
	`ALTER TABLE "messages" ADD COLUMN "archive_state" probe_status NOT NULL DEFAULT 'archived'`,
}

// TestValidateAtlasSchemaApplyDiffPolicy_FailurePath pins what the
// compatibility surface refuses, and in whose terms.
//
// The enum row is the defect: a substring scan for CONCURRENTLY sees nothing in
// that plan, so it reached the server and failed with a bare 55P04. The
// concurrent-index rows are the reason the surface keeps a sentence of its own:
// there the statement came from a diff policy the project configured, and the
// refusal names that setting rather than only the flag.
func TestValidateAtlasSchemaApplyDiffPolicy_FailurePath(t *testing.T) {
	tests := []struct {
		name       string
		txMode     migrator.MigrationTxMode
		statements []string
		want       string
	}{
		{
			name:       "an enum value added to an existing type and used",
			txMode:     migrator.MigrationTxModeFile,
			statements: enumValueUsedPlan,
			want: `(?s)the planned changes cannot run inside a transaction: statement 2 of 2: ` +
				`it uses 'archived'.*rerun with --tx-mode none.*`,
		},
		{
			name:       "the same plan under tx-mode all",
			txMode:     migrator.MigrationTxModeAll,
			statements: enumValueUsedPlan,
			want:       `(?s)the planned changes cannot run inside a transaction: statement 2 of 2: it uses 'archived'.*`,
		},
		{
			name:       "a concurrent index build names its diff policy",
			txMode:     migrator.MigrationTxModeFile,
			statements: []string{`CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_widgets_a" ON "widgets" ("a")`},
			want:       `^atlas\.hcl diff\.concurrent_index\.create requires --tx-mode none for schema apply$`,
		},
		{
			name:       "a concurrent unique index build names the same policy",
			txMode:     migrator.MigrationTxModeFile,
			statements: []string{`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_widgets_a" ON "widgets" ("a")`},
			want:       `^atlas\.hcl diff\.concurrent_index\.create requires --tx-mode none for schema apply$`,
		},
		{
			name:       "a concurrent index drop names the drop policy",
			txMode:     migrator.MigrationTxModeFile,
			statements: []string{`DROP INDEX CONCURRENTLY IF EXISTS "idx_widgets_a"`},
			want:       `^atlas\.hcl diff\.concurrent_index\.drop requires --tx-mode none for schema apply$`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			err := validateAtlasSchemaApplyDiffPolicy(tt.txMode, postgresInfo(), tt.statements)
			c.Assert(err, qt.ErrorMatches, tt.want)
		})
	}
}

// TestValidateAtlasSchemaApplyDiffPolicy_HappyPath pins what it accepts.
//
// The created-type row is why the recognition reads the plan in order rather
// than matching keywords: PostgreSQL accepts a value added to a type the same
// transaction created, and a keyword check would refuse it.
func TestValidateAtlasSchemaApplyDiffPolicy_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		txMode     migrator.MigrationTxMode
		statements []string
	}{
		{
			name:       "no statements",
			txMode:     migrator.MigrationTxModeFile,
			statements: nil,
		},
		{
			name:       "the enum plan under tx-mode none",
			txMode:     migrator.MigrationTxModeNone,
			statements: enumValueUsedPlan,
		},
		{
			name:       "a concurrent index under tx-mode none",
			txMode:     migrator.MigrationTxModeNone,
			statements: []string{`CREATE INDEX CONCURRENTLY IF NOT EXISTS "idx_widgets_a" ON "widgets" ("a")`},
		},
		{
			name:   "a value added to a type the same plan creates",
			txMode: migrator.MigrationTxModeFile,
			statements: []string{
				`CREATE TYPE "fresh_status" AS ENUM ('draft')`,
				`ALTER TYPE "fresh_status" ADD VALUE 'archived'`,
				`ALTER TABLE "messages" ADD COLUMN "archive_state" fresh_status NOT NULL DEFAULT 'archived'`,
			},
		},
		{
			name:   "an ordinary plan",
			txMode: migrator.MigrationTxModeFile,
			statements: []string{
				`CREATE TABLE "widgets" ("a" TEXT)`,
				`CREATE INDEX IF NOT EXISTS "idx_widgets_a" ON "widgets" ("a")`,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(validateAtlasSchemaApplyDiffPolicy(tt.txMode, postgresInfo(), tt.statements), qt.IsNil)
		})
	}
}
