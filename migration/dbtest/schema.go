package dbtest

import (
	"context"
	"fmt"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/generator"
	"github.com/stokaro/ptah/migration/migrator"
)

// SchemaOptions configures a single [RunSchemaTest] invocation.
type SchemaOptions struct {
	// Cases are the test cases to run, in order.
	Cases []Case
	// RootDir is a directory of Go entity annotations describing the desired
	// schema. It is parsed with goschema.ParseDir, rendered to dependency-ordered
	// CREATE DDL for the target dialect, and applied once per database before any
	// case runs against it (once per ephemeral per-case database, or once for a
	// shared explicit database).
	RootDir string
	// DBURL is an optional database URL to run the tests against. It must point at
	// a throwaway database, because tests mutate schema and data. When empty, an
	// ephemeral SQLite database is provisioned per case in a temporary directory
	// and removed afterwards.
	DBURL string
}

// RunSchemaTest applies a desired schema — parsed from the Go annotations in
// opts.RootDir — to a fresh database and runs each case's exec and assert steps
// against it. Unlike [RunMigrationTest] there are no migrations: the desired
// schema is rendered to CREATE DDL and applied once per case before that case's
// steps run, so a migrate_to step is invalid and fails with an explanatory
// detail rather than being silently skipped.
//
// Isolation mirrors [RunMigrationTest]: with an empty DBURL each case runs
// against its own fresh ephemeral SQLite database; with an explicit DBURL all
// cases share that one throwaway database and the caller owns their isolation.
//
// A returned error indicates the run itself could not be set up (invalid cases,
// an unparseable or unrenderable desired schema, an unreachable database, or a
// desired schema that fails to apply); ordinary assertion failures are captured
// in the report, so callers should inspect [Report.Failed].
func RunSchemaTest(ctx context.Context, opts SchemaOptions) (*Report, error) {
	if err := validateCases(opts.Cases); err != nil {
		return nil, fmt.Errorf("invalid test cases: %w", err)
	}

	schema, err := goschema.ParseDir(opts.RootDir)
	if err != nil {
		return nil, fmt.Errorf("parse desired schema from %s: %w", opts.RootDir, err)
	}

	// Provision the desired schema once per database — once per ephemeral
	// per-case database, or once for a shared explicit database — rather than
	// per case, so re-creating already-created objects never collides on the
	// shared-database path.
	provision := func(ctx context.Context, conn *dbschema.DatabaseConnection) error {
		return applyDesiredSchema(ctx, conn, schema)
	}
	run := func(ctx context.Context, conn *dbschema.DatabaseConnection, c Case) (CaseResult, error) {
		r := &runner{conn: conn}
		r.migrateTo = rejectMigrateToInSchemaTest
		return r.runCase(ctx, c), nil
	}
	return runCases(ctx, opts.DBURL, "SCHEMA", opts.Cases, provision, run)
}

// applyDesiredSchema renders schema to CREATE DDL for conn's dialect and applies
// it, so a case's steps run against the fully created desired schema. The DDL is
// split into individual statements (mirroring how the migrator applies migration
// bodies) because some drivers execute only the first statement of a multi-
// statement string. An empty schema renders to no statements and applies as a
// no-op.
func applyDesiredSchema(ctx context.Context, conn *dbschema.DatabaseConnection, schema *goschema.Database) error {
	dialect := conn.Info().Dialect
	upSQL, _, err := generator.GenerateCheckpoint(schema, dialect)
	if err != nil {
		return fmt.Errorf("render desired schema for dialect %q: %w", dialect, err)
	}
	for _, stmt := range migrator.SplitSQLStatements(upSQL) {
		if strings.TrimSpace(stmt) == "" {
			continue
		}
		if _, err := conn.ExecContext(ctx, stmt); err != nil {
			return fmt.Errorf("apply desired schema: %w", err)
		}
	}
	return nil
}

// rejectMigrateToInSchemaTest is the migrate_to handler for schema tests. A
// schema test applies a desired schema directly and has no migration history to
// move between, so a migrate_to step is a mistake rather than a runnable action.
func rejectMigrateToInSchemaTest(context.Context, string) (passed bool, detail string) {
	return false, `migrate_to is not valid in a schema test; use "ptah migrations test"`
}
