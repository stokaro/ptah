package integration

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/generator"
)

// testDynamicFKActionEvolution is the live versioned-fixture coverage for
// issue #196: a field-level foreign key's on_delete/on_update CHANGE
// round-trips through REAL generated migrations — up and down files produced
// by migration/generator (the issue #189 / PR #190 machinery) and applied by
// the real migrator — on PostgreSQL, MySQL and MariaDB alike. Unlike the
// pre-existing unit coverage, every assertion here reads the LIVE applied
// referential actions back from information_schema.referential_constraints,
// so a migration that "runs fine" but leaves the wrong ON DELETE rule in the
// database fails loudly.
//
// Flow (mirrors the testMigrationGeneratorValidation / PR #175 pattern):
//
//  1. 021-fk-actions-initial: users + orders with a field-level FK
//     (fk_orders_user) carrying NO referential actions — the engine default,
//     NO ACTION (MariaDB reports the equivalent RESTRICT).
//  2. Assert the live default actions; re-diff for idempotency (this is also
//     the spot where a missing RESTRICT==NO ACTION normalization would
//     produce a spurious FK diff on MariaDB).
//  3. 022-fk-actions-changed: the SAME constraint name gains
//     on_delete="SET NULL" and on_update="CASCADE" — a modification
//     (drop + re-add) in the comparator's terms. Apply the generated UP.
//  4. Assert the live actions actually changed; re-diff for idempotency.
//  5. Apply the generated DOWN (one MigrateDown step) and assert the PRIOR
//     actions are restored — the #190 down path reconstructs the old FK body
//     from the introspected pre-change schema.
//  6. Full comparator consistency check against the 021 fixture.
func testDynamicFKActionEvolution(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	migrationsDir, err := os.MkdirTemp("", "ptah_fk_action_evolution_*")
	if err != nil {
		return fmt.Errorf("failed to create migrations directory: %w", err)
	}
	defer os.RemoveAll(migrationsDir)
	migrationsFs := os.DirFS(migrationsDir)
	dh := NewDatabaseHelper(conn)

	generateAndMigrateUp := func(versionDir string) error {
		if loadErr := vem.LoadEntityVersion(versionDir); loadErr != nil {
			return loadErr
		}
		if _, genErr := generator.GenerateMigration(ctx, generator.GenerateMigrationOptions{
			GoEntitiesDir: vem.GetEntitiesDir(),
			DBConn:        conn,
			OutputDir:     migrationsDir,
		}); genErr != nil {
			return genErr
		}
		return dh.MigrateUp(ctx, migrationsFs)
	}

	if err := recorder.RecordStep("Apply FK Baseline", "Generate and apply real migration files for 021-fk-actions-initial", func() error {
		return generateAndMigrateUp("021-fk-actions-initial")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Verify Default FK Actions", "Live fk_orders_user must carry the engine-default referential actions", func() error {
		return assertLiveFKActions(ctx, conn, "fk_orders_user", "NO ACTION", "NO ACTION")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Idempotency At Baseline", "Re-diff against 021 — no further changes expected", func() error {
		return assertNoPendingChanges(ctx, conn, vem, "after baseline")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Apply FK Action Change", "Generate and apply real migration files for 022-fk-actions-changed", func() error {
		return generateAndMigrateUp("022-fk-actions-changed")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Verify Changed FK Actions", "Live fk_orders_user must now be ON UPDATE CASCADE / ON DELETE SET NULL", func() error {
		return assertLiveFKActions(ctx, conn, "fk_orders_user", "CASCADE", "SET NULL")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Idempotency After Change", "Re-diff against 022 — no further changes expected", func() error {
		return assertNoPendingChanges(ctx, conn, vem, "after action change")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Rollback FK Action Change", "Apply the GENERATED down migration (issue #190 path)", func() error {
		return dh.MigrateDown(ctx, migrationsFs)
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Verify Actions Restored", "Live fk_orders_user must be back to the engine-default actions", func() error {
		return assertLiveFKActions(ctx, conn, "fk_orders_user", "NO ACTION", "NO ACTION")
	}); err != nil {
		return err
	}

	return recorder.RecordStep("Schema Consistency After Rollback", "Comparator must see no drift against 021-fk-actions-initial", func() error {
		return validateSchemaConsistency(ctx, conn, vem, "021-fk-actions-initial")
	})
}

// assertNoPendingChanges re-diffs the currently loaded fixture version against
// the live database and fails on any non-comment statement.
func assertNoPendingChanges(ctx context.Context, conn *dbschema.DatabaseConnection, vem *VersionedEntityManager, when string) error {
	statements, err := vem.GenerateMigrationSQL(ctx, conn)
	if err != nil {
		return fmt.Errorf("failed to regenerate migration SQL %s: %w", when, err)
	}
	if hasNonCommentStatement(statements) {
		return fmt.Errorf("idempotency violation %s: %s", when, strings.Join(statements, "\n"))
	}
	return nil
}

// assertLiveFKActions reads the applied referential actions of a foreign key
// straight from information_schema.referential_constraints and compares them
// (normalized) against the expectation. NO ACTION and RESTRICT are equivalent
// on the MySQL family — InnoDB even reports the default as RESTRICT on
// MariaDB — so both are normalized to NO ACTION before comparing there.
func assertLiveFKActions(ctx context.Context, conn *dbschema.DatabaseConnection, constraintName, wantUpdate, wantDelete string) error {
	updateRule, deleteRule, err := readFKReferentialActions(ctx, conn, constraintName)
	if err != nil {
		return err
	}

	dialect := conn.Info().Dialect
	gotUpdate := normalizeReferentialAction(dialect, updateRule)
	gotDelete := normalizeReferentialAction(dialect, deleteRule)
	wantUpdate = normalizeReferentialAction(dialect, wantUpdate)
	wantDelete = normalizeReferentialAction(dialect, wantDelete)

	if gotUpdate != wantUpdate || gotDelete != wantDelete {
		return fmt.Errorf("constraint %s: live actions ON UPDATE %s / ON DELETE %s, want ON UPDATE %s / ON DELETE %s",
			constraintName, updateRule, deleteRule, wantUpdate, wantDelete)
	}
	return nil
}

// readFKReferentialActions returns the update_rule / delete_rule of a named
// foreign key in the current schema. information_schema.referential_constraints
// exists on PostgreSQL, MySQL and MariaDB; only the schema scoping function
// and the bind-parameter style differ.
func readFKReferentialActions(ctx context.Context, conn *dbschema.DatabaseConnection, constraintName string) (updateRule, deleteRule string, err error) {
	var query string
	if conn.Info().Dialect == "postgres" {
		query = `
			SELECT update_rule, delete_rule
			FROM information_schema.referential_constraints
			WHERE constraint_schema = current_schema() AND constraint_name = $1`
	} else {
		query = `
			SELECT update_rule, delete_rule
			FROM information_schema.referential_constraints
			WHERE constraint_schema = DATABASE() AND constraint_name = ?`
	}
	row := conn.QueryRowContext(ctx, query, constraintName)
	if scanErr := row.Scan(&updateRule, &deleteRule); scanErr != nil {
		return "", "", fmt.Errorf("constraint %s: failed to read referential_constraints: %w", constraintName, scanErr)
	}
	return updateRule, deleteRule, nil
}

// normalizeReferentialAction maps RESTRICT to NO ACTION on the MySQL family,
// where the two are semantically identical and interchangeable in server
// reporting (mirrors the comparator's dialect-aware normalization).
func normalizeReferentialAction(dialect, action string) string {
	if (dialect == "mysql" || dialect == "mariadb") && strings.EqualFold(action, "RESTRICT") {
		return "NO ACTION"
	}
	return strings.ToUpper(action)
}
