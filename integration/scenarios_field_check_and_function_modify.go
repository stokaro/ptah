package integration

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"github.com/stokaro/ptah/dbschema"
	"github.com/stokaro/ptah/migration/schemadiff"
)

// testDynamicFunctionAttributeModification migrates from 014-rls-functions to
// 018-rls-functions-modified and asserts that the live `pg_proc` rows for the
// two affected functions actually changed:
//
//   - set_tenant_context: body switches from set_config(..., false) to
//     set_config(..., true), and SECURITY DEFINER is dropped (back to INVOKER).
//   - get_current_tenant_id: volatility tightens from STABLE to IMMUTABLE.
//
// Before PR #129 the diff comparator already populated diff.FunctionsModified
// for these changes, but the postgres planner had no handler for that field —
// the rewrites silently never reached the database. This scenario is what the
// natural-migration fixture suite was missing to catch that class of bug.
func testDynamicFunctionAttributeModification(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	if err := skipNonPostgreSQL(conn, recorder); err != nil {
		return err
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	if err := recorder.RecordStep("Apply Initial Functions", "Apply 014-rls-functions baseline", func() error {
		return vem.MigrateToVersion(ctx, conn, "014-rls-functions", "Baseline RLS functions")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Verify Baseline DB State", "Read pg_proc and assert set_tenant_context is DEFINER + session-scoped, get_current_tenant_id is STABLE", func() error {
		setCtx, err := readFunctionAttributes(ctx, conn, "set_tenant_context")
		if err != nil {
			return err
		}
		if setCtx.security != "DEFINER" {
			return fmt.Errorf("baseline set_tenant_context: expected SECURITY DEFINER, got %s", setCtx.security)
		}
		if !strings.Contains(setCtx.body, "false") {
			return fmt.Errorf("baseline set_tenant_context: expected session-scoped set_config (false), got body: %s", setCtx.body)
		}
		getCtx, err := readFunctionAttributes(ctx, conn, "get_current_tenant_id")
		if err != nil {
			return err
		}
		if getCtx.volatility != "STABLE" {
			return fmt.Errorf("baseline get_current_tenant_id: expected STABLE, got %s", getCtx.volatility)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Apply Modified Functions", "Migrate to 018-rls-functions-modified", func() error {
		return vem.MigrateToVersion(ctx, conn, "018-rls-functions-modified", "Modify function body, SECURITY, volatility")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Verify Modified DB State", "Read pg_proc and assert body/security/volatility actually changed in the database", func() error {
		setCtx, err := readFunctionAttributes(ctx, conn, "set_tenant_context")
		if err != nil {
			return err
		}
		if setCtx.security != "INVOKER" {
			return fmt.Errorf("modified set_tenant_context: SECURITY DEFINER must be dropped, got %s", setCtx.security)
		}
		if strings.Contains(setCtx.body, "false") {
			return fmt.Errorf("modified set_tenant_context: body must no longer contain `false`, got: %s", setCtx.body)
		}
		if !strings.Contains(setCtx.body, "true") {
			return fmt.Errorf("modified set_tenant_context: body must contain `true` (transaction-local), got: %s", setCtx.body)
		}

		getCtx, err := readFunctionAttributes(ctx, conn, "get_current_tenant_id")
		if err != nil {
			return err
		}
		if getCtx.volatility != "IMMUTABLE" {
			return fmt.Errorf("modified get_current_tenant_id: expected IMMUTABLE, got %s", getCtx.volatility)
		}
		return nil
	}); err != nil {
		return err
	}

	return recorder.RecordStep("Verify Function Idempotency", "Re-diff and confirm no further FunctionsModified entries are emitted", func() error {
		// We deliberately don't assert a fully-clean re-diff here: the
		// fixture inherits NOW()/CURRENT_TIMESTAMP-style column defaults
		// from 014-rls-functions, and Ptah's column-diff path is currently
		// case-sensitive against pg_attrdef (which lowercases function
		// names). That's a pre-existing comparator bug unrelated to issue
		// #89, and surfacing it under this scenario would obscure the
		// thing this test is actually about: that the function modify
		// reached the database and stays put.
		//
		// What we DO assert: a re-diff produces no FunctionsModified
		// entries — i.e. PR #129's planner emit path landed and is
		// idempotent over body / SECURITY / volatility / language /
		// parameters / returns.
		generated, err := vem.GenerateSchemaFromEntities()
		if err != nil {
			return fmt.Errorf("failed to parse entities: %w", err)
		}
		dbSchema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("failed to read database schema: %w", err)
		}
		diff := schemadiff.Compare(generated, dbSchema)
		if len(diff.FunctionsModified) > 0 {
			summary := make([]string, 0, len(diff.FunctionsModified))
			for _, f := range diff.FunctionsModified {
				summary = append(summary, fmt.Sprintf("%s: %v", f.FunctionName, f.Changes))
			}
			return fmt.Errorf("functions idempotency violation: post-migration diff still flags %d function(s) as modified:\n%s",
				len(diff.FunctionsModified), strings.Join(summary, "\n"))
		}
		return nil
	})
}

// testDynamicFieldCheckConstraintEvolution exercises the field-level CHECK
// constraint lifecycle (PR #123 / issue #112) through the natural fixture
// migration path:
//
//  1. Migrate to 000-initial (baseline, no field-level CHECKs).
//  2. Migrate to 019-field-check-constraints: introduces three CHECKs —
//     `products_price_check` (auto-named, on an existing column),
//     `products_stock_check` (inline on a new column), and
//     `products_status_valid` (explicit check_name= on a new column).
//  3. Migrate to 020-field-check-constraints-modified:
//     - price gets an explicit check_name → constraint is renamed
//     (drop old auto-name + add new name).
//     - stock loses its CHECK annotation → ConstraintsRemoved.
//     - status keeps the same check_name and tightens the IN list; the
//     "trust the name" contract from #112 means the diff does NOT regen
//     this constraint — same-name CHECK is treated as unchanged.
//  4. Re-diff to confirm idempotency.
//
// The synthesized CHECK drop here is exactly the path that was a silent no-op
// in master before PR #129's planner fix. With the DO-block drop in place,
// this test verifies the natural-migration flow round-trips correctly.
func testDynamicFieldCheckConstraintEvolution(ctx context.Context, conn *dbschema.DatabaseConnection, fixtures fs.FS, recorder *StepRecorder) error {
	if err := skipNonPostgreSQL(conn, recorder); err != nil {
		return err
	}

	vem, err := NewVersionedEntityManager(fixtures)
	if err != nil {
		return fmt.Errorf("failed to create versioned entity manager: %w", err)
	}
	defer vem.Cleanup()

	if err := recorder.RecordStep("Apply Baseline", "Apply 000-initial", func() error {
		return vem.MigrateToVersion(ctx, conn, "000-initial", "Baseline schema")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Introduce Field CHECKs", "Migrate to 019-field-check-constraints", func() error {
		return vem.MigrateToVersion(ctx, conn, "019-field-check-constraints", "Add field-level CHECK constraints")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Verify CHECKs Landed", "Assert all three CHECK constraints exist in the live schema", func() error {
		names, err := readUserCheckConstraints(ctx, conn, "products")
		if err != nil {
			return err
		}
		for _, want := range []string{"products_price_check", "products_stock_check", "products_status_valid"} {
			if !names[want] {
				return fmt.Errorf("expected CHECK constraint %q on products; got: %v", want, names)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Idempotency After Add", "Re-diff against 019 — no further changes expected", func() error {
		statements, err := vem.GenerateMigrationSQL(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to regenerate migration SQL: %w", err)
		}
		if hasNonCommentStatement(statements) {
			return fmt.Errorf("idempotency violation after add: %s", strings.Join(statements, "\n"))
		}
		return nil
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Rename + Drop + Same-name CHECKs", "Migrate to 020-field-check-constraints-modified", func() error {
		return vem.MigrateToVersion(ctx, conn, "020-field-check-constraints-modified", "Rename one CHECK, drop one, leave same-name unchanged")
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Verify Modify Lifecycle", "Old auto-name gone, new name present, dropped constraint absent, same-name untouched", func() error {
		names, err := readUserCheckConstraints(ctx, conn, "products")
		if err != nil {
			return err
		}
		if names["products_price_check"] {
			return fmt.Errorf("products_price_check should have been dropped on rename; got: %v", names)
		}
		if !names["products_price_positive"] {
			return fmt.Errorf("products_price_positive (renamed) should exist; got: %v", names)
		}
		if names["products_stock_check"] {
			return fmt.Errorf("products_stock_check should have been dropped; got: %v", names)
		}
		if !names["products_status_valid"] {
			return fmt.Errorf("products_status_valid should still exist (same-name CHECK is treated as unchanged); got: %v", names)
		}
		return nil
	}); err != nil {
		return err
	}

	return recorder.RecordStep("Idempotency After Modify", "Re-diff against 020 — no further changes expected", func() error {
		statements, err := vem.GenerateMigrationSQL(ctx, conn)
		if err != nil {
			return fmt.Errorf("failed to regenerate migration SQL: %w", err)
		}
		if hasNonCommentStatement(statements) {
			return fmt.Errorf("idempotency violation after modify: %s", strings.Join(statements, "\n"))
		}
		return nil
	})
}

// functionAttrs holds the live pg_proc values we care about for an integration
// assertion. Grouped into a struct so readFunctionAttributes can return a
// single value alongside its error.
type functionAttrs struct {
	body, security, volatility string
}

// readFunctionAttributes pulls body / security / volatility for a function in
// the current schema, normalized to the same uppercase / lowercase
// representation that dbschema/postgres/reader.go uses.
func readFunctionAttributes(ctx context.Context, conn *dbschema.DatabaseConnection, name string) (functionAttrs, error) {
	row := conn.QueryRowContext(ctx, `
		SELECT
			p.prosrc,
			CASE p.prosecdef WHEN true THEN 'DEFINER' ELSE 'INVOKER' END,
			CASE p.provolatile
				WHEN 'i' THEN 'IMMUTABLE'
				WHEN 's' THEN 'STABLE'
				WHEN 'v' THEN 'VOLATILE'
			END
		FROM pg_proc p
		JOIN pg_namespace n ON n.oid = p.pronamespace
		WHERE n.nspname = current_schema() AND p.proname = $1
		LIMIT 1
	`, name)
	var attrs functionAttrs
	if scanErr := row.Scan(&attrs.body, &attrs.security, &attrs.volatility); scanErr != nil {
		return functionAttrs{}, fmt.Errorf("function %s: failed to read pg_proc: %w", name, scanErr)
	}
	return attrs, nil
}

// readUserCheckConstraints returns the names of user-defined CHECK constraints
// on a table. Synthetic `<col>_not_null` constraints created by PG18 to back
// NOT NULL declarations are filtered out — they are owned by the column, not
// by the user's annotation, and the diff layer already ignores them.
func readUserCheckConstraints(_ctx context.Context, conn *dbschema.DatabaseConnection, table string) (map[string]bool, error) {
	rows, err := conn.Query(`
		SELECT con.conname
		FROM pg_constraint con
		JOIN pg_class c ON c.oid = con.conrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		WHERE n.nspname = current_schema() AND c.relname = $1 AND con.contype = 'c'
	`, table)
	if err != nil {
		return nil, fmt.Errorf("table %s: failed to read pg_constraint: %w", table, err)
	}
	defer rows.Close()

	names := make(map[string]bool)
	for rows.Next() {
		var name string
		if scanErr := rows.Scan(&name); scanErr != nil {
			return nil, fmt.Errorf("table %s: scan: %w", table, scanErr)
		}
		if strings.HasSuffix(name, "_not_null") {
			continue
		}
		names[name] = true
	}
	if rowsErr := rows.Err(); rowsErr != nil {
		return nil, fmt.Errorf("table %s: row iteration: %w", table, rowsErr)
	}
	return names, nil
}

// hasNonCommentStatement reports whether `statements` contains anything other
// than SQL comments. VersionedEntityManager.GenerateMigrationSQL emits no
// statements when nothing changed, but the planner may still produce a bare
// "-- Drop constraint …" line when a constraint name is otherwise unsafe;
// that's not a real change for the purposes of an idempotency check.
func hasNonCommentStatement(statements []string) bool {
	for _, s := range statements {
		trimmed := strings.TrimSpace(s)
		if trimmed == "" {
			continue
		}
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		return true
	}
	return false
}
