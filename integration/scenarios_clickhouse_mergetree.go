package integration

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema"
)

// testClickHouseMergeTreeEngine is the ClickHouse-only counterpart to the
// existing dialect-specific scenarios. It exercises the engine annotation
// pipeline end-to-end:
//
//  1. Build a goschema.Database with a `platform.clickhouse.engine=`
//     override on the table.
//  2. Render CREATE TABLE for ClickHouse and apply it to a live ClickHouse
//     server.
//  3. Read back the live engine + ORDER BY clause via system.tables and
//     assert they match what the annotation promised.
//
// The scenario is a no-op in CI because CI does not provision a ClickHouse
// service; both the live-connection guard (Dialect != "clickhouse") and the
// CLICKHOUSE_URL guard above must be true for the test body to execute.
func testClickHouseMergeTreeEngine(ctx context.Context, conn *dbschema.DatabaseConnection, _ fs.FS, recorder *StepRecorder) error {
	if conn.Info().Dialect != "clickhouse" {
		return recorder.RecordStep("Skip Non-ClickHouse", "MergeTree engine assertions read system.tables; ClickHouse-only", func() error {
			return nil
		})
	}
	if os.Getenv("CLICKHOUSE_URL") == "" {
		return recorder.RecordStep("Skip No CLICKHOUSE_URL", "CLICKHOUSE_URL not set; ClickHouse integration is opt-in", func() error {
			return nil
		})
	}

	const tableName = "ptah_clickhouse_engine_scenario"

	// Clean any leftover state from a previous run so the scenario is idempotent.
	if err := recorder.RecordStep("Reset Table", "Drop the scenario table if it exists", func() error {
		_, err := conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s` SYNC", tableName))
		return err
	}); err != nil {
		return err
	}

	schema := &goschema.Database{
		Tables: []goschema.Table{
			{
				StructName: "PtahClickHouseEngineScenario",
				Name:       tableName,
				Overrides: map[string]map[string]string{
					"clickhouse": {
						"engine":       "MergeTree",
						"order_by":     "id, created_at",
						"partition_by": "toYYYYMM(created_at)",
					},
				},
			},
		},
		Fields: []goschema.Field{
			{StructName: "PtahClickHouseEngineScenario", Name: "id", Type: "BIGINT", Primary: true, Nullable: false},
			{StructName: "PtahClickHouseEngineScenario", Name: "created_at", Type: "TIMESTAMP", Nullable: false},
			{StructName: "PtahClickHouseEngineScenario", Name: "payload", Type: "TEXT", Nullable: true},
		},
	}

	if err := recorder.RecordStep("Generate + Apply CREATE TABLE", "Render MergeTree DDL from annotations and apply", func() error {
		statements := renderer.GetOrderedCreateStatements(schema, "clickhouse")
		for _, stmt := range statements {
			stmt = strings.TrimSpace(stmt)
			if stmt == "" {
				continue
			}
			if _, err := conn.ExecContext(ctx, stmt); err != nil {
				return fmt.Errorf("apply DDL: %w\nstmt: %s", err, stmt)
			}
		}
		return nil
	}); err != nil {
		return err
	}

	// Always clean up the scenario table so subsequent runs (and unrelated
	// scenarios sharing the database) don't see leftover state. Registered
	// via t.Cleanup-like ordering: schedule the drop before doing the
	// assertions, then rely on the recorder's deferred error semantics.
	defer func() {
		_, _ = conn.ExecContext(ctx, fmt.Sprintf("DROP TABLE IF EXISTS `%s` SYNC", tableName))
	}()

	return recorder.RecordStep("Verify Engine + ORDER BY", "Read system.tables and assert MergeTree + ORDER BY (id, created_at)", func() error {
		var engine, orderBy, partitionBy string
		row := conn.QueryRowContext(ctx, `
			SELECT engine, sorting_key, partition_key
			FROM system.tables
			WHERE database = currentDatabase() AND name = ?
		`, tableName)
		if err := row.Scan(&engine, &orderBy, &partitionBy); err != nil {
			return fmt.Errorf("read system.tables for %s: %w", tableName, err)
		}
		if engine != "MergeTree" {
			return fmt.Errorf("expected engine MergeTree, got %q", engine)
		}
		// ClickHouse reports the sorting key as a comma-separated list with
		// inconsistent whitespace across versions; strip whitespace before
		// comparing.
		got := strings.ReplaceAll(strings.ReplaceAll(orderBy, " ", ""), "\t", "")
		want := "id,created_at"
		if got != want {
			return fmt.Errorf("expected ORDER BY %q, got %q (raw %q)", want, got, orderBy)
		}
		if !strings.Contains(partitionBy, "created_at") {
			return fmt.Errorf("expected PARTITION BY referencing created_at, got %q", partitionBy)
		}
		return nil
	})
}
