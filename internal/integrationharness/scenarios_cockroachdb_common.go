package integrationharness

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/renderer"
	"go.5x5.cz/ptah/dbschema"
)

// testCockroachDBCommonSubset validates a PostgreSQL-family subset that
// CockroachDB accepts without relying on target-specific features such as XML,
// advisory locks, or CREATE INDEX CONCURRENTLY.
func testCockroachDBCommonSubset(ctx context.Context, conn *dbschema.DatabaseConnection, _ fs.FS, recorder *StepRecorder) error {
	if conn.Info().Dialect != platform.CockroachDB {
		return recorder.RecordStep("Skip Non-CockroachDB", "Common subset scenario is CockroachDB-only", func() error {
			return nil
		})
	}

	return testPostgresDistributedCommonSubset(
		ctx,
		conn,
		recorder,
		"CockroachDB",
		"crdb_common_users",
		[]capability.Capability{
			capability.CreateIndexConcurrently,
			capability.DropIndexConcurrently,
			capability.XMLType,
			capability.AdvisoryLocks,
		},
	)
}

// testYugabyteDBCommonSubset validates the same conservative PostgreSQL-family
// common subset against a live YugabyteDB YSQL connection.
func testYugabyteDBCommonSubset(ctx context.Context, conn *dbschema.DatabaseConnection, _ fs.FS, recorder *StepRecorder) error {
	if conn.Info().Dialect != platform.YugabyteDB {
		return recorder.RecordStep("Skip Non-YugabyteDB", "Common subset scenario is YugabyteDB-only", func() error {
			return nil
		})
	}

	return testPostgresDistributedCommonSubset(
		ctx,
		conn,
		recorder,
		"YugabyteDB",
		"yb_common_users",
		[]capability.Capability{capability.DropIndexConcurrently},
	)
}

// capabilitiesClaimed lists which of the named capabilities the connection
// claims, so a scenario defined by an absence can say which part of that
// absence went missing.
//
// This replaced an equality check against a named preset. That check read as a
// stronger assertion than it was: it passed only while CockroachDB26 was
// literally CockroachDB23, and the moment those lines gained a difference --
// any difference, in any key, related to this scenario or not -- it failed
// while the subset it names was still intact (stokaro/ptah#1735). Naming the
// keys the scenario is about survives a release the ladder learns about, and
// still fails if one of them turns true.
func capabilitiesClaimed(have capability.Capabilities, named []capability.Capability) []capability.Capability {
	var claimed []capability.Capability
	for _, key := range named {
		if have.Has(key) {
			claimed = append(claimed, key)
		}
	}
	return claimed
}

func testPostgresDistributedCommonSubset(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	recorder *StepRecorder,
	label,
	tableName string,
	absent []capability.Capability,
) error {
	createUsers := ast.NewCreateTable(tableName).
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("email", "TEXT").SetNotNull()).
		AddColumn(ast.NewColumn("profile", "JSONB"))
	createEmailIndex := ast.NewIndex("idx_"+tableName+"_email", tableName, "email").SetIfNotExists()

	var sqlText string
	if err := recorder.RecordStep("Render "+label+" DDL", "Render common-subset table and index through the distributed-SQL renderer", func() error {
		var err error
		info := conn.Info()
		if claimed := capabilitiesClaimed(info.Capabilities, absent); len(claimed) > 0 {
			return fmt.Errorf(
				"%s connection claims %v, which this common-subset scenario is defined by the absence of",
				label, claimed)
		}
		sqlText, err = renderer.RenderSQLWithCapabilities(info.Dialect, info.Capabilities, createUsers, createEmailIndex)
		if err != nil {
			return fmt.Errorf("render %s SQL: %w", label, err)
		}
		if strings.Contains(sqlText, "CONCURRENTLY") {
			return fmt.Errorf("%s common-subset SQL must not contain CONCURRENTLY:\n%s", label, sqlText)
		}
		if strings.Contains(sqlText, "XML") {
			return fmt.Errorf("%s common-subset SQL must not contain XML:\n%s", label, sqlText)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Apply "+label+" DDL", "Apply rendered common-subset SQL to the live distributed-SQL connection", func() error {
		writer := conn.SchemaWriter()
		tx, err := writer.BeginTransaction(ctx)
		if err != nil {
			return fmt.Errorf("begin %s transaction: %w", label, err)
		}
		defer func() {
			_ = tx.Rollback()
		}()
		if err := tx.ExecuteSQL(ctx, sqlText); err != nil {
			return fmt.Errorf("apply %s SQL: %w", label, err)
		}
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("commit %s transaction: %w", label, err)
		}
		return nil
	}); err != nil {
		return err
	}

	return recorder.RecordStep("Read "+label+" Schema", "Verify the created table is visible through the PostgreSQL-family reader", func() error {
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("read %s schema: %w", label, err)
		}
		for _, table := range schema.Tables {
			if table.Name == tableName {
				return nil
			}
		}
		return fmt.Errorf("expected %s table in %s schema", tableName, label)
	})
}
