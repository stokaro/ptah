package integration

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"github.com/stokaro/ptah/core/ast"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/dbschema"
)

// testCockroachDBCommonSubset validates the PostgreSQL-family common subset
// that CockroachDB accepts without relying on PostgreSQL-only features such as
// SERIAL, XML, foreign keys, advisory locks, or CREATE INDEX CONCURRENTLY.
func testCockroachDBCommonSubset(ctx context.Context, conn *dbschema.DatabaseConnection, _ fs.FS, recorder *StepRecorder) error {
	if conn.Info().Dialect != platform.CockroachDB {
		return recorder.RecordStep("Skip Non-CockroachDB", "Common subset scenario is CockroachDB-only", func() error {
			return nil
		})
	}

	return testPostgresDistributedCommonSubset(ctx, conn, recorder, "CockroachDB", "crdb_common_users")
}

// testYugabyteDBCommonSubset validates the same conservative PostgreSQL-family
// common subset against a live YugabyteDB YSQL connection.
func testYugabyteDBCommonSubset(ctx context.Context, conn *dbschema.DatabaseConnection, _ fs.FS, recorder *StepRecorder) error {
	if conn.Info().Dialect != platform.YugabyteDB {
		return recorder.RecordStep("Skip Non-YugabyteDB", "Common subset scenario is YugabyteDB-only", func() error {
			return nil
		})
	}

	return testPostgresDistributedCommonSubset(ctx, conn, recorder, "YugabyteDB", "yb_common_users")
}

func testPostgresDistributedCommonSubset(ctx context.Context, conn *dbschema.DatabaseConnection, recorder *StepRecorder, label, tableName string) error {
	createUsers := ast.NewCreateTable(tableName).
		AddColumn(ast.NewColumn("id", "BIGINT").SetPrimary()).
		AddColumn(ast.NewColumn("email", "TEXT").SetNotNull()).
		AddColumn(ast.NewColumn("profile", "JSONB"))
	createEmailIndex := ast.NewIndex("idx_"+tableName+"_email", tableName, "email").SetIfNotExists()

	var sqlText string
	if err := recorder.RecordStep("Render "+label+" DDL", "Render common-subset table and index through the distributed-SQL renderer", func() error {
		var err error
		sqlText, err = renderer.RenderSQL(conn.Info().Dialect, createUsers, createEmailIndex)
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
		writer := conn.Writer()
		if err := writer.BeginTransaction(); err != nil {
			return fmt.Errorf("begin %s transaction: %w", label, err)
		}
		defer func() {
			_ = writer.RollbackTransaction()
		}()
		if err := writer.ExecuteSQL(ctx, sqlText); err != nil {
			return fmt.Errorf("apply %s SQL: %w", label, err)
		}
		if err := writer.CommitTransaction(); err != nil {
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
