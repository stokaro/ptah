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

	createUsers := ast.NewCreateTable("crdb_common_users").
		AddColumn(ast.NewColumn("id", "INT8").SetPrimary()).
		AddColumn(ast.NewColumn("email", "STRING").SetNotNull()).
		AddColumn(ast.NewColumn("profile", "JSONB"))
	createEmailIndex := ast.NewIndex("idx_crdb_common_users_email", "crdb_common_users", "email").
		SetIfNotExists()

	var sqlText string
	if err := recorder.RecordStep("Render CockroachDB DDL", "Render common-subset table and index through the CockroachDB renderer", func() error {
		var err error
		sqlText, err = renderer.RenderSQL(platform.CockroachDB, createUsers, createEmailIndex)
		if err != nil {
			return fmt.Errorf("render CockroachDB SQL: %w", err)
		}
		if strings.Contains(sqlText, "CONCURRENTLY") {
			return fmt.Errorf("CockroachDB common-subset SQL must not contain CONCURRENTLY:\n%s", sqlText)
		}
		if strings.Contains(sqlText, "XML") {
			return fmt.Errorf("CockroachDB common-subset SQL must not contain XML:\n%s", sqlText)
		}
		return nil
	}); err != nil {
		return err
	}

	if err := recorder.RecordStep("Apply CockroachDB DDL", "Apply rendered common-subset SQL to the live CockroachDB connection", func() error {
		return conn.Writer().ExecuteSQL(ctx, sqlText)
	}); err != nil {
		return err
	}

	return recorder.RecordStep("Read CockroachDB Schema", "Verify the created table is visible through the PostgreSQL-family reader", func() error {
		schema, err := conn.Reader().ReadSchema()
		if err != nil {
			return fmt.Errorf("read CockroachDB schema: %w", err)
		}
		for _, table := range schema.Tables {
			if table.Name == "crdb_common_users" {
				return nil
			}
		}
		return fmt.Errorf("expected crdb_common_users table in CockroachDB schema")
	})
}
