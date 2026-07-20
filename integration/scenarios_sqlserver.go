package integration

import (
	"context"
	"fmt"
	"io/fs"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform"
	"github.com/stokaro/ptah/core/renderer"
	"github.com/stokaro/ptah/core/sqlutil"
	"github.com/stokaro/ptah/dbschema"
	dbschematypes "github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/internal/convert/fromschema"
)

const sqlServerAcceptanceSchema = "ptah_mssql_acceptance"

func sqlServerDynamicScenario() TestScenario {
	return TestScenario{
		Name:                "dynamic_sqlserver_identity_schema_bracket_reserved_words",
		Description:         "Test SQL Server IDENTITY columns, explicit schemas, and bracket-quoted reserved identifiers",
		EnhancedTestFunc:    testDynamicSQLServerIdentitySchemaBracketReservedWords,
		SQLServerCompatible: true,
	}
}

func testDynamicSQLServerIdentitySchemaBracketReservedWords(
	ctx context.Context,
	conn *dbschema.DatabaseConnection,
	_ fs.FS,
	recorder *StepRecorder,
) error {
	if platform.NormalizeDialect(conn.Info().Dialect) != platform.SQLServer {
		return recorder.RecordStep("Skip Non-SQL-Server", "SQL Server acceptance scenario is SQL Server-only", func() error {
			return nil
		})
	}

	return recorder.RecordStep("Apply SQL Server Schema", "Render and apply T-SQL for schema, IDENTITY, and reserved identifiers", func() error {
		if err := cleanupSQLServerAcceptanceSchema(ctx, conn); err != nil {
			return err
		}
		defer func() {
			_ = cleanupSQLServerAcceptanceSchema(ctx, conn)
		}()

		database := goschema.Database{
			Schemas: []goschema.Schema{{Name: sqlServerAcceptanceSchema}},
			Tables: []goschema.Table{{
				StructName: "Order",
				Schema:     sqlServerAcceptanceSchema,
				Name:       "order",
			}},
			Fields: []goschema.Field{
				{StructName: "Order", Name: "id", Type: "SERIAL", Primary: true, AutoInc: true},
				{StructName: "Order", Name: "select", Type: "VARCHAR(100)", Nullable: false},
				{StructName: "Order", Name: "from", Type: "INTEGER", Nullable: false},
			},
			Indexes: []goschema.Index{{
				StructName: "Order",
				TableName:  sqlServerAcceptanceSchema + ".order",
				Name:       "idx_order_select",
				Fields:     []string{"select"},
			}},
		}

		sqlText, err := renderer.RenderSQL(platform.SQLServer, fromschema.FromDatabase(database, platform.SQLServer))
		if err != nil {
			return fmt.Errorf("render SQL Server acceptance schema: %w", err)
		}
		for _, expected := range []string{
			"CREATE TABLE [ptah_mssql_acceptance].[order]",
			"[id] INT IDENTITY(1,1) PRIMARY KEY",
			"[select] NVARCHAR(100) NOT NULL",
			"[from] INT NOT NULL",
			"CREATE INDEX [idx_order_select] ON [ptah_mssql_acceptance].[order] ([select]);",
		} {
			if !strings.Contains(sqlText, expected) {
				return fmt.Errorf("rendered SQL Server schema missing %q in:\n%s", expected, sqlText)
			}
		}

		for _, stmt := range sqlutil.SplitSQLStatementsForDialect(sqlText, platform.SQLServer) {
			if err := conn.Writer().ExecuteSQL(ctx, stmt); err != nil {
				return fmt.Errorf("execute rendered SQL Server statement %q: %w", stmt, err)
			}
		}
		if err := conn.Writer().ExecuteSQL(
			ctx,
			"INSERT INTO [ptah_mssql_acceptance].[order] ([select], [from]) VALUES (@p1, @p2)",
			"chosen",
			42,
		); err != nil {
			return fmt.Errorf("insert row without identity value: %w", err)
		}

		var id int
		if err := conn.QueryRowContext(ctx, "SELECT [id] FROM [ptah_mssql_acceptance].[order] WHERE [select] = @p1", "chosen").Scan(&id); err != nil {
			return fmt.Errorf("read inserted identity value: %w", err)
		}
		if id != 1 {
			return fmt.Errorf("expected first SQL Server IDENTITY value 1, got %d", id)
		}

		schema, err := dbschema.ReadSchemaWithSchemas(conn, []string{sqlServerAcceptanceSchema})
		if err != nil {
			return fmt.Errorf("read SQL Server schema: %w", err)
		}
		table := findSQLServerScenarioTable(schema.Tables, sqlServerAcceptanceSchema, "order")
		if table == nil {
			return fmt.Errorf("missing table %s.order in introspected schema", sqlServerAcceptanceSchema)
		}
		if col := findSQLServerScenarioColumn(table.Columns, "id"); col == nil || !col.IsAutoIncrement {
			return fmt.Errorf("expected id column to round-trip as IDENTITY, got %#v", col)
		}
		for _, columnName := range []string{"select", "from"} {
			if findSQLServerScenarioColumn(table.Columns, columnName) == nil {
				return fmt.Errorf("missing reserved-word column %s in introspected schema", columnName)
			}
		}
		return nil
	})
}

func cleanupSQLServerAcceptanceSchema(ctx context.Context, conn *dbschema.DatabaseConnection) error {
	for _, stmt := range []string{
		"DROP TABLE IF EXISTS " + quoteSQLServerScenarioIdentifier(sqlServerAcceptanceSchema) + ".[order]",
		"DROP SCHEMA IF EXISTS " + quoteSQLServerScenarioIdentifier(sqlServerAcceptanceSchema),
	} {
		if err := conn.Writer().ExecuteSQL(ctx, stmt); err != nil {
			return fmt.Errorf("cleanup SQL Server acceptance schema: %w", err)
		}
	}
	return nil
}

func quoteSQLServerScenarioIdentifier(identifier string) string {
	return "[" + strings.ReplaceAll(identifier, "]", "]]") + "]"
}

func findSQLServerScenarioTable(
	tables []dbschematypes.DBTable,
	schemaName string,
	tableName string,
) *dbschematypes.DBTable {
	for i := range tables {
		table := &tables[i]
		if table.Schema == schemaName && table.Name == tableName {
			return table
		}
	}
	return nil
}

func findSQLServerScenarioColumn(columns []dbschematypes.DBColumn, columnName string) *dbschematypes.DBColumn {
	for i := range columns {
		column := &columns[i]
		if column.Name == columnName {
			return column
		}
	}
	return nil
}
