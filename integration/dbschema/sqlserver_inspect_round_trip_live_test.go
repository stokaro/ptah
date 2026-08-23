//go:build integration

package dbschema_test

import (
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/convert/dbschematogo"
	"go.5x5.cz/ptah/internal/dbtarget"
)

// TestSQLServerLiveInspectDescribesSynonymsAndProperties is stokaro/ptah#2001
// on a live server: the objects the reader finds have to reach the document.
//
// `ptah schema inspect` described neither, in any format, while the read found
// both — the loss was in the conversion between them, so every test that
// renders from a hand-built schema passed. This one starts from the catalog.
//
// The int-valued property is the control, and it is a decision rather than a
// gap: a value SQL Server stores under a base type Ptah cannot write back must
// not become a declaration, because the renderer emits an N” literal and the
// next apply would change its type.
func TestSQLServerLiveInspectDescribesSynonymsAndProperties(t *testing.T) {
	dbURL := dbtarget.URL(t, dbtarget.SQLServer)
	c := qt.New(t)
	ctx := t.Context()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	schemaName := fmt.Sprintf("ptah_rt_%d", time.Now().UnixNano())
	quoted := quoteSQLServerIdentifier(schemaName)
	_, err = conn.ExecContext(ctx, "EXEC('CREATE SCHEMA "+quoted+"')")
	c.Assert(err, qt.IsNil)
	defer func() {
		_, _ = conn.ExecContext(ctx, "DROP SYNONYM IF EXISTS "+quoted+".[s_gauge]")
		_, _ = conn.ExecContext(ctx, "DROP TABLE IF EXISTS "+quoted+".[gauge]")
		_, _ = conn.ExecContext(ctx, "DROP SCHEMA IF EXISTS "+quoted)
	}()

	for _, statement := range []string{
		"CREATE TABLE " + quoted + ".[gauge] ([id] INT PRIMARY KEY, [title] NVARCHAR(50) NOT NULL)",
		"CREATE SYNONYM " + quoted + ".[s_gauge] FOR " + quoted + ".[gauge]",
		"EXEC sp_addextendedproperty @name = N'ptah_text', @value = N'hello', " +
			"@level0type = N'SCHEMA', @level0name = N'" + schemaName + "', " +
			"@level1type = N'TABLE', @level1name = N'gauge'",
		"EXEC sp_addextendedproperty @name = N'ptah_int', @value = 42, " +
			"@level0type = N'SCHEMA', @level0name = N'" + schemaName + "', " +
			"@level1type = N'TABLE', @level1name = N'gauge'",
	} {
		_, execErr := conn.ExecContext(ctx, statement)
		c.Assert(execErr, qt.IsNil, qt.Commentf("statement:\n%s", statement))
	}

	live, err := dbschema.ReadSchemaWithSchemas(conn, []string{schemaName})
	c.Assert(err, qt.IsNil)
	// Non-vacuity: the read really found all three, so an empty document below
	// cannot pass as agreement about what is describable.
	c.Assert(live.Synonyms, qt.HasLen, 1)
	c.Assert(live.ExtendedProperties, qt.HasLen, 2)

	rendered, err := atlashclrender.RenderInspected(
		dbschematogo.ConvertDBSchemaToGoSchema(live), platform.SQLServer, schemaName)
	c.Assert(err, qt.IsNil)
	document := string(rendered.Data)

	c.Assert(document, qt.Contains, `synonym "s_gauge"`)
	c.Assert(document, qt.Contains, `extended_property "ptah_text"`)
	// The one no declaration could restore is left out of the document and
	// left alone on the server.
	c.Assert(document, qt.Not(qt.Contains), "ptah_int")
}
