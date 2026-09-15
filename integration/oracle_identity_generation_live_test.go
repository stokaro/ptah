//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestOracleReaderRecordsIdentityGenerationLive reads identity columns of each
// generation mode back from a live Oracle server.
//
// IDENTITY_COLUMN answers YES for all three, so the only thing telling them
// apart is ALL_TAB_IDENTITY_COLS.GENERATION_TYPE, and what Oracle writes there
// is the measurement: ALWAYS, and BY DEFAULT with a space for both BY DEFAULT
// spellings. ON NULL does not reach that column, and it does not need to: an ON
// NULL identity accepts an explicit value like any other BY DEFAULT one.
func TestOracleReaderRecordsIdentityGenerationLive(t *testing.T) {
	c := qt.New(t)
	dbURL := dbtarget.URL(c, dbtarget.Oracle)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	conn, err := dbschema.ConnectToDatabase(ctx, dbURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	suffix := time.Now().UnixNano() % 100000000
	tests := []struct {
		table      string
		generation string
		want       string
	}{
		{table: fmt.Sprintf("PTAH_ID3295_ALWAYS_%d", suffix), generation: "ALWAYS", want: "ALWAYS"},
		{table: fmt.Sprintf("PTAH_ID3295_BYDEF_%d", suffix), generation: "BY DEFAULT", want: "BY_DEFAULT"},
		{table: fmt.Sprintf("PTAH_ID3295_ONNULL_%d", suffix), generation: "BY DEFAULT ON NULL", want: "BY_DEFAULT"},
	}
	for _, test := range tests {
		dropOracleTable(ctx, conn, test.table)
		defer dropOracleTable(context.WithoutCancel(ctx), conn, test.table)
		c.Assert(conn.SchemaWriter().ExecuteSQL(ctx, fmt.Sprintf(
			`CREATE TABLE %s (ID NUMBER(10) GENERATED %s AS IDENTITY PRIMARY KEY, CODE VARCHAR2(40) NOT NULL)`,
			test.table, test.generation)), qt.IsNil)
	}

	read, err := conn.Reader().ReadSchemaContext(ctx)
	c.Assert(err, qt.IsNil)

	for _, test := range tests {
		t.Run(test.generation, func(t *testing.T) {
			c := qt.New(t)
			columns := oracleTableByName(c, read.Tables, test.table).Columns

			key := oracleColumnByName(c, columns, "ID")
			c.Assert(key.IsAutoIncrement, qt.IsTrue)
			c.Assert(key.IdentityGeneration, qt.Equals, test.want)

			// The control: an ordinary column in the same table carries no
			// generation, so the value above is read per column rather than
			// spread across the table.
			code := oracleColumnByName(c, columns, "CODE")
			c.Assert(code.IsAutoIncrement, qt.IsFalse)
			c.Assert(code.IdentityGeneration, qt.Equals, "")
		})
	}
}
