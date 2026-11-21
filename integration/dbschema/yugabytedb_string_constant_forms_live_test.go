//go:build integration

package dbschema_test

import (
	"context"
	"fmt"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/schemafile"
)

// newYugabyteFormsSchema creates a scratch schema on YugabyteDB, dropped when
// the test ends.
func newYugabyteFormsSchema(c *qt.C) (*dbschema.DatabaseConnection, string) {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), dbtarget.URL(c, dbtarget.YugabyteDB))
	c.Assert(err, qt.IsNil)
	schemaName := fmt.Sprintf("ptah_forms_%d", time.Now().UnixNano())
	_, err = conn.ExecContext(c.Context(), `CREATE SCHEMA "`+schemaName+`"`)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() {
		_, _ = conn.ExecContext(context.Background(), `DROP SCHEMA IF EXISTS "`+schemaName+`" CASCADE`)
		dbschema.CloseAndWarn(conn)
	})
	return conn, schemaName
}

// YugabyteDB reads a continued string and a U&'...' string by PostgreSQL's
// rules. Each spelling is applied by the server, the stored text is read back,
// and the document read with the YugabyteDB dialect then compares clean
// against it. Read without the continued and Unicode strings of the dialect's
// descriptor, each document is refused.
func TestYugabyteLiveStringConstantSpellingsCompareClean(t *testing.T) {
	tests := []struct {
		name     string
		spelling string
		want     string
	}{
		{name: "a continued string", spelling: "'first part, '\n'second part'", want: "first part, second part"},
		{name: "a line comment in the gap", spelling: "'a'  -- note\n  'b'", want: "ab"},
		{name: "a continued escape string", spelling: "E'a\\t'\n'b\\t'", want: "a\tb\t"},
		{name: "a Unicode escape string", spelling: `U&'d\0061t\+000061 \\'`, want: `data \`},
		{name: "a continued Unicode escape string", spelling: "U&'d\\0061'\n'\\0074a'", want: "data"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			conn, schemaName := newYugabyteFormsSchema(c)
			_, err := conn.ExecContext(c.Context(), fmt.Sprintf(stringConstantFormsDocument, schemaName, test.spelling))
			c.Assert(err, qt.IsNil)
			c.Assert(storedStringConstants(c, conn, schemaName), qt.DeepEquals, []string{test.want, test.want, test.want})

			declared, err := schemafile.LoadSources(c.Context(), []schemafile.Source{{URL: writeFormsDocument(c, schemaName, map[string]string{
				"schema.sql": fmt.Sprintf(stringConstantFormsDocument, "%[1]s", test.spelling),
			})}},
				schemafile.Options{YAML: builtintest.Runtime().YAML(), SQL: builtintest.Runtime().SQL(), HCL: builtintest.Runtime().HCL(), Dialect: platform.YugabyteDB},
			)
			c.Assert(err, qt.IsNil)

			c.Assert(planDocumentAgainstLive(c, conn, declared, schemaName), qt.HasLen, 0)
		})
	}
}
