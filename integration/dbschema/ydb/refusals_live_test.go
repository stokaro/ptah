//go:build integration

package ydb_test

import (
	"bytes"
	"regexp"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/introspect"
	"ptah.run/internal/cli/schema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devlock"
	"ptah.run/internal/migrateclean"
	"ptah.run/internal/ydbgap"
)

// Once a YDB connection opens, every layer behind it that does not reach YDB
// yet refuses in the words of the gap that plans it, rather than sending the
// server another dialect's SQL. Each row is reached through a connection to a
// live server, which is the only way to reach it at all.
func TestYDBConnectedLayersRefuse(t *testing.T) {
	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			c := qt.New(t)
			conn := openYDB(c, line)
			ctx := c.Context()

			devErr := migrateclean.DevRefusal(ctx, conn)
			_, realmErr := devlock.SameRealm(ctx, conn, conn)

			c.Assert(devErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.DevDatabases.Message()))
			c.Assert(realmErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.DevDatabases.Message()))
		})
	}
}

// The commands whose answer would be built from what the reader does not read
// refuse after they connect.
func TestYDBCommandsThatNeedMoreThanTheReaderRefuse(t *testing.T) {
	tests := []struct {
		name    string
		command func() *cobra.Command
		args    []string
	}{
		{name: "introspect", command: introspect.NewIntrospectCommand, args: []string{"--out", "models"}},
		{name: "schema security", command: schema.NewSchemaSecurityCommand},
		{name: "schema lineage", command: schema.NewSchemaLineageCommand},
	}

	for _, line := range ydbLines {
		t.Run(line.name, func(t *testing.T) {
			url := dbtarget.URL(t, line.engine)
			for _, test := range tests {
				t.Run(test.name, func(t *testing.T) {
					c := qt.New(t)
					cmd := test.command()
					var stdout, stderr bytes.Buffer
					cmd.SetOut(&stdout)
					cmd.SetErr(&stderr)
					cmd.SetArgs(append([]string{"--db-url", url}, test.args...))

					err := cmd.Execute()

					c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.OtherSurfaces.Message()))
					c.Assert(stdout.String(), qt.Equals, "")
				})
			}
		})
	}
}
