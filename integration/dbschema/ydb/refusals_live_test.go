//go:build integration

package ydb_test

import (
	"bytes"
	"regexp"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cli/introspect"
	"ptah.run/internal/cli/schema"
	"ptah.run/internal/dbtarget"
	"ptah.run/internal/devlock"
	"ptah.run/internal/migrateclean"
	"ptah.run/internal/ydbgap"
	"ptah.run/migration/migrator"
	"ptah.run/migration/seeder"
)

// Once a YDB connection opens, every layer behind it that does not reach YDB
// yet refuses in the words of the gap that plans it, rather than sending the
// server another dialect's SQL. Each row is reached through a connection to a
// live server, which is the only way to reach it at all.
func TestYDBConnectedLayersRefuse(t *testing.T) {
	c := qt.New(t)
	conn := openYDB(c)
	ctx := c.Context()

	migrationErr := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider()).Initialize(ctx)
	_, presenceErr := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider()).MetadataPresent(ctx)
	tagErr := migrator.NewMigrator(conn, migrator.NewRegisteredMigrationProvider()).RecordMigrationTag(ctx, "v1", 1)
	_, seedErr := seeder.Apply(ctx, conn, fstest.MapFS{}, seeder.Options{Env: "dev"})
	devErr := migrateclean.DevRefusal(ctx, conn)
	_, realmErr := devlock.SameRealm(ctx, conn, conn)

	c.Assert(migrationErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.Migrating.Message()))
	c.Assert(presenceErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.Migrating.Message()))
	c.Assert(tagErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.Migrating.Message()))
	c.Assert(seedErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.DataChanges.Message()))
	c.Assert(devErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.DevDatabases.Message()))
	c.Assert(realmErr, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.DevDatabases.Message()))
}

// The commands whose answer would be built from what the reader does not read
// refuse after they connect.
func TestYDBCommandsThatNeedMoreThanTheReaderRefuse(t *testing.T) {
	url := dbtarget.URL(t, dbtarget.YDB)
	tests := []struct {
		name    string
		command func() *cobra.Command
		args    []string
	}{
		{name: "introspect", command: introspect.NewIntrospectCommand,
			args: []string{"--db-url", url, "--out", "models"}},
		{name: "schema security", command: schema.NewSchemaSecurityCommand, args: []string{"--db-url", url}},
		{name: "schema lineage", command: schema.NewSchemaLineageCommand, args: []string{"--db-url", url}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			cmd := test.command()
			var stdout, stderr bytes.Buffer
			cmd.SetOut(&stdout)
			cmd.SetErr(&stderr)
			cmd.SetArgs(test.args)

			err := cmd.Execute()

			c.Assert(err, qt.ErrorMatches, `(?s).*`+regexp.QuoteMeta(ydbgap.OtherSurfaces.Message()))
			c.Assert(stdout.String(), qt.Equals, "")
		})
	}
}
