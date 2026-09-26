//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
)

// `ptah-compat schema diff` refuses a schema file compared with a PostgreSQL
// database when no dev database is given, in the words of the pinned community
// binary v1.3.0, measured against it on PostgreSQL 18 on 2026-09-26
// (stokaro/ptah#3676). Native `ptah schema diff` keeps comparing the two as
// written, and PTAH_ATLAS_DIFF_WITHOUT_DEV_URL reaches that on the compat
// surface.

// devURLGateTable is the table the target database holds.
const devURLGateTable = `CREATE TABLE widgets (id bigint PRIMARY KEY);`

// devURLGateSQL declares that table and one more.
const devURLGateSQL = devURLGateTable + "\nCREATE TABLE gadgets (id bigint PRIMARY KEY);\n"

// devURLGateHCL declares the same two tables as HCL.
const devURLGateHCL = `schema "public" {}
table "widgets" {
  schema = schema.public
  column "id" {
    null = false
    type = bigint
  }
  primary_key {
    columns = [column.id]
  }
}
table "gadgets" {
  schema = schema.public
  column "id" {
    null = false
    type = bigint
  }
  primary_key {
    columns = [column.id]
  }
}
`

// devURLGateFixture is a PostgreSQL database holding widgets, and a SQL file,
// an HCL file and a directory of HCL files each declaring widgets and gadgets.
type devURLGateFixture struct {
	target, sqlFile, hclFile, hclDir string
}

func newDevURLGateFixture(c *qt.C) devURLGateFixture {
	c.Helper()
	root := c.TempDir()
	hclDir := filepath.Join(root, "hcldir")
	c.Assert(os.MkdirAll(hclDir, 0o755), qt.IsNil)
	writeFileIn(c, hclDir, "schema.hcl", devURLGateHCL)
	return devURLGateFixture{
		target:  databaseBuiltFrom(c, devURLGateTable),
		sqlFile: "file://" + writeFileIn(c, root, "schema.sql", devURLGateSQL),
		hclFile: "file://" + writeFileIn(c, root, "schema.hcl", devURLGateHCL),
		hclDir:  "file://" + hclDir,
	}
}

// TestCompatSchemaDiffWithoutDevURLE2E_FailurePath is the community binary's
// answer to each argv: exit 1, standard output empty, and one of two
// sentences, chosen by the format of the first side that needs a dev database.
func TestCompatSchemaDiffWithoutDevURLE2E_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    func(fx devURLGateFixture) []string
		wantErr string
	}{
		{
			name:    "a SQL file on --from",
			args:    func(fx devURLGateFixture) []string { return []string{"--from", fx.sqlFile, "--to", fx.target} },
			wantErr: `--dev-url cannot be empty. See: https://atlasgo.io/atlas-schema/sql#dev-database`,
		},
		{
			name:    "an HCL file on --to",
			args:    func(fx devURLGateFixture) []string { return []string{"--from", fx.target, "--to", fx.hclFile} },
			wantErr: `--dev-url cannot be empty`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newDevURLGateFixture(c)

			out, err := runCompatVerb(append([]string{"schema", "diff"}, tt.args(fx)...)...)

			c.Assert(err, qt.ErrorMatches, tt.wantErr)
			c.Assert(out, qt.Equals, "Error: "+tt.wantErr+"\n")
		})
	}
}

// TestCompatSchemaDiffWithoutDevURLE2E_HappyPath: the same comparison runs with
// a dev database, and without one under the opt-in, and plans the table the
// database lacks.
func TestCompatSchemaDiffWithoutDevURLE2E_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		optIn string
		args  func(fx devURLGateFixture, dev string) []string
	}{
		{
			name:  "with --dev-url",
			optIn: "0",
			args: func(fx devURLGateFixture, dev string) []string {
				return []string{"--from", fx.target, "--to", fx.hclFile, "--dev-url", dev}
			},
		},
		{
			name:  "without --dev-url, under the opt-in",
			optIn: "1",
			args: func(fx devURLGateFixture, _ string) []string {
				return []string{"--from", fx.target, "--to", fx.hclFile}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			t.Setenv("PTAH_ATLAS_DIFF_WITHOUT_DEV_URL", tt.optIn)
			fx := newDevURLGateFixture(c)
			dev, _ := scratchReplayDatabase(c)

			out, err := runCompatVerb(append([]string{"schema", "diff"}, tt.args(fx, dev)...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Contains, `CREATE TABLE "public"."gadgets"`)
		})
	}
}

// TestNativeSchemaDiffWithoutDevURLE2E: native `ptah schema diff` compares a
// schema file with the database with no dev database and no opt-in.
func TestNativeSchemaDiffWithoutDevURLE2E(t *testing.T) {
	c := qt.New(t)
	fx := newDevURLGateFixture(c)

	out := runPtahNative(c, "schema", "diff", "--from", fx.target, "--to", fx.sqlFile)

	c.Assert(out, qt.Contains, `CREATE TABLE "gadgets"`)
}

// TestCompatSchemaApplyHCLDirectoryWithoutDevURLE2E: a directory of HCL files
// is a schema definition, so `schema apply` applies it with no dev database,
// as the community binary does on PostgreSQL. The table is read back from the
// target.
func TestCompatSchemaApplyHCLDirectoryWithoutDevURLE2E(t *testing.T) {
	c := qt.New(t)
	fx := newDevURLGateFixture(c)

	out, err := runCompatVerb("schema", "apply", "--url", fx.target, "--to", fx.hclDir, "--auto-approve")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	conn, err := dbschema.ConnectToDatabase(c.Context(), fx.target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	var tables int
	c.Assert(conn.QueryRowContext(c.Context(),
		`SELECT count(*) FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename = 'gadgets'`,
	).Scan(&tables), qt.IsNil)
	c.Assert(tables, qt.Equals, 1)
}
