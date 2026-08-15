//go:build integration

package sqlitecmd_test

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/schema"
	"go.5x5.cz/ptah/dbschema"
	"go.5x5.cz/ptah/internal/atlashclrender"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
	"go.5x5.cz/ptah/migration/migrator"
)

const nativeInspectIncludeDDL = `CREATE TABLE users (id INTEGER PRIMARY KEY, email TEXT);
CREATE TABLE posts (id INTEGER PRIMARY KEY, author_id INTEGER REFERENCES users(id));
CREATE TABLE archive (id INTEGER PRIMARY KEY);`

func TestSchemaInspectExplicitSQLTemplateWritesExactTerminatedStatement(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "inspect-sql.db")
	seedSQLite(c.TB, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runCompatInspect(dbPath, "--format", `{{ sql . }}`)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "CREATE TABLE \"users\" (\n  \"id\" INTEGER PRIMARY KEY\n);\n")
}

func TestSchemaInspectExplicitSQLTemplateWritesNoBytesForEmptyDatabase(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "empty.db")

	out, err := runCompatInspect(dbPath, "--format", `{{ sql . }}`)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "")
}

func TestSchemaInspectLiveDatabaseWritesSQL(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "live.db")
	seedSQLite(c.TB, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);")

	out, err := runNativeInspect(dbPath, "--format", "sql")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "CREATE TABLE \"users\" (\n  \"id\" INTEGER PRIMARY KEY\n);\n")
}

func TestSchemaInspectEmptyLiveDatabaseWritesEmptySQL(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "empty.db")

	out, err := runNativeInspect(dbPath, "--format", "sql")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "")
}

func TestSchemaInspectCompatibilityHCLFraming_EmptySQLiteExactBytes(t *testing.T) {
	envbooltest.Unset(atlashclrender.KeepAtlasRefusedBlocksEnvVar)(t)
	tests := []struct {
		name string
		args []string
		want string
	}{
		{name: "default", want: "schema \"main\" {\n}\n"},
		{name: "hcl name is literal", args: []string{"--format", "hcl"}, want: "hcl"},
		{name: "hcl helper", args: []string{"--format", `{{ hcl . }}`}, want: "schema \"main\" {\n}\n"},
		{name: "MarshalHCL method", args: []string{"--format", `{{ $.MarshalHCL }}`}, want: "schema \"main\" {\n}\n"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := filepath.Join(t.TempDir(), "empty.db")

			out, err := runCompatInspect(dbPath, test.args...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
			c.Assert(out, qt.Equals, test.want)
		})
	}
}

func TestSchemaInspectCompatibilityHCLFraming_AllBlocksOptInKeepsFraming(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(atlashclrender.KeepAtlasRefusedBlocksEnvVar, "1")(t)
	dbPath := filepath.Join(t.TempDir(), "empty.db")

	out, err := runCompatInspect(dbPath)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "schema \"main\" {\n}\n")
}

func TestSchemaInspectCompatibilityHCLFraming_OutputFileUsesExactBytes(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(atlashclrender.KeepAtlasRefusedBlocksEnvVar)(t)
	dir := t.TempDir()
	target := filepath.Join(dir, "schema.hcl")

	out, err := runCompatInspect(filepath.Join(dir, "empty.db"), "--output", target)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals, "")
	written, err := os.ReadFile(target)
	c.Assert(err, qt.IsNil)
	c.Assert(string(written), qt.Equals, "schema \"main\" {\n}\n")
}

func TestSchemaInspectNativeHCLFraming_EmptySQLiteExactBytes(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "empty.db")

	out, err := runNativeInspect(dbPath, "--format", "hcl")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", out))
	c.Assert(out, qt.Equals,
		atlashclrender.GeneratedCodeMarker+"\n\nschema \"main\" {\n}\n\n")
}

// TestSchemaInspectAndAtlasSchemaInspectShareSchemaContent proves the native
// verb and its compatibility twin render the same objects even though their
// process surfaces deliberately frame HCL differently.
func TestSchemaInspectAndAtlasSchemaInspectShareSchemaContent(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "live.db")
	seedSQLite(c.TB, dbPath, "CREATE TABLE users (id INTEGER PRIMARY KEY);\nCREATE TABLE orders (id INTEGER PRIMARY KEY);")

	nativeOut, err := runNativeInspect(dbPath, "--format", "hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", nativeOut))
	compatOut, err := runCompatInspect(dbPath, "--format", `{{ hcl . }}`)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", compatOut))

	wantCompat, hasNativeFrame := compatHCLFromNative(nativeOut)
	c.Assert(hasNativeFrame, qt.IsTrue)
	c.Assert(compatOut, qt.Equals, wantCompat)
}

// TestSchemaInspectIncludeMatchesAtlasSchemaInspectContent pins that both
// surfaces resolve the same include selection despite their HCL framing.
func TestSchemaInspectIncludeMatchesAtlasSchemaInspectContent(t *testing.T) {
	c := qt.New(t)
	dbPath := filepath.Join(t.TempDir(), "live.db")
	seedSQLite(c.TB, dbPath, nativeInspectIncludeDDL)

	nativeOut, err := runNativeInspect(dbPath, "--include", "users", "--format", "hcl")
	c.Assert(err, qt.IsNil, qt.Commentf("%s", nativeOut))
	compatOut, err := runCompatInspect(dbPath, "--include", "users", "--format", `{{ hcl . }}`)
	c.Assert(err, qt.IsNil, qt.Commentf("%s", compatOut))

	wantCompat, hasNativeFrame := compatHCLFromNative(nativeOut)
	c.Assert(hasNativeFrame, qt.IsTrue)
	c.Assert(compatOut, qt.Equals, wantCompat)
	c.Assert(wantCompat, qt.Contains, `table "users"`)
	c.Assert(wantCompat, qt.Not(qt.Contains), `table "archive"`)
}

func runCompatInspect(dbPath string, extra ...string) (string, error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"schema", "inspect", "--url", "sqlite://" + dbPath}, extra...))
	err := cmd.Execute()
	return out.String(), err
}

func runNativeInspect(dbPath string, extra ...string) (string, error) {
	cmd := schema.NewSchemaCommand()
	var out bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&out)
	cmd.SetArgs(append([]string{"inspect", "--db-url", "sqlite://" + dbPath}, extra...))
	err := cmd.Execute()
	return out.String(), err
}

func seedSQLite(tb testing.TB, dbPath, schemaSQL string) {
	c := qt.New(tb)
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(context.Background(), "sqlite://"+dbPath)
	c.Assert(err, qt.IsNil)
	applyErr := atlasschema.ApplySQL(context.Background(), conn, migrator.MigrationTxModeAll, schemaSQL)
	dbschema.CloseAndWarn(conn)
	c.Assert(applyErr, qt.IsNil)
}

func compatHCLFromNative(native string) (string, bool) {
	body, ok := strings.CutPrefix(native, atlashclrender.GeneratedCodeMarker+"\n\n")
	return strings.TrimRight(body, "\n") + "\n", ok
}
