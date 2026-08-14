package atlas_test

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
)

// inspectIncludeDDL mirrors the fixture shape the `schema inspect --include`
// transcripts were recorded against: two
// tables joined by a foreign key plus one independent table, so selection,
// dependency refusal, and non-selection are all observable.
const inspectIncludeDDL = `
CREATE TABLE inspect_users (
  id INTEGER PRIMARY KEY,
  email TEXT
);
CREATE TABLE inspect_posts (
  id INTEGER PRIMARY KEY,
  author_id INTEGER REFERENCES inspect_users(id)
);
CREATE TABLE inspect_archive (
  id INTEGER PRIMARY KEY
);
`

// runCompatInspect executes `atlas schema inspect` with args and returns
// stdout and stderr separately, so a test can assert that a refusal left
// stdout empty.
func runCompatInspect(args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out bytes.Buffer
	var errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(append([]string{"schema", "inspect"}, args...))
	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// TestSchemaInspectAdvertisesIncludeInItsFlagList asserts on the rendered
// flag entry rather than on the help text as a whole: the command's long
// description explains --include in prose, so a bare substring check against
// the full help would pass even with the flag unregistered.
func TestSchemaInspectAdvertisesIncludeInItsFlagList(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCompatInspect("--help")

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "--include stringArray   Schema objects to include in inspection")
}

func TestSchemaInspectIncludeSelectsTopLevelResources(t *testing.T) {
	c := qt.New(t)
	dbPath := seedSQLiteDB(t, inspectIncludeDDL)

	stdout, stderr, err := runCompatInspect("--url", "sqlite://"+dbPath, "--include", "inspect_users")

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(stderr, qt.Equals, "")
	c.Assert(stdout, qt.Contains, `table "inspect_users"`)
	// The selected table is rendered whole, not as an empty shell.
	c.Assert(stdout, qt.Contains, `column "email"`)
	c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_posts"`)
	c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_archive"`)
}

func TestSchemaInspectIncludeUnionsValues(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "repeated flag", args: []string{"--include", "inspect_users", "--include", "inspect_archive"}},
		{name: "comma separated", args: []string{"--include", "inspect_users,inspect_archive"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := seedSQLiteDB(t, inspectIncludeDDL)

			stdout, stderr, err := runCompatInspect(append([]string{"--url", "sqlite://" + dbPath}, test.args...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Contains, `table "inspect_users"`)
			c.Assert(stdout, qt.Contains, `table "inspect_archive"`)
			c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_posts"`)
		})
	}
}

func TestSchemaInspectIncludeAcceptsQualifiedNames(t *testing.T) {
	// Both spellings name one top-level table. Atlas treats
	// the wildcard form as a child-level pattern instead and renders the
	// tables as empty shells; Ptah keeps the selected table whole.
	tests := []struct {
		name    string
		pattern string
	}{
		{name: "schema qualified", pattern: "main.inspect_users"},
		{name: "wildcard schema", pattern: "*.inspect_users"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := seedSQLiteDB(t, inspectIncludeDDL)

			stdout, stderr, err := runCompatInspect("--url", "sqlite://"+dbPath, "--include", test.pattern)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Contains, `table "inspect_users"`)
			c.Assert(stdout, qt.Contains, `column "email"`)
			c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_archive"`)
		})
	}
}

func TestSchemaInspectIncludeSelectsQuotedDottedIdentifier(t *testing.T) {
	// The qualified candidate for a dotted identifier quotes the dotted part
	// (`main."dotted.table"`), so the selector matching it holds two dot
	// characters and one separator. Depth is measured on separators outside
	// quotes for exactly this reason.
	tests := []struct {
		name    string
		pattern string
	}{
		{name: "schema qualified", pattern: `main."dotted.table"`},
		{name: "wildcard schema", pattern: `*."dotted.table"`},
		{name: "bare name", pattern: `dotted.table`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := seedSQLiteDB(t,
				"CREATE TABLE \"dotted.table\" (id INTEGER PRIMARY KEY, email TEXT);\n"+
					"CREATE TABLE inspect_archive (id INTEGER PRIMARY KEY);")

			stdout, stderr, err := runCompatInspect("--url", "sqlite://"+dbPath, "--include", test.pattern)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Contains, `table "dotted.table"`)
			c.Assert(stdout, qt.Contains, `column "email"`)
			c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_archive"`)
		})
	}
}

func TestSchemaInspectIncludeComposesWithExclude(t *testing.T) {
	c := qt.New(t)
	dbPath := seedSQLiteDB(t, inspectIncludeDDL)

	stdout, stderr, err := runCompatInspect(
		"--url", "sqlite://"+dbPath,
		"--include", "inspect_users,inspect_archive",
		"--exclude", "inspect_archive",
	)

	// The positive selection defines the universe first; --exclude subtracts
	// from it afterward.
	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	c.Assert(stdout, qt.Contains, `table "inspect_users"`)
	c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_archive"`)
	c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_posts"`)
}

func TestSchemaInspectIncludeComposesWithSchemaScope(t *testing.T) {
	t.Run("selected schema keeps the selection", func(t *testing.T) {
		c := qt.New(t)
		dbPath := seedSQLiteDB(t, inspectIncludeDDL)

		stdout, stderr, err := runCompatInspect(
			"--url", "sqlite://"+dbPath,
			"--schema", "main",
			"--include", "inspect_users",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Contains, `table "inspect_users"`)
		c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_archive"`)
	})

	t.Run("unknown schema does not narrow SQLite inspection", func(t *testing.T) {
		c := qt.New(t)
		dbPath := seedSQLiteDB(t, inspectIncludeDDL)

		stdout, stderr, err := runCompatInspect(
			"--url", "sqlite://"+dbPath,
			"--schema", "other",
			"--include", "inspect_users",
		)

		// Measured against the pinned Atlas CE v1.2.0 binary: SQLite has one
		// schema, and `schema inspect --schema other` narrows nothing there —
		// it renders the whole database. Ptah matches that, so on SQLite the
		// include selection alone governs what survives.
		c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Contains, `table "inspect_users"`)
		c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_archive"`)
	})
}

func TestSchemaInspectIncludeDegenerateValues(t *testing.T) {
	allTables := []string{"inspect_users", "inspect_posts", "inspect_archive"}

	tests := []struct {
		name string
		args []string
		// wantTables are the table blocks the rendered output must contain,
		// wantAbsent the ones it must not.
		wantTables []string
		wantAbsent []string
	}{
		{
			// A selection that matches nothing renders no objects rather than
			// failing: inspection is read-only, so an empty description of an
			// empty selection is a legitimate answer. It is no longer silent
			// about it — see
			// TestSchemaInspectIncludeEmptySelectionIsReportedOnStderr for the
			// notice, which this row deliberately does not assert because it
			// is about the rendered output.
			name:       "matches nothing",
			args:       []string{"--include", "no_such_table"},
			wantAbsent: allTables,
		},
		{
			name:       "matches everything",
			args:       []string{"--include", "*"},
			wantTables: allTables,
		},
		{
			// An empty value carries no selection, so inspection stays unfiltered.
			name:       "empty value",
			args:       []string{"--include", ""},
			wantTables: allTables,
		},
		{
			// Control: with the flag absent the established unfiltered
			// behavior must be unchanged.
			name:       "flag absent",
			args:       nil,
			wantTables: allTables,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := seedSQLiteDB(t, inspectIncludeDDL)

			stdout, stderr, err := runCompatInspect(append([]string{"--url", "sqlite://" + dbPath}, test.args...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			for _, table := range test.wantTables {
				c.Assert(stdout, qt.Contains, `table "`+table+`"`)
			}
			for _, table := range test.wantAbsent {
				c.Assert(stdout, qt.Not(qt.Contains), `table "`+table+`"`)
			}
		})
	}
}

func TestSchemaInspectIncludeCrossScopeDependencyFails(t *testing.T) {
	c := qt.New(t)
	dbPath := seedSQLiteDB(t, inspectIncludeDDL)

	stdout, _, err := runCompatInspect("--url", "sqlite://"+dbPath, "--include", "inspect_posts")

	// Rendering the selected table alone would emit a foreign key pointing at
	// a table the same output omits, so the projection is refused instead.
	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains,
		`table "main.inspect_posts" depends on table "main.inspect_users" via a foreign key, but "main.inspect_users" is not selected`)
	c.Assert(stdout, qt.Equals, "")
}

func TestSchemaInspectIncludeValidatesSelectorsBeforeConnecting(t *testing.T) {
	tests := []struct {
		name    string
		pattern string
		wantErr string
	}{
		{
			name:    "type selector spelling",
			pattern: "*[type=column]",
			wantErr: `unsupported Atlas include selector "\*\[type=column\]": column resources ride along with their parent and cannot be included on their own`,
		},
		{
			// The dotted spelling is not one of them. It is indistinguishable
			// from a table literally named "inspect_users.email", so it is
			// carried to the projection, which is the only place the answer
			// exists; the closed port is what fails.
			name:    "positional spelling reaches the connection",
			pattern: "main.inspect_users.email",
			wantErr: `(?s)connect to --url: .*`,
		},
		{
			name:    "unknown resource type",
			pattern: "*[type=widget]",
			wantErr: `unsupported Atlas include resource type "widget" in selector "\*\[type=widget\]"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// The URL points at a closed port: reaching it would fail with a
			// connection error instead of the selector error asserted below.
			_, _, err := runCompatInspect(
				"--url", "postgres://127.0.0.1:1/unreachable",
				"--include", test.pattern,
			)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

func TestSchemaInspectIncludeValidationRunsBeforeDevDatabaseReset(t *testing.T) {
	c := qt.New(t)
	dir := t.TempDir()
	schemaPath := filepath.Join(dir, "schema.sql")
	devPath := filepath.Join(dir, "dev.db")
	c.Assert(os.WriteFile(schemaPath, []byte(inspectIncludeDDL), 0o600), qt.IsNil)

	_, _, err := runCompatInspect(
		"--url", "file://"+schemaPath,
		"--dev-url", "sqlite://"+devPath,
		"--include", "*[type=column]",
	)

	c.Assert(err, qt.IsNotNil)
	// Validation runs before the dev database is touched, so its destructive
	// reset never happened and the file was never created.
	_, statErr := os.Stat(devPath)
	c.Assert(os.IsNotExist(statErr), qt.IsTrue)
}

func TestSchemaInspectIncludeAppliesToEverySourceKind(t *testing.T) {
	t.Run("local schema file", func(t *testing.T) {
		c := qt.New(t)
		dir := t.TempDir()
		schemaPath := filepath.Join(dir, "schema.sql")
		c.Assert(os.WriteFile(schemaPath, []byte(inspectIncludeDDL), 0o600), qt.IsNil)

		stdout, stderr, err := runCompatInspect(
			"--url", "file://"+schemaPath,
			"--dev-url", "sqlite://"+filepath.Join(dir, "dev.db"),
			"--include", "inspect_users",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Contains, `table "inspect_users"`)
		c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_archive"`)
	})

	t.Run("migration directory", func(t *testing.T) {
		c := qt.New(t)
		migrationsDir := writeAtlasFormatMigrations(t, inspectIncludeDDL)

		stdout, stderr, err := runCompatInspect(
			"--url", "file://"+migrationsDir,
			"--dev-url", "sqlite://"+filepath.Join(t.TempDir(), "dev.db"),
			"--include", "inspect_users",
		)

		c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
		c.Assert(stdout, qt.Contains, `table "inspect_users"`)
		c.Assert(stdout, qt.Not(qt.Contains), `table "inspect_archive"`)
	})
}

func TestSchemaInspectIncludeAppliesToEveryOutputFormat(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "hcl", format: `{{ hcl . }}`, want: `table "inspect_users"`},
		{name: "sql", format: `{{ sql . }}`, want: `CREATE TABLE "inspect_users"`},
		{name: "json", format: `{{ json . }}`, want: `"name":"inspect_users"`},
		{name: "template", format: `{{ range (index .Schema.Schemas 0).Tables }}{{ .Name }};{{ end }}`, want: "inspect_users;"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dbPath := seedSQLiteDB(t, inspectIncludeDDL)

			stdout, stderr, err := runCompatInspect(
				"--url", "sqlite://"+dbPath,
				"--include", "inspect_users",
				"--format", test.format,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
			c.Assert(stdout, qt.Contains, test.want)
			c.Assert(stdout, qt.Not(qt.Contains), "inspect_archive")
		})
	}
}

func TestSchemaInspectIncludeAppliesToSplitWriteExport(t *testing.T) {
	c := qt.New(t)
	dbPath := seedSQLiteDB(t, inspectIncludeDDL)
	outDir := filepath.Join(t.TempDir(), "export")

	_, stderr, err := runCompatInspect(
		"--url", "sqlite://"+dbPath,
		"--include", "inspect_users",
		"--format", `{{ hcl . | split | write "`+outDir+`" }}`,
	)

	c.Assert(err, qt.IsNil, qt.Commentf("%s", stderr))
	written := exportedFileNames(c, outDir)
	// Both names follow from the table declaring its schema, which the render
	// must do for the document to be valid HCL at all (stokaro/ptah#1234). The
	// schema file is new; the table file gained its schema prefix.
	c.Assert(written, qt.DeepEquals, []string{"main.hcl", "main_inspect_users.hcl"})
}

// exportedFileNames returns the base names of every file below root, sorted by
// walk order, so a split/write export can be asserted on exactly.
func exportedFileNames(c *qt.C, root string) []string {
	c.Helper()
	var names []string
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			names = append(names, filepath.Base(path))
		}
		return nil
	})
	c.Assert(err, qt.IsNil)
	return names
}
