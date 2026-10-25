package root_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/cliobs"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/rundiag"
)

// longName is the i-th index name of a schema file: 68 bytes, distinct in its
// first byte, so the 63 the server keeps tell them apart.
func longName(i int) string {
	return "abcdefghij"[i:i+1] + strings.Repeat("x", 63) + "_idx"
}

// longNamesSchema writes a PostgreSQL schema file with the index names.
func longNamesSchema(c *qt.C, names ...string) string {
	c.Helper()
	var sql strings.Builder
	sql.WriteString("CREATE TABLE t (a int);\n")
	for _, name := range names {
		sql.WriteString("CREATE INDEX \"" + name + "\" ON t (a);\n")
	}
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(sql.String()), 0o600), qt.IsNil)
	return path
}

// run runs args on the native tree as one run and returns its exit code and
// what it wrote.
func run(c *qt.C, args ...string) (int, string) {
	c.Helper()
	cmd := root.NewRootCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)
	code := root.RunContext(c.Context(), cmd, args...)
	return code, stdout.String() + stderr.String()
}

// render runs `schema render` on path as one run and returns its exit code
// and what it wrote.
func render(c *qt.C, path string) (int, string) {
	c.Helper()
	return run(c, "schema", "render", "--schema-file", path, "--dialect", "postgres")
}

// runWarnings is the warning lines of a successful run of args.
func runWarnings(c *qt.C, args ...string) []string {
	c.Helper()
	code, output := run(c, args...)
	c.Assert(code, qt.Equals, 0, qt.Commentf("output:\n%s", output))
	var warnings []string
	for line := range strings.SplitSeq(output, "\n") {
		if strings.HasPrefix(line, "warning: ") {
			warnings = append(warnings, line)
		}
	}
	return warnings
}

// renderWarnings is the warning lines of a successful render.
func renderWarnings(c *qt.C, path string) []string {
	c.Helper()
	return runWarnings(c, "schema", "render", "--schema-file", path, "--dialect", "postgres")
}

// Names PostgreSQL cuts are one warning for the run, with how many, and the
// names themselves go to the debug log. Every command that loads a desired
// schema says so the same way, whatever it prints on its standard output:
// the loader raises the names for the run, not for the command.
func TestRunContext_ReportsCutNamesTogether(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema render", args: []string{"schema", "render", "--dialect", "postgres"}},
		{name: "schema validate", args: []string{"schema", "validate", "--dialect", "postgres"}},
		{name: "schema lineage", args: []string{"schema", "lineage", "--dialect", "postgres"}},
		{name: "viz", args: []string{"viz", "--dialect", "postgres"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := longNamesSchema(c, longName(0), longName(1), longName(2))

			warnings := runWarnings(c, append(test.args, "--schema-file", path)...)

			c.Assert(warnings, qt.DeepEquals, []string{
				"warning: 3 names are longer than 63 bytes; the server keeps their first 63 bytes, and they are read that way; PTAH_LOG_LEVEL=debug lists them",
			})
		})
	}
}

// A PRIMARY KEY or UNIQUE constraint builds an index under its own name, and
// the server stores both under the same 63 bytes. The file wrote one name, so
// the run reports one, as itself.
func TestRunContext_ReportsAConstraintNameOnce(t *testing.T) {
	c := qt.New(t)
	long := strings.Repeat("k", 70)
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE t (a int, CONSTRAINT "`+long+`" PRIMARY KEY (a));`+"\n"), 0o600), qt.IsNil)

	warnings := renderWarnings(c, path)

	c.Assert(warnings, qt.HasLen, 1)
	c.Assert(warnings[0], qt.Matches, `warning: schema file .*schema\.sql: primary key "`+long+
		`" in table "public\.t" is 70 bytes; the server keeps its first 63: "`+long[:63]+`"`)
}

// What a DBML document declares and the schema model drops is reported by the
// native commands too, one line for the run.
func TestRunContext_ReportsDroppedDBMLConstructsTogether(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "schema render", args: []string{"schema", "render", "--dialect", "postgres"}},
		{name: "schema validate", args: []string{"schema", "validate", "--dialect", "postgres"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(c.TempDir(), "schema.dbml")
			c.Assert(os.WriteFile(path, []byte("Project shop {\n  database_type: 'PostgreSQL'\n}\n"+
				"TableGroup g {\n  users\n}\nTable users {\n  id int [pk]\n}\n"), 0o600), qt.IsNil)

			warnings := runWarnings(c, append(test.args, "--schema-file", path)...)

			c.Assert(warnings, qt.DeepEquals, []string{
				"warning: 2 schema file constructs are ignored and have no effect; PTAH_LOG_LEVEL=debug lists them",
			})
		})
	}
}

// A cut name is an object: the kind, the place it is unique in, and the name
// written. The same name on two tables is two objects, and the same object read
// from two files is one, whichever file it was read from.
func TestRunContext_CountsCutNamesByObject(t *testing.T) {
	long := strings.Repeat("f", 70)
	twoTables := "CREATE TABLE p (id int PRIMARY KEY);\n" +
		`CREATE TABLE c1 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `" FOREIGN KEY (a) REFERENCES p (id));` + "\n" +
		`CREATE TABLE c2 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `" FOREIGN KEY (a) REFERENCES p (id));` + "\n"
	oneTable := "CREATE TABLE p (id int PRIMARY KEY);\n" +
		`CREATE TABLE c1 (id int PRIMARY KEY, a int, CONSTRAINT "` + long + `" FOREIGN KEY (a) REFERENCES p (id));` + "\n" +
		`CREATE INDEX "` + strings.Repeat("i", 70) + `" ON c1 (a);` + "\n"
	tests := []struct {
		name  string
		files []string
	}{
		{name: "one foreign key name on two tables", files: []string{twoTables}},
		{name: "one file read twice", files: []string{oneTable, oneTable}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			args := []string{"schema", "render", "--dialect", "postgres"}
			for index, sql := range test.files {
				path := filepath.Join(c.TempDir(), "schema"+strconv.Itoa(index)+".sql")
				c.Assert(os.WriteFile(path, []byte(sql), 0o600), qt.IsNil)
				args = append(args, "--schema-file", path)
			}

			warnings := runWarnings(c, args...)

			c.Assert(warnings, qt.DeepEquals, []string{
				"warning: 2 names are longer than 63 bytes; the server keeps their first 63 bytes, and they are read that way; PTAH_LOG_LEVEL=debug lists them",
			})
		})
	}
}

// recordLevels is the level, category and count of each JSON record in
// logged, in order.
func recordLevels(c *qt.C, logged string) []string {
	c.Helper()
	var levels []string
	for line := range strings.SplitSeq(strings.TrimSpace(logged), "\n") {
		var record struct {
			Level    string  `json:"level"`
			Category string  `json:"category"`
			Count    float64 `json:"count"`
		}
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil, qt.Commentf("%s", line))
		levels = append(levels, record.Level+" "+record.Category+" "+strconv.Itoa(int(record.Count)))
	}
	return levels
}

// With PTAH_LOG_FORMAT=json the summary is a JSON record at warning level on
// the error stream, beside the objects' records in the log, so neither holds
// a text line.
func TestRunContext_ReportsCutNamesAsRecordsInAJSONLog(t *testing.T) {
	c := qt.New(t)
	t.Setenv(cliobs.LogFormatEnvVar, "json")
	var logged bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
	c.Cleanup(func() { slog.SetDefault(previous) })
	path := longNamesSchema(c, longName(0), longName(1), longName(2))
	cmd := root.NewRootCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	code := root.RunContext(c.Context(), cmd, "schema", "validate", "--dialect", "postgres", "--schema-file", path)

	c.Assert(code, qt.Equals, 0, qt.Commentf("stderr:\n%s", stderr.String()))
	c.Assert(recordLevels(c, logged.String()), qt.DeepEquals, []string{"DEBUG truncated-name 0", "DEBUG truncated-name 0", "DEBUG truncated-name 0"})
	c.Assert(recordLevels(c, stderr.String()), qt.DeepEquals, []string{"WARN truncated-name 3"})
}

// --log-format json on a command that takes it makes the summary a JSON
// record too: `migrations status` reading a project file that holds two
// settings Atlas ignores.
func TestRunContext_ReportsRecordsUnderTheLogFormatFlag(t *testing.T) {
	c := qt.New(t)
	dir := c.TempDir()
	t.Chdir(dir)
	c.Assert(os.Mkdir("migrations", 0o755), qt.IsNil)
	c.Assert(os.WriteFile("atlas.hcl", []byte(`env "local" {
  url     = "sqlite://`+filepath.ToSlash(filepath.Join(dir, "status.db"))+`"
  project = "ignored"
  owner   = "ignored"
  migration {
    dir = "file://migrations"
  }
}
`), 0o600), qt.IsNil)
	cmd := root.NewRootCommand()
	var stdout, stderr bytes.Buffer
	cmd.SetOut(&stdout)
	cmd.SetErr(&stderr)

	code := root.RunContext(c.Context(), cmd, "migrations", "status", "--config", "atlas.hcl", "--env", "local", "--log-format", "json")

	c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout.String(), stderr.String()))
	c.Assert(warningLinesOf(stdout.String()+stderr.String()), qt.HasLen, 0)
	c.Assert(recordLevels(c, lastLine(stderr.String())), qt.DeepEquals, []string{"WARN ignored-atlas-hcl 2"})
}

// warningLinesOf is the text warning lines of output.
func warningLinesOf(output string) []string {
	var warnings []string
	for line := range strings.SplitSeq(output, "\n") {
		if strings.HasPrefix(line, "warning: ") {
			warnings = append(warnings, line)
		}
	}
	return warnings
}

// lastLine is the last line of output.
func lastLine(output string) string {
	lines := strings.Split(strings.TrimRight(output, "\n"), "\n")
	return lines[len(lines)-1]
}

// The words a summary uses to list its objects name the switches they mean,
// so renaming a variable cannot leave a summary pointing at the old name.
func TestDiagnosticsListings_NameTheirSwitches(t *testing.T) {
	c := qt.New(t)

	c.Assert(string(rundiag.ListedAtDebugLevel), qt.Equals, cliobs.LogLevelEnvVar+"=debug lists them")
	c.Assert(string(rundiag.ListedOutsideStrictCompat), qt.Equals,
		cliobs.LogLevelEnvVar+"=debug lists them when "+atlascompatpolicy.StrictCompatEnvVar+" is off")
}

// One cut name is reported as itself, with the name written and the name kept.
func TestRunContext_ReportsOneCutNameAsItself(t *testing.T) {
	c := qt.New(t)
	long := longName(0)
	path := longNamesSchema(c, long)

	warnings := renderWarnings(c, path)

	c.Assert(warnings, qt.HasLen, 1)
	c.Assert(warnings[0], qt.Matches, `warning: schema file .*`+regexp.QuoteMeta(filepath.Base(path))+`: index "`+long+
		`" in schema "public" is 68 bytes; the server keeps its first 63: "`+long[:63]+`"`)
}

// Two names cut to one are refused, whatever else the run reports: the
// aggregation is for names the server keeps apart, never for a collision.
func TestRunContext_RefusesCutNamesThatCollide(t *testing.T) {
	c := qt.New(t)
	shared := strings.Repeat("x", 63)

	code, output := render(c, longNamesSchema(c, shared+"a_idx", shared+"b_idx", longName(0)))

	c.Assert(code, qt.Not(qt.Equals), 0)
	c.Assert(output, qt.Contains, `are both stored as "`+shared+`", and the server refuses the second`)
}

// A key a schema file drops and adds again only to make it NOT VALID is
// reported, pointing at the directive that allows it without a second
// declaration.
func TestRunContext_ReportsAKeyDeclaredAgainForNotValid(t *testing.T) {
	c := qt.New(t)
	path := filepath.Join(c.TempDir(), "schema.sql")
	c.Assert(os.WriteFile(path, []byte(`CREATE TABLE owners (id bigint PRIMARY KEY);
CREATE TABLE items (id bigint PRIMARY KEY, owner_id bigint,
  CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id));
ALTER TABLE items DROP CONSTRAINT items_owner_fk;
ALTER TABLE items ADD CONSTRAINT items_owner_fk FOREIGN KEY (owner_id) REFERENCES owners (id) NOT VALID;
`), 0o600), qt.IsNil)

	warnings := renderWarnings(c, path)

	c.Assert(warnings, qt.HasLen, 1)
	c.Assert(warnings[0], qt.Matches, `warning: schema file .*schema\.sql: foreign key "items_owner_fk" on table "items" is dropped and added again `+
		`only to allow NOT VALID; -- ptah:allow-not-valid CONSTRAINT "items_owner_fk" ON "items" allows it without declaring the key a second time`)
}
