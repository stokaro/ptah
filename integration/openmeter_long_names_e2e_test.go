//go:build integration

package integration_test

import (
	"bytes"
	"cmp"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/core/platform"
	"ptah.run/core/sqlext"
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/root"
	"ptah.run/internal/rundiag"
	"ptah.run/internal/sqlschema"
)

// These tests hold the run-level report of the names PostgreSQL cuts to the
// case that asked for it: OpenMeter's Ent export, whose foreign keys and
// indexes Ent names past 63 bytes. testdata/openmeter/README.md says where the
// files come from and how long-names.sql was reduced from the export.

// openMeterLongNames is the reduced export.
var openMeterLongNames = filepath.Join("testdata", "openmeter", "long-names.sql")

// cutNamesOf is every name the reader cuts in the file at path, read as
// PostgreSQL, sorted by kind, scope and written name.
func cutNamesOf(c *qt.C, path string) []sqlschema.StoredName {
	c.Helper()
	data, err := os.ReadFile(path)
	c.Assert(err, qt.IsNil)
	var cut []sqlschema.StoredName
	_, _, err = sqlschema.ReadWith(c.Context(), data, platform.Postgres, nil, sqlschema.Options{
		Owners:      sqlext.None(),
		StoredNames: func(name sqlschema.StoredName) { cut = append(cut, name) },
	})
	c.Assert(err, qt.IsNil)
	slices.SortFunc(cut, compareStoredNames)
	return cut
}

func compareStoredNames(a, b sqlschema.StoredName) int {
	return cmp.Or(cmp.Compare(a.Kind, b.Kind), cmp.Compare(a.Scope, b.Scope), cmp.Compare(a.Written, b.Written))
}

// openMeterWarning is the one line a run that reads the fixture writes, in
// the words of listing.
func openMeterWarning(c *qt.C, listing rundiag.Listing) string {
	c.Helper()
	return fmt.Sprintf("warning: %d names are longer than 63 bytes; the server keeps their first 63 bytes, "+
		"and they are read that way; %s\n", len(cutNamesOf(c, openMeterLongNames)), listing)
}

// The command trees of the two binaries, and of ptah-compat under the strict
// compatibility profile.
var (
	nativeTree       = root.NewRootCommand
	compatTree       = func() *cobra.Command { return atlas.NewCompatCommand("atlas") }
	strictCompatTree = func() *cobra.Command {
		return atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())
	}
)

// runAsOneRun runs args on the tree newTree builds as the binaries run a
// command, with its run-level report, and returns the status and both
// streams.
func runAsOneRun(c *qt.C, newTree func() *cobra.Command, args ...string) (code int, stdout, stderr string) {
	c.Helper()
	cmd := newTree()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	code = root.RunContext(c.Context(), cmd, args...)
	return code, out.String(), errOut.String()
}

// openMeterRun is the state a run of a test below starts from. The target
// database is built from schema, and the migration directory holds schema as
// its one migration, with the long names as written, as the history Atlas
// writes for Ent holds them. Nothing differs from schema, so a diff of any two
// of them has no changes.
type openMeterRun struct {
	schema, target, dev, dir, out string
}

func newOpenMeterRun(c *qt.C, schemaSQL string) openMeterRun {
	c.Helper()
	fixture, err := os.ReadFile(openMeterLongNames)
	c.Assert(err, qt.IsNil)
	dir, schema := writeRewrittenMigrationProject(c, string(fixture), schemaSQL)
	dev, _ := scratchReplayDatabase(c)
	return openMeterRun{schema: schema, target: databaseBuiltFrom(c, string(fixture)), dev: dev, dir: dir, out: c.TempDir()}
}

// args spells the run's paths and URLs into a command line: {schema} is the
// schema file's path and {schema-url} its file URL, {dir} the migration
// directory's URL, {out} an empty directory.
func (r openMeterRun) args(template []string) []string {
	replacer := strings.NewReplacer(
		"{schema-url}", "file://"+filepath.ToSlash(r.schema), "{schema}", r.schema,
		"{target}", r.target, "{dev}", r.dev, "{dir}", "file://"+filepath.ToSlash(r.dir), "{out}", r.out,
	)
	args := make([]string, 0, len(template))
	for _, arg := range template {
		args = append(args, replacer.Replace(arg))
	}
	return args
}

// TestOpenMeterLongNamesCutWhatTheExportCuts holds the reduced fixture to the
// export it was cut from: the reader cuts the same names in both, so a count
// taken on the fixture is OpenMeter's.
func TestOpenMeterLongNamesCutWhatTheExportCuts(t *testing.T) {
	c := qt.New(t)

	reduced := cutNamesOf(c, openMeterLongNames)

	c.Assert(reduced, qt.Not(qt.HasLen), 0)
	c.Assert(reduced, qt.DeepEquals, cutNamesOf(c, filepath.Join("testdata", "openmeter", "ent-export.sql")))
}

// TestOpenMeterLongNamesNoChangesAreTheResultAndOneWarningE2E runs each
// command on a state that already matches the fixture: it prints its result
// and one warning with how many names the server cuts, on both binaries, and
// nothing else. The strict profile names the switch it allows.
func TestOpenMeterLongNamesNoChangesAreTheResultAndOneWarningE2E(t *testing.T) {
	tests := []struct {
		name    string
		tree    func() *cobra.Command
		args    []string
		stdout  string
		listing rundiag.Listing
	}{
		{
			name:    "ptah schema diff",
			tree:    nativeTree,
			args:    []string{"schema", "diff", "--from", "{target}", "--to", "{schema-url}"},
			stdout:  regexp.QuoteMeta("Schemas are synced, no changes to be made.\n"),
			listing: rundiag.ListedAtDebugLevel,
		},
		{
			name:    "ptah schema apply",
			tree:    nativeTree,
			args:    []string{"schema", "apply", "--schema-file", "{schema}", "--db-url", "{target}", "--auto-approve"},
			stdout:  regexp.QuoteMeta("Schema is synced, no changes to be made.\n"),
			listing: rundiag.ListedAtDebugLevel,
		},
		{
			name:    "ptah schema drift",
			tree:    nativeTree,
			args:    []string{"schema", "drift", "--schema-file", "{schema}", "--db-url", "{target}"},
			stdout:  regexp.QuoteMeta("No schema drift detected.\n"),
			listing: rundiag.ListedAtDebugLevel,
		},
		{
			name:    "ptah migrations generate",
			tree:    nativeTree,
			args:    []string{"migrations", "generate", "--schema-file", "{schema}", "--db-url", "{target}", "--migrations-dir", "{out}"},
			stdout:  `Schema is synced with \S+, no migration files generated\.\n`,
			listing: rundiag.ListedAtDebugLevel,
		},
		{
			name:    "ptah-compat schema diff",
			tree:    compatTree,
			args:    []string{"schema", "diff", "--from", "{target}", "--to", "{schema-url}", "--dev-url", "{dev}"},
			stdout:  regexp.QuoteMeta("Schemas are synced, no changes to be made.\n"),
			listing: rundiag.ListedAtDebugLevel,
		},
		{
			name:    "ptah-compat migrate diff",
			tree:    compatTree,
			args:    []string{"migrate", "diff", "--dir", "{dir}", "--to", "{schema-url}", "--dev-url", "{dev}"},
			stdout:  regexp.QuoteMeta("The migration directory is synced with the desired state, no changes to be made\n"),
			listing: rundiag.ListedAtDebugLevel,
		},
		{
			name:    "ptah-compat migrate diff under the strict profile",
			tree:    strictCompatTree,
			args:    []string{"migrate", "diff", "--dir", "{dir}", "--to", "{schema-url}", "--dev-url", "{dev}"},
			stdout:  regexp.QuoteMeta("The migration directory is synced with the desired state, no changes to be made\n"),
			listing: rundiag.ListedOutsideStrictCompat,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture, err := os.ReadFile(openMeterLongNames)
			c.Assert(err, qt.IsNil)
			state := newOpenMeterRun(c, string(fixture))

			code, stdout, stderr := runAsOneRun(c, test.tree, state.args(test.args)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stdout, qt.Matches, test.stdout)
			c.Assert(stderr, qt.Equals, openMeterWarning(c, test.listing))
		})
	}
}

// debugStoredNames is the names the truncated-name records of a JSON debug
// log carry, sorted as cutNamesOf sorts them.
func debugStoredNames(c *qt.C, logged string) []sqlschema.StoredName {
	c.Helper()
	var names []sqlschema.StoredName
	for line := range strings.SplitSeq(strings.TrimSpace(logged), "\n") {
		var record struct {
			Category string  `json:"category"`
			Kind     string  `json:"kind"`
			Scope    string  `json:"scope"`
			Written  string  `json:"written"`
			Stored   string  `json:"stored"`
			Limit    float64 `json:"limit"`
		}
		c.Assert(json.Unmarshal([]byte(line), &record), qt.IsNil, qt.Commentf("%s", line))
		if record.Category == rundiag.TruncatedNames.Name {
			names = append(names, sqlschema.StoredName{
				Kind: record.Kind, Scope: record.Scope, Written: record.Written, Stored: record.Stored, Limit: int(record.Limit),
			})
		}
	}
	slices.SortFunc(names, compareStoredNames)
	return names
}

// TestOpenMeterLongNamesAreListedOnceAtDebugLevelE2E reads the names the way
// PTAH_LOG_LEVEL=debug and PTAH_LOG_FORMAT=json show them: each cut name once,
// with the name the file wrote and the name the server keeps, also where a
// diff reads the fixture on both of its sides.
func TestOpenMeterLongNamesAreListedOnceAtDebugLevelE2E(t *testing.T) {
	tests := []struct {
		name string
		tree func() *cobra.Command
		args []string
	}{
		{
			name: "ptah schema diff of the file with itself",
			tree: nativeTree,
			args: []string{"schema", "diff", "--from", "{schema-url}", "--to", "{schema-url}", "--dev-url", "{dev}"},
		},
		{
			name: "ptah migrations generate",
			tree: nativeTree,
			args: []string{"migrations", "generate", "--schema-file", "{schema}", "--db-url", "{target}", "--migrations-dir", "{out}"},
		},
		{
			name: "ptah-compat schema diff of the file with itself",
			tree: compatTree,
			args: []string{"schema", "diff", "--from", "{schema-url}", "--to", "{schema-url}", "--dev-url", "{dev}"},
		},
		{
			name: "ptah-compat migrate diff of the history and the file",
			tree: compatTree,
			args: []string{"migrate", "diff", "--dir", "{dir}", "--to", "{schema-url}", "--dev-url", "{dev}"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			fixture, err := os.ReadFile(openMeterLongNames)
			c.Assert(err, qt.IsNil)
			state := newOpenMeterRun(c, string(fixture))
			var logged bytes.Buffer
			previous := slog.Default()
			slog.SetDefault(slog.New(slog.NewJSONHandler(&logged, &slog.HandlerOptions{Level: slog.LevelDebug})))
			c.Cleanup(func() { slog.SetDefault(previous) })

			code, stdout, stderr := runAsOneRun(c, test.tree, state.args(test.args)...)

			c.Assert(code, qt.Equals, 0, qt.Commentf("stdout:\n%s\nstderr:\n%s", stdout, stderr))
			c.Assert(stderr, qt.Equals, openMeterWarning(c, rundiag.ListedAtDebugLevel))
			c.Assert(debugStoredNames(c, logged.String()), qt.DeepEquals, cutNamesOf(c, openMeterLongNames))
		})
	}
}

// collidingOpenMeterSchema is the fixture with one foreign key renamed so that
// its first 63 bytes are another key's on the same table, and the two names
// the refusal gives. The keys are the first table's, in sorted order, that
// holds two long foreign key names.
func collidingOpenMeterSchema(c *qt.C) (sql, kept, renamed, stored string) {
	c.Helper()
	byTable := make(map[string][]sqlschema.StoredName)
	for _, name := range cutNamesOf(c, openMeterLongNames) {
		if name.Kind == "foreign key" {
			byTable[name.Scope] = append(byTable[name.Scope], name)
		}
	}
	var tables []string
	for table, keys := range byTable {
		if len(keys) > 1 {
			tables = append(tables, table)
		}
	}
	slices.Sort(tables)
	c.Assert(tables, qt.Not(qt.HasLen), 0)
	first, second := byTable[tables[0]][0], byTable[tables[0]][1]
	renamed = first.Stored + "_collides"
	fixture, err := os.ReadFile(openMeterLongNames)
	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(string(fixture), `"`+second.Written+`"`), qt.Equals, 1)
	return strings.Replace(string(fixture), `"`+second.Written+`"`, `"`+renamed+`"`, 1), first.Written, renamed, first.Stored
}

// TestOpenMeterLongNamesThatCollideFailE2E renames one of OpenMeter's keys so
// that the server would store two keys of one table under one name. That is
// not a cut name to count: every command on both binaries refuses the file,
// naming both keys, and reports no count of cut names.
func TestOpenMeterLongNamesThatCollideFailE2E(t *testing.T) {
	tests := []struct {
		name string
		tree func() *cobra.Command
		args []string
	}{
		{name: "ptah schema render", tree: nativeTree, args: []string{"schema", "render", "--schema-file", "{schema}", "--dialect", "postgres"}},
		{name: "ptah schema diff", tree: nativeTree, args: []string{"schema", "diff", "--from", "{target}", "--to", "{schema-url}"}},
		{
			name: "ptah migrations generate",
			tree: nativeTree,
			args: []string{"migrations", "generate", "--schema-file", "{schema}", "--db-url", "{target}", "--migrations-dir", "{out}"},
		},
		{
			name: "ptah-compat schema diff",
			tree: compatTree,
			args: []string{"schema", "diff", "--from", "{target}", "--to", "{schema-url}", "--dev-url", "{dev}"},
		},
		{
			name: "ptah-compat migrate diff",
			tree: compatTree,
			args: []string{"migrate", "diff", "--dir", "{dir}", "--to", "{schema-url}", "--dev-url", "{dev}"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			sql, kept, renamed, stored := collidingOpenMeterSchema(c)
			state := newOpenMeterRun(c, sql)

			code, stdout, stderr := runAsOneRun(c, test.tree, state.args(test.args)...)

			c.Assert(code, qt.Not(qt.Equals), 0)
			c.Assert(stderr, qt.Contains, `foreign key "`+kept+`"`)
			c.Assert(stderr, qt.Contains, `foreign key "`+renamed+`"`)
			c.Assert(stderr, qt.Contains, `are both stored as "`+stored+`", and the server refuses the second`)
			c.Assert(warningLines(stdout+stderr), qt.HasLen, 0)
		})
	}
}

// warningLines is the lines of output a run wrote as warnings.
func warningLines(output string) []string {
	var warnings []string
	for line := range strings.SplitSeq(output, "\n") {
		if strings.HasPrefix(line, "warning: ") {
			warnings = append(warnings, line)
		}
	}
	return warnings
}
