//go:build integration

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// A dev database is reset before and after a run uses it, so a dev database
// that already holds a table is refused before anything drops it -- as the
// pinned community binary v1.3.0 refuses it, measured per verb on 2026-09-26
// against PostgreSQL 18, MySQL 8.4 and MariaDB 11.8. Without the refusal each
// verb below exits 0 and leaves the dev database empty (stokaro/ptah#3797).
// Each test reads `kept` back from the dev database after the refusal.

// devNotCleanReasons is the object each engine's refusal names in a scratch
// database holding `kept`. The PostgreSQL URL pins no search_path, so the
// binary judges the whole database and names the schema too.
var devNotCleanReasons = map[string]string{
	"PostgreSQL": `found table "kept" in schema "public"`,
	"MySQL":      `found table "kept" in schema "ptah_dev_identity_\d+"`,
	"MariaDB":    `found table "kept" in schema "ptah_dev_identity_\d+"`,
}

// devNotCleanArgs spells a row's arguments with a run's databases and files.
func devNotCleanArgs(template []string, dev, target, schemaFile, dir, out string) []string {
	replacer := strings.NewReplacer(
		"{dev}", dev,
		"{target}", target,
		"{schema}", "file://"+schemaFile,
		"{rawschema}", schemaFile,
		"{dir}", "file://"+dir,
		"{rawdir}", dir,
		"{out}", "file://"+out,
	)
	args := make([]string, len(template))
	for i, arg := range template {
		args[i] = replacer.Replace(arg)
	}
	return args
}

// TestCompatVerbsRefuseADevDatabaseThatHoldsATableE2E runs every
// Atlas-compatible verb that takes a snapshot of the dev database, with the dev
// database holding `kept`, and expects the binary's sentence for the verb.
func TestCompatVerbsRefuseADevDatabaseThatHoldsATableE2E(t *testing.T) {
	verbs := []struct {
		name   string
		args   []string
		prefix string
	}{
		{
			name:   "migrate diff",
			args:   []string{"migrate", "diff", "x", "--dir", "{out}", "--to", "{schema}", "--dev-url", "{dev}"},
			prefix: `sql/migrate: taking database snapshot: `,
		},
		{
			name:   "migrate lint",
			args:   []string{"migrate", "lint", "--dir", "{dir}", "--dev-url", "{dev}", "--latest", "1"},
			prefix: `taking database snapshot: `,
		},
		{
			name:   "migrate validate",
			args:   []string{"migrate", "validate", "--dir", "{dir}", "--dev-url", "{dev}"},
			prefix: `replaying the migration directory: sql/migrate: taking database snapshot: `,
		},
		{
			name:   "schema inspect",
			args:   []string{"schema", "inspect", "-u", "{schema}", "--dev-url", "{dev}"},
			prefix: `sql/migrate: taking database snapshot: `,
		},
		{
			name:   "schema diff",
			args:   []string{"schema", "diff", "--from", "{schema}", "--to", "{schema}", "--dev-url", "{dev}"},
			prefix: `sql/migrate: taking database snapshot: `,
		},
		{
			name:   "schema apply",
			args:   []string{"schema", "apply", "-u", "{target}", "--to", "{schema}", "--dev-url", "{dev}", "--auto-approve"},
			prefix: `sql/migrate: taking database snapshot: `,
		},
	}

	for _, engine := range devIdentityEngines {
		for _, verb := range verbs {
			t.Run(engine.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				dev := newDevIdentityTarget(c, engine.engine, engine.dropSuffix)
				target := newDevIdentityTarget(c, engine.engine, engine.dropSuffix)
				schemaFile, dir := writeDevIdentitySources(c)
				out := filepath.Join(c.TempDir(), "out")
				c.Assert(os.MkdirAll(out, 0o755), qt.IsNil)

				output, err := runCompatVerb(devNotCleanArgs(verb.args, dev.url, target.url, schemaFile, dir, out)...)

				c.Assert(err, qt.ErrorMatches,
					verb.prefix+`sql/migrate: connected database is not clean: `+devNotCleanReasons[engine.name],
					qt.Commentf("%s", output))
				c.Assert(dev.keptRows(c), qt.Equals, 1)
			})
		}
	}
}

// TestNativeVerbsRefuseADevDatabaseThatHoldsATableE2E is the same refusal on
// the native verbs that reset a dev database, in Ptah's own words.
func TestNativeVerbsRefuseADevDatabaseThatHoldsATableE2E(t *testing.T) {
	verbs := []struct {
		name string
		args []string
	}{
		{name: "migrations validate", args: []string{"migrations", "validate", "--dir", "{rawdir}", "--dev-url", "{dev}"}},
		{name: "migrations lint", args: []string{"migrations", "lint", "--dir", "{rawdir}", "--dev-url", "{dev}"}},
		{
			name: "schema apply",
			args: []string{"schema", "apply", "--db-url", "{target}", "--schema-file", "{rawschema}", "--dev-url", "{dev}", "--auto-approve"},
		},
	}

	for _, engine := range devIdentityEngines {
		for _, verb := range verbs {
			t.Run(engine.name+"/"+verb.name, func(t *testing.T) {
				c := qt.New(t)
				dev := newDevIdentityTarget(c, engine.engine, engine.dropSuffix)
				target := newDevIdentityTarget(c, engine.engine, engine.dropSuffix)
				schemaFile, dir := writeDevIdentitySources(c)

				output, err := runPtahNativeWithError(devNotCleanArgs(verb.args, dev.url, target.url, schemaFile, dir, "")...)

				c.Assert(err, qt.ErrorMatches, `(?s).*connected database is not clean: `+devNotCleanReasons[engine.name]+
					`; Ptah resets a dev database before and after it uses one, so point --dev-url at an empty database.*`,
					qt.Commentf("%s", output))
				c.Assert(dev.keptRows(c), qt.Equals, 1)
			})
		}
	}
}
