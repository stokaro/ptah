//go:build integration

package integration_test

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
	_ "github.com/jackc/pgx/v5/stdlib"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/envbool/envbooltest"
)

// atlasCompatEnvSchemasCase is one `env { schemas }` value, the --schema flag
// it is run beside, and the schema universe the run must describe.
type atlasCompatEnvSchemasCase struct {
	name string
	// attribute is the `schemas` line written into the env block, empty for an
	// env block that has none.
	attribute string
	// flagSchemas are the --schema values passed on the command line.
	flagSchemas []string
	// wantSchemas is the set of `schema "<name>"` blocks the output must carry,
	// in no particular order.
	wantSchemas []string
	// why records the measurement on the pinned binary behind the expectation.
	why string
}

// TestAtlasCompatEnvSchemasRestrictsTheInspectedUniverseE2E establishes the
// behavioral half of `env { schemas }`, which no SQLite fixture can reach.
//
// The attribute had no parser arm before stokaro/ptah#934: it fell to the
// unknown-name tolerance, was warned about, and selected nothing. The decode
// half was already provable anywhere — the pinned Atlas community binary v1.3.0
// refuses `schemas = "one"` with `field is of type slice but attr "schemas" is
// type: string` at exit 1, and a binary that refuses on a field's TYPE has
// decoded that field. What the decode proof cannot say is what the decoded
// value then DOES, and SQLite cannot say either: on the pinned binary
// `--schema nosuchschema` against a SQLite database exits 0 and prints the full
// output, byte-identical to naming a real schema, so no SQLite fixture
// discriminates.
//
// PostgreSQL does. Measured on 2026-08-13 against a database holding schemas
// `one` (table alpha), `two` (table beta) and `public` (table pubtbl), with the
// pinned binary at exit 0 on every row except the type refusal:
//
//	env { schemas = ["one"] }                          -> schema "one"
//	env { schemas = ["one","two"] }                    -> schema "one", schema "two"
//	env { schemas = ["nosuchschema"] }                 -> nothing at all
//	env { schemas = [] }                               -> one, two, public
//	(no schemas attribute)                             -> one, two, public
//	env { schemas = ["one"] }        --schema two      -> schema "two"
//	env { schemas = ["one","two"] }  --schema one      -> schema "one"
//	env { schemas = ["nosuchschema"] } --schema one    -> schema "one"
//
// Ptah described all three schemas on the first, second, third and sixth of
// those before this change; the flag rows already agreed, because --schema
// reached the reader and the attribute did not.
//
// The `public` schema is what makes the rows discriminating. A fixture with
// only `one` and `two` would pass an implementation that restricted to
// "every non-system schema" without reading the attribute at all.
func TestAtlasCompatEnvSchemasRestrictsTheInspectedUniverseE2E(t *testing.T) {
	adminURL := requirePostgresE2EDatabaseURL(t)

	tests := []atlasCompatEnvSchemasCase{
		{
			name:        "one named schema",
			attribute:   `schemas = ["one"]`,
			wantSchemas: []string{"one"},
			why:         "the pinned binary describes one alone, and public is the proof it read the attribute",
		},
		{
			name:        "both named schemas",
			attribute:   `schemas = ["one", "two"]`,
			wantSchemas: []string{"one", "two"},
			why:         "naming both keeps both and still drops public",
		},
		{
			name:        "a schema that does not exist",
			attribute:   `schemas = ["nosuchschema"]`,
			wantSchemas: []string{},
			why:         "the pinned binary describes nothing and exits 0; an empty answer to an empty selection",
		},
		{
			name:        "an empty list",
			attribute:   `schemas = []`,
			wantSchemas: []string{"one", "two", "public"},
			why:         "the pinned binary treats an empty list exactly as it treats an absent attribute",
		},
		{
			name:        "no schemas attribute at all",
			attribute:   "",
			wantSchemas: []string{"one", "two", "public"},
			why:         "the control: without the attribute the universe is whatever the connection can see",
		},
		{
			name:        "the flag replaces the configured list",
			attribute:   `schemas = ["one"]`,
			flagSchemas: []string{"two"},
			wantSchemas: []string{"two"},
			why:         "measured: --schema wins outright, it does not intersect with the attribute",
		},
		{
			name:        "the flag narrows a two-element configured list",
			attribute:   `schemas = ["one", "two"]`,
			flagSchemas: []string{"one"},
			wantSchemas: []string{"one"},
			why:         "the same precedence seen from the other side",
		},
		{
			name:        "the flag rescues a configured schema that does not exist",
			attribute:   `schemas = ["nosuchschema"]`,
			flagSchemas: []string{"one"},
			wantSchemas: []string{"one"},
			why: "the row that separates 'the flag wins' from 'the two are unioned':" +
				" a union would still describe nothing extra, but an intersection would describe nothing at all",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar)(t)
			sourceURL := newAtlasCompatEnvSchemasDatabase(c, adminURL)
			configPath := writeAtlasCompatEnvSchemasConfig(c, sourceURL, test.attribute)

			rendered := runAtlasCompatEnvSchemasInspect(c, configPath, test.flagSchemas)

			c.Check(
				atlasCompatRenderedSchemaNames(rendered), qt.ContentEquals, test.wantSchemas,
				qt.Commentf("%s\nrendered:\n%s", test.why, rendered),
			)
		})
	}
}

// TestAtlasCompatEnvSchemasOptOutRestoresTheRealmDescriptionE2E is the
// capability half.
//
// Acting on the attribute REMOVES a description Ptah used to emit — the first
// row here is exactly what every run produced before the change — so AGENTS.md
// requires the fuller behavior to stay reachable rather than be deleted. The
// second row is the non-vacuity control: without it the opt-out could be doing
// nothing at all and the first row would still pass if the restriction had
// never been implemented.
func TestAtlasCompatEnvSchemasOptOutRestoresTheRealmDescriptionE2E(t *testing.T) {
	adminURL := requirePostgresE2EDatabaseURL(t)

	tests := []struct {
		name        string
		env         func(testing.TB)
		wantSchemas []string
	}{
		{
			name:        "the opt-out describes the whole realm",
			env:         envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "1"),
			wantSchemas: []string{"one", "two", "public"},
		},
		{
			name:        "unset restricts, which is what the opt-out has to be undoing",
			env:         envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar),
			wantSchemas: []string{"one"},
		},
		{
			name:        "a valid false restricts too",
			env:         envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "false"),
			wantSchemas: []string{"one"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)
			sourceURL := newAtlasCompatEnvSchemasDatabase(c, adminURL)
			configPath := writeAtlasCompatEnvSchemasConfig(c, sourceURL, `schemas = ["one"]`)

			rendered := runAtlasCompatEnvSchemasInspect(c, configPath, nil)

			c.Check(atlasCompatRenderedSchemaNames(rendered), qt.ContentEquals, test.wantSchemas)
		})
	}
}

// TestAtlasCompatEnvSchemasRefusesAValueTheFieldCannotHoldE2E closes
// compatibility rule (a) on the surface the rule is about.
//
// The pinned binary exits 1 on this project file; `ptah-compat schema inspect
// --env local` exited 0 with a warning that the attribute had no effect. The
// opt-out row is here because an environment variable must not be able to
// reopen an exit 0 where the pinned binary exits 1.
func TestAtlasCompatEnvSchemasRefusesAValueTheFieldCannotHoldE2E(t *testing.T) {
	adminURL := requirePostgresE2EDatabaseURL(t)

	tests := []struct {
		name string
		env  func(testing.TB)
	}{
		{
			name: "by default",
			env:  envbooltest.Unset(projectconfig.IgnoreEnvSchemasEnvVar),
		},
		{
			name: "and with the selection opt-out set",
			env:  envbooltest.Set(projectconfig.IgnoreEnvSchemasEnvVar, "1"),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			test.env(t)
			sourceURL := newAtlasCompatEnvSchemasDatabase(c, adminURL)
			configPath := writeAtlasCompatEnvSchemasConfig(c, sourceURL, `schemas = "one"`)

			cmd := atlas.NewCompatCommand("atlas")
			var out, errOut bytes.Buffer
			cmd.SetOut(&out)
			cmd.SetErr(&errOut)
			cmd.SetArgs([]string{"schema", "inspect", "-c", "file://" + configPath, "--env", "local"})

			err := cmd.Execute()

			c.Check(err, qt.ErrorMatches, `atlas\.hcl "schemas" at .* must be a list of strings`)
			c.Check(out.String(), qt.Equals, "",
				qt.Commentf("a refused project file must not also render a document"))
		})
	}
}

// newAtlasCompatEnvSchemasDatabase creates a throwaway database holding one
// table in each of `one`, `two` and `public`, and returns its URL.
func newAtlasCompatEnvSchemasDatabase(c *qt.C, adminURL string) string {
	c.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	c.Cleanup(cancel)

	adminDB, err := sql.Open("pgx", adminURL)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { _ = adminDB.Close() })

	name := fmt.Sprintf("ptah_env_schemas_%d", time.Now().UnixNano())
	createE2EDatabase(c, ctx, adminDB, name)
	c.Cleanup(func() { dropE2EDatabase(c, context.Background(), adminDB, name) })

	sourceURL := replaceDatabaseName(c, adminURL, name)
	db, err := sql.Open("pgx", sourceURL)
	c.Assert(err, qt.IsNil)
	defer db.Close()

	seed := []string{
		"CREATE SCHEMA one",
		"CREATE SCHEMA two",
		"CREATE TABLE one.alpha (id integer PRIMARY KEY, note text)",
		"CREATE TABLE two.beta (id integer PRIMARY KEY, tag text)",
		"CREATE TABLE public.pubtbl (id integer PRIMARY KEY)",
	}
	for _, statement := range seed {
		_, err := db.ExecContext(ctx, statement)
		c.Assert(err, qt.IsNil, qt.Commentf("seed: %s", statement))
	}

	// Read the catalog back rather than trusting that the statements returned
	// without error: every expectation in this file is a statement about which
	// of these three schemas reached the output, and a fixture missing one of
	// them would make the restricted rows pass for the wrong reason.
	var seeded int
	err = db.QueryRowContext(ctx,
		"SELECT count(*) FROM information_schema.tables WHERE table_schema IN ('one','two','public')",
	).Scan(&seeded)
	c.Assert(err, qt.IsNil)
	c.Assert(seeded, qt.Equals, 3)

	return sourceURL
}

// writeAtlasCompatEnvSchemasConfig writes an atlas.hcl carrying one env block
// and returns its path. An empty attribute writes an env block without one.
func writeAtlasCompatEnvSchemasConfig(c *qt.C, sourceURL, attribute string) string {
	c.Helper()

	body := fmt.Sprintf("env \"local\" {\n  url = %q\n", sourceURL)
	body += strings.TrimSuffix("  "+attribute+"\n", "  \n")
	body += "}\n"

	path := filepath.Join(c.TempDir(), "atlas.hcl")
	c.Assert(os.WriteFile(path, []byte(body), 0o600), qt.IsNil)
	return path
}

// runAtlasCompatEnvSchemasInspect runs `schema inspect` against the project
// file and returns the rendered document.
//
// Roles are excluded for the reason [runAtlasCompatInspect] gives: PostgreSQL
// roles are cluster-scoped, so every role any database on this server ever
// created would otherwise ride along and drown the schema blocks this test
// counts.
func runAtlasCompatEnvSchemasInspect(c *qt.C, configPath string, flagSchemas []string) string {
	c.Helper()

	args := []string{
		"schema", "inspect",
		"-c", "file://" + configPath,
		"--env", "local",
		"--exclude", "*[type=role]",
	}
	for _, schema := range flagSchemas {
		args = append(args, "--schema", schema)
	}

	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	c.Assert(cmd.Execute(), qt.IsNil, qt.Commentf("stderr: %s", errOut.String()))

	return out.String()
}

var atlasCompatSchemaBlockPattern = regexp.MustCompile(`(?m)^schema "([^"]+)" \{`)

// atlasCompatRenderedSchemaNames returns the schema names a rendered document
// declares, sorted, so a row's expectation is about the SET and not about the
// order the reader happened to walk.
func atlasCompatRenderedSchemaNames(rendered string) []string {
	matches := atlasCompatSchemaBlockPattern.FindAllStringSubmatch(rendered, -1)
	names := make([]string, 0, len(matches))
	for _, match := range matches {
		names = append(names, match[1])
	}
	slices.Sort(names)
	return names
}
