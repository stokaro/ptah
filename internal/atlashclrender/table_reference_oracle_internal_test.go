package atlashclrender

// White-box testing required: this run reuses the oracle harness the other two
// conformance runs in this package define -- oracleVersion, requireTypeOracle,
// requireDevURL, schemaNameByDialect and the warmup that absorbs the binary's
// one-off notice. A private copy would drift from the pinned version constant,
// and the whole value of these runs is that they measure the SAME build.

import (
	"maps"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

// TestOracleReadsTheReferenceSpellingPtahRenders measures the rendered document
// itself, not a hand-written fixture, so the thing under measurement is the
// output a user gets.
//
// Both spellings are put to the binary in the same run, off the same document,
// with only the reference text differing. The refusal has to be the attribute
// error and not merely a non-zero status: a document rejected for some unrelated
// reason would otherwise look like confirmation and this run would keep passing
// after it stopped measuring anything.
//
// Three positions ride on one document -- a foreign key's `ref_columns`, a
// trigger's `on`, and a permission's `for`. `policy.on` is left out because a
// PostgreSQL policy block never gets past that binary's own feature gap, and
// `data.table` because it refuses Ptah's unlabeled data block on its shape
// (`data block "data" must have exactly 2 labels`) before evaluating anything
// in it; neither can answer a question about a reference.
//
// SQLite needs no server and produces the identical pair of verdicts, so this
// run measures something even where no dev database is configured.
func TestOracleReadsTheReferenceSpellingPtahRenders(t *testing.T) {
	oracle := requireTypeOracle(t)

	for _, dialect := range slices.Sorted(maps.Keys(schemaNameByDialect)) {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			devURL := requireDevURL(t, dialect)
			schema := schemaNameByDialect[dialect]

			rendered, err := RenderForDialect(referenceOracleSchema(schema), dialect)
			c.Assert(err, qt.IsNil)
			short := string(rendered.Data)
			c.Assert(short, qt.Contains, "ref_columns = [table.users.column.id]")
			c.Assert(short, qt.Contains, "on = table.users")
			c.Assert(short, qt.Contains, "for = table.users")

			t.Run("rendered", func(t *testing.T) {
				c := qt.New(t)

				out, code := runReferenceOracle(c, oracle, devURL, short)

				c.Assert(code, qt.Equals, 0,
					qt.Commentf("the binary refuses the document Ptah renders on %s: %s\n%s", dialect, out, short))
			})

			t.Run("qualified", func(t *testing.T) {
				c := qt.New(t)
				// The spelling Ptah wrote before stokaro/ptah#1260, produced from
				// the accepted document so that nothing else differs between the
				// two runs.
				long := strings.ReplaceAll(short, "table.users", "table."+schema+".users")
				c.Assert(long, qt.Not(qt.Equals), short,
					qt.Commentf("the qualified variant is identical to the accepted one, so this row measures nothing"))

				out, code := runReferenceOracle(c, oracle, devURL, long)

				c.Assert(code, qt.Not(qt.Equals), 0,
					qt.Commentf("the binary now reads `table.%s.users` on %s; the short form is no longer required and this rule can go: %s",
						schema, dialect, out))
				c.Assert(out, qt.Contains, `does not have an attribute named "`+schema+`"`,
					qt.Commentf("the qualified document is refused for a reason that is not the reference, so this row is not measuring the spelling: %s", out))
			})
		})
	}
}

// referenceOracleSchema is the smallest IR carrying every reference position
// this run measures.
//
// It declares nothing the binary refuses as a feature -- no extension, sequence
// or policy -- so a non-zero status is attributable to a reference and not to a
// construct that build does not model.
func referenceOracleSchema(schema string) *goschema.Database {
	return &goschema.Database{
		Schemas: []goschema.Schema{{Name: schema}},
		Tables: []goschema.Table{
			{StructName: "User", Name: "users", Schema: schema},
			{StructName: "Post", Name: "posts", Schema: schema},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "id", Type: "integer", Primary: true},
			{
				StructName:     "Post",
				Name:           "author_id",
				Type:           "integer",
				Foreign:        schema + ".users(id)",
				ForeignKeyName: "posts_author_fk",
			},
		},
		Roles: []goschema.Role{{Name: "probe_role"}},
		Triggers: []goschema.Trigger{{
			Name:    "users_touch",
			Table:   schema + ".users",
			Timing:  "BEFORE",
			Event:   "UPDATE",
			ForEach: "ROW",
			Body:    "SELECT 1",
		}},
		Grants: []goschema.Grant{{
			Role:       "probe_role",
			Privileges: []string{"SELECT"},
			OnTable:    schema + ".users",
		}},
	}
}

// runReferenceOracle asks the pinned binary to read one document and returns its
// combined output and exit status.
func runReferenceOracle(c *qt.C, oracle, devURL, source string) (string, int) {
	c.Helper()

	typeOracleWarmup.Do(func() {})

	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

	//nolint:gosec // operator-provided oracle path, and path is a test temp dir
	cmd := exec.Command(oracle, "schema", "inspect", "-u", "file://"+path, "--dev-url", devURL)
	// The error is the exit status, which is the measurement; a process that
	// never started leaves ProcessState nil and fails the assertion instead.
	out, _ := cmd.CombinedOutput() //nolint:errcheck // exit status is read from ProcessState below
	c.Assert(cmd.ProcessState, qt.IsNotNil, qt.Commentf("the oracle did not run: %s", out))
	return string(out), cmd.ProcessState.ExitCode()
}
