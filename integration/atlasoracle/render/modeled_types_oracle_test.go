//go:build integration

package render_test

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/internal/atlashclrender"
)

// oracleEnv names the environment variable holding the path to the pinned
// Atlas CE binary this conformance run compares against.
const oracleEnv = "PTAH_ATLAS_ORACLE"

// oracleVersion is the only build this conformance run trusts. A different
// build may model a different set of types, which is the very thing under
// measurement here, so comparing against it would report drift that is really
// version drift.
const oracleVersion = "atlas community version v1.3.0"

// devURLEnvByDialect names the environment variable carrying each dialect's dev
// database URL.
//
// SQLite needs none: in-memory keeps the common case self-contained. PostgreSQL
// cannot be faked, because the binary resolves a type against the dev database
// it is given -- that is precisely why sql("hstore") is refused there while
// sql("timestamp with time zone") is accepted, and a run without a real server
// would measure neither.
var devURLEnvByDialect = map[string]string{
	platform.Postgres: "PTAH_ATLAS_ORACLE_POSTGRES_DEV_URL",
}

// defaultDevURLByDialect is the dev URL for a dialect that needs no server.
var defaultDevURLByDialect = map[string]string{
	platform.SQLite: "sqlite://file?mode=memory",
}

// schemaNameByDialect is the schema an inspected database of that dialect puts
// its tables in, and therefore the one these probes declare.
var schemaNameByDialect = map[string]string{
	platform.SQLite:   "main",
	platform.Postgres: "public",
}

// unmodeledControls are type names deliberately ABSENT from each dialect's
// modeled set, which the pinned binary must refuse when they are written bare.
//
// Without them this run could pass with an empty map, or with a map the binary
// happens to accept every name in for an unrelated reason. They are what make
// "this list is a real boundary" a measurement rather than a claim.
//
// `timestamp` on SQLite is the sharpest of them: an ordinary type name Ptah
// models, which that binary does not model for that dialect. It is why the
// modeled set cannot be derived from Ptah's own vocabulary.
var unmodeledControls = map[string][]string{
	platform.SQLite: {
		"timestamp",
		"bytea",
		"inet",
		"USER_DEFINED",
	},
	platform.Postgres: {
		"hstore",
		"citext",
		"ltree",
		"USER_DEFINED",
	},
}

// TestOracleModeledColumnTypesMatchTheBinary re-measures every entry of
// modeledColumnTypes against the pinned community binary.
//
// The asymmetry this guards is recorded on the map itself: a list that is too
// SHORT costs a needless sql() wrap, which round trips, while a list that is too
// LONG emits HCL the binary refuses to read at all. So each entry is asserted in
// the direction that can break drop-in -- written bare, it must be accepted --
// and the controls are asserted in the other, so an entry silently added without
// measurement is caught by the run rather than by a user.
//
// The probes deliberately do NOT run in parallel, and adding t.Parallel() back
// would be worse than slow. Every probe of a dialect shares one dev database,
// and the binary materializes the schema there to resolve a type, so concurrent
// probes collide inside the server rather than in this process. Measured: with
// the subtests parallel, 34 of the 57 PostgreSQL rows failed with
// `pq: duplicate key value violates unique constraint "pg_type_typname_nsp_index"`
// -- and every one of them would have been reported as "the binary refuses
// `type = uuid`", a parity finding about a type the binary accepts perfectly
// well when asked on its own.
func TestOracleModeledColumnTypesMatchTheBinary(t *testing.T) {
	oracle := requireTypeOracle(t)
	c := qt.New(t)
	c.Assert(sortedDialects(), qt.DeepEquals, []string{platform.Postgres, platform.SQLite},
		qt.Commentf("every measured dialect must remain in the oracle matrix"))

	for _, dialect := range sortedDialects() {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			devURL := requireDevURL(t, dialect)

			modeled := atlashclrender.ModeledColumnTypes(dialect)
			c.Assert(modeled, qt.Not(qt.HasLen), 0,
				qt.Commentf("a dialect present in the map with no entries would make this run measure nothing"))

			for _, name := range modeled {
				t.Run("modeled/"+name, func(t *testing.T) {
					c := qt.New(t)

					out, code := runTypeOracle(c, oracle, devURL, dialect, name)
					c.Assert(code, qt.Equals, 0,
						qt.Commentf("the binary refuses `type = %s` on %s, so rendering it bare emits unreadable HCL: %s",
							name, dialect, out))
				})
			}

			for _, name := range unmodeledControls[dialect] {
				t.Run("control/"+name, func(t *testing.T) {
					c := qt.New(t)

					c.Assert(atlashclrender.IsModeledColumnType(dialect, name), qt.IsFalse,
						qt.Commentf("%q is a control: it must stay absent from the modeled set", name))

					out, code := runTypeOracle(c, oracle, devURL, dialect, name)
					c.Assert(code, qt.Not(qt.Equals), 0,
						qt.Commentf("the binary now accepts `type = %s` on %s; the control has stopped controlling anything: %s",
							name, dialect, out))
				})
			}
		})
	}
}

// TestOracleAcceptsWrappedTypeExpressions measures the fallback the renderer
// reaches for whenever a type is not modeled, which is what makes a short list
// safe.
//
// TestOracleRefusesUnsupportedTypeExpressions keeps this honest in the other
// direction: wrapping rescues a type the HCL schema does not MODEL, but does
// nothing for a type the dev database does not HAVE.
func TestOracleAcceptsWrappedTypeExpressions(t *testing.T) {
	oracle := requireTypeOracle(t)
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		expr    string
	}{
		{
			name:    "sqlite wraps a type it does not model",
			dialect: platform.SQLite,
			expr:    `sql("timestamp")`,
		},
		{
			name:    "sqlite wrapping a modeled type is still readable",
			dialect: platform.SQLite,
			expr:    `sql("integer")`,
		},
		{
			name:    "postgres wraps the catalog's own multi-word spelling",
			dialect: platform.Postgres,
			expr:    `sql("timestamp with time zone")`,
		},
		{
			name:    "postgres wraps an array, which the catalog reports as a category",
			dialect: platform.Postgres,
			expr:    `sql("character varying(100)[]")`,
		},
		// The array rows below are the accepted half of the measurement
		// isArrayColumnType rests on. The refusal test pins the quoted and bare
		// alternatives that Ptah emitted before stokaro/ptah#1138.
		{
			name:    "postgres wraps a sized array whose element name is modeled",
			dialect: platform.Postgres,
			expr:    `sql("numeric(10,2)[]")`,
		},
		{
			name:    "postgres wraps a bit array",
			dialect: platform.Postgres,
			expr:    `sql("bit(8)[]")`,
		},
		{
			name:    "postgres wraps a character array",
			dialect: platform.Postgres,
			expr:    `sql("character(5)[]")`,
		},
		{
			name:    "postgres wraps a sized multi-word array",
			dialect: platform.Postgres,
			expr:    `sql("timestamp(3) with time zone[]")`,
		},
		{
			name:    "postgres wrapping a modeled type is still readable",
			dialect: platform.Postgres,
			expr:    `sql("integer")`,
		},
	}
	c.Assert(tests, qt.HasLen, 9,
		qt.Commentf("all accepted fallback controls must remain in the oracle matrix"))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			devURL := requireDevURL(t, test.dialect)

			out, code := runTypeOracle(c, oracle, devURL, test.dialect, test.expr)
			c.Assert(code, qt.Equals, 0,
				qt.Commentf("`type = %s` on %s exited %d: %s", test.expr, test.dialect, code, out))
		})
	}
}

// TestOracleRefusesUnsupportedTypeExpressions measures the unsafe alternatives
// to the sql() fallback and its limit for a type absent from the dev database.
func TestOracleRefusesUnsupportedTypeExpressions(t *testing.T) {
	oracle := requireTypeOracle(t)
	c := qt.New(t)

	tests := []struct {
		name    string
		dialect string
		expr    string
	}{
		{
			name:    "postgres refuses a sized array quoted",
			dialect: platform.Postgres,
			expr:    `"numeric(10,2)[]"`,
		},
		{
			name:    "postgres refuses a bit array quoted",
			dialect: platform.Postgres,
			expr:    `"bit(8)[]"`,
		},
		{
			name:    "postgres refuses a character array quoted",
			dialect: platform.Postgres,
			expr:    `"character(5)[]"`,
		},
		{
			name:    "postgres refuses a sized multi-word array quoted",
			dialect: platform.Postgres,
			expr:    `"timestamp(3) with time zone[]"`,
		},
		{
			// An array is not one HCL expression when written bare.
			name:    "postgres refuses an array written bare",
			dialect: platform.Postgres,
			expr:    `text[]`,
		},
		{
			// The binary evaluates the body against the dev database, so a type
			// no engine there has is refused wrapped exactly as it is bare.
			name:    "postgres wrapping does not conjure an absent extension type",
			dialect: platform.Postgres,
			expr:    `sql("hstore")`,
		},
	}
	c.Assert(tests, qt.HasLen, 6,
		qt.Commentf("all refusal controls must remain in the oracle matrix"))

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			devURL := requireDevURL(t, test.dialect)

			out, code := runTypeOracle(c, oracle, devURL, test.dialect, test.expr)
			c.Assert(code, qt.Not(qt.Equals), 0,
				qt.Commentf("`type = %s` on %s exited %d: %s", test.expr, test.dialect, code, out))
		})
	}
}

// sortedDialects returns the dialects under measurement in a stable order, so a
// failing subtest names the same thing on every run.
func sortedDialects() []string {
	return atlashclrender.ModeledColumnTypeDialects()
}

func requireTypeOracle(t *testing.T) string {
	t.Helper()

	oracle := os.Getenv(oracleEnv)
	if oracle == "" {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s) to run the column-type conformance run",
			oracleEnv, oracleVersion)
	}

	out, err := exec.Command(oracle, "version").Output() // #nosec G204 G702 -- the oracle path is operator-provided via PTAH_ATLAS_ORACLE
	if err != nil {
		t.Fatalf("%s=%s is not runnable: %v", oracleEnv, oracle, err)
	}
	got, _, _ := strings.Cut(string(out), "\n")
	if strings.TrimSpace(got) != oracleVersion {
		t.Fatalf("%s=%s reports %q, want %q; a different build may model a different set of types",
			oracleEnv, oracle, strings.TrimSpace(got), oracleVersion)
	}
	return oracle
}

// requireDevURL returns the dev database URL for a dialect, skipping when a
// dialect that needs a server was not given one.
//
// The skip is deliberately loud, and the Atlas CE Oracle job fails on any
// SKIPPED line: a dialect measured against nothing is the failure mode this run
// exists to prevent, not an acceptable partial result.
func requireDevURL(t *testing.T, dialect string) string {
	t.Helper()

	if url, ok := defaultDevURLByDialect[dialect]; ok {
		return url
	}
	env, ok := devURLEnvByDialect[dialect]
	if !ok {
		t.Fatalf("dialect %q is in the modeled map with no dev URL source; add one before adding the dialect", dialect)
	}
	url := os.Getenv(env)
	if url == "" {
		t.Skipf("SKIPPED: set %s to a %s dev database to measure its modeled types", env, dialect)
	}
	return url
}

// typeOracleWarmup absorbs a once-per-environment notice before the first
// measurement.
//
// The binary prints an edition notice on its first `schema inspect` in a fresh
// environment and on that run only. Nothing here compares output text, so the
// notice cannot corrupt a verdict -- but it is spent on a throwaway anyway, so
// the first subtest is not the one paying for it in an otherwise parallel run.
var typeOracleWarmup sync.Once

func runTypeOracle(c *qt.C, oracle, devURL, dialect, typeExpression string) (string, int) {
	c.Helper()

	schema := schemaNameByDialect[dialect]
	c.Assert(schema, qt.Not(qt.Equals), "",
		qt.Commentf("dialect %q has no schema name; add one before adding the dialect", dialect))

	source := fmt.Sprintf(`schema %q {
}
table "probe" {
  schema = schema.%s
  column "c" {
    type = %s
  }
}
`, schema, schema, typeExpression)

	typeOracleWarmup.Do(func() {
		path := filepath.Join(c.TempDir(), "warmup.hcl")
		//nolint:errcheck // this run exists only to spend the notice
		_ = os.WriteFile(path, []byte(source), 0o600)
		// #nosec G204 -- operator-provided oracle path, and path is a test temp dir
		cmd := exec.Command(oracle, "schema", "inspect", "-u", "file://"+path, "--dev-url", devURL)
		//nolint:errcheck // output and status are both discarded
		_, _ = cmd.CombinedOutput()
	})

	path := filepath.Join(c.TempDir(), "probe.hcl")
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

	// #nosec G204 -- operator-provided oracle path, and path is a test temp dir
	cmd := exec.Command(oracle, "schema", "inspect", "-u", "file://"+path, "--dev-url", devURL)
	// The error is the exit status, which is the measurement; a process that
	// never started leaves ProcessState nil and fails the assertion instead.
	out, _ := cmd.CombinedOutput() //nolint:errcheck // exit status is read from ProcessState below
	c.Assert(cmd.ProcessState, qt.IsNotNil, qt.Commentf("the oracle did not run: %s", out))
	return string(out), cmd.ProcessState.ExitCode()
}
