//go:build integration

package schemahcl_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/internal/atlashcl"
)

// oracleEnv names the environment variable holding the path to the pinned
// Atlas CE binary this conformance run compares against.
const oracleEnv = "PTAH_ATLAS_ORACLE"

// oracleVersion is the only build this conformance run trusts. A different
// build may have changed the very rules under test, so comparing against it
// would report divergences that are really version drift.
const oracleVersion = "atlas community version v1.3.0"

// oracleDevURL is the dev database the oracle evaluates a `file://` schema
// source on. In-memory SQLite keeps the gate self-contained: no container, no
// credentials, nothing to leave running. The verdicts below were cross-checked
// against a PostgreSQL 18 dev database and a MySQL 9.7 dev database and are
// identical on all three, so none of them is a dialect artifact -- except the
// `partition` row, which is dialect-split and is recorded as such rather than
// pinned to one verdict.
const oracleDevURL = "sqlite://file?mode=memory"

// TestOracleAcceptsAndDropsUnknownSchemaHCLNames is the conformance half of
// stokaro/ptah#1016's definition of done: every position where Ptah drops an
// unmodeled name is re-measured against the pinned community binary here,
// rather than frozen in a comment nothing can invalidate.
//
// Each case asserts three things, and the second is the one that carries the
// weight: the oracle exits 0, the DDL it emits is byte-identical to the DDL of
// the same schema with the construct deleted (so the name was dropped and not
// implemented), and Ptah's tolerant parse produces exactly the IR of that same
// deleted-construct file.
//
// The nonsense controls matter more than the real names: they behave
// identically to `annotation` and `invisible` in every position, which is what
// makes this a general "drop names I do not model" policy rather than support
// for any particular name.
func TestOracleAcceptsAndDropsUnknownSchemaHCLNames(t *testing.T) {
	oracle := requireSchemaOracle(t)
	c := qt.New(t)

	tests := []struct {
		name       string
		hcl        string
		equivalent string
	}{
		{
			name: "top-level annotation block",
			hcl: `schema "main" {
}
annotation "gql" {
  attr "name" {
    type = string
  }
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "top-level nonsense block control",
			hcl: `schema "main" {
}
frobnicate_nonsense "zz" {
  anything = "here"
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "invisible column attribute",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  column "secret" {
    type      = int
    invisible = true
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  column "secret" {
    type = int
  }
}
`,
		},
		{
			name: "nonsense column attribute control",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type              = int
    zzz_nonsense_attr = true
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "nonsense block nested in a column",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
    zzz_nonsense_block {
      anything = "here"
    }
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "nonsense index attribute",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  index "i" {
    columns           = [column.id]
    zzz_nonsense_attr = true
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  index "i" {
    columns = [column.id]
  }
}
`,
		},
		{
			name: "nonsense block nested in an index",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  index "i" {
    columns = [column.id]
    zzz_nonsense_block {
      anything = "here"
    }
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  index "i" {
    columns = [column.id]
  }
}
`,
		},
		{
			name: "nonsense schema attribute",
			hcl: `schema "main" {
  zzz_nonsense_attr = true
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "nonsense primary_key attribute",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns           = [column.id]
    zzz_nonsense_attr = true
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
}
`,
		},
		{
			name: "type keyword inside the dropped block",
			hcl: `schema "main" {
}
annotation "gql" {
  attr "name" {
    type = string
  }
  ref = int
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
			equivalent: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			withDDL, withCode := runSchemaOracle(c.TB, oracle, tt.hcl)
			withoutDDL, withoutCode := runSchemaOracle(c.TB, oracle, tt.equivalent)
			c.Assert(withCode, qt.Equals, 0, qt.Commentf("oracle output: %s", withDDL))
			c.Assert(withoutCode, qt.Equals, 0, qt.Commentf("oracle output: %s", withoutDDL))
			c.Assert(withDDL, qt.Equals, withoutDDL)

			tolerant, err := atlashcl.ParseWithOptions(
				[]byte(tt.hcl),
				"schema.hcl",
				atlashcl.Options{IgnoreUnknownNames: true},
			)
			c.Assert(err, qt.IsNil)
			without, err := atlashcl.Parse([]byte(tt.equivalent), "schema.hcl")
			c.Assert(err, qt.IsNil)
			c.Assert(tolerant, qt.DeepEquals, without)
		})
	}
}

// TestOracleRefusesUnevaluableDroppedBodies is the other half, and the half a
// tolerance change is most likely to break: every case here is a file the
// community binary REFUSES, and the tolerant parser must refuse it too.
//
// Without this, relaxing a name silently converts each row into an
// exit-0-where-the-oracle-exits-1 divergence -- the dangerous direction, and
// invisible to any test that only checks that valid files still parse.
//
// The rows are chosen so no single mechanism covers them. The first four fail
// on four different diagnostics (unknown variable, unknown function, invalid
// operand, unsupported attribute), so a check that only resolved reference
// roots would let three through. The rows from "unlabeled block nested in the
// dropped block" down are the class the earlier revision of this file could
// not see at all: every fixture it had used a LABELED reference root, so the
// wildcards that stood in for unlabeled blocks and for declared variables
// stayed invisible while accepting all twelve of these.
func TestOracleRefusesUnevaluableDroppedBodies(t *testing.T) {
	oracle := requireSchemaOracle(t)
	c := qt.New(t)

	tests := []struct {
		name string
		hcl  string
	}{
		{
			name: "unresolvable reference root",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = not_a_real_identifier
}
`,
		},
		{
			name: "variable is not the reference root, var is",
			hcl: `variable "v" {
  type    = string
  default = "x"
}
schema "main" {
}
annotation "gql" {
  ref = variable.v
}
`,
		},
		{
			name: "call to a function the oracle does not have",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = frobnicate_nonsense("a")
}
`,
		},
		{
			name: "operand of the wrong type",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = 1 + "abc"
}
`,
		},
		{
			name: "member of a real root that does not exist",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
annotation "gql" {
  ref = table.nope
}
`,
		},
		{
			name: "nested member of a real root that does not exist",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
annotation "gql" {
  ref = table.t.column.nope
}
`,
		},
		{
			name: "var member with no variable block declaring it",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = var.v
}
`,
		},
		{
			name: "scoped enum name is not a reference root",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = HASH
}
`,
		},
		{
			name: "column root is out of scope at the top level",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
annotation "gql" {
  ref = column.id
}
`,
		},
		{
			name: "unlabeled block nested in the dropped block",
			hcl: `schema "main" {
}
annotation "gql" {
  inner {
    a = 1
  }
  ref = inner.a
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "member that does not exist under an unlabeled nested block",
			hcl: `schema "main" {
}
annotation "gql" {
  inner {
    a = 1
  }
  ref = inner.typo
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "unlabeled block under a labeled block in the dropped body",
			hcl: `schema "main" {
}
annotation "gql" {
  attr "name" {
    nested {
      x = 1
    }
  }
  ref = attr.name.nested.typo
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "unlabeled top-level dropped block referenced by its own name",
			hcl: `schema "main" {
}
frobnicate_nonsense {
  a = 1
}
annotation "gql" {
  ref = frobnicate_nonsense.typo
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "index into an unlabeled nested block",
			hcl: `schema "main" {
}
annotation "gql" {
  inner {
    a = 1
  }
  ref = inner["typo"]
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "member that does not exist on a table-body primary_key",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
  annotation {
    ref = primary_key.nope
  }
}
`,
		},
		{
			name: "attribute of a table-body primary_key is not a member either",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  primary_key {
    columns = [column.id]
  }
  annotation {
    ref = primary_key.columns
  }
}
`,
		},
		{
			name: "member that does not exist on a table-body partition",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  partition {
    type    = "HASH"
    columns = [column.id]
  }
  annotation {
    ref = partition.typo
  }
}
`,
		},
		{
			name: "attribute access on a declared string variable",
			hcl: `variable "v" {
  type    = string
  default = "x"
}
schema "main" {
}
annotation "gql" {
  ref = var.v.nope
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "attribute access on a declared list variable",
			hcl: `variable "v" {
  type    = list(string)
  default = ["x"]
}
schema "main" {
}
annotation "gql" {
  ref = var.v.typo
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "arithmetic over a declared string variable",
			hcl: `variable "v" {
  type    = string
  default = "x"
}
schema "main" {
}
annotation "gql" {
  ref = 1 + var.v
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "reference that stops on a block type rather than a block",
			hcl: `schema "main" {
}
table "p" {
  schema = schema.main
  column "id" {
    type = int
  }
}
annotation "gql" {
  ref = table.p.column
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			out, code := runSchemaOracle(c.TB, oracle, tt.hcl)
			c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("oracle output: %s", out))

			_, err := atlashcl.ParseWithOptions(
				[]byte(tt.hcl),
				"schema.hcl",
				atlashcl.Options{IgnoreUnknownNames: true},
			)
			c.Assert(err, qt.IsNotNil, qt.Commentf("oracle refused this file: %s", out))
		})
	}
}

// TestOracleAcceptsReferencesPtahRefuses measures the divergence the closed
// scope buys, instead of asserting it against a frozen expectation.
//
// The oracle exits 0 on each of these and the tolerant parser refuses each of
// them, because resolving the reference would need a reference root built from
// the file's own blocks or variables -- and every attempt to build those roots
// so far has had to guess at some member it could not enumerate, turning into
// an accept-where-the-oracle-refuses divergence. Refusing is the direction that
// costs a user a message rather than a wrong schema.
//
// This test fails in BOTH directions, which is the point: if the oracle stops
// accepting one of these the row is no longer a divergence and should move, and
// if Ptah starts accepting one the trade recorded here has been reversed
// without being re-argued.
//
// The "declared variable" row was removed here rather than adjusted, and this
// is that argument. `annotation "gql" { ref = var.v }` beside a declared
// `variable "v"` was a divergence only because the dropped body's scope did not
// bind `var`; #926 gives it the same evaluation context the rest of the file
// uses, so the reference resolves. Measured on both, the row is no longer a
// divergence in either direction:
//
//	oracle:      exit 0, table "t" with column "id"
//	ptah-compat: exit 0, table "t" with column "id"
//
// The remaining rows keep the trade, and for the reason above: their roots are
// `column` and `table`, which need a reference root built from the file's own
// blocks — the thing every attempt so far has had to guess a member of.
func TestOracleAcceptsReferencesPtahRefuses(t *testing.T) {
	oracle := requireSchemaOracle(t)
	c := qt.New(t)

	tests := []struct {
		name string
		hcl  string
	}{
		{
			name: "column root inside a table body",
			hcl: `schema "main" {
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
  column "b" {
    type              = int
    zzz_nonsense_attr = column.id
  }
}
`,
		},
		{
			name: "table declared later in the file",
			hcl: `schema "main" {
}
annotation "gql" {
  ref = table.t
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
		{
			name: "block nested in the dropped block",
			hcl: `schema "main" {
}
annotation "gql" {
  attr "name" {
    type = string
  }
  ref = attr.name
}
table "t" {
  schema = schema.main
  column "id" {
    type = int
  }
}
`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			out, code := runSchemaOracle(c.TB, oracle, tt.hcl)
			c.Assert(code, qt.Equals, 0, qt.Commentf("oracle output: %s", out))

			_, err := atlashcl.ParseWithOptions(
				[]byte(tt.hcl),
				"schema.hcl",
				atlashcl.Options{IgnoreUnknownNames: true},
			)
			c.Assert(err, qt.ErrorMatches, `.*unknown variable .*`)
		})
	}
}

// TestOraclePartitionIsDialectSplit records the one row stokaro/ptah#1016 asked
// to pin as "not a parity gap" and that turns out not to have a single verdict.
//
// On the SQLite and PostgreSQL dev databases the community binary accepts the
// unquoted `type = HASH` form; on MySQL it exits 1 with `There is no variable
// named "HASH"`. A parser that never sees the dialect cannot match both, so the
// dialect-independent fact is pinned instead: `HASH` is not a reference root in
// a dropped body anywhere, which is the mechanism behind the MySQL refusal.
func TestOraclePartitionIsDialectSplit(t *testing.T) {
	oracle := requireSchemaOracle(t)
	c := qt.New(t)

	const bare = `schema "main" {
}
annotation "gql" {
  ref = HASH
}
`

	out, code := runSchemaOracle(c.TB, oracle, bare)
	c.Assert(code, qt.Not(qt.Equals), 0, qt.Commentf("oracle output: %s", out))
	c.Assert(out, qt.Contains, `There is no variable named "HASH"`)

	_, err := atlashcl.ParseWithOptions(
		[]byte(bare),
		"schema.hcl",
		atlashcl.Options{IgnoreUnknownNames: true},
	)
	c.Assert(err, qt.ErrorMatches, `.*unknown variable "HASH"`)
}

// requireSchemaOracle resolves the pinned binary, refusing to compare against
// any other build. The skip is deliberately loud: a silently absent oracle is
// the failure mode this whole file exists to avoid.
func requireSchemaOracle(t *testing.T) string {
	t.Helper()

	oracle := os.Getenv(oracleEnv)
	if oracle == "" {
		t.Skipf("SKIPPED: set %s to the pinned Atlas CE binary (%s) to run the schema-HCL conformance run",
			oracleEnv, oracleVersion)
	}

	out, err := exec.Command(oracle, "version").Output() //nolint:gosec // the oracle path is operator-provided via PTAH_ATLAS_ORACLE
	if err != nil {
		t.Fatalf("%s=%s is not runnable: %v", oracleEnv, oracle, err)
	}
	got, _, _ := strings.Cut(string(out), "\n")
	if strings.TrimSpace(got) != oracleVersion {
		t.Fatalf("%s=%s reports %q, want %q; a different build may have changed the rules under test",
			oracleEnv, oracle, strings.TrimSpace(got), oracleVersion)
	}
	return oracle
}

// schemaOracleWarmup absorbs a once-per-environment notice before the first
// measurement.
//
// The binary prints an edition notice on its first `schema inspect` in a fresh
// environment -- a CI runner -- and on that run only. Every comparison here runs
// the oracle twice and asserts the two captures are equal, so a notice attached
// to the first of the pair but not the second makes a row unequal whose DDL is
// in fact identical. That is exactly how this failed on its first CI run: the
// first subtest failed and every later one passed, because the notice was spent
// on the first invocation of the job.
//
// A throwaway inspect is preferred over matching the notice by its wording. The
// text is not part of any contract the oracle owes us, so a matcher for it would
// rot silently the next time it is reworded, and this run would go green while
// comparing whatever the new text happened to be. `requireSchemaOracle` cannot
// serve as the warm-up either: it runs `version`, whose stdout it parses, and
// the notice does not appear there.
var schemaOracleWarmup sync.Once

func warmUpSchemaOracle(tb testing.TB, oracle string) {
	c := qt.New(tb)
	c.Helper()

	schemaOracleWarmup.Do(func() {
		path := filepath.Join(c.TempDir(), "warmup.hcl")
		//nolint:errcheck // best effort: a failed write still runs the oracle, which is all the warm-up needs
		_ = os.WriteFile(path, []byte("schema \"main\" {\n}\n"), 0o600)
		//nolint:gosec // operator-provided oracle path, and path is a test temp dir
		cmd := exec.Command(oracle, "schema", "inspect", "-u", "file://"+path, "--dev-url", oracleDevURL)
		//nolint:errcheck // output and status are both discarded; this run exists only to spend the notice
		_, _ = cmd.CombinedOutput()
	})
}

// runSchemaOracle inspects one schema source with the pinned binary and returns
// its combined output and exit code.
func runSchemaOracle(tb testing.TB, oracle, source string) (string, int) {
	c := qt.New(tb)
	c.Helper()

	warmUpSchemaOracle(c.TB, oracle)

	path := filepath.Join(c.TempDir(), "schema.hcl")
	c.Assert(os.WriteFile(path, []byte(source), 0o600), qt.IsNil)

	//nolint:gosec // operator-provided oracle path, and path is a test temp dir
	cmd := exec.Command(oracle, "schema", "inspect", "-u", "file://"+path, "--dev-url", oracleDevURL)
	// The error is the exit status, which is the measurement; a process that
	// never started leaves ProcessState nil and fails the assertion instead.
	out, _ := cmd.CombinedOutput() //nolint:errcheck // exit status is read from ProcessState below
	c.Assert(cmd.ProcessState, qt.IsNotNil, qt.Commentf("the oracle did not run: %s", out))
	return string(out), cmd.ProcessState.ExitCode()
}
