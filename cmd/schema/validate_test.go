package schema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// `schema validate` exists so a pre-commit hook can use the status alone. The
// exit code therefore carries three distinct answers, and these tests hold each
// one apart from the others (stokaro/ptah#1711).

// validateExitCode runs the verb and returns its output and process exit code,
// resolving an ordinary error to the usage code the root command would use.
func validateExitCode(c *qt.C, args ...string) (string, int) {
	c.Helper()
	out, err := runSchema("", append([]string{"validate"}, args...)...)
	if err == nil {
		return out, 0
	}
	return out, exitcode.Code(err, 2)
}

func TestSchemaValidateCleanSchemaExitsZeroAndPrintsNothing(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER);\nCREATE INDEX i ON orders (total);\n")

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres")

	c.Assert(code, qt.Equals, 0)
	c.Assert(out, qt.Equals, "")
}

func TestSchemaValidateReportsEveryProblemAndExitsOne(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER);\n"+
			"CREATE INDEX a ON orders (missing_one);\n"+
			"CREATE INDEX b ON orders (missing_two);\n")

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres")

	c.Assert(code, qt.Equals, 1)
	c.Assert(out, qt.Contains, `index "a": names column "missing_one"`)
	c.Assert(out, qt.Contains, `index "b": names column "missing_two"`)
	c.Assert(out, qt.Contains, "2 structural problems")
}

// TestSchemaValidateSeparatesProblemsFromUsageErrors is the point of the exit
// codes: a caller that cannot tell "the schema is wrong" from "you passed a bad
// flag" cannot use the status alone.
func TestSchemaValidateSeparatesProblemsFromUsageErrors(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY);\nCREATE INDEX a ON orders (missing);\n")

	_, problems := validateExitCode(c, "--schema-file", path, "--dialect", "postgres")
	_, noDialect := validateExitCode(c, "--schema-file", path)
	_, noSource := validateExitCode(c, "--dialect", "postgres")

	c.Assert(problems, qt.Equals, 1)
	c.Assert(noDialect, qt.Equals, 2)
	c.Assert(noSource, qt.Equals, 2)
}

// TestSchemaValidateReportsPerDialect holds the reason --dialect is repeatable:
// each target gets its own answer, under its own name.
func TestSchemaValidateReportsPerDialect(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY);\nCREATE INDEX a ON orders (missing);\n")

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres", "--dialect", "mysql")

	c.Assert(code, qt.Equals, 1)
	c.Assert(out, qt.Contains, "postgres: index")
	c.Assert(out, qt.Contains, "mysql: index")
	c.Assert(out, qt.Contains, "2 structural problems")
}

// TestSchemaValidateSingleProblemReadsAsOne guards the summary's grammar: a
// count rendered straight into "%d structural problems" says "1 problems".
func TestSchemaValidateSingleProblemReadsAsOne(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY);\nCREATE INDEX a ON orders (missing);\n")

	out, _ := validateExitCode(c, "--schema-file", path, "--dialect", "postgres")

	c.Assert(out, qt.Contains, "1 structural problem\n")
	c.Assert(out, qt.Not(qt.Contains), "1 structural problems")
}

// TestSchemaValidateReportsAnUnreadableSourceRatherThanPassing is the control
// for the loader: a source that cannot be read is not a clean schema.
func TestSchemaValidateReportsAnUnreadableSourceRatherThanPassing(t *testing.T) {
	c := qt.New(t)

	out, code := validateExitCode(c, "--schema-file", "/nonexistent/schema.sql", "--dialect", "postgres")

	c.Assert(code, qt.Equals, 1)
	c.Assert(out, qt.Contains, "postgres: source:")
}

// TestSchemaValidateAcceptsAFunctionalIndex is the fixture the package-level
// expression test could not stand in for. A declaration that fills Parts fills
// Fields from the same loop, and for an expression key it puts the whole
// expression in Fields: `CREATE INDEX i ON orders (lower(total))` parses to
// Fields ["lower(total)"] and one Part carrying the same string as Expr.
// Reading both spellings reported a valid functional index as a column no
// table declares.
func TestSchemaValidateAcceptsAFunctionalIndex(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER);\n"+
			"CREATE INDEX i_expr ON orders (lower(total));\n")

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres")

	c.Assert(code, qt.Equals, 0)
	c.Assert(out, qt.Equals, "")
}

// TestSchemaValidateStillCatchesAPlainMissingColumn is the paired control: an
// index with no expression keys carries its columns in Fields, and preferring
// Parts must not stop that path being checked.
func TestSchemaValidateStillCatchesAPlainMissingColumn(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER);\n"+
			"CREATE INDEX i_plain ON orders (missing);\n")

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres")

	c.Assert(code, qt.Equals, 1)
	c.Assert(out, qt.Contains, `names column "missing"`)
}

// TestSchemaValidateAcceptsAPinnedServerVersion holds the flag's normal path:
// --dialect selects a capability preset and --server-version refines it, the
// same contract the other offline verbs carry.
func TestSchemaValidateAcceptsAPinnedServerVersion(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER);\nCREATE INDEX i ON orders (total);\n")

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres", "--server-version", "16.2")

	c.Assert(code, qt.Equals, 0)
	c.Assert(out, qt.Equals, "")
}

// TestSchemaValidateRefusesAVersionThatNamesNoServer is why the pin goes
// through the resolver: answering an unreadable version with the dialect
// default would report a clean schema against a server nobody asked about.
func TestSchemaValidateRefusesAVersionThatNamesNoServer(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, code := validateExitCode(c, "--schema-file", path, "--dialect", "postgres", "--server-version", "not-a-version")

	c.Assert(code, qt.Equals, 2)
	c.Assert(out, qt.Contains, "--server-version")
}

// TestSchemaValidateRefusesAPinAcrossSeveralTargets holds the other half of the
// contract: one server version cannot describe two dialects, so pinning while
// naming several is a usage error rather than a version silently applied to
// whichever target happened to be first.
func TestSchemaValidateRefusesAPinAcrossSeveralTargets(t *testing.T) {
	c := qt.New(t)
	path := writeSchemaSQLFile(c, t.TempDir(), "schema.sql",
		"CREATE TABLE orders (id INTEGER PRIMARY KEY);\n")

	out, code := validateExitCode(c,
		"--schema-file", path, "--dialect", "postgres", "--dialect", "mysql", "--server-version", "16.2")

	c.Assert(code, qt.Equals, 2)
	c.Assert(out, qt.Contains, "one server version cannot describe all of them")
}
