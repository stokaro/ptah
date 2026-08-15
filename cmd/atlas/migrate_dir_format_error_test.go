package atlas_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/atlasmigrate"
)

// unknownDirFormatStderr is the whole of what the pinned community binary
// v1.3.0 writes when a migration directory names a layout it does not know.
// Measured on 2026-08-12, exit code read from an unpiped invocation, on every
// verb and spelling the table below covers:
//
//	$ atlas migrate <verb> --dir 'file://migrations?format=bogus' …
//	$ atlas migrate <verb> --dir file://migrations --dir-format bogus …
//	exit 1, stdout 0 bytes, stderr 34 bytes:
//	Error: unknown dir format "bogus"
const unknownDirFormatStderr = "Error: unknown dir format \"bogus\"\n"

// unknownDirFormatSemantics is the diagnostic Ptah resolves internally. It must
// stay reachable through the error chain: the compatibility layer adapts what
// is DISPLAYED and discards nothing, which is what keeps this a display
// adapter rather than a loss of information (compatibility policy (c)).
func unknownDirFormatSemantics(verb, spelling string) string {
	return "atlas migrate " + verb + " " + spelling +
		`: unknown Atlas migration directory format "bogus":` +
		" expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate"
}

// dirFormatFixture is an existing migration directory plus the extra sources
// the writing verbs need, so a row fails on the format value and on nothing
// else.
type dirFormatFixture struct {
	dir      string
	toFile   string
	dbURL    string
	devURL   string
	emptyDir string
}

func newDirFormatFixture(tb testing.TB) dirFormatFixture {
	c := qt.New(tb)
	c.Helper()
	root := c.TempDir()
	dir := filepath.Join(root, "migrations")
	c.Assert(os.MkdirAll(dir, 0o755), qt.IsNil)
	c.Assert(os.WriteFile(
		filepath.Join(dir, "20240101000000_init.sql"),
		[]byte("CREATE TABLE users (id integer PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	toFile := filepath.Join(root, "schema.sql")
	c.Assert(os.WriteFile(
		toFile,
		[]byte("CREATE TABLE users (id integer PRIMARY KEY);\n"),
		0o600,
	), qt.IsNil)
	return dirFormatFixture{
		dir:      dir,
		toFile:   toFile,
		dbURL:    "sqlite://" + filepath.Join(root, "app.db") + "?_fk=1",
		devURL:   "sqlite://dev?mode=memory&_fk=1",
		emptyDir: filepath.Join(root, "target"),
	}
}

// TestCompatUnknownDirFormatIsTheSameStringOnEveryVerb pins stokaro/ptah#1235
// cell 9.8 as the CLASS it is.
//
// `unknown dir format "bogus"` is the pinned community binary v1.3.0's answer
// on every path where it reaches migration-directory layout resolution. Ptah
// gave it on two of them, because the adaptation was a block inside the
// `migrate hash` / `migrate validate` wrapper rather than a property of the
// refusal. Every other verb printed its own semantic wording, three to five
// times longer, prefixed with the command and the flag.
//
// There is one row per VERB and per SPELLING rather than a single row over a
// shared helper, and that is the point of the table. A shared adapter makes
// the class fix cheap; it does not make it durable. A later change that stops
// routing ONE call site through [atlasDirFormatError] leaves the helper intact
// and every other verb passing, so a test that only exercised the helper would
// stay green while a verb silently regressed. These rows fail one at a time.
//
// `migrate hash` could not be measured by a direct shell oracle invocation in
// this sandbox. The two rows below drive the same command tree in process, and
// the tagged integration contour independently drives the built process.
func TestCompatUnknownDirFormatIsTheSameStringOnEveryVerb(t *testing.T) {
	tests := []struct {
		verb string
		// spelling is the flag the semantic chain blames, which is `--dir`
		// when the value rode in on the URL query and `--dir-format`
		// otherwise. The DISPLAYED text names neither, on either binary.
		spelling string
		args     func(fx dirFormatFixture) []string
	}{
		{
			verb:     "new",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "new", "demo", "--dir", "file://" + fx.dir + "?format=bogus"}
			},
		},
		{
			verb:     "new",
			spelling: "--dir-format",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "new", "demo", "--dir", "file://" + fx.dir, "--dir-format", "bogus"}
			},
		},
		{
			verb:     "hash",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "hash", "--dir", "file://" + fx.dir + "?format=bogus"}
			},
		},
		{
			verb:     "hash",
			spelling: "--dir-format",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "hash", "--dir", "file://" + fx.dir, "--dir-format", "bogus"}
			},
		},
		{
			verb:     "validate",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "validate", "--dir", "file://" + fx.dir + "?format=bogus"}
			},
		},
		{
			verb:     "validate",
			spelling: "--dir-format",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "validate", "--dir", "file://" + fx.dir, "--dir-format", "bogus"}
			},
		},
		{
			verb:     "lint",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "lint",
					"--dir", "file://" + fx.dir + "?format=bogus",
					"--dev-url", fx.devURL, "--latest", "1",
				}
			},
		},
		{
			verb:     "lint",
			spelling: "--dir-format",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "lint",
					"--dir", "file://" + fx.dir, "--dir-format", "bogus",
					"--dev-url", fx.devURL, "--latest", "1",
				}
			},
		},
		{
			verb:     "status",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "status", "--dir", "file://" + fx.dir + "?format=bogus", "--url", fx.dbURL}
			},
		},
		{
			verb:     "status",
			spelling: "--dir-format",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "status",
					"--dir", "file://" + fx.dir, "--dir-format", "bogus", "--url", fx.dbURL,
				}
			},
		},
		{
			verb:     "set",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "set", "20240101000000",
					"--dir", "file://" + fx.dir + "?format=bogus", "--url", fx.dbURL,
				}
			},
		},
		{
			verb:     "set",
			spelling: "--dir-format",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "set", "20240101000000",
					"--dir", "file://" + fx.dir, "--dir-format", "bogus", "--url", fx.dbURL,
				}
			},
		},
		{
			verb:     "diff",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "diff", "demo",
					"--dir", "file://" + fx.dir + "?format=bogus",
					"--dev-url", fx.devURL, "--to", "file://" + fx.toFile,
				}
			},
		},
		{
			verb:     "diff",
			spelling: "--dir-format",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "diff", "demo",
					"--dir", "file://" + fx.dir, "--dir-format", "bogus",
					"--dev-url", fx.devURL, "--to", "file://" + fx.toFile,
				}
			},
		},
		{
			// `migrate apply` registers no --dir-format on either binary, so
			// the query is its only spelling. Measured: both binaries answer
			// `unknown flag: --dir-format` when it is passed there.
			verb:     "apply",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "apply", "--dir", "file://" + fx.dir + "?format=bogus", "--url", fx.dbURL}
			},
		},
		{
			verb:     "import",
			spelling: "--dir",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "import",
					"--from", "file://" + fx.dir + "?format=bogus",
					"--to", "file://" + fx.emptyDir,
				}
			},
		},
		{
			verb:     "import",
			spelling: "--dir-format",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "import",
					"--from", "file://" + fx.dir,
					"--to", "file://" + fx.emptyDir,
					"--dir-format", "bogus",
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.verb+" "+tt.spelling, func(t *testing.T) {
			c := qt.New(t)
			fx := newDirFormatFixture(c.TB)
			var unknownFormat *atlasmigrate.UnknownDirFormatError

			stdout, stderr, err := runCompatExit(tt.args(fx)...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, unknownDirFormatStderr)
			c.Assert(err.Error(), qt.Equals, `unknown dir format "bogus"`)
			c.Assert(err, qt.ErrorAs, &unknownFormat)
			c.Assert(unknownFormat.Value, qt.Equals, "bogus")
			c.Assert(
				errorChainContainsText(err, unknownDirFormatSemantics(tt.verb, tt.spelling)),
				qt.IsTrue,
				qt.Commentf("semantic chain lost for %s %s", tt.verb, tt.spelling),
			)
		})
	}
}

// TestCompatUnknownDirFormatAdapterLeavesOtherRefusalsAlone is the control that
// keeps the adapter from becoming "rewrite whatever this verb was about to
// say". Each row is a refusal from the same resolution step, on the same
// flags, that is NOT a rejected format value; each must keep its own text.
//
// The rows are not symmetric with the table above on purpose: a control that
// only ran on one verb would pass while a per-verb adaptation grew too wide
// somewhere else.
func TestCompatUnknownDirFormatAdapterLeavesOtherRefusalsAlone(t *testing.T) {
	tests := []struct {
		name string
		want string
		args func(fx dirFormatFixture) []string
	}{
		{
			name: "malformed query on hash",
			want: "atlas migrate hash --dir: parse migration directory URL query:" +
				" invalid semicolon separator in query",
			args: func(fx dirFormatFixture) []string {
				return []string{"migrate", "hash", "--dir", "file://" + fx.dir + "?format=flyway;x=1"}
			},
		},
		{
			name: "malformed query on lint",
			want: "atlas migrate lint --dir: parse migration directory URL query:" +
				" invalid semicolon separator in query",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "lint",
					"--dir", "file://" + fx.dir + "?format=flyway;x=1",
					"--dev-url", fx.devURL, "--latest", "1",
				}
			},
		},
		{
			name: "remote source URL on import",
			want: "import --from: only local file:// migration directories are supported",
			args: func(fx dirFormatFixture) []string {
				return []string{
					"migrate", "import",
					"--from", "atlas://repo/migrations?format=flyway",
					"--to", "file://" + fx.emptyDir,
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			fx := newDirFormatFixture(c.TB)
			var unknownFormat *atlasmigrate.UnknownDirFormatError

			stdout, stderr, err := runCompatExit(tt.args(fx)...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(err.Error(), qt.Equals, tt.want)
			c.Assert(stderr, qt.Equals, "Error: "+tt.want+"\n")
			c.Assert(err, qt.Not(qt.ErrorAs), &unknownFormat)
		})
	}
}
