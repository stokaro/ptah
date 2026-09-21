package migrationlintgate_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migrationlintgate"
	"ptah.run/migration/lint"
)

// The apply path is where a policy's server-version finally has a dialect to
// be resolved against: the configuration may leave the dialect out, and the
// connection reported it. Without the resolution a value that names no server
// is refused by `migrations lint` and accepted in silence by `migrations up`,
// on the same policy file (stokaro/ptah#3420).
func TestLoadPolicy_ServerVersion_HappyPath(t *testing.T) {
	t.Run("a version the connected dialect recognizes", func(t *testing.T) {
		c := qt.New(t)
		fsys := fstest.MapFS{
			lint.ConfigFileName: {Data: []byte("server-version: \"16\"\n")},
		}

		policy, err := migrationlintgate.LoadPolicy(fsys, "postgres")

		c.Assert(err, qt.IsNil)
		c.Assert(policy.BlockingFamilies(), qt.Contains, "DS")
	})

	t.Run("no version at all", func(t *testing.T) {
		c := qt.New(t)
		fsys := fstest.MapFS{
			lint.ConfigFileName: {Data: []byte("dialect: postgres\n")},
		}

		_, err := migrationlintgate.LoadPolicy(fsys, "postgres")

		c.Assert(err, qt.IsNil)
	})
}

func TestLoadPolicy_ServerVersion_FailurePath(t *testing.T) {
	t.Run("a version that names no server", func(t *testing.T) {
		c := qt.New(t)
		fsys := fstest.MapFS{
			lint.ConfigFileName: {Data: []byte("server-version: seventeen\n")},
		}

		_, err := migrationlintgate.LoadPolicy(fsys, "postgres")

		c.Assert(err, qt.ErrorMatches, `"seventeen" is not a recognized postgres server version.*`)
	})

	t.Run("a version naming another product than the connection", func(t *testing.T) {
		c := qt.New(t)
		fsys := fstest.MapFS{
			lint.ConfigFileName: {Data: []byte("server-version: 10.11.6-MariaDB\n")},
		}

		_, err := migrationlintgate.LoadPolicy(fsys, "postgres")

		c.Assert(err, qt.ErrorMatches, `.*mariadb.*`)
	})
}

// The apply-time gate runs on engines the linter has no rules for: the DS
// family is dialect-independent and protects an Oracle apply the same way it
// protects every other. Refusing the policy there would fail every Oracle
// apply before planning (stokaro/ptah#3420).
func TestLoadPolicy_UnsupportedLintDialect_HappyPath(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {Data: []byte("disabled-rules:\n  - MF103\n")},
	}

	policy, err := migrationlintgate.LoadPolicy(fsys, "oracle")

	c.Assert(err, qt.IsNil)
	c.Assert(policy.BlockingFamilies(), qt.Contains, "DS")
}

// A version declared for such a connection cannot be honored, so it is refused
// rather than accepted and ignored.
func TestLoadPolicy_UnsupportedLintDialect_FailurePath(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {Data: []byte("server-version: \"23\"\n")},
	}

	_, err := migrationlintgate.LoadPolicy(fsys, "oracle")

	c.Assert(err, qt.ErrorMatches,
		`lint server-version "23" cannot be resolved against database dialect "oracle".*`)
}

// The gate is what an Oracle apply actually runs, and it reaches further than
// LoadPolicy: AnalyzeWithPolicy hands the policy's target to AnalyzeFS, which
// resolves a default of its own when none was given. A refusal there fails
// every Oracle apply just as surely (stokaro/ptah#3420).
func TestAnalyze_UnsupportedLintDialect_HappyPath(t *testing.T) {
	c := qt.New(t)
	fsys := fstest.MapFS{
		lint.ConfigFileName: {Data: []byte("disabled-rules:\n  - MF103\n")},
		"0000000001_drop_column.up.sql": {
			Data: []byte("ALTER TABLE users DROP COLUMN legacy;\n"),
		},
		"0000000001_drop_column.down.sql": {
			Data: []byte("ALTER TABLE users ADD COLUMN legacy TEXT;\n"),
		},
	}

	findings, err := migrationlintgate.Analyze(fsys, []int64{1}, "oracle", "")

	c.Assert(err, qt.IsNil)
	// The dialect-independent data-safety rule still protects the apply.
	c.Assert(findings, qt.Not(qt.HasLen), 0)
}
