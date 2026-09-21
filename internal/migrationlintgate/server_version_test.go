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
