package lint_test

import (
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/migration/lint"
)

func TestResolveTarget_HappyPath(t *testing.T) {
	t.Run("a measured release line resolves without a note", func(t *testing.T) {
		c := qt.New(t)

		target, err := lint.ResolveTarget("postgres", "16")

		c.Assert(err, qt.IsNil)
		c.Assert(target.Named(), qt.IsTrue)
		c.Assert(target.Version, qt.Equals, "16")
		c.Assert(target.Note, qt.Equals, "")
		c.Assert(target.Capabilities.Has(capability.AlterGeneratedColumnExpression), qt.IsFalse)
	})

	t.Run("a newer release line answers differently", func(t *testing.T) {
		c := qt.New(t)

		target, err := lint.ResolveTarget("postgres", "18")

		c.Assert(err, qt.IsNil)
		c.Assert(target.Capabilities.Has(capability.NamedNotNullConstraints), qt.IsTrue)
	})

	// A run that named no server is not a mistake, so it resolves to the
	// dialect default rather than to an empty set a rule would have to tell
	// apart from a server that lacks everything.
	t.Run("no version resolves to the dialect default", func(t *testing.T) {
		c := qt.New(t)

		target, err := lint.ResolveTarget("postgres", "")

		c.Assert(err, qt.IsNil)
		c.Assert(target.Named(), qt.IsFalse)
		c.Assert(target.Capabilities, qt.DeepEquals, capability.ForDialect("postgres"))
	})

	// A version Ptah recognizes but has not measured plans against something
	// the operator did not name, and saying nothing there is how such a run
	// reads as a run against the server they asked for.
	t.Run("a version past the measured lines carries a note", func(t *testing.T) {
		c := qt.New(t)

		target, err := lint.ResolveTarget("postgres", "99")

		c.Assert(err, qt.IsNil)
		c.Assert(target.Named(), qt.IsTrue)
		c.Assert(target.Note, qt.Matches, `postgres 99 is newer than the newest measured release line.*`)
	})
}

func TestResolveTarget_FailurePath(t *testing.T) {
	t.Run("a value that names no server", func(t *testing.T) {
		c := qt.New(t)

		target, err := lint.ResolveTarget("postgres", "seventeen")

		c.Assert(err, qt.ErrorMatches, `"seventeen" is not a recognized postgres server version.*`)
		c.Assert(target, qt.DeepEquals, lint.Target{})
	})

	t.Run("a version naming another product", func(t *testing.T) {
		c := qt.New(t)

		target, err := lint.ResolveTarget("postgres", "10.11.6-MariaDB")

		c.Assert(err, qt.ErrorMatches, `.*mariadb.*`)
		c.Assert(target, qt.DeepEquals, lint.Target{})
	})

	t.Run("a version with no dialect", func(t *testing.T) {
		c := qt.New(t)

		target, err := lint.ResolveTarget("", "17")

		c.Assert(err, qt.ErrorMatches,
			`server version "17" needs a dialect: with none, every rule runs and there is no single target the version could describe`)
		c.Assert(target, qt.DeepEquals, lint.Target{})
	})
}

// versionedRule is the shape a rule takes once it can read the server: it asks
// the capability set what this release line does, never the version string.
// NamedNotNullConstraints is false on PostgreSQL 17 and true on 18, so the same
// statement is a finding on one and silence on the other.
func versionedRule() lint.Rule {
	return lint.Rule{
		Code:     "ZZ101",
		Title:    "named NOT NULL constraints",
		Severity: lint.SeverityWarning,
		CheckStatement: func(stmt *lint.Statement) (bool, string) {
			if !stmt.Target.Capabilities.Has(capability.NamedNotNullConstraints) {
				return false, ""
			}
			return true, "this server records a NOT NULL constraint under a name"
		},
	}
}

func versionedRuleFS() fstest.MapFS {
	return fstest.MapFS{
		"0000000001_users.up.sql":   {Data: []byte("CREATE TABLE users (id BIGINT PRIMARY KEY);\n")},
		"0000000001_users.down.sql": {Data: []byte("DROP TABLE users;\n")},
	}
}

// The path the server version travels: an option, an analysis, every statement,
// and a rule that reads it. Without it the rule answers the same on every
// release line, which is the state this closes (stokaro/ptah#3420).
func TestAnalyzeFS_CarriesTheTargetToARule(t *testing.T) {
	t.Run("the newer release line fires the rule", func(t *testing.T) {
		c := qt.New(t)
		target, err := lint.ResolveTarget("postgres", "18")
		c.Assert(err, qt.IsNil)

		analysis, err := lint.AnalyzeFS(versionedRuleFS(), lint.Options{
			Dialect:    "postgres",
			Target:     target,
			ExtraRules: []lint.Rule{versionedRule()},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(analysis.Findings(), qt.HasLen, 1)
		c.Assert(analysis.Target().Version, qt.Equals, "18")
	})

	t.Run("the older release line does not", func(t *testing.T) {
		c := qt.New(t)
		target, err := lint.ResolveTarget("postgres", "17")
		c.Assert(err, qt.IsNil)

		analysis, err := lint.AnalyzeFS(versionedRuleFS(), lint.Options{
			Dialect:    "postgres",
			Target:     target,
			ExtraRules: []lint.Rule{versionedRule()},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(analysis.Findings(), qt.HasLen, 0)
	})

	t.Run("a run that named no server plans against the dialect default", func(t *testing.T) {
		c := qt.New(t)

		analysis, err := lint.AnalyzeFS(versionedRuleFS(), lint.Options{
			Dialect:    "postgres",
			ExtraRules: []lint.Rule{versionedRule()},
		})

		c.Assert(err, qt.IsNil)
		c.Assert(analysis.Findings(), qt.HasLen, 0)
		c.Assert(analysis.Target().Named(), qt.IsFalse)
		c.Assert(analysis.Target().Capabilities, qt.DeepEquals, capability.ForDialect("postgres"))
	})
}

// The configuration key is the declaration a project checks in, so what it
// accepts and what --server-version accepts have to be the same set.
func TestLoadConfigFS_ServerVersion_HappyPath(t *testing.T) {
	t.Run("a version beside its dialect", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := lint.LoadConfigFS(fstest.MapFS{
			".ptah-lint.yaml": {Data: []byte("dialect: postgres\nserver-version: \"16\"\n")},
		}, ".ptah-lint.yaml")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ServerVersion, qt.Equals, "16")
	})

	// A policy may pin the version and leave the dialect to the command line
	// or to a dev database. Refusing the pair here would refuse a
	// configuration that is complete by the time it is used.
	t.Run("a version with the dialect left to the caller", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := lint.LoadConfigFS(fstest.MapFS{
			".ptah-lint.yaml": {Data: []byte("server-version: \"16\"\n")},
		}, ".ptah-lint.yaml")

		c.Assert(err, qt.IsNil)
		c.Assert(cfg.ServerVersion, qt.Equals, "16")
		c.Assert(cfg.Dialect, qt.Equals, "")
	})
}

func TestLoadConfigFS_ServerVersion_FailurePath(t *testing.T) {
	t.Run("a version that names no server", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := lint.LoadConfigFS(fstest.MapFS{
			".ptah-lint.yaml": {Data: []byte("dialect: postgres\nserver-version: seventeen\n")},
		}, ".ptah-lint.yaml")

		c.Assert(err, qt.ErrorMatches, `(?s)failed to parse lint config .*is not a recognized postgres server version.*`)
		c.Assert(cfg, qt.IsNil)
	})

	t.Run("a version naming another product", func(t *testing.T) {
		c := qt.New(t)

		cfg, err := lint.LoadConfigFS(fstest.MapFS{
			".ptah-lint.yaml": {Data: []byte("dialect: postgres\nserver-version: 10.11.6-MariaDB\n")},
		}, ".ptah-lint.yaml")

		c.Assert(err, qt.ErrorMatches, `(?s)failed to parse lint config .*mariadb.*`)
		c.Assert(cfg, qt.IsNil)
	})
}
