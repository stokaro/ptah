package dbcli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/config"
	"ptah.run/config/projectconfig"
	"ptah.run/internal/cli/internal/dbcli"
)

// ignoreExtensionCommand builds a command carrying the flag, and parses args
// through it so the "was this flag given" answer is the real one rather than a
// value set on a struct.
func ignoreExtensionCommand(c *qt.C, args ...string) (*cobra.Command, []string) {
	c.Helper()

	var target []string
	cmd := &cobra.Command{Use: "plan", RunE: func(*cobra.Command, []string) error { return nil }}
	dbcli.RegisterIgnoreExtensionFlag(cmd.Flags(), &target)
	cmd.SetArgs(args)
	cmd.SetOut(nil)
	cmd.SetErr(nil)
	c.Assert(cmd.Execute(), qt.IsNil)
	return cmd, target
}

// configIgnoring builds a project config that names extensions, through the
// merge the loaders use, so presence is set the way a parsed file sets it.
func configIgnoring(names ...string) projectconfig.Config {
	return projectconfig.Merge(projectconfig.Config{}, projectconfig.Config{IgnoredExtensions: names})
}

// TestCompareOptionsIgnoringExtensions_HappyPath covers the three sources a run
// can take its list from and the shape they produce together.
func TestCompareOptionsIgnoringExtensions_HappyPath(t *testing.T) {
	t.Run("the flag adds to the defaults", func(t *testing.T) {
		c := qt.New(t)
		cmd, target := ignoreExtensionCommand(c, "--ignore-extension", "pg_trgm")

		got := dbcli.CompareOptionsIgnoringExtensions(cmd, target, projectconfig.Config{}, nil)

		c.Assert(got.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql", "pg_trgm"})
	})

	t.Run("the flag is repeatable", func(t *testing.T) {
		c := qt.New(t)
		cmd, target := ignoreExtensionCommand(c,
			"--ignore-extension", "pg_trgm", "--ignore-extension", "vector")

		got := dbcli.CompareOptionsIgnoringExtensions(cmd, target, projectconfig.Config{}, nil)

		c.Assert(got.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql", "pg_trgm", "vector"})
	})

	t.Run("the project config is used when the flag is absent", func(t *testing.T) {
		c := qt.New(t)
		cmd, target := ignoreExtensionCommand(c)

		got := dbcli.CompareOptionsIgnoringExtensions(cmd, target, configIgnoring("postgis"), nil)

		c.Assert(got.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql", "postgis"})
	})

	t.Run("an explicit flag wins over the project config", func(t *testing.T) {
		c := qt.New(t)
		cmd, target := ignoreExtensionCommand(c, "--ignore-extension", "pg_trgm")

		got := dbcli.CompareOptionsIgnoringExtensions(cmd, target, configIgnoring("postgis"), nil)

		c.Assert(got.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql", "pg_trgm"})
	})

	t.Run("neither source leaves the defaults alone", func(t *testing.T) {
		c := qt.New(t)
		cmd, target := ignoreExtensionCommand(c)

		got := dbcli.CompareOptionsIgnoringExtensions(cmd, target, projectconfig.Config{}, nil)

		c.Assert(got.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql"})
	})

	t.Run("a name the defaults already carry is not repeated", func(t *testing.T) {
		c := qt.New(t)
		cmd, target := ignoreExtensionCommand(c, "--ignore-extension", "plpgsql")

		got := dbcli.CompareOptionsIgnoringExtensions(cmd, target, projectconfig.Config{}, nil)

		c.Assert(got.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql"})
	})

	t.Run("a blank entry names no extension", func(t *testing.T) {
		c := qt.New(t)
		cmd, target := ignoreExtensionCommand(c, "--ignore-extension", "  ")

		got := dbcli.CompareOptionsIgnoringExtensions(cmd, target, projectconfig.Config{}, nil)

		c.Assert(got.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql"})
	})
}

// TestCompareOptionsIgnoringExtensions_CarriesTheBase keeps the other options a
// caller already resolved -- the server-answered expression maps are the ones
// that cost a round trip -- and leaves the caller's value unchanged.
func TestCompareOptionsIgnoringExtensions_CarriesTheBase(t *testing.T) {
	c := qt.New(t)
	cmd, target := ignoreExtensionCommand(c, "--ignore-extension", "pg_trgm")
	base := config.DefaultCompareOptions()
	base.Dialect = "postgres"
	base.GeneratedExpressions = map[string]config.GeneratedExpression{
		"notes.slug": {Expression: "lower(body)", Resolved: true},
	}

	got := dbcli.CompareOptionsIgnoringExtensions(cmd, target, projectconfig.Config{}, base)

	c.Assert(got.Dialect, qt.Equals, "postgres")
	c.Assert(got.GeneratedExpressions, qt.DeepEquals, map[string]config.GeneratedExpression{
		"notes.slug": {Expression: "lower(body)", Resolved: true},
	})
	c.Assert(got.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql", "pg_trgm"})
	c.Assert(base.IgnoredExtensions, qt.DeepEquals, []string{"plpgsql"})
}
