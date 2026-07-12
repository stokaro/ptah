package seeder

import (
	"errors"
	"io/fs"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"
)

func TestDiscoverSortsSeedFiles(t *testing.T) {
	c := qt.New(t)

	seeds, err := Discover(fstest.MapFS{
		"020_states.test.sql":        {Data: []byte("INSERT INTO states VALUES (1);")},
		"010_countries.all.sql":      {Data: []byte("INSERT INTO countries VALUES (1);")},
		"030_cities.dev.sql":         {Data: []byte("INSERT INTO cities VALUES (1);")},
		"README.md":                  {Data: []byte("ignored")},
		"020_states.dev.sql":         {Data: []byte("INSERT INTO states VALUES (2);")},
		"nested/015_regions.all.sql": {Data: []byte("INSERT INTO regions VALUES (1);")},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(seedNames(seeds), qt.DeepEquals, []string{
		"010_countries.all.sql",
		"015_regions.all.sql",
		"020_states.dev.sql",
		"020_states.test.sql",
		"030_cities.dev.sql",
	})
	c.Assert(seeds[0].Checksum, qt.Not(qt.Equals), "")
}

func TestDiscoverRejectsInvalidSQLFilename(t *testing.T) {
	c := qt.New(t)

	_, err := Discover(fstest.MapFS{
		"010_countries.sql": {Data: []byte("INSERT INTO countries VALUES (1);")},
	})

	c.Assert(err, qt.ErrorMatches, `scan seeds: invalid seed filename "010_countries.sql": expected NNN_description.env.sql`)
}

func TestDiscoverKeepsRelativePathIdentity(t *testing.T) {
	c := qt.New(t)

	seeds, err := Discover(fstest.MapFS{
		"countries/010_reference.test.sql": {Data: []byte("INSERT INTO countries VALUES (1);")},
		"regions/010_reference.test.sql":   {Data: []byte("INSERT INTO regions VALUES (1);")},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(seedPaths(seeds), qt.DeepEquals, []string{
		"countries/010_reference.test.sql",
		"regions/010_reference.test.sql",
	})
	c.Assert(seedNames(seeds), qt.DeepEquals, []string{
		"010_reference.test.sql",
		"010_reference.test.sql",
	})
}

func TestSelectIncludesAllAndRequestedEnv(t *testing.T) {
	c := qt.New(t)

	seeds := []SeedFile{
		{Filename: "010_common.all.sql", Env: "all"},
		{Filename: "020_dev.dev.sql", Env: "dev"},
		{Filename: "020_test.test.sql", Env: "test"},
	}

	selected := Select(seeds, "DEV")

	c.Assert(seedNames(selected), qt.DeepEquals, []string{"010_common.all.sql", "020_dev.dev.sql"})
}

func TestValidateOptionsRequiresAllowProdForProtectedEnv(t *testing.T) {
	c := qt.New(t)

	err := ValidateOptions(Options{Env: "prod", ProtectedEnvs: DefaultProtectedEnvs()})
	c.Assert(err, qt.ErrorMatches, `refusing to seed protected environment "prod" without --allow-prod`)

	err = ValidateOptions(Options{Env: "prod", ProtectedEnvs: DefaultProtectedEnvs(), AllowProd: true})
	c.Assert(err, qt.IsNil)
}

func TestDiscoverReturnsReadErrors(t *testing.T) {
	c := qt.New(t)

	_, err := Discover(errorFS{})

	c.Assert(err, qt.ErrorMatches, `scan seeds: boom`)
}

func TestIsConflictErrorMatchesPortableMessages(t *testing.T) {
	c := qt.New(t)

	c.Assert(IsConflictError(errors.New(`SQL execution failed: ERROR: duplicate key value violates unique constraint "users_email_key"`)), qt.IsTrue)
	c.Assert(IsConflictError(errors.New(`Error 1062: Duplicate entry 'a@example.com' for key 'users.email'`)), qt.IsTrue)
	c.Assert(IsConflictError(errors.New("syntax error near INSERT")), qt.IsFalse)
}

func seedNames(seeds []SeedFile) []string {
	names := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		names = append(names, seed.Filename)
	}
	return names
}

func seedPaths(seeds []SeedFile) []string {
	paths := make([]string, 0, len(seeds))
	for _, seed := range seeds {
		paths = append(paths, seed.Path)
	}
	return paths
}

type errorFS struct{}

func (errorFS) Open(string) (fs.File, error) {
	return nil, errors.New("boom")
}
