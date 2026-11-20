package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// repeatableShape is what a rule reads about a file's kind.
type repeatableShape struct {
	Name            string
	Repeatable      bool
	IsUp            bool
	Version         int64
	RevisionVersion string
	Direction       string
	WellFormedName  bool
	Selected        bool
}

func repeatableShapes(files []lint.File) []repeatableShape {
	shapes := make([]repeatableShape, 0, len(files))
	for _, file := range files {
		shapes = append(shapes, repeatableShape{
			Name:            file.Name,
			Repeatable:      file.Repeatable,
			IsUp:            file.IsUp,
			Version:         file.Version,
			RevisionVersion: file.RevisionVersion,
			Direction:       file.Direction,
			WellFormedName:  file.WellFormedName,
			Selected:        file.Selected,
		})
	}
	return shapes
}

// repeatableDirectory is a Ptah directory with one versioned migration and two
// repeatable ones, one of them nested. The root-level one drops a table.
func repeatableDirectory() map[string]string {
	return map[string]string{
		"0000000001_widgets.up.sql":   "CREATE TABLE widgets (id INTEGER PRIMARY KEY);\n",
		"0000000001_widgets.down.sql": "SELECT 1;\n",
		"R__cleanup.sql":              "DROP TABLE widgets;\n",
		"views/R__names.sql":          "CREATE VIEW names AS SELECT 1 AS id;\n",
	}
}

func TestAnalyzeFS_RepeatableMigrations_HappyPath(t *testing.T) {
	t.Run("a repeatable migration is linted as an up migration with no down file", func(t *testing.T) {
		c := qt.New(t)
		analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(repeatableDirectory()), lint.Options{
			Dialect: "postgres",
		})
		c.Assert(err, qt.IsNil)
		c.Assert(findingSites(analysis.Findings()), qt.DeepEquals, []string{
			"R__cleanup.sql:1:destructive/drop-table",
			"R__cleanup.sql:1:rollout/drop-table",
		})
		c.Assert(repeatableShapes(analysis.Files()), qt.DeepEquals, []repeatableShape{
			{Name: "0000000001_widgets.down.sql", Version: 1, RevisionVersion: "1", Direction: "down", WellFormedName: true, Selected: true},
			{Name: "0000000001_widgets.up.sql", IsUp: true, Version: 1, RevisionVersion: "1", Direction: "up", WellFormedName: true, Selected: true},
			{Name: "R__cleanup.sql", Repeatable: true, IsUp: true, WellFormedName: true, Selected: true},
			{Name: "views/R__names.sql", Repeatable: true, IsUp: true, WellFormedName: true, Selected: true},
		})
	})

	t.Run("the Ptah format reads a directory that holds repeatable migrations only", func(t *testing.T) {
		c := qt.New(t)
		fsys := fixture(map[string]string{"R__cleanup.sql": "DROP TABLE widgets;\n"})
		findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{
			Dialect:   "postgres",
			DirFormat: migrationfile.DirFormatPtah,
		})
		c.Assert(err, qt.IsNil)
		c.Assert(findingSites(findings), qt.DeepEquals, []string{"R__cleanup.sql:1:destructive/drop-table", "R__cleanup.sql:1:rollout/drop-table"})
	})

	t.Run("a restricted selection selects a repeatable migration by its path", func(t *testing.T) {
		c := qt.New(t)
		analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(repeatableDirectory()), lint.Options{
			Dialect:   "postgres",
			Selection: lint.VersionSelection{Repeatables: []string{"R__cleanup.sql"}, Restricted: true},
		})
		c.Assert(err, qt.IsNil)
		c.Assert(selectedNames(analysis), qt.DeepEquals, []string{"R__cleanup.sql"})
		c.Assert(findingSites(analysis.Findings()), qt.DeepEquals, []string{"R__cleanup.sql:1:destructive/drop-table", "R__cleanup.sql:1:rollout/drop-table"})
	})

	t.Run("a selection by version selects no repeatable migration", func(t *testing.T) {
		c := qt.New(t)
		analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(repeatableDirectory()), lint.Options{
			Dialect:   "postgres",
			Selection: lint.VersionSelection{Versions: []int64{1}, Restricted: true},
		})
		c.Assert(err, qt.IsNil)
		c.Assert(selectedNames(analysis), qt.DeepEquals, []string{"0000000001_widgets.down.sql", "0000000001_widgets.up.sql"})
		c.Assert(analysis.Findings(), qt.HasLen, 0)
	})

	t.Run("the Atlas profile reads R__ as the Atlas version R", func(t *testing.T) {
		c := qt.New(t)
		analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(repeatableDirectory()), lint.Options{
			Compatibility:  lint.CompatibilityProfileAtlas,
			AtlasSelectors: atlasStub,
		})
		c.Assert(err, qt.IsNil)
		c.Assert(revisionVersionOf(analysis, "R__cleanup.sql"), qt.Equals, "R")
	})

	t.Run("an auto directory with no Ptah versioned file reads R__ as the Atlas version R", func(t *testing.T) {
		c := qt.New(t)
		fsys := fixture(map[string]string{"1_widgets.sql": "CREATE TABLE widgets (id INTEGER);\n", "R__cleanup.sql": "SELECT 1;\n"})
		analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{})
		c.Assert(err, qt.IsNil)
		c.Assert(revisionVersionOf(analysis, "R__cleanup.sql"), qt.Equals, "R")
	})

	t.Run("a directory that claims no repeatable migration keeps its findings when its files match no format", func(t *testing.T) {
		c := qt.New(t)
		fsys := fixture(map[string]string{"notes.sql": "SELECT 1;\n"})
		findings, err := lint.LintFS(c.Context(), builtintest.Runtime(), fsys, lint.Options{DirFormat: migrationfile.DirFormatPtah})
		c.Assert(err, qt.IsNil)
		c.Assert(findingSites(findings), qt.DeepEquals, []string{"notes.sql:0:file-form/file-name"})
	})
}

func TestAnalyzeFS_RepeatableMigrations_FailurePath(t *testing.T) {
	t.Run("a misspelled repeatable name is refused as the migrator refuses it", func(t *testing.T) {
		c := qt.New(t)
		files := repeatableDirectory()
		files["R__view.up.sql"] = "SELECT 1;\n"
		analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(files), lint.Options{})
		c.Assert(err, qt.ErrorMatches, `repeatable migration file "R__view.up.sql": .*`)
		c.Assert(analysis.Files(), qt.HasLen, 0)
	})

	t.Run("two identities that differ only in letter case are refused", func(t *testing.T) {
		c := qt.New(t)
		files := repeatableDirectory()
		files["R__Cleanup.sql"] = "SELECT 1;\n"
		analysis, err := lint.AnalyzeFS(c.Context(), builtintest.Runtime(), fixture(files), lint.Options{})
		c.Assert(err, qt.ErrorMatches, `repeatable migrations R__Cleanup.sql and R__cleanup.sql carry identities that differ only in letter case`)
		c.Assert(analysis.Files(), qt.HasLen, 0)
	})
}

func selectedNames(analysis lint.Analysis) []string {
	names := make([]string, 0)
	for _, file := range analysis.SelectedFiles() {
		names = append(names, file.Name)
	}
	return names
}

func revisionVersionOf(analysis lint.Analysis, name string) string {
	for _, file := range analysis.Files() {
		if file.Name == name {
			return file.RevisionVersion
		}
	}
	return ""
}
