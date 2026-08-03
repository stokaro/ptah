package atlasreport

// White-box testing required: these tests pin deterministic report rendering
// through writeMigrateLintText's injected clock and the Atlas-measured wrapping
// boundary. Neither implementation detail belongs in the exported API, so
// these assertions cannot be expressed through WriteMigrateLintText alone.

import (
	"bytes"
	"cmp"
	"slices"
	"strings"
	"testing"
	"testing/fstest"
	"time"

	qt "github.com/frankban/quicktest"

	migrationlint "go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

func fixedZeroClock() func() time.Time {
	return func() time.Time { return time.Unix(0, 0) }
}

func TestWrapContent_AtlasWidthBoundary(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		text string
		want []string
	}{
		{
			name: "88 content columns remain on one line",
			text: strings.Repeat("x", 88),
			want: []string{strings.Repeat("x", 88)},
		},
		{
			name: "word reaching column 89 wraps",
			text: strings.Repeat("x", 87) + " y",
			want: []string{strings.Repeat("x", 87), "y"},
		},
		{
			name: "word reaching column 90 wraps",
			text: strings.Repeat("x", 87) + " yy",
			want: []string{strings.Repeat("x", 87), "yy"},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(wrapContent(test.text, lintWrapWidth), qt.DeepEquals, test.want)
		})
	}
}

// analyzeMigrations builds a lint analysis for the given migration files,
// selecting the latest N versions exactly as the migrate lint command does.
func analyzeMigrations(t *testing.T, files map[string]string, latest int) migrationlint.Analysis {
	t.Helper()
	fsys := fstest.MapFS{}
	for name, body := range files {
		fsys[name] = &fstest.MapFile{Data: []byte(body)}
	}
	discovery, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{DirFormat: migrator.MigrationDirFormatAtlas})
	qt.Assert(t, err, qt.IsNil)
	seen := map[int64]struct{}{}
	for _, file := range discovery.Files() {
		if file.Direction == "up" && !file.Repeatable && file.Version > 0 {
			seen[file.Version] = struct{}{}
		}
	}
	versions := make([]int64, 0, len(seen))
	for version := range seen {
		versions = append(versions, version)
	}
	slices.SortFunc(versions, func(a, b int64) int { return cmp.Compare(b, a) })
	if latest < len(versions) {
		versions = versions[:latest]
	}
	slices.Sort(versions)

	analysis, err := migrationlint.AnalyzeFS(fsys, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Dialect:       "sqlite",
		Selection:     migrationlint.VersionSelection{Versions: versions, Restricted: true},
	})
	qt.Assert(t, err, qt.IsNil)
	return analysis
}

func TestWriteMigrateLintText_RendersAtlasDiagnostics(t *testing.T) {
	destructiveFiles := map[string]string{
		"1.sql": "CREATE TABLE users (id int);\n\nCREATE TABLE pets (id int);\n\nALTER TABLE users RENAME COLUMN id TO oid;\n",
		"2.sql": "DROP TABLE users;\n",
		"3.sql": "DROP TABLE pets;\n",
	}

	// Two destructive statements in ONE version, so both diagnostics land in a
	// single group. Every other destructive fixture here puts them in separate
	// versions, where "a fix after each diagnostic" and "fixes collected at the
	// end of the group" render identically -- so none of them can tell the two
	// layouts apart. Atlas collects them, and pluralizes the header.
	groupedDestructiveFiles := map[string]string{
		"1.sql": "CREATE TABLE users (id int);\n\nCREATE TABLE pets (id int);\n",
		"2.sql": "DROP TABLE users;\nDROP TABLE pets;\n",
	}

	tests := []struct {
		name   string
		files  map[string]string
		latest int
		want   string
	}{
		{
			name:   "two diagnostics in one group collect their fixes",
			files:  groupedDestructiveFiles,
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L2: Dropping table \"pets\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fixes:\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"pets\" is empty before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 2 diagnostics\n",
		},
		{
			name:   "destructive latest 1",
			files:  destructiveFiles,
			latest: 1,
			want: "Analyzing changes from version 2 to 3 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 3\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"pets\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure table \"pets\" is empty before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name:   "destructive latest 2",
			files:  destructiveFiles,
			latest: 2,
			want: "Analyzing changes from version 1 to 3 (2 migrations in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -- analyzing version 3\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"pets\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure table \"pets\" is empty before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 2 versions with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 2 diagnostics\n",
		},
		{
			name: "clean until header and ok summary",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\n",
			},
			latest: 1,
			want: "Analyzing changes until version 1 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 1\n" +
				"    -- no diagnostics found\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version ok\n" +
				"  -- 1 schema change\n",
		},
		{
			name: "clean version then destructive error",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\n",
				"2.sql": "DROP TABLE users;\n",
			},
			latest: 2,
			want: "Analyzing changes until version 2 (2 migrations in total):\n" +
				"\n" +
				"  -- analyzing version 1\n" +
				"    -- no diagnostics found\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version ok, 1 with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name: "data dependent warning wraps message",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\nCREATE TABLE pets (id int);\n",
				"2.sql": "ALTER TABLE users ADD COLUMN name text NOT NULL;\n-- atlas:nolint DS102\nDROP TABLE pets;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L1: Adding a non-nullable \"text\" column \"name\" will fail in case table \"users\" is not\n" +
				"         empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with warnings\n" +
				"  -- 2 schema changes\n" +
				"  -- 1 diagnostic\n",
		},
		{
			// Version 1 adds a NOT NULL column to a table created in the same file
			// (exempt). Version 2 adds one to a pre-existing table (MF103) and one
			// with a DEFAULT (no report).
			name: "add-notnull reports the unsafe change",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\n\n/* Adding a not-null column without default to a table created in this file should not report. */\nALTER TABLE users ADD COLUMN c1 int NOT NULL;\n",
				"2.sql": "ALTER TABLE users ADD COLUMN c2 int NOT NULL;\n\nALTER TABLE users ADD COLUMN c3 int NOT NULL DEFAULT 1;\n",
			},
			latest: 2,
			want: "Analyzing changes until version 2 (2 migrations in total):\n" +
				"\n" +
				"  -- analyzing version 1\n" +
				"    -- no diagnostics found\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L1: Adding a non-nullable \"int\" column \"c2\" will fail in case table \"users\" is not empty\n" +
				"         https://atlasgo.io/lint/analyzers#MF103\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version ok, 1 with warnings\n" +
				"  -- 4 schema changes\n" +
				"  -- 1 diagnostic\n",
		},
		{
			name: "inline suppressed diagnostics report ok",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\nCREATE TABLE pets (id int);\n",
				"2.sql": "\n-- atlas:nolint\nALTER TABLE users ADD COLUMN name text NOT NULL;\n\n-- atlas:nolint\nDROP TABLE pets;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- no diagnostics found\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version ok\n" +
				"  -- 2 schema changes\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			analysis := analyzeMigrations(t, tc.files, tc.latest)
			var out bytes.Buffer

			err := writeMigrateLintText(&out, MigrateLintOptions{Analysis: &analysis}, fixedZeroClock())

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Equals, tc.want)
		})
	}
}

func TestWriteMigrateLintText_NilAnalysisErrors(t *testing.T) {
	c := qt.New(t)
	var out bytes.Buffer

	err := writeMigrateLintText(&out, MigrateLintOptions{}, fixedZeroClock())

	c.Assert(err, qt.IsNotNil)
	c.Assert(out.String(), qt.Equals, "")
}

// TestMigrateLintProseIsAtlasCompatible pins the ptah-compat boundary rather
// than only one snapshot. Native `ptah migrations lint` owns Ptah's richer
// prose; this renderer must remain suitable for drop-in Atlas scripts.
func TestMigrateLintProseIsAtlasCompatible(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeMigrations(t, map[string]string{
		"1.sql": "CREATE TABLE pets (id int);\n",
		"2.sql": "DROP TABLE pets;\n",
	}, 1)
	var out bytes.Buffer

	err := writeMigrateLintText(&out, MigrateLintOptions{Analysis: &analysis}, fixedZeroClock())
	c.Assert(err, qt.IsNil)
	rendered := out.String()

	c.Assert(rendered, qt.Contains, `L1: Dropping table "pets"`)
	c.Assert(rendered, qt.Contains, "https://atlasgo.io/lint/analyzers#DS102")
	c.Assert(rendered, qt.Contains, "-- suggested fix:")
	c.Assert(rendered, qt.Contains, `ensure table "pets" is empty before dropping it`)
	c.Assert(rendered, qt.Not(qt.Contains), "L1 [DS102]")
	c.Assert(rendered, qt.Not(qt.Contains), "rename-and-retire window")
}
