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

func TestAtlasDiagnosticText_FallsBackForUnmeasuredSubjects(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		code    string
		subject migrationlint.Subject
	}{
		{
			name:    "three-part identifier",
			code:    "DS102",
			subject: migrationlint.Subject{Name: "database.schema.users"},
		},
		{
			name: "array suffix after parameterized type",
			code: "MF103",
			subject: migrationlint.Subject{
				Name:     "name",
				Parent:   "users",
				DataType: "VARCHAR(100)[]",
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			diagnostic, mapped := atlasDiagnosticText(test.code, migrationlint.Finding{
				Context: &migrationlint.FindingContext{Subjects: []migrationlint.Subject{test.subject}},
			})

			c.Assert(mapped, qt.IsFalse)
			c.Assert(diagnostic, qt.Equals, atlasDiagnosticCopy{})
		})
	}
}

// TestAtlasDiagnosticText_FallsBackForMultiSubjectSingleObjectCodes pins the
// fail-closed direction for the two codes that are one diagnostic per object.
// A finding carrying several objects is a shape this wording cannot render, and
// the safe answer is Ptah's own labeled prose naming all of them -- NOT the
// first subject. Rendering the first is precisely how the other dropped tables
// went missing: narrowing a destructive finding to one of its objects reads as
// complete output and is not.
func TestAtlasDiagnosticText_FallsBackForMultiSubjectSingleObjectCodes(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name     string
		code     string
		subjects []migrationlint.Subject
	}{
		{
			name: "two dropped tables in one finding",
			code: "DS102",
			subjects: []migrationlint.Subject{
				{Kind: migrationlint.SubjectTable, Name: "users"},
				{Kind: migrationlint.SubjectTable, Name: "audit_log"},
			},
		},
		{
			name: "two added non-nullable columns in one finding",
			code: "MF103",
			subjects: []migrationlint.Subject{
				{Kind: migrationlint.SubjectColumn, Name: "a", Parent: "t", DataType: "int"},
				{Kind: migrationlint.SubjectColumn, Name: "b", Parent: "t", DataType: "int"},
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			diagnostic, mapped := atlasDiagnosticText(test.code, migrationlint.Finding{
				Context: &migrationlint.FindingContext{Subjects: test.subjects},
			})

			c.Assert(mapped, qt.IsFalse)
			c.Assert(diagnostic, qt.Equals, atlasDiagnosticCopy{})
		})
	}
}

// analyzeMigrations builds a lint analysis for the given migration files,
// selecting the latest N versions exactly as the migrate lint command does.
func analyzeMigrations(t *testing.T, files map[string]string, latest int) migrationlint.Analysis {
	t.Helper()
	return analyzeMigrationsWithBaseline(t, files, latest, nil)
}

// analyzeMigrationsWithBaseline is [analyzeMigrations] for the diagnostics that
// need the schema state a version starts from -- the add side of a rename, whose
// column type and nullability live in an earlier file. A live run reads that
// state off the dev database mid-replay; here it is supplied directly so the
// rendering can be pinned without one.
func analyzeMigrationsWithBaseline(
	t *testing.T,
	files map[string]string,
	latest int,
	baseline []migrationlint.BaselineColumn,
) migrationlint.Analysis {
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
		Baseline:      baseline,
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
		name     string
		files    map[string]string
		latest   int
		baseline []migrationlint.BaselineColumn
		want     string
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
			// Measured, not derived. Three tables created mid/zeta/alpha and
			// dropped zeta/alpha/mid: source order, creation order and reverse
			// creation order each give a different answer, so this is the
			// smallest fixture that shows the order is the table name. The fix
			// header pluralizes with the count, which a one-diagnostic fixture
			// cannot show.
			name: "multi-target drop is one diagnostic per table",
			files: map[string]string{
				"1.sql": "CREATE TABLE mid (id int);\nCREATE TABLE zeta (id int);\nCREATE TABLE alpha (id int);\n",
				"2.sql": "DROP TABLE zeta, alpha, mid;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"alpha\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L1: Dropping table \"mid\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L1: Dropping table \"zeta\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fixes:\n" +
				"      -> Add a pre-migration check to ensure table \"alpha\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"mid\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"zeta\" is empty before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 3 schema changes\n" +
				"  -- 3 diagnostics\n",
		},
		{
			// Dropped COLUMNS go the other way from dropped tables: ONE
			// diagnostic naming all of them, in clause order, with one fix.
			// Measured, because it cannot be inferred from the table case --
			// the two shapes are opposites.
			name: "multi-column drop is one diagnostic naming every column",
			files: map[string]string{
				"1.sql": "CREATE TABLE t (id int, zeta int, alpha int, mid int);\n",
				"2.sql": "ALTER TABLE t DROP COLUMN zeta, DROP COLUMN alpha, DROP COLUMN mid;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual columns \"zeta\", \"alpha\" and \"mid\"\n" +
				"         https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add pre-migration checks to ensure columns \"zeta\", \"alpha\" and \"mid\" are NULL before\n" +
				"         dropping them\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 3 schema changes\n" +
				"  -- 1 diagnostic\n",
		},
		{
			// Two columns separate the list separator from the conjunction: at
			// three the list carries both a comma and an "and", at two only the
			// "and". A three-column fixture alone cannot tell a ", "-joined
			// list from one that also puts "and" before the last element.
			name: "two-column drop joins the pair with and",
			files: map[string]string{
				"1.sql": "CREATE TABLE t (id int, zeta int, alpha int);\n",
				"2.sql": "ALTER TABLE t DROP COLUMN zeta, DROP COLUMN alpha;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual columns \"zeta\" and \"alpha\"\n" +
				"         https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add pre-migration checks to ensure columns \"zeta\" and \"alpha\" are NULL before dropping\n" +
				"         them\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 1 diagnostic\n",
		},
		{
			// The single-column control: the nouns and the trailing pronoun are
			// singular, which is what shows the plural forms above are keyed to
			// the count rather than always printed.
			name: "single-column drop keeps the singular wording",
			files: map[string]string{
				"1.sql": "CREATE TABLE t (id int, zeta int);\n",
				"2.sql": "ALTER TABLE t DROP COLUMN zeta;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"zeta\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"zeta\" is NULL before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
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
		{
			// #1074: several column renames in one statement. Measured on
			// MySQL, whose grammar accepts the multi-clause form -- one
			// diagnostic naming both columns in clause order, one suggested fix
			// with the plural nouns and pronoun, both wrapped at the measured
			// boundary. Rendered here rather than through a command because no
			// SQLite dev database can replay this statement.
			name: "several column renames in one statement are one diagnostic",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int, nick text, email text);\n",
				"2.sql": "ALTER TABLE users RENAME COLUMN nick TO handle, RENAME COLUMN email TO mail;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual columns \"nick\" and \"email\"\n" +
				"         https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add pre-migration checks to ensure columns \"nick\" and \"email\" are NULL before dropping\n" +
				"         them\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 1 diagnostic\n",
		},
		{
			// #1074: several table renames in one statement are the opposite
			// shape -- one diagnostic each, ordered by logical name rather than
			// by the order the pairs are written, so the fix header pluralizes
			// and the diagnostic count rises. Written users first, reported pets
			// first.
			//
			// The missing schema-change line is a known divergence, not parity:
			// Ptah's parser does not model the standalone RENAME TABLE
			// statement, so it contributes no semantic change, where the
			// analyzer this tool matches counts four. It is pre-existing --
			// master reports zero here too -- and dialect-specific to the MySQL
			// grammar; the diagnostics above are what #1074 aligned. A count of
			// zero prints no line at all (see the INSERT-only case below), so
			// the divergence now shows as an absent line rather than as
			// "0 schema changes".
			name: "several table renames in one statement are ordered by name",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\nCREATE TABLE pets (id int);\n",
				"2.sql": "RENAME TABLE users TO accounts, pets TO animals;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping table \"pets\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n" +
				"    -- suggested fixes:\n" +
				"      -> Add a pre-migration check to ensure table \"pets\" is empty before dropping it\n" +
				"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 diagnostics\n",
		},
		{
			// #1074 S1: a version that expresses no schema change prints no
			// schema-change line at all. Measured against the pinned community
			// binary v1.3.0: a version whose only statement is an INSERT ends at
			// the version-summary line, where Ptah used to add
			// "-- 0 schema changes". The scope filter made this reachable on
			// versions that do carry DDL, because DDL against a schema the dev
			// URL does not cover counts nothing.
			name: "a version expressing no schema change prints no change line",
			files: map[string]string{
				"1.sql": "CREATE TABLE t (id int);\n",
				"2.sql": "INSERT INTO t (id) VALUES (1);\n",
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
				"  -- 1 version ok\n",
		},
		{
			// stokaro/ptah#1074. The compatibility surface models a rename as the
			// retirement of one name plus the introduction of another, and prints
			// both: the drop of "id" and the add of an `integer` column "oid".
			// Verbatim from atlas community version v1.3.0 on PostgreSQL 16, which
			// is also where the `integer` spelling comes from -- the statement says
			// `int` and the state says `integer`.
			name: "rename prints its add side from the baseline",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int NOT NULL);\n",
				"2.sql": "ALTER TABLE users RENAME COLUMN id TO oid;\n",
			},
			latest: 1,
			baseline: []migrationlint.BaselineColumn{{
				Version: 2, Table: "users", Name: "id", DataType: "integer", NotNull: true,
			}},
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L1: Adding a non-nullable \"integer\" column \"oid\" will fail in case table \"users\" is not\n" +
				"         empty https://atlasgo.io/lint/analyzers#MF103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 2 diagnostics\n",
		},
		{
			// The same fixture with a NULLABLE retired column, which is the control
			// that separates "the rename is reported" from "the rename's add side
			// is reported". Measured: one diagnostic, no data dependent group.
			name: "nullable rename prints no add side",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int);\n",
				"2.sql": "ALTER TABLE users RENAME COLUMN id TO oid;\n",
			},
			latest: 1,
			baseline: []migrationlint.BaselineColumn{{
				Version: 2, Table: "users", Name: "id", DataType: "integer",
			}},
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L1: Dropping non-virtual column \"id\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"id\" is NULL before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 1 schema change\n" +
				"  -- 1 diagnostic\n",
		},
		{
			// Analyzer groups print in analyzer order, not in the order their first
			// diagnostic appears. The add is on line 1 and the drop on line 2, so a
			// renderer ordering groups by first appearance prints them the other way
			// round -- which is what it used to do. Verbatim from the pinned binary.
			name: "analyzer groups print in measured order",
			files: map[string]string{
				"1.sql": "CREATE TABLE users (id int, nick int);\n",
				"2.sql": "ALTER TABLE users ADD COLUMN x text NOT NULL;\nALTER TABLE users DROP COLUMN nick;\n",
			},
			latest: 1,
			want: "Analyzing changes from version 1 to 2 (1 migration in total):\n" +
				"\n" +
				"  -- analyzing version 2\n" +
				"    -- destructive changes detected:\n" +
				"      -- L2: Dropping non-virtual column \"nick\" https://atlasgo.io/lint/analyzers#DS103\n" +
				"    -- data dependent changes detected:\n" +
				"      -- L1: Adding a non-nullable \"text\" column \"x\" will fail in case table \"users\" is not empty\n" +
				"         https://atlasgo.io/lint/analyzers#MF103\n" +
				"    -- suggested fix:\n" +
				"      -> Add a pre-migration check to ensure column \"nick\" is NULL before dropping it\n" +
				"  -- ok (0s)\n" +
				"\n" +
				"  -------------------------\n" +
				"  -- 0s\n" +
				"  -- 1 version with errors\n" +
				"  -- 2 schema changes\n" +
				"  -- 2 diagnostics\n",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := qt.New(t)
			analysis := analyzeMigrationsWithBaseline(t, tc.files, tc.latest, tc.baseline)
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

func TestWriteMigrateLintText_RendersAtlasRepeatableVersionKey(t *testing.T) {
	c := qt.New(t)
	analysis, err := migrationlint.AnalyzeFS(fstest.MapFS{
		"1_create_users.sql": {Data: []byte("CREATE TABLE users (id INTEGER);\n")},
		"2R_drop_users.sql":  {Data: []byte("DROP TABLE users;\n")},
	}, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Dialect:       "sqlite",
		Selection: migrationlint.VersionSelection{
			Versions:   []int64{2},
			Restricted: true,
		},
	})
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = writeMigrateLintText(&out, MigrateLintOptions{Analysis: &analysis}, fixedZeroClock())

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals,
		"Analyzing changes from version 1 to 2R (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version 2R\n"+
			"    -- destructive changes detected:\n"+
			"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n"+
			"    -- suggested fix:\n"+
			"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n"+
			"  -- ok (0s)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- 0s\n"+
			"  -- 1 version with errors\n"+
			"  -- 1 schema change\n"+
			"  -- 1 diagnostic\n")
}

func TestWriteMigrateLintText_RendersAtlasBareRepeatableVersionKey(t *testing.T) {
	c := qt.New(t)
	analysis, err := migrationlint.AnalyzeFS(fstest.MapFS{
		"R__drop_users.sql": {Data: []byte("DROP TABLE users;\n")},
	}, migrationlint.Options{
		Compatibility: migrationlint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Dialect:       "sqlite",
	})
	c.Assert(err, qt.IsNil)
	var out bytes.Buffer

	err = writeMigrateLintText(&out, MigrateLintOptions{Analysis: &analysis}, fixedZeroClock())

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals,
		"Analyzing changes until version R (1 migration in total):\n"+
			"\n"+
			"  -- analyzing version R\n"+
			"    -- destructive changes detected:\n"+
			"      -- L1: Dropping table \"users\" https://atlasgo.io/lint/analyzers#DS102\n"+
			"    -- suggested fix:\n"+
			"      -> Add a pre-migration check to ensure table \"users\" is empty before dropping it\n"+
			"  -- ok (0s)\n"+
			"\n"+
			"  -------------------------\n"+
			"  -- 0s\n"+
			"  -- 1 version with errors\n"+
			"  -- 1 schema change\n"+
			"  -- 1 diagnostic\n")
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

// TestWriteMigrateLintText_KeepsNativeProseForUnmappedRules pins what the
// compatibility surface does with a rule that has no measured Atlas identity:
// it keeps Ptah's own sentence, keeps the native rule code visible in the
// `[CODE]` label form, and invents no analyzer link. Being wordier and clearly
// labeled is a smaller divergence than printing a URL for an analyzer that
// never emitted the finding.
//
// TRUNCATE is the fixture because DS108 has no Atlas counterpart. The property
// arrived here from cmd/atlas, where it was pinned on a rename until #1074 gave
// renames a measured Atlas identity; the renderer can analyze TRUNCATE without
// a dev-database replay, which no SQLite-backed command test can.
//
// Reverting #1074 keeps this green -- it is a non-interference control for the
// fallback path. The inverse mutant that kills it is setting atlas: true on the
// unmapped branch of compatibilityDiagnostic, which drops the `[DS108]` label
// and appends an analyzer link for copy that was never measured.
func TestWriteMigrateLintText_KeepsNativeProseForUnmappedRules(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeMigrations(t, map[string]string{
		"1.sql": "CREATE TABLE pets (id int);\n",
		"2.sql": "TRUNCATE pets;\n",
	}, 1)
	var out bytes.Buffer

	err := writeMigrateLintText(&out, MigrateLintOptions{Analysis: &analysis}, fixedZeroClock())
	c.Assert(err, qt.IsNil)
	rendered := out.String()

	c.Assert(rendered, qt.Contains, "L1 [DS108]: TRUNCATE deletes all rows from the table")
	c.Assert(rendered, qt.Not(qt.Contains), "https://atlasgo.io/lint/analyzers#DS108")
	c.Assert(rendered, qt.Not(qt.Contains), "-- suggested fix")
}
