package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/migration/lint"
	"go.5x5.cz/ptah/migration/migrator"
)

// renameBaselineFS is the fixture every case below analyzes: one file creates a
// table, the next renames one of its columns. The rename is the whole point --
// the retired column's type and nullability are in file 1, so nothing in file 2
// can tell an analyzer what the new name introduces.
func renameBaselineFS() map[string]string {
	return map[string]string{
		"1_base.sql":   "CREATE TABLE users (id int NOT NULL);",
		"2_rename.sql": "ALTER TABLE users RENAME COLUMN id TO oid;",
	}
}

func renameBaselineOptions(baseline []lint.BaselineColumn) lint.Options {
	return lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		Dialect:       "postgres",
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Selection:     lint.VersionSelection{Versions: []int64{2}, Restricted: true},
		Baseline:      baseline,
	}
}

// notNullBaseline is the state version 2 starts from when `users.id` is the
// non-nullable, default-less column the pinned community binary reports an
// MF103 for.
func notNullBaseline() []lint.BaselineColumn {
	return []lint.BaselineColumn{{
		Version:  2,
		Table:    "users",
		Name:     "id",
		DataType: "integer",
		NotNull:  true,
	}}
}

// TestAnalyzeFS_RenameAddSideNeedsTheBaseline pins the whole point of
// [lint.BaselineColumn]: the add side of a rename is reportable only once the
// caller has established what the retired column was.
//
// Measured against atlas community version v1.3.0 on PostgreSQL 16 for
// stokaro/ptah#1074: `CREATE TABLE users (id int NOT NULL)` then `ALTER TABLE
// users RENAME COLUMN id TO oid` reports the drop of "id" AND `Adding a
// non-nullable "integer" column "oid" will fail in case table "users" is not
// empty`, and exits 1.
func TestAnalyzeFS_RenameAddSideNeedsTheBaseline(t *testing.T) {
	tests := []struct {
		name     string
		baseline []lint.BaselineColumn
		want     []string
	}{
		{
			// No dev database was read, so the retired column's shape was never
			// established and the add side stays unreported.
			name: "no baseline reports the retirement alone",
			want: []string{"DS102"},
		},
		{
			name:     "baseline reports both halves of the rename",
			baseline: notNullBaseline(),
			want:     []string{"DD101", "DS102"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(fixture(renameBaselineFS()), renameBaselineOptions(test.baseline))

			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_RenameAddSideSubjectCarriesTheIntroducedColumn pins what the
// finding is about. The retirement names the OLD column and the add names the
// NEW one, with the retired column's type: getting either half backwards would
// print a diagnostic about a column that does not exist after the migration.
func TestAnalyzeFS_RenameAddSideSubjectCarriesTheIntroducedColumn(t *testing.T) {
	c := qt.New(t)

	analysis, err := lint.AnalyzeFS(fixture(renameBaselineFS()), renameBaselineOptions(notNullBaseline()))

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 2)
	c.Assert(findings[0].Rule, qt.Equals, "DD101")
	c.Assert(findings[0].Severity, qt.Equals, lint.SeverityWarning)
	c.Assert(findings[0].Line, qt.Equals, 1)
	c.Assert(findings[0].Context, qt.IsNotNil)
	c.Assert(findings[0].Context.Subjects, qt.DeepEquals, []lint.Subject{{
		Kind:     lint.SubjectColumn,
		Name:     "oid",
		Parent:   "users",
		DataType: "integer",
	}})
	c.Assert(findings[1].Rule, qt.Equals, "DS102")
	c.Assert(findings[1].Context.Subjects, qt.DeepEquals, []lint.Subject{{
		Kind:   lint.SubjectColumn,
		Name:   "id",
		Parent: "users",
	}})
}

// TestAnalyzeFS_RenameAddSideSilentCases keeps the rule to the shape it was
// measured on. Each row is a fixture the pinned community binary reports NO
// MF103 for, and each differs from the reporting fixture on exactly one axis, so
// a rule that ignored that axis would fail here while the reporting case passed.
func TestAnalyzeFS_RenameAddSideSilentCases(t *testing.T) {
	tests := []struct {
		name     string
		files    map[string]string
		baseline []lint.BaselineColumn
		want     []string
	}{
		{
			// A nullable retired column introduces a nullable one, which cannot
			// fail on existing rows. Measured: one diagnostic, the drop only.
			name:  "nullable retired column",
			files: renameBaselineFS(),
			baseline: []lint.BaselineColumn{{
				Version: 2, Table: "users", Name: "id", DataType: "integer",
			}},
			want: []string{"DS102"},
		},
		{
			// Measured: `CREATE TABLE users (id int NOT NULL DEFAULT 7)` then the
			// same rename reports the drop alone. A default is what makes the
			// add survive existing rows.
			name:  "retired column carries a default",
			files: renameBaselineFS(),
			baseline: []lint.BaselineColumn{{
				Version: 2, Table: "users", Name: "id", DataType: "integer",
				NotNull: true, HasDefault: true,
			}},
			want: []string{"DS102"},
		},
		{
			// The table is created in the file doing the rename, so the table is
			// empty and neither half is reportable. Measured: no diagnostics at
			// all, exit 0.
			name: "table created in the same file",
			files: map[string]string{
				"1_base.sql":   "CREATE TABLE seed (x int);",
				"2_rename.sql": "CREATE TABLE users (id int NOT NULL);\nALTER TABLE users RENAME COLUMN id TO oid;",
			},
			baseline: notNullBaseline(),
			want:     []string{},
		},
		{
			// A table rename introduces no column, so it has no add side.
			// Measured: DS102 for the retired table and nothing else.
			name: "table rename",
			files: map[string]string{
				"1_base.sql":   "CREATE TABLE users (id int NOT NULL);",
				"2_rename.sql": "ALTER TABLE users RENAME TO accounts;",
			},
			baseline: notNullBaseline(),
			want:     []string{"DS101"},
		},
		{
			// The baseline describes a different table, so this run never
			// established what `users.id` was.
			name:  "baseline carries another table",
			files: renameBaselineFS(),
			baseline: []lint.BaselineColumn{{
				Version: 2, Table: "accounts", Name: "id", DataType: "integer", NotNull: true,
			}},
			want: []string{"DS102"},
		},
		{
			// The baseline describes the state of a different version, which is
			// not the state this file starts from.
			name:  "baseline carries another version",
			files: renameBaselineFS(),
			baseline: []lint.BaselineColumn{{
				Version: 1, Table: "users", Name: "id", DataType: "integer", NotNull: true,
			}},
			want: []string{"DS102"},
		},
		{
			// A bare table name carried by two schemas resolves to neither:
			// naming the wrong table's column is worse than saying nothing.
			name:  "ambiguous bare table name",
			files: renameBaselineFS(),
			baseline: []lint.BaselineColumn{
				{Version: 2, Schema: "app", Table: "users", Name: "id", DataType: "integer", NotNull: true},
				{Version: 2, Schema: "audit", Table: "users", Name: "id", DataType: "bigint", NotNull: true},
			},
			want: []string{"DS102"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(fixture(test.files), renameBaselineOptions(test.baseline))

			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_RenameAddSideIsCompatibilitySurfaceOnly keeps the native surface
// out of it. Native `ptah migrations lint` models a rename as a rename and
// reports BC101; a rename does not fail on a populated table, so claiming it
// might would be a statement about the statement that is false in the model that
// surface uses.
func TestAnalyzeFS_RenameAddSideIsCompatibilitySurfaceOnly(t *testing.T) {
	c := qt.New(t)

	options := renameBaselineOptions(notNullBaseline())
	options.Compatibility = lint.CompatibilityProfileNative

	analysis, err := lint.AnalyzeFS(fixture(renameBaselineFS()), options)

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"BC101"})
}

// TestAnalyzeFS_RenameAddSideResolvesQualifiedReference covers the spelling a
// migration is free to use for the same table. A schema-scoped read does not
// repeat the schema name on every table, so a qualified reference has a
// qualifier the state cannot match literally, and dropping it is what keeps the
// diagnostic. Without this the same fixture written `ALTER TABLE public.users`
// silently reported one diagnostic where the pinned binary reports two.
func TestAnalyzeFS_RenameAddSideResolvesQualifiedReference(t *testing.T) {
	c := qt.New(t)

	files := map[string]string{
		"1_base.sql":   "CREATE TABLE users (id int NOT NULL);",
		"2_rename.sql": "ALTER TABLE public.users RENAME COLUMN id TO oid;",
	}

	analysis, err := lint.AnalyzeFS(fixture(files), renameBaselineOptions(notNullBaseline()))

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DD101", "DS102"})
}

// TestAnalyzeFS_BaselineVersionsAsksOnlyForWhatItCanUse pins the request side.
// A caller reads the dev database once per version listed here, so listing a
// version with nothing to resolve spends a round trip to learn nothing, and
// failing to list one leaves the diagnostic unreachable.
func TestAnalyzeFS_BaselineVersionsAsksOnlyForWhatItCanUse(t *testing.T) {
	tests := []struct {
		name    string
		files   map[string]string
		options lint.Options
		want    []int64
	}{
		{
			name:    "column rename",
			files:   renameBaselineFS(),
			options: renameBaselineOptions(nil),
			want:    []int64{2},
		},
		{
			name: "no rename",
			files: map[string]string{
				"1_base.sql": "CREATE TABLE users (id int NOT NULL);",
				"2_drop.sql": "DROP TABLE users;",
			},
			options: renameBaselineOptions(nil),
			want:    nil,
		},
		{
			name: "rename exempted by a same-file create",
			files: map[string]string{
				"1_base.sql":   "CREATE TABLE seed (x int);",
				"2_rename.sql": "CREATE TABLE users (id int NOT NULL);\nALTER TABLE users RENAME COLUMN id TO oid;",
			},
			options: renameBaselineOptions(nil),
			want:    nil,
		},
		{
			name: "table rename introduces no column",
			files: map[string]string{
				"1_base.sql":   "CREATE TABLE users (id int NOT NULL);",
				"2_rename.sql": "ALTER TABLE users RENAME TO accounts;",
			},
			options: renameBaselineOptions(nil),
			want:    nil,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis, err := lint.AnalyzeFS(fixture(test.files), test.options)

			c.Assert(err, qt.IsNil)
			c.Assert(analysis.BaselineVersions(), qt.DeepEquals, test.want)
		})
	}
}

// TestAnalyzeFS_BaselineVersionsIsNativeSurfaceEmpty keeps the native surface
// from paying for a lookup it will not use.
func TestAnalyzeFS_BaselineVersionsIsNativeSurfaceEmpty(t *testing.T) {
	c := qt.New(t)

	options := renameBaselineOptions(nil)
	options.Compatibility = lint.CompatibilityProfileNative

	analysis, err := lint.AnalyzeFS(fixture(renameBaselineFS()), options)

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.BaselineVersions(), qt.HasLen, 0)
}
