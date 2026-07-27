package lint_test

import (
	"io/fs"
	"slices"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/migration/lint"
	"github.com/stokaro/ptah/migration/migrator"
)

func TestAnalyzeFS_VersionSelectionPreservesCompleteSnapshot(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_create.sql": "CREATE TABLE users (id INTEGER);",
		"2_drop.sql":   "DROP TABLE users;",
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Selection: lint.VersionSelection{
			Versions:   []int64{2},
			Restricted: true,
		},
	})

	c.Assert(err, qt.IsNil)
	files := analysis.Files()
	c.Assert(files, qt.HasLen, 2)
	c.Assert(files[0].Name, qt.Equals, "1_create.sql")
	c.Assert(files[0].Selected, qt.IsFalse)
	c.Assert(files[1].Name, qt.Equals, "2_drop.sql")
	c.Assert(files[1].Selected, qt.IsTrue)
	selected := analysis.SelectedFiles()
	c.Assert(selected, qt.HasLen, 1)
	c.Assert(selected[0].Name, qt.Equals, "2_drop.sql")
	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DS101"})
	c.Assert(fstest.TestFS(analysis.SnapshotFS(), "1_create.sql", "2_drop.sql"), qt.IsNil)
}

func TestAnalyzeFS_ExplicitEmptyVersionSelectionSelectsNothing(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_create.sql": "CREATE TABLE users (id INTEGER);",
		"2_drop.sql":   "DROP TABLE users;",
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Selection: lint.VersionSelection{Restricted: true},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(analysis.Files(), qt.HasLen, 2)
	c.Assert(analysis.SelectedFiles(), qt.HasLen, 0)
	c.Assert(analysis.Findings(), qt.HasLen, 0)
}

func TestAnalyzeFS_DataDependentExemptsSameFileCreatedTable(t *testing.T) {
	tests := []struct {
		name  string
		sql   string
		rules []string
	}{
		{
			// Adding a NOT NULL column to a table created earlier in the same
			// file targets an empty table, so the add cannot fail on data.
			name:  "same-file created table is exempt",
			sql:   "CREATE TABLE users (id INTEGER);\nALTER TABLE users ADD COLUMN tenant_id INTEGER NOT NULL;\n",
			rules: []string{},
		},
		{
			name:  "pre-existing table still reports",
			sql:   "ALTER TABLE users ADD COLUMN tenant_id INTEGER NOT NULL;\n",
			rules: []string{"DD101"},
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			fsys := fixture(map[string]string{"1_users.sql": tc.sql})

			analysis, err := lint.AnalyzeFS(fsys, lint.Options{
				DirFormat: migrator.MigrationDirFormatAtlas,
				Dialect:   "sqlite",
			})

			c.Assert(err, qt.IsNil)
			c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, tc.rules)
		})
	}
}

func TestAnalyzeFS_DataDependentReportsColumnAddedToLaterCreatedTable(t *testing.T) {
	c := qt.New(t)
	// The CREATE follows the ALTER, so the table is not yet known to be empty
	// when the column is added; the finding must still fire.
	fsys := fixture(map[string]string{
		"1_users.sql": "ALTER TABLE users ADD COLUMN tenant_id INTEGER NOT NULL;\nCREATE TABLE users (id INTEGER);\n",
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "sqlite",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DD101"})
}

func TestAnalyzeFS_RejectsUnknownCompatibilityProfile(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_select.sql": "SELECT 1;",
	})

	_, err := lint.AnalyzeFS(fsys, lint.Options{
		Compatibility: lint.CompatibilityProfile("unknown"),
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.ErrorMatches, `unsupported lint compatibility profile "unknown"`)
}

func TestAnalyzeFS_ReturnedDataCannotMutateSnapshot(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_drop.sql": "DROP TABLE users;",
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)

	files := analysis.Files()
	files[0].Statements[0].Words[0] = "MUTATED"
	files[0].Statements[0].SQL = "MUTATED"
	findings := analysis.Findings()
	findings[0].Context.Subjects[0].Name = "MUTATED"
	source, err := fs.ReadFile(analysis.SnapshotFS(), "1_drop.sql")
	c.Assert(err, qt.IsNil)
	source[0] = 'X'

	freshFiles := analysis.Files()
	c.Assert(freshFiles[0].Statements[0].Words[0], qt.Equals, "DROP")
	c.Assert(freshFiles[0].Statements[0].SQL, qt.Equals, "DROP TABLE users")
	freshFindings := analysis.Findings()
	c.Assert(freshFindings[0].Context.Subjects[0].Name, qt.Equals, "users")
	freshSource, err := fs.ReadFile(analysis.SnapshotFS(), "1_drop.sql")
	c.Assert(err, qt.IsNil)
	c.Assert(string(freshSource), qt.Equals, "DROP TABLE users;")
}

func TestAnalyzeFS_SameLineStatementsHaveStableContexts(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_hazards.sql": "DROP TABLE users; ALTER TABLE accounts DROP COLUMN legacy;\n",
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)

	files := analysis.Files()
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].Statements, qt.HasLen, 2)
	first := files[0].Statements[0]
	second := files[0].Statements[1]
	c.Assert(files[0].SQL[first.Span.Start:first.Span.End], qt.Equals, first.SQL)
	c.Assert(files[0].SQL[second.Span.Start:second.Span.End], qt.Equals, second.SQL)
	findings := analysis.Findings()
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"DS101", "DS102"})
	c.Assert(findings[0].Context.StatementIndex, qt.Equals, 0)
	c.Assert(findings[1].Context.StatementIndex, qt.Equals, 1)
}

func TestAnalyzeFS_MultiTargetDropReportsOnlyUnsafeTables(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_cleanup.sql": "CREATE TABLE staging (id INTEGER); DROP TABLE staging, users, audit_log;",
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)

	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
	c.Assert(findings[0].Context.StatementIndex, qt.Equals, 1)
	c.Assert(findings[0].Context.Subjects, qt.DeepEquals, []lint.Subject{
		{Kind: lint.SubjectTable, Name: "users"},
		{Kind: lint.SubjectTable, Name: "audit_log"},
	})
}

func TestAnalyzeFS_DropTableSubjectsPreserveIdentifierSpelling(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_cleanup.sql": `DROP TABLE ONLY "Users", ONLY public."users" *;`,
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "postgres",
	})

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Context.Subjects, qt.DeepEquals, []lint.Subject{
		{Kind: lint.SubjectTable, Name: `"Users"`},
		{Kind: lint.SubjectTable, Name: `public."users"`},
	})
}

func TestAnalyzeFS_SQLiteBracketedDropTableRetainsFindingContext(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_cleanup.sql": "DROP TABLE [Users];",
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "sqlite",
	})

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Rule, qt.Equals, "DS101")
	c.Assert(findings[0].Context.Subjects, qt.DeepEquals, []lint.Subject{
		{Kind: lint.SubjectTable, Name: "[Users]"},
	})
}

func TestAnalyzeFS_ColumnFindingsExposeStructuredSubjects(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_change.sql": `ALTER TABLE public."Users" DROP COLUMN "Legacy", ADD COLUMN "TenantID" UUID NOT NULL;`,
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "postgres",
	})

	c.Assert(err, qt.IsNil)
	findings := analysis.Findings()
	c.Assert(rulesOf(findings), qt.DeepEquals, []string{"DD101", "DS102"})
	c.Assert(findings[0].Context.Subjects, qt.DeepEquals, []lint.Subject{
		{
			Kind:     lint.SubjectColumn,
			Name:     `"TenantID"`,
			Parent:   `public."Users"`,
			DataType: "UUID",
		},
	})
	c.Assert(findings[1].Context.Subjects, qt.DeepEquals, []lint.Subject{
		{
			Kind:   lint.SubjectColumn,
			Name:   `"Legacy"`,
			Parent: `public."Users"`,
		},
	})
}

func TestAnalyzeFS_AtlasCompatibilityMapsStatementSuppressions(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_suppressed.sql": `-- atlas:nolint DS102
DROP TABLE users;
-- atlas:nolint DS103
ALTER TABLE accounts DROP COLUMN legacy;
-- atlas:nolint MF103
ALTER TABLE accounts ADD COLUMN tenant_id INTEGER NOT NULL;
`,
	})

	native, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "sqlite",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(native.Findings()), qt.DeepEquals, []string{"DS101", "DS102", "DD101"})

	atlas, err := lint.AnalyzeFS(fsys, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Dialect:       "sqlite",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(atlas.Findings(), qt.HasLen, 0)
}

func TestAnalyzeFS_AtlasFileSuppressionIsCompatibilityScoped(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_suppressed.sql": `-- atlas:nolint destructive

DROP TABLE users;
ALTER TABLE accounts DROP COLUMN legacy;
`,
	})

	native, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(native.Findings()), qt.DeepEquals, []string{"DS101", "DS102"})

	atlas, err := lint.AnalyzeFS(fsys, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(atlas.Findings(), qt.HasLen, 0)
}

func TestAnalyzeFS_AtlasAnalyzerSuppressionsAreCompatibilityScoped(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_index.sql": `-- atlas:nolint concurrent_index
CREATE INDEX idx_users_id ON users (id);
`,
		"2_rename.sql": `-- atlas:nolint incompatible
ALTER TABLE users RENAME COLUMN old_name TO new_name;
`,
		"3_transaction.sql": `-- atlas:nolint nestedtx
BEGIN;
`,
	})

	native, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		Dialect:   "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(native.Findings()), qt.DeepEquals, []string{"PG101", "BC101", "TX201"})

	atlas, err := lint.AnalyzeFS(fsys, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Dialect:       "postgres",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(atlas.Findings(), qt.HasLen, 0)
}

func TestAnalyzeFS_AtlasAnalyzerFileHeaderSuppressesMappedFamilies(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_suppressed.sql": `-- atlas:nolint concurrent_index,incompatible,nestedtx

CREATE INDEX idx_users_id ON users (id);
ALTER TABLE users RENAME COLUMN old_name TO new_name;
BEGIN;
`,
	})

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
		Dialect:       "postgres",
	})

	c.Assert(err, qt.IsNil)
	files := analysis.Files()
	c.Assert(files, qt.HasLen, 1)
	c.Assert(files[0].Ignored, qt.IsFalse)
	c.Assert(analysis.Findings(), qt.HasLen, 0)
}

func TestAnalyzeFS_BareAtlasHeaderMarksOnlyCompatibilityFileIgnored(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_ignored.sql": `-- atlas:nolint

DROP TABLE users;
`,
	})

	native, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	nativeFiles := native.Files()
	c.Assert(nativeFiles, qt.HasLen, 1)
	c.Assert(nativeFiles[0].Ignored, qt.IsFalse)
	c.Assert(native.Findings(), qt.HasLen, 0)

	atlas, err := lint.AnalyzeFS(fsys, lint.Options{
		Compatibility: lint.CompatibilityProfileAtlas,
		DirFormat:     migrator.MigrationDirFormatAtlas,
	})
	c.Assert(err, qt.IsNil)
	atlasFiles := atlas.Files()
	c.Assert(atlasFiles, qt.HasLen, 1)
	c.Assert(atlasFiles[0].Ignored, qt.IsTrue)
	c.Assert(atlas.Findings(), qt.HasLen, 0)
}

func TestAnalyzeFS_CustomRuleCannotMutateCompletedAnalysis(t *testing.T) {
	c := qt.New(t)
	fsys := fixture(map[string]string{
		"1_select.sql": "SELECT 1;",
	})
	retainedContext := &lint.FindingContext{
		StatementIndex: 0,
		Subjects: []lint.Subject{
			{Kind: lint.SubjectTable, Name: "users"},
		},
	}

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
		ExtraRules: []lint.Rule{
			{
				Code:     "ZZ101",
				Title:    "custom finding",
				Severity: lint.SeverityWarning,
				CheckFile: func(file *lint.File) []lint.Finding {
					return []lint.Finding{
						{
							Rule:     "ZZ101",
							Title:    "custom finding",
							Severity: lint.SeverityWarning,
							File:     file.Path,
							Line:     file.Statements[0].Line,
							Message:  "custom finding",
							Context:  retainedContext,
						},
					}
				},
			},
		},
	})
	c.Assert(err, qt.IsNil)

	retainedContext.Subjects[0].Name = "mutated"
	findings := analysis.Findings()
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Context.Subjects[0].Name, qt.Equals, "users")
}

func TestAnalyzeFS_CapturesAtlasTemplateInputsOnce(t *testing.T) {
	c := qt.New(t)
	fsys := &changingSQLFS{
		files: fstest.MapFS{
			"atlas.sum": {
				Data: []byte("h1:test\n"),
			},
			"1_template.sql": {
				Data: []byte(`{{ template "shared/drop" . }}`),
			},
			"shared/drop.sql": {
				Data: []byte(`{{ define "shared/drop" }}DROP TABLE users;{{ end }}`),
			},
		},
		reads: map[string]int{},
	}

	analysis, err := lint.AnalyzeFS(fsys, lint.Options{
		DirFormat: migrator.MigrationDirFormatAtlas,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(rulesOf(analysis.Findings()), qt.DeepEquals, []string{"DS101"})
	c.Assert(fsys.reads, qt.DeepEquals, map[string]int{
		"1_template.sql":  1,
		"atlas.sum":       1,
		"shared/drop.sql": 1,
	})
	c.Assert(analysis.Files(), qt.HasLen, 1)
	c.Assert(fstest.TestFS(
		analysis.SnapshotFS(),
		"1_template.sql",
		"atlas.sum",
		"shared/drop.sql",
	), qt.IsNil)
}

type changingSQLFS struct {
	files fstest.MapFS
	reads map[string]int
}

func (f *changingSQLFS) Open(name string) (fs.File, error) {
	return f.files.Open(name)
}

func (f *changingSQLFS) ReadFile(name string) ([]byte, error) {
	f.reads[name]++
	contents, err := fs.ReadFile(f.files, name)
	responses := [][]byte{contents, []byte("SELECT 1;")}
	return slices.Clone(responses[min(f.reads[name]-1, 1)]), err
}
