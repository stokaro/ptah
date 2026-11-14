package atlasreport_test

import (
	"bytes"
	"errors"
	"fmt"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/migrationreplay"
	migrationlint "ptah.run/migration/lint"
	"ptah.run/migration/migrationfile"
)

// The values below are the ones the pinned community binary v1.3.0 hands a
// `migrate lint --format` template on the same directories, measured on SQLite
// with --latest selecting the analyzed versions (stokaro/ptah#6000).

// templateDataTwoFiles is a base and one analyzed version creating one table.
func templateDataTwoFiles() fstest.MapFS {
	return fstest.MapFS{
		"1.sql": {Data: []byte("CREATE TABLE t1 (id int);\n")},
		"2.sql": {Data: []byte("CREATE TABLE t2 (id int);\n")},
	}
}

// templateDataThreeFiles holds two analyzed versions, the first with two
// statements, so a count per file and a count per statement differ.
func templateDataThreeFiles() fstest.MapFS {
	return fstest.MapFS{
		"1.sql": {Data: []byte("CREATE TABLE t1 (id int);\n")},
		"2.sql": {Data: []byte("CREATE TABLE t2 (id int);\nCREATE TABLE t3 (id int);\n")},
		"3.sql": {Data: []byte("CREATE TABLE t4 (id int);\n")},
	}
}

// templateDataDestructive drops a table on line 3 of version 2, then a column
// and a table on lines 1 and 2 of version 3: one report per file, three
// diagnostics, each with a fix.
func templateDataDestructive() fstest.MapFS {
	return fstest.MapFS{
		"1.sql": {Data: []byte("CREATE TABLE t1 (id int, a int);\nCREATE TABLE t2 (id int);\nCREATE TABLE t3 (id int);\n")},
		"2.sql": {Data: []byte("-- comment\n\nDROP TABLE t2;\n")},
		"3.sql": {Data: []byte("ALTER TABLE t1 DROP COLUMN a;\nDROP TABLE t3;\n")},
	}
}

func analyzeTemplateData(c *qt.C, fsys fstest.MapFS, opts migrationlint.Options, versions ...int64) migrationlint.Analysis {
	c.Helper()
	opts.Compatibility = migrationlint.CompatibilityProfileAtlas
	opts.DirFormat = migrationfile.DirFormatAtlas
	opts.Selection = migrationlint.VersionSelection{Versions: versions, Restricted: true}
	analysis, err := migrationlint.AnalyzeFS(c.Context(), builtintest.Runtime(), fsys, opts)
	c.Assert(err, qt.IsNil)
	return analysis
}

func renderTemplateData(c *qt.C, format string, opts atlasreport.MigrateLintOptions) (string, error) {
	c.Helper()
	var out bytes.Buffer
	err := atlasreport.WriteMigrateLintFormat(&out, format, opts)
	return out.String(), err
}

const templateDataCounts = "{{ .TotalFiles }} {{ .TotalChanges }} {{ .DiagnosticsCount }}"

func TestMigrateLintCounts_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		files    fstest.MapFS
		versions []int64
		want     string
	}{
		{name: "one analyzed file with one statement", files: templateDataTwoFiles(), versions: []int64{2}, want: "1 1 0"},
		{name: "two analyzed files, one with two statements", files: templateDataThreeFiles(), versions: []int64{2, 3}, want: "2 3 0"},
		{name: "three diagnostics in two files", files: templateDataDestructive(), versions: []int64{2, 3}, want: "2 3 3"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, test.files, migrationlint.Options{Dialect: "sqlite"}, test.versions...)

			got, err := renderTemplateData(c, templateDataCounts, atlasreport.MigrateLintOptions{Analysis: &analysis})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestMigrateLintCounts_IntegrityFailureCountsNoMigrationFile: the atlas.sum
// entry is a file entry and not a migration, so the community binary prints
// 0 files beside one entry.
func TestMigrateLintCounts_IntegrityFailureCountsNoMigrationFile(t *testing.T) {
	c := qt.New(t)

	got, err := renderTemplateData(c, templateDataCounts+" {{ len .Files }}", atlasreport.MigrateLintOptions{
		Integrity: atlasreport.MigrateLintIntegrity{Checked: true, Error: "checksum mismatch"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "0 0 0 1")
}

// TestMigrateLintCounts_ReplayFailureCountsTheFailedFile: the file a failed
// replay stopped at is counted, and it carries no change.
func TestMigrateLintCounts_ReplayFailureCountsTheFailedFile(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeTemplateData(c, templateDataTwoFiles(), migrationlint.Options{Dialect: "sqlite"}, 2)

	got, err := renderTemplateData(c, templateDataCounts, atlasreport.MigrateLintOptions{
		Analysis:      &analysis,
		ReplayFailure: &atlasreport.MigrateLintReplayFailure{File: "2.sql", Error: "executing statement: " + replayAnswer},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "1 0 0")
}

func TestMigrateLintFileLine_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   string
	}{
		{name: "the first byte is on line 1", format: "{{ range .Files }}{{ .Line 0 }};{{ end }}", want: "1;1;"},
		{name: "the end of the text is accepted", format: "{{ range .Files }}{{ .Line (len .Text) }};{{ end }}", want: "4;3;"},
		{
			name: "a diagnostic's position is the line it reports",
			format: "{{ range .Files }}{{ $f := . }}{{ range .Reports }}{{ range .Diagnostics }}" +
				"{{ $f.Name }}:{{ $f.Line .Pos }};{{ end }}{{ end }}{{ end }}",
			want: "2.sql:3;3.sql:1;3.sql:2;",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, templateDataDestructive(), migrationlint.Options{Dialect: "sqlite"}, 2, 3)

			got, err := renderTemplateData(c, test.format, atlasreport.MigrateLintOptions{Analysis: &analysis})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestMigrateLintFileLine_FailurePath: an offset outside the text fails the
// template, as it does on the community binary, and prints nothing.
func TestMigrateLintFileLine_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		pos  int
		want string
	}{
		{name: "a negative offset", pos: -1, want: `position -1 is outside the 27 bytes of 2\.sql`},
		{name: "an offset past the end", pos: 28, want: `position 28 is outside the 27 bytes of 2\.sql`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, templateDataDestructive(), migrationlint.Options{Dialect: "sqlite"}, 2)

			got, err := renderTemplateData(c, fmt.Sprintf("{{ range .Files }}{{ .Line %d }}{{ end }}", test.pos),
				atlasreport.MigrateLintOptions{Analysis: &analysis})

			c.Assert(err, qt.ErrorMatches, `(?s).*error calling Line: `+test.want)
			c.Assert(got, qt.Equals, "")
		})
	}
}

func TestMigrateLintFileSuggestedFixes_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		files    fstest.MapFS
		versions []int64
		format   string
		want     string
	}{
		{
			name: "a file with no fix prints an empty list", files: templateDataTwoFiles(), versions: []int64{2},
			format: "{{ range .Files }}{{ .SuggestedFixes }}{{ end }}", want: "[]",
		},
		{
			name: "a file with no fix encodes null", files: templateDataTwoFiles(), versions: []int64{2},
			format: "{{ range .Files }}{{ json .SuggestedFixes }}{{ end }}", want: "null",
		},
		{
			name: "a fix prints with its empty text edit", files: templateDataDestructive(), versions: []int64{2},
			format: "{{ range .Files }}{{ .SuggestedFixes }}{{ end }}",
			want:   `[{Add a pre-migration check to ensure table "t2" is empty before dropping it <nil>}]`,
		},
		{
			name: "the fixes of every diagnostic, in order", files: templateDataDestructive(), versions: []int64{3},
			format: "{{ range .Files }}{{ json .SuggestedFixes }}{{ end }}",
			want: `[{"Message":"Add a pre-migration check to ensure column \"a\" is NULL before dropping it"},` +
				`{"Message":"Add a pre-migration check to ensure table \"t3\" is empty before dropping it"}]`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, test.files, migrationlint.Options{Dialect: "sqlite"}, test.versions...)

			got, err := renderTemplateData(c, test.format, atlasreport.MigrateLintOptions{Analysis: &analysis})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestMigrateLintURL_HappyPath: the field evaluates, empty, and JSON leaves it
// out, which is what the community binary prints with no published report.
func TestMigrateLintURL_HappyPath(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeTemplateData(c, templateDataTwoFiles(), migrationlint.Options{Dialect: "sqlite"}, 2)

	got, err := renderTemplateData(c, "[{{ .URL }}]{{ json . }}", atlasreport.MigrateLintOptions{Analysis: &analysis})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Matches, `\[\]\{"Env":.*`)
}

func TestMigrateLintFileError_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		files    fstest.MapFS
		versions []int64
		dialect  string
		rules    map[string]migrationlint.RuleConfig
		want     string
	}{
		{
			name: "no finding", files: templateDataTwoFiles(), versions: []int64{2}, dialect: "sqlite",
			want: "[]|[]",
		},
		{
			name: "a destructive change is an analyzer error", files: templateDataDestructive(), versions: []int64{2}, dialect: "sqlite",
			want: "[destructive changes detected]|[destructive changes detected]",
		},
		{
			name: "a warning is not an error",
			files: fstest.MapFS{
				"1.sql": {Data: []byte("CREATE TABLE users (id int, nick text);\n")},
				"2.sql": {Data: []byte("ALTER TABLE users ADD COLUMN x int NOT NULL;\nALTER TABLE users DROP COLUMN nick;\n")},
			},
			versions: []int64{2}, dialect: "postgres",
			want: "[destructive changes detected]|[destructive changes detected]",
		},
		{
			name: "two failing analyzers, in report order",
			files: fstest.MapFS{
				"1.sql": {Data: []byte("CREATE TABLE users (id int, nick text);\n")},
				"2.sql": {Data: []byte("ALTER TABLE users ADD COLUMN x int NOT NULL;\nALTER TABLE users DROP COLUMN nick;\n")},
			},
			versions: []int64{2}, dialect: "postgres",
			rules: map[string]migrationlint.RuleConfig{"DD101": {Severity: migrationlint.SeverityError}},
			want:  "[destructive changes detected; data dependent changes detected]|[destructive changes detected; data dependent changes detected]",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, test.files,
				migrationlint.Options{Dialect: test.dialect, RuleConfigs: test.rules}, test.versions...)

			got, err := renderTemplateData(c,
				"{{ range .Files }}[{{ .Error }}]{{ end }}|{{ range .Steps }}{{ with .Result }}[{{ .Error }}]{{ end }}{{ end }}",
				atlasreport.MigrateLintOptions{Analysis: &analysis})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestMigrateLintAnalyzeStep_CountsReports: the step counts the reports, one
// per analyzer, not the diagnostics. Version 3 holds two destructive
// diagnostics in one report.
func TestMigrateLintAnalyzeStep_CountsReports(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeTemplateData(c, templateDataDestructive(), migrationlint.Options{Dialect: "sqlite"}, 2, 3)

	got, err := renderTemplateData(c, "{{ range .Steps }}{{ .Text }};{{ end }}", atlasreport.MigrateLintOptions{Analysis: &analysis})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "Found 2 new migration files (from 3 total);Loaded 2 changes on dev database;"+
		"1 reports were found in analysis;1 reports were found in analysis;")
}

// templateDataSchema is the schema the report receives, framed the way Ptah's
// HCL renderer writes a document: the generated-code marker, a blank line,
// and blocks separated by blank lines.
var templateDataSchema = atlasreport.MigrateLintSchema{
	Current: "// Code generated by ptah; DO NOT EDIT.\n\nschema \"main\" {\n}\n\n",
	Desired: "// Code generated by ptah; DO NOT EDIT.\n\nschema \"main\" {\n}\n\ntable \"t2\" {\n  schema = schema.main\n}\n\n",
}

// TestMigrateLintSchema_HappyPath: a finished replay reports both documents
// without the marker and with one trailing line feed, as `schema inspect`
// frames them on this surface.
func TestMigrateLintSchema_HappyPath(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeTemplateData(c, templateDataTwoFiles(), migrationlint.Options{Dialect: "sqlite"}, 2)

	got, err := renderTemplateData(c, "{{ json .Schema }}", atlasreport.MigrateLintOptions{
		Analysis: &analysis,
		Schema:   templateDataSchema,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, `{"Current":"schema \"main\" {\n}\n","Desired":"schema \"main\" {\n}\n\ntable \"t2\" {\n  schema = schema.main\n}\n"}`)
}

// TestMigrateLintSchema_EmptyWhenTheReplayDidNotFinish: the community binary
// writes the schema only after a replay finishes, and prints `{}` otherwise.
// Each row hands the report a schema it must not carry.
func TestMigrateLintSchema_EmptyWhenTheReplayDidNotFinish(t *testing.T) {
	tests := []struct {
		name          string
		integrity     atlasreport.MigrateLintIntegrity
		replayFailure *atlasreport.MigrateLintReplayFailure
		errText       string
	}{
		{name: "an integrity failure", integrity: atlasreport.MigrateLintIntegrity{Checked: true, Error: "checksum mismatch"}},
		{
			name:          "a replay that failed inside a migration",
			replayFailure: &atlasreport.MigrateLintReplayFailure{File: "2.sql", Error: "executing statement: " + replayAnswer},
			errText:       "error validating migration SQL on dev database: the whole chain",
		},
		{name: "a replay that failed outside any migration", errText: "connect: refused"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, templateDataTwoFiles(), migrationlint.Options{Dialect: "sqlite"}, 2)

			got, err := renderTemplateData(c, "{{ json .Schema }}|{{ if .Schema.Current }}current{{ end }}", atlasreport.MigrateLintOptions{
				Analysis:      &analysis,
				Integrity:     test.integrity,
				ReplayFailure: test.replayFailure,
				Error:         test.errText,
				Schema:        templateDataSchema,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, "{}|")
		})
	}
}

// TestMigrateLintFileNames_HappyPath: a converted layout's files are named as
// the directory spells them, in the file entries and in the step names.
func TestMigrateLintFileNames_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		names map[int64]string
		want  string
	}{
		{name: "renamed", names: map[int64]string{2: "V1.5__b.sql"}, want: "V1.5__b.sql|Analyze V1.5__b.sql;"},
		{name: "no name for the key", names: map[int64]string{1: "V1__a.sql"}, want: "2.sql|Analyze 2.sql;"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, templateDataTwoFiles(), migrationlint.Options{Dialect: "sqlite"}, 2)

			got, err := renderTemplateData(c,
				"{{ range .Files }}{{ .Name }}{{ end }}|{{ range .Steps }}{{ if .Result }}{{ .Name }};{{ end }}{{ end }}",
				atlasreport.MigrateLintOptions{Analysis: &analysis, FileNames: test.names})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// TestMigrateLintFileNames_ReplayFailure: the file a failed replay stopped at
// is named as the directory spells it too.
func TestMigrateLintFileNames_ReplayFailure(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeTemplateData(c, templateDataTwoFiles(), migrationlint.Options{Dialect: "sqlite"}, 2)
	failure := atlasreport.NewMigrateLintReplayFailure(analysis, nil, lintReplayError(&migrationreplay.MigrationError{
		Version:    2,
		VersionKey: "2",
		Statement: &migrationreplay.FailedStatement{
			SQL:     "CREATE TABLE t2 (id int)",
			Written: "CREATE TABLE t2 (id int);",
			Index:   1,
			Answer:  errors.New(replayAnswer),
		},
	}))
	c.Assert(failure, qt.IsNotNil)

	got, err := renderTemplateData(c, "{{ range .Files }}{{ .Name }}{{ end }}", atlasreport.MigrateLintOptions{
		Analysis:      &analysis,
		ReplayFailure: failure,
		FileNames:     map[int64]string{2: "V1.5__b.sql"},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "V1.5__b.sql")
}
