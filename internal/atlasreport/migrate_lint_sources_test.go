package atlasreport_test

import (
	"bytes"
	"errors"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/atlasreport"
	"ptah.run/internal/migrationreplay"
	migrationlint "ptah.run/migration/lint"
)

// convertedDrop is a converted Liquibase directory: version 2 is what the
// conversion of the source file [liquibaseDropSource] wrote, which drops t2 on
// line 2 where the source drops it on line 4.
func convertedDrop() fstest.MapFS {
	return fstest.MapFS{
		"1.sql": {Data: []byte("CREATE TABLE t2 (id int);\n")},
		"2.sql": {Data: []byte("--changeset a:2\nDROP TABLE t2;\n")},
	}
}

// liquibaseDropSource is the source file version 2 of [convertedDrop] was
// converted from.
var liquibaseDropSource = atlasreport.MigrateLintSource{
	Name:  "2_drop.sql",
	Text:  "--liquibase formatted sql\n\n--changeset a:2\nDROP TABLE t2;\n",
	Lines: []int{3, 4},
}

// The template data of a converted file is the source file's: its name, its
// text, and each diagnostic's position in that text, which the pinned community
// binary reports for the same Liquibase directory as `2_drop.sql`, the
// file as read and offset 43 (line 4). A name FileNames gives wins over the
// source file's.
func TestMigrateLintSources_TemplateData(t *testing.T) {
	const format = `{{ range .Files }}{{ $f := . }}{{ .Name }}|{{ printf "%q" .Text }}` +
		`{{ range .Reports }}{{ range .Diagnostics }}|{{ .Pos }}|{{ $f.Line .Pos }}{{ end }}{{ end }}{{ end }}`
	tests := []struct {
		name    string
		names   map[int64]string
		sources map[int64]atlasreport.MigrateLintSource
		want    string
	}{
		{
			name: "a source file", sources: map[int64]atlasreport.MigrateLintSource{2: liquibaseDropSource},
			want: `2_drop.sql|"--liquibase formatted sql\n\n--changeset a:2\nDROP TABLE t2;\n"|43|4`,
		},
		{
			name: "a source file FileNames names", names: map[int64]string{2: "V2__drop.sql"},
			sources: map[int64]atlasreport.MigrateLintSource{2: liquibaseDropSource},
			want:    `V2__drop.sql|"--liquibase formatted sql\n\n--changeset a:2\nDROP TABLE t2;\n"|43|4`,
		},
		{
			name: "no source for the key", sources: map[int64]atlasreport.MigrateLintSource{1: liquibaseDropSource},
			want: `2.sql|"--changeset a:2\nDROP TABLE t2;\n"|16|2`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, convertedDrop(), migrationlint.Options{Dialect: "sqlite"}, 2)

			got, err := renderTemplateData(c, format, atlasreport.MigrateLintOptions{
				Analysis: &analysis, FileNames: test.names, Sources: test.sources,
			})

			c.Assert(err, qt.IsNil)
			c.Assert(got, qt.Equals, test.want)
		})
	}
}

// The text report places a finding in a converted file on its source line,
// as the pinned community binary prints `-- L4:` for the same directory.
func TestMigrateLintSources_TextLine(t *testing.T) {
	tests := []struct {
		name    string
		sources map[int64]atlasreport.MigrateLintSource
		want    string
	}{
		{
			name: "a source file", sources: map[int64]atlasreport.MigrateLintSource{2: liquibaseDropSource},
			want: "      -- L4: Dropping table \"t2\" https://atlasgo.io/lint/analyzers#DS102\n",
		},
		{
			name: "no source", sources: nil,
			want: "      -- L2: Dropping table \"t2\" https://atlasgo.io/lint/analyzers#DS102\n",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			analysis := analyzeTemplateData(c, convertedDrop(), migrationlint.Options{Dialect: "sqlite"}, 2)
			var out bytes.Buffer

			err := atlasreport.WriteMigrateLintText(&out, atlasreport.MigrateLintOptions{AtlasCodes: true, Analysis: &analysis, Sources: test.sources})

			c.Assert(err, qt.IsNil)
			c.Assert(out.String(), qt.Contains, test.want)
		})
	}
}

// The file a failed replay stopped at is named as its source file.
func TestMigrateLintSources_ReplayFailureName(t *testing.T) {
	c := qt.New(t)
	analysis := analyzeTemplateData(c, convertedDrop(), migrationlint.Options{Dialect: "sqlite"}, 2)
	failure := atlasreport.NewMigrateLintReplayFailure(analysis, nil, lintReplayError(&migrationreplay.MigrationError{
		Version:    2,
		VersionKey: "2",
		Statement: &migrationreplay.FailedStatement{
			SQL:     "DROP TABLE t2",
			Written: "DROP TABLE t2;",
			Index:   1,
			Answer:  errors.New(replayAnswer),
		},
	}))
	c.Assert(failure, qt.IsNotNil)

	got, err := renderTemplateData(c, "{{ range .Files }}{{ .Name }}{{ end }}", atlasreport.MigrateLintOptions{
		Analysis:      &analysis,
		ReplayFailure: failure,
		Sources:       map[int64]atlasreport.MigrateLintSource{2: liquibaseDropSource},
	})

	c.Assert(err, qt.IsNil)
	c.Assert(got, qt.Equals, "2_drop.sql")
}
