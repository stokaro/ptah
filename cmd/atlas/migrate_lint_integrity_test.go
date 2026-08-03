package atlas_test

import (
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/internal/exitcode"
)

// TestCompatCommand_MigrateLintIntegrityFindingIsReportContent pins the one
// stderr line on the compat surface that carries no diagnostic prefix, so the
// exception recorded in docs/exit_codes.md stays a measured fact rather than a
// remembered one.
//
// This is a characterization pin, not a regression pin for stokaro/ptah#1019:
// the behavior predates that change and the rows are green without it. It
// exists because the #1019 rule ("every process-level diagnostic on this
// surface is prefixed `Error: `") is only true once this line is classified
// out of that set, and the classification is what the two rows measure. The
// `--format` row is the evidence: the identical `checksum mismatch` text is
// report content, rendered into the lint report on stdout, and the default
// text report merely spills it to stderr instead of embedding it. A future
// change that prefixes this line would be changing a report body, and should
// have to say so here.
//
// The stream itself is a known divergence and is not endorsed by this test.
func TestCompatCommand_MigrateLintIntegrityFindingIsReportContent(t *testing.T) {
	tests := []struct {
		name       string
		formatArgs []string
		wantStdout string
		wantStderr string
	}{
		{
			name:       "default text report spills the finding to stderr unprefixed",
			formatArgs: nil,
			wantStdout: "",
			wantStderr: "checksum mismatch\n",
		},
		{
			name:       "format report renders the same finding to stdout",
			formatArgs: []string{"--format", "{{ with .Steps }}{{ range . }}{{ .Error }}{{ end }}{{ end }}"},
			wantStdout: "checksum mismatch",
			wantStderr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := t.TempDir()
			devDB := "sqlite://" + filepath.Join(t.TempDir(), "integrity.db")
			writeAtlasLintFile(c, dir, "20240101000000_init.sql", "CREATE TABLE t1 (id integer);\n")
			writeAtlasApplyProjectSum(c, dir)
			// Edit the hashed file so the recorded sum no longer matches it.
			writeAtlasLintFile(c, dir, "20240101000000_init.sql", "CREATE TABLE t1 (id integer, extra text);\n")

			args := append([]string{"migrate", "lint", "--dir", dir, "--dev-url", devDB, "--latest", "1"}, tt.formatArgs...)
			stdout, stderr, err := runAtlasMigrateLint(c, args...)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(err, qt.ErrorMatches, "checksum mismatch")
			c.Assert(stdout, qt.Equals, tt.wantStdout)
			c.Assert(stderr, qt.Equals, tt.wantStderr)
		})
	}
}
