package atlas_test

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/cmd/atlas"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/exitcode"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/migratesum"
)

// These tests pin `ptah-compat migrate hash` and `migrate validate` against
// Atlas CE v1.2.0, measured through the pinned binary at
// ptah-atlas-conformance/bin/atlas on 2026-08-02 (stokaro/ptah#973, #983).
//
// CE accepts a foreign tool's directory layout on both verbs under two
// spellings — `--dir file://d?format=goose` and `--dir file://d --dir-format
// goose` — and writes a byte-identical atlas.sum from either. Where they
// disagree the query wins, measured in both directions:
//
//	$ atlas migrate hash --dir 'file://d?format=goose'  --dir-format flyway
//	  -> 1_init.sql U1__undo.sql V1__x.sql        (goose covered the set)
//	$ atlas migrate hash --dir 'file://d?format=flyway' --dir-format goose
//	  -> V1__x.sql                                (flyway covered the set)
//
// The per-format covered sets below are the ones the corpus in
// internal/atlasmigrateimport/testdata/ce-sums captures from the same binary.

// integrityFixture is one layout every supported format reads differently, so
// no assertion about a format can pass by accident on another format's rule.
var integrityFixture = map[string]string{
	"1_init.sql":         "-- +goose Up\nCREATE TABLE a (id int);\n",
	"2_more.up.sql":      "-- +goose Up\nCREATE TABLE b (id int);\n",
	"2_more.down.sql":    "DROP TABLE b;\n",
	"V1__x.sql":          "CREATE TABLE v (id int);\n",
	"V10__y.sql":         "CREATE TABLE v2 (id int);\n",
	"U1__undo.sql":       "DROP TABLE v;\n",
	"R__view.sql":        "CREATE VIEW r AS SELECT 1;\n",
	"B0__base.sql":       "CREATE TABLE base (id int);\n",
	"sub/V2__nested.sql": "CREATE TABLE nested (id int);\n",
	"notes.txt":          "not sql at all\n",
}

// Covered sets measured from the pinned CE binary over integrityFixture.
var (
	sqlSuffixCoveredSet = []string{
		"1_init.sql",
		"2_more.down.sql",
		"2_more.up.sql",
		"B0__base.sql",
		"R__view.sql",
		"U1__undo.sql",
		"V10__y.sql",
		"V1__x.sql",
	}
	golangMigrateCoveredSet = []string{"2_more.up.sql"}
	flywayCoveredSet        = []string{
		"B0__base.sql",
		"V1__x.sql",
		"sub/V2__nested.sql",
		"V10__y.sql",
		"R__view.sql",
	}
)

// runCompatExit runs the compat command tree and applies the same exit-code
// normalization the ptah-compat binary applies, so these tests observe the exit
// codes and stderr a script would.
func runCompatExit(args ...string) (stdout, stderr string, err error) {
	cmd := atlas.NewCompatCommand("atlas")
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)
	executed, execErr := cmd.ExecuteC()
	normalizedErr := cmdutil.NormalizeCommandError(executed, execErr, 2)
	return out.String(), errOut.String(), normalizedErr
}

func errorChainContainsText(err error, want string) bool {
	if err == nil {
		return false
	}
	if err.Error() == want {
		return true
	}
	if multiErr, ok := err.(interface{ Unwrap() []error }); ok {
		for _, child := range multiErr.Unwrap() {
			if errorChainContainsText(child, want) {
				return true
			}
		}
	}
	return errorChainContainsText(errors.Unwrap(err), want)
}

func writeIntegrityFixture(tb testing.TB) string {
	c := qt.New(tb)
	c.Helper()
	dir := c.TempDir()
	for name, content := range integrityFixture {
		path := filepath.Join(dir, filepath.FromSlash(name))
		c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
		c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
	}
	return dir
}

// sumFileExists reports whether the directory carries an atlas.sum, so a
// refusal can be checked to have written nothing rather than only to have
// returned an error.
func sumFileExists(tb testing.TB, dir string) bool {
	c := qt.New(tb)
	c.Helper()
	_, err := os.Stat(filepath.Join(dir, migratesum.AtlasFileName))
	return err == nil
}

func readSumFile(tb testing.TB, dir string) *migratesum.SumFile {
	c := qt.New(tb)
	c.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	sum, err := migratesum.Parse(raw)
	c.Assert(err, qt.IsNil)
	return sum
}

func sumEntryNames(tb testing.TB, dir string) []string {
	c := qt.New(tb)
	c.Helper()
	names := make([]string, 0)
	for _, entry := range readSumFile(c.TB, dir).Entries {
		names = append(names, entry.Name)
	}
	return names
}

func sumBytes(tb testing.TB, dir string) string {
	c := qt.New(tb)
	c.Helper()
	raw, err := os.ReadFile(filepath.Join(dir, migratesum.AtlasFileName))
	c.Assert(err, qt.IsNil)
	return string(raw)
}

// TestCompatMigrateHashSourceFormat_HappyPath covers the file set each source
// layout selects. The fixture is read differently by every rule, so a format
// that was ignored would produce another format's names here.
func TestCompatMigrateHashSourceFormat_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		want   []string
	}{
		{name: "goose", format: "goose", want: sqlSuffixCoveredSet},
		{name: "dbmate", format: "dbmate", want: sqlSuffixCoveredSet},
		{name: "liquibase", format: "liquibase", want: sqlSuffixCoveredSet},
		{name: "atlas", format: "atlas", want: sqlSuffixCoveredSet},
		{name: "golang_migrate", format: "golang-migrate", want: golangMigrateCoveredSet},
		{name: "flyway", format: "flyway", want: flywayCoveredSet},
	}

	for _, tt := range tests {
		t.Run("query_"+tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			stdout, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format="+tt.format)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
			c.Assert(sumEntryNames(c.TB, dir), qt.DeepEquals, tt.want)
		})
		t.Run("flag_"+tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			stdout, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir, "--dir-format", tt.format)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
			c.Assert(sumEntryNames(c.TB, dir), qt.DeepEquals, tt.want)
		})
	}
}

// TestCompatMigrateHashSpellingsAgree_HappyPath is the invariant #983 exists
// for: the two spellings Atlas accepts are the same instruction, so they must
// produce the same bytes, not merely both succeed.
func TestCompatMigrateHashSpellingsAgree_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
	}{
		{name: "atlas", format: "atlas"},
		{name: "goose", format: "goose"},
		{name: "dbmate", format: "dbmate"},
		{name: "liquibase", format: "liquibase"},
		{name: "golang_migrate", format: "golang-migrate"},
		{name: "flyway", format: "flyway"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			queryDir := writeIntegrityFixture(c.TB)
			flagDir := writeIntegrityFixture(c.TB)

			queryOut, _, queryErr := runCompatExit("migrate", "hash", "--dir", "file://"+queryDir+"?format="+tt.format)
			flagOut, _, flagErr := runCompatExit("migrate", "hash", "--dir", "file://"+flagDir, "--dir-format", tt.format)

			c.Assert(queryErr, qt.IsNil, qt.Commentf("output:\n%s", queryOut))
			c.Assert(flagErr, qt.IsNil, qt.Commentf("output:\n%s", flagOut))
			c.Assert(sumBytes(c.TB, flagDir), qt.Equals, sumBytes(c.TB, queryDir))
		})
	}
}

// TestCompatMigrateSourceFormatPrecedence_HappyPath pins which spelling wins
// when the two disagree. Both directions are measured because a resolver that
// always preferred the flag, and one that always preferred the query, agree on
// every input where the two name the same format.
func TestCompatMigrateSourceFormatPrecedence_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		query string
		flag  string
		want  []string
	}{
		{name: "goose_query_beats_flyway_flag", query: "goose", flag: "flyway", want: sqlSuffixCoveredSet},
		{name: "flyway_query_beats_goose_flag", query: "flyway", flag: "goose", want: flywayCoveredSet},
		{name: "golang_migrate_query_beats_goose_flag", query: "golang-migrate", flag: "goose", want: golangMigrateCoveredSet},
		// An empty query value selects the atlas layout and still outranks a
		// non-empty flag: the query is authoritative whenever it is present.
		{name: "empty_query_beats_flyway_flag", query: "", flag: "flyway", want: sqlSuffixCoveredSet},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			stdout, _, err := runCompatExit(
				"migrate", "hash",
				"--dir", "file://"+dir+"?format="+tt.query,
				"--dir-format", tt.flag,
			)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
			c.Assert(sumEntryNames(c.TB, dir), qt.DeepEquals, tt.want)
		})
	}
}

// TestCompatMigrateHashAtlasLayoutUnmoved_HappyPath holds the native path
// exactly where it was: naming the atlas layout through the query, through the
// flag, or not at all must produce one sum and one stdout. The query spellings
// reach the native command through an argument rewrite, so this is what catches
// the rewrite dropping or mangling a value.
func TestCompatMigrateHashAtlasLayoutUnmoved_HappyPath(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		args func(dir string) []string
	}{
		{
			name: "no_format",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir} },
		},
		{
			name: "flag_atlas",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir, "--dir-format", "atlas"} },
		},
		{
			name: "query_atlas",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir + "?format=atlas"} },
		},
		{
			name: "query_atlas_inline_dir",
			args: func(dir string) []string { return []string{"--dir=file://" + dir + "?format=atlas"} },
		},
		{
			name: "query_empty",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir + "?format="} },
		},
		{
			// The query wins, so the goose flag beside it is irrelevant rather
			// than an error: the rewrite replaces it instead of appending.
			name: "query_atlas_overrides_goose_flag",
			args: func(dir string) []string {
				return []string{"--dir", "file://" + dir + "?format=atlas", "--dir-format", "goose"}
			},
		},
		{
			name: "query_atlas_overrides_inline_goose_flag",
			args: func(dir string) []string {
				return []string{"--dir=file://" + dir + "?format=atlas", "--dir-format=goose"}
			},
		},
	}

	baseline := writeIntegrityFixture(c.TB)
	baselineOut, baselineErrOut, err := runCompatExit("migrate", "hash", "--dir", "file://"+baseline)
	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", baselineOut, baselineErrOut))
	c.Assert(baselineOut, qt.Equals, "")
	c.Assert(baselineErrOut, qt.Equals, "")
	wantSum := sumBytes(c.TB, baseline)

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			stdout, stderr, err := runCompatExit(append([]string{"migrate", "hash"}, tt.args(dir)...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
			c.Assert(sumBytes(c.TB, dir), qt.Equals, wantSum)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
		})
	}
}

// TestCompatMigrateHashConvertedDirOutput_HappyPath pins Atlas CE's silent
// success contract for converted migration directory formats.
func TestCompatMigrateHashConvertedDirOutput_HappyPath(t *testing.T) {
	c := qt.New(t)
	dir := writeIntegrityFixture(c.TB)

	stdout, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=golang-migrate")

	c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "")
	c.Assert(sumEntryNames(c.TB, dir), qt.DeepEquals, golangMigrateCoveredSet)
}

// TestCompatMigrateHashEmptySourceSet_HappyPath covers the seam with the apply
// gate (#973 PR 3): an empty covered set is not an error on either verb, so
// LoadFS — and with it #980's "no importable migration files found" — is never
// reached from the integrity path. Atlas CE writes the same empty-set sum.
func TestCompatMigrateHashEmptySourceSet_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
		files  map[string]string
	}{
		{name: "empty_directory", format: "goose", files: nil},
		{name: "no_sql_files", format: "goose", files: map[string]string{"readme.txt": "nope\n"}},
		{name: "subdirectory_only", format: "goose", files: map[string]string{"sub/1_init.sql": "CREATE TABLE s (id int);\n"}},
		{name: "golang_migrate_down_only", format: "golang-migrate", files: map[string]string{"1_init.down.sql": "DROP TABLE d;\n"}},
		{name: "flyway_undo_only", format: "flyway", files: map[string]string{"U1__undo.sql": "DROP TABLE u;\n"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			for name, content := range tt.files {
				path := filepath.Join(dir, filepath.FromSlash(name))
				c.Assert(os.MkdirAll(filepath.Dir(path), 0o755), qt.IsNil)
				c.Assert(os.WriteFile(path, []byte(content), 0o600), qt.IsNil)
			}

			stdout, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format="+tt.format)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
			c.Assert(stdout, qt.Equals, "")
			// Measured CE empty-set sum, identical across formats.
			c.Assert(sumBytes(c.TB, dir), qt.Equals, "h1:47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=\n")

			validateOut, validateErrOut, validateErr := runCompatExit(
				"migrate", "validate", "--dir", "file://"+dir+"?format="+tt.format)

			c.Assert(validateErr, qt.IsNil)
			c.Assert(validateOut, qt.Equals, "")
			c.Assert(validateErrOut, qt.Equals, "")
		})
	}
}

// TestCompatMigrateValidateSourceFormat_HappyPath covers a clean converted
// directory on both spellings, and the covered-set rule seen from the
// verification side: golang-migrate never hashes the down file of a pair, so
// editing it leaves the directory clean where editing the up file does not
// (see the FailurePath twin).
func TestCompatMigrateValidateSourceFormat_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		format string
	}{
		{name: "goose", format: "goose"},
		{name: "dbmate", format: "dbmate"},
		{name: "liquibase", format: "liquibase"},
		{name: "golang_migrate", format: "golang-migrate"},
		{name: "flyway", format: "flyway"},
	}

	for _, tt := range tests {
		t.Run("query_"+tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)
			_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format="+tt.format)
			c.Assert(err, qt.IsNil)

			stdout, stderr, err := runCompatExit("migrate", "validate", "--dir", "file://"+dir+"?format="+tt.format)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
		})
		t.Run("flag_"+tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)
			_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir, "--dir-format", tt.format)
			c.Assert(err, qt.IsNil)

			stdout, stderr, err := runCompatExit("migrate", "validate", "--dir", "file://"+dir, "--dir-format", tt.format)

			c.Assert(err, qt.IsNil)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, "")
		})
	}

	t.Run("golang_migrate_down_file_is_outside_the_covered_set", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c.TB)
		_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=golang-migrate")
		c.Assert(err, qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "2_more.down.sql"), []byte("DROP TABLE b CASCADE;\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runCompatExit("migrate", "validate", "--dir", "file://"+dir+"?format=golang-migrate")

		c.Assert(err, qt.IsNil)
		c.Assert(stdout, qt.Equals, "")
		c.Assert(stderr, qt.Equals, "")
	})
}

// TestCompatMigrateValidateSourceFormat_FailurePath covers the refusals. Both
// are rendered by the helpers the native Atlas path and the apply-time gate
// use, so all three surfaces stay byte-identical, and the mismatch line names
// the SOURCE file rather than a converted name.
func TestCompatMigrateValidateSourceFormat_FailurePath(t *testing.T) {
	t.Run("unhashed converted directory", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c.TB)

		stdout, stderr, err := runCompatExit("migrate", "validate", "--dir", "file://"+dir+"?format=goose")

		c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+
			"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
		c.Assert(stderr, qt.Equals, "Error: checksum file not found\n")
	})

	tampered := []struct {
		name   string
		format string
		file   string
		want   string
	}{
		{name: "goose", format: "goose", file: "1_init.sql", want: "\n\tL2: 1_init.sql was edited\n\n"},
		{name: "golang_migrate", format: "golang-migrate", file: "2_more.up.sql", want: "\n\tL2: 2_more.up.sql was edited\n\n"},
		{name: "flyway", format: "flyway", file: "V1__x.sql", want: "\n\tL3: V1__x.sql was edited\n\n"},
	}

	for _, tt := range tampered {
		t.Run("tampered "+tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)
			_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format="+tt.format)
			c.Assert(err, qt.IsNil)
			c.Assert(os.WriteFile(filepath.Join(dir, tt.file), []byte("CREATE TABLE pwned (id int);\n"), 0o600), qt.IsNil)

			stdout, stderr, err := runCompatExit("migrate", "validate", "--dir", "file://"+dir+"?format="+tt.format)

			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+tt.want+
				"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
			c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
		})
	}

	t.Run("malformed sum file", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c.TB)
		_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=goose")
		c.Assert(err, qt.IsNil)
		c.Assert(os.WriteFile(filepath.Join(dir, "atlas.sum"), []byte("not a sum file\n"), 0o600), qt.IsNil)

		stdout, stderr, err := runCompatExit("migrate", "validate", "--dir", "file://"+dir+"?format=goose")

		c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
		c.Assert(stdout, qt.Equals, "You have a checksum error in your migration directory.\n"+
			"Please check your migration files and run 'atlas migrate hash' to re-hash the contents\n\n")
		c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
	})

	t.Run("a directory hashed as one layout does not verify as another", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c.TB)
		_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+"?format=flyway")
		c.Assert(err, qt.IsNil)

		stdout, stderr, err := runCompatExit("migrate", "validate", "--dir", "file://"+dir+"?format=goose")

		c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
		c.Assert(stdout, qt.Contains, "\tL2: 1_init.sql was added\n")
		c.Assert(stderr, qt.Equals, "Error: checksum mismatch\n")
	})
}

// TestCompatMigrateSourceFormat_FailurePathUnknownFormat covers the values CE
// refuses. CE matches format names case-sensitively and does not trim, so
// "GOOSE" and " goose " are unknown formats rather than goose.
func TestCompatMigrateSourceFormat_FailurePathUnknownFormat(t *testing.T) {
	tests := []struct {
		name string
		args func(dir string) []string
		want string
	}{
		{
			name: "unknown query value",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir + "?format=sqitch"} },
			want: `unknown dir format "sqitch"`,
		},
		{
			name: "unknown flag value",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir, "--dir-format", "sqitch"} },
			want: `unknown dir format "sqitch"`,
		},
		{
			name: "uppercase query value",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir + "?format=GOOSE"} },
			want: `unknown dir format "GOOSE"`,
		},
		{
			name: "uppercase flag value",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir, "--dir-format", "GOOSE"} },
			want: `unknown dir format "GOOSE"`,
		},
		{
			name: "uppercase atlas flag value",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir, "--dir-format", "ATLAS"} },
			want: `unknown dir format "ATLAS"`,
		},
		{
			name: "padded flag value",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir, "--dir-format", " goose "} },
			want: `unknown dir format " goose "`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			_, _, err := runCompatExit(append([]string{"migrate", "hash"}, tt.args(dir)...)...)

			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			_, statErr := os.Stat(filepath.Join(dir, migratesum.AtlasFileName))
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
		})
	}
}

func TestCompatMigrateIntegrityUnknownFormatPreservesSemanticErrorChain(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runCompatExit(
		"migrate", "validate",
		"--dir", "file://migrations",
		"--dir-format", " bogus ",
	)
	var unknownFormat *atlasmigrate.UnknownDirFormatError

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err.Error(), qt.Equals, `unknown dir format " bogus "`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: unknown dir format \" bogus \"\n")
	c.Assert(err, qt.ErrorAs, &unknownFormat)
	c.Assert(unknownFormat.Value, qt.Equals, " bogus ")
	c.Assert(errorChainContainsText(err,
		`atlas migrate validate --dir-format: unknown Atlas migration directory format " bogus ": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`),
		qt.IsTrue)
}

func TestCompatMigrateIntegrityUnknownFormatAdapterLeavesOtherErrorsUnchanged(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runCompatExit(
		"migrate", "hash",
		"--dir", "file://migrations?format=flyway;x=1",
	)
	const want = "atlas migrate hash --dir: parse migration directory URL query: invalid semicolon separator in query"
	var unknownFormat *atlasmigrate.UnknownDirFormatError

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err.Error(), qt.Equals, want)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: "+want+"\n")
	c.Assert(err, qt.Not(qt.ErrorAs), &unknownFormat)
}

// TestCompatMigrateNewUnknownFormatKeepsTheSemanticDiagnosticReachable is what
// is left of TestCompatMigrateNewUnknownFormatKeepsVerboseDiagnostic, and the
// rename records a decision that changed.
//
// That test pinned `migrate new` PRINTING the semantic wording, on the reading
// that the verb was outside the adaptation. It was not outside it — the pinned
// community binary v1.3.0 answers `unknown dir format "bogus"` on `migrate new`
// exactly as it does on `migrate hash` and `migrate validate`
// (stokaro/ptah#1235 cell 9.8), and the split existed only because the adapter
// was a block inside a wrapper this verb does not use.
//
// What survives is the half that is still true and is the reason the adapter is
// a display layer: the semantic diagnostic, with the list of accepted layouts,
// is still in the chain. The printed text is now pinned with every other verb's
// in TestCompatUnknownDirFormatIsTheSameStringOnEveryVerb.
func TestCompatMigrateNewUnknownFormatKeepsTheSemanticDiagnosticReachable(t *testing.T) {
	c := qt.New(t)

	stdout, stderr, err := runCompatExit(
		"migrate", "new", "demo",
		"--dir", "file://migrations",
		"--dir-format", "bogus",
	)
	var unknownFormat *atlasmigrate.UnknownDirFormatError

	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
	c.Assert(err.Error(), qt.Equals, `unknown dir format "bogus"`)
	c.Assert(stdout, qt.Equals, "")
	c.Assert(stderr, qt.Equals, "Error: unknown dir format \"bogus\"\n")
	c.Assert(err, qt.ErrorAs, &unknownFormat)
	c.Assert(unknownFormat.Value, qt.Equals, "bogus")
	c.Assert(errorChainContainsText(err,
		`atlas migrate new --dir-format: unknown Atlas migration directory format "bogus": expected atlas, golang-migrate, goose, flyway, liquibase, or dbmate`),
		qt.IsTrue)
}

// TestCompatMigrateSourceFormatQuery_HappyPath pins the two query-parsing rules
// that used to be refusals (stokaro/ptah#990 items 2 and 3, stokaro/ptah#1013
// section 2). Both were measured on the pinned community binary v1.3.0:
//
//	$ atlas migrate hash --dir 'file://d?format=flyway&other=1'      exit=0, flyway set
//	$ atlas migrate hash --dir 'file://d?other=1'                    exit=0, atlas set
//	$ atlas migrate hash --dir 'file://d?format=flyway&format=goose' exit=0, flyway set
//	$ atlas migrate hash --dir 'file://d?format=goose&format=flyway' exit=0, goose set
//
// Every row asserts the COVERED SET, not just the exit code. That is what makes
// them separable: integrityFixture reads differently under every format, so an
// implementation that merely ignored the whole query would write the atlas set
// and fail rows 1, 3 and 4, and one that took the last repeated value rather
// than the first would swap rows 3 and 4.
func TestCompatMigrateSourceFormatQuery_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{
			name:  "ignored parameter beside format keeps the format",
			query: "?format=flyway&other=1",
			want:  flywayCoveredSet,
		},
		{
			name:  "ignored parameter alone reads the atlas layout",
			query: "?other=1",
			want:  sqlSuffixCoveredSet,
		},
		{
			name:  "repeated format takes the first value",
			query: "?format=flyway&format=goose",
			want:  flywayCoveredSet,
		},
		{
			name:  "repeated format takes the first value in the other order",
			query: "?format=goose&format=flyway",
			want:  sqlSuffixCoveredSet,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			stdout, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+tt.query)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
			c.Assert(sumEntryNames(c.TB, dir), qt.DeepEquals, tt.want)
		})
	}
}

// TestCompatMigrateSourceFormat_FailurePathUnsupportedQuery pins the query
// shapes that stay refused once the two above were relaxed.
//
// The semicolon is a deliberate, recorded divergence rather than an oversight.
// Measured, `?format=flyway;x=1` exits 0 on the community binary but silently
// drops the WHOLE pair and reads the directory as the atlas layout — covering
// nine files where the caller asked for the five flyway covers. A semicolon
// there costs you the format you asked for, so refusing is the safe side
// (stokaro/ptah#990 item 6).
//
// The bogus value is the control that keeps the relaxation honest: query keys
// are ignored, but the format VALUE still goes through the strict verbatim
// parser, so an unknown layout is refused whether or not an ignored key rides
// along with it.
func TestCompatMigrateSourceFormat_FailurePathUnsupportedQuery(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  string
	}{
		{
			name:  "semicolon separator",
			query: "?format=flyway;x=1",
			want:  "atlas migrate hash --dir: parse migration directory URL query: invalid semicolon separator in query",
		},
		{
			name:  "unknown format value",
			query: "?format=totally-bogus",
			want:  `unknown dir format "totally-bogus"`,
		},
		{
			name:  "unknown format value beside an ignored key",
			query: "?format=totally-bogus&other=1",
			want:  `unknown dir format "totally-bogus"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			_, _, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir+tt.query)

			c.Assert(err, qt.ErrorMatches, tt.want)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			c.Assert(sumFileExists(c.TB, dir), qt.IsFalse)
		})
	}
}

// TestCompatMigrateIntegrityRepeatedDir_HappyPath covers a repeated --dir. Both
// tools take the last one, so the layout is whatever that one names.
//
// The FailurePath twin is the case the resolution deliberately does not chase:
// a query on a --dir that is later overridden. Only the authoritative --dir is
// parsed for a layout, so an earlier one keeps a query the forwarding mapper
// refuses, where Atlas CE ignores the overridden value entirely.
func TestCompatMigrateIntegrityRepeatedDir_HappyPath(t *testing.T) {
	t.Run("the last --dir names the directory and the layout", func(t *testing.T) {
		c := qt.New(t)
		first := writeIntegrityFixture(c.TB)
		last := writeIntegrityFixture(c.TB)

		stdout, _, err := runCompatExit("migrate", "hash",
			"--dir", "file://"+first,
			"--dir", "file://"+last+"?format=golang-migrate")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s", stdout))
		c.Assert(sumEntryNames(c.TB, last), qt.DeepEquals, golangMigrateCoveredSet)
		_, statErr := os.Stat(filepath.Join(first, migratesum.AtlasFileName))
		c.Assert(os.IsNotExist(statErr), qt.IsTrue)
	})
}

// TestCompatMigrateIntegrityRepeatedDir_FailurePath pins the overridden-query
// refusal described above.
//
//	$ atlas migrate hash --dir 'file://a?format=goose' --dir file://b   exit=0
//
// Ptah refuses instead. It is the same class as the other query refusals in
// stokaro/ptah#990: loud, unable to produce a wrong sum, and not worth a second
// notion of "which --dir counts" inside the resolver.
func TestCompatMigrateIntegrityRepeatedDir_FailurePath(t *testing.T) {
	c := qt.New(t)
	first := writeIntegrityFixture(c.TB)
	last := writeIntegrityFixture(c.TB)

	_, _, err := runCompatExit("migrate", "hash",
		"--dir", "file://"+first+"?format=goose",
		"--dir", "file://"+last)

	c.Assert(err, qt.ErrorMatches,
		"atlas migrate hash --dir: migration directory URL query parameters are not supported for this command")
	c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
}

// TestCompatMigrateIntegrityEmptyDirFormat_HappyPath pins an empty
// --dir-format as the atlas layout (stokaro/ptah#990 item 1). The community
// binary exits 0 and reads the directory as Atlas; Ptah refused it on all nine
// metadata verbs through one shared mapper, so it is relaxed on all of them at
// once.
//
// hash asserts the covered set rather than only the exit code: the empty value
// has to resolve to the ATLAS layout specifically, and on integrityFixture that
// set differs from every other format's.
func TestCompatMigrateIntegrityEmptyDirFormat_HappyPath(t *testing.T) {
	t.Run("hash writes the atlas covered set", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c.TB)

		stdout, stderr, err := runCompatExit("migrate", "hash", "--dir", "file://"+dir, "--dir-format", "")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
		c.Assert(sumEntryNames(c.TB, dir), qt.DeepEquals, sqlSuffixCoveredSet)
	})

	t.Run("validate accepts the directory hash wrote", func(t *testing.T) {
		c := qt.New(t)
		dir := writeIntegrityFixture(c.TB)
		_, _, hashErr := runCompatExit("migrate", "hash", "--dir", "file://"+dir, "--dir-format", "")
		c.Assert(hashErr, qt.IsNil)

		stdout, stderr, err := runCompatExit("migrate", "validate", "--dir", "file://"+dir, "--dir-format", "")

		c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
	})
}

// TestCompatMigrateIntegrityConvertedDir_FailurePathBadArgs holds the converted
// path's diagnostics identical to the forwarding path's. The converted path
// executes directly instead of handing the arguments to the native command, so
// without this it would silently ignore what the other spelling refuses.
func TestCompatMigrateIntegrityConvertedDir_FailurePathBadArgs(t *testing.T) {
	tests := []struct {
		name string
		verb string
		args []string
		want string
	}{
		{
			name: "hash stray positional",
			verb: "hash",
			args: []string{"stray"},
			want: `unexpected positional arguments \["stray"\]: name the migration directory with --dir`,
		},
		{
			name: "hash unknown flag",
			verb: "hash",
			args: []string{"--bogus"},
			want: "unknown flag: --bogus",
		},
		{
			name: "validate stray positional",
			verb: "validate",
			args: []string{"stray"},
			want: `unexpected positional arguments \["stray"\]: name the migration directory with --dir`,
		},
		{
			name: "validate unknown flag",
			verb: "validate",
			args: []string{"--bogus"},
			want: "unknown flag: --bogus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)
			base := []string{"migrate", tt.verb, "--dir", "file://" + dir}

			convertedArgs := append(append(append([]string{}, base...), "--dir-format", "goose"), tt.args...)
			_, _, convertedErr := runCompatExit(convertedArgs...)
			_, _, forwardedErr := runCompatExit(append(append([]string{}, base...), tt.args...)...)

			c.Assert(convertedErr, qt.ErrorMatches, tt.want)
			c.Assert(forwardedErr, qt.ErrorMatches, tt.want)
			c.Assert(exitcode.Code(convertedErr, 0), qt.Equals, exitcode.Code(forwardedErr, 0))
		})
	}
}

// TestCompatMigrateIntegrityArgumentTerminator_FailurePath pins that `--`
// survives the forwarding rewrite.
//
// Rewriting argv is the one place on this path whose failure mode is not a bad
// diagnostic but hashing a directory the operator never named. Dropping the
// terminator instead of preserving it turns everything after `--` back into
// flags, and the last `--dir` among them wins:
//
//	$ atlas migrate hash --dir 'file://d?format=atlas' -- --dir file://other
//	exit=0, wrote d/atlas.sum                      # CE ignores the positionals
//	shipped: exit 1, unexpected positional arguments ["--dir" "file://other"]
//	with `tail = args[i+1:]`: exit 0, wrote OTHER/atlas.sum
//
// So the assertion that matters here is not the exit code but that neither
// directory was hashed.
func TestCompatMigrateIntegrityArgumentTerminator_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		verb string
	}{
		{name: "hash", verb: "hash"},
		{name: "validate", verb: "validate"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			named := writeIntegrityFixture(c.TB)
			other := writeIntegrityFixture(c.TB)

			_, _, err := runCompatExit("migrate", tt.verb,
				"--dir", "file://"+named+"?format=atlas",
				"--", "--dir", "file://"+other)

			c.Assert(err, qt.ErrorMatches, `unexpected positional arguments \["--dir" "file://.*"\]: name the migration directory with --dir`)
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			_, namedStat := os.Stat(filepath.Join(named, migratesum.AtlasFileName))
			c.Assert(os.IsNotExist(namedStat), qt.IsTrue)
			_, otherStat := os.Stat(filepath.Join(other, migratesum.AtlasFileName))
			c.Assert(os.IsNotExist(otherStat), qt.IsTrue)
		})
	}
}

// TestCompatMigrateIntegrityHelp_HappyPath pins that --help is answered before
// the source layout is resolved, so an invocation whose flags cannot resolve
// still gets its help instead of a resolution error. Atlas CE exits 0 on all
// four of these.
func TestCompatMigrateIntegrityHelp_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		verb string
		args func(dir string) []string
	}{
		{
			name: "hash with an unknown dir-format",
			verb: "hash",
			args: func(string) []string { return []string{"--dir-format", "bogus", "--help"} },
		},
		{
			name: "hash with an unsupported query parameter",
			verb: "hash",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir + "?other=1", "--help"} },
		},
		{
			name: "validate with an unknown dir-format",
			verb: "validate",
			args: func(string) []string { return []string{"--dir-format", "bogus", "--help"} },
		},
		{
			name: "validate with an unsupported query parameter",
			verb: "validate",
			args: func(dir string) []string { return []string{"--dir", "file://" + dir + "?other=1", "--help"} },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			stdout, stderr, err := runCompatExit(append([]string{"migrate", tt.verb}, tt.args(dir)...)...)

			c.Assert(err, qt.IsNil, qt.Commentf("output:\n%s%s", stdout, stderr))
			// runCompatExit names the root "atlas"; the shipped binary names it
			// ptah-compat.
			c.Assert(stdout, qt.Contains, "Usage:\n  atlas migrate "+tt.verb)
			c.Assert(stdout, qt.Contains, "--dir-format string")
		})
	}
}

// TestCompatMigrateIntegritySemicolonQuery_FailurePath pins the query shape
// Atlas CE drops whole. It arrives through atlasargs.ParseLocalDir's
// url.ParseQuery rather than through the format validation, which is why an
// enumeration written from the validation function misses it:
//
//	$ atlas migrate hash --dir 'file://d?format=flyway;x=1'
//	exit=0, and it hashed the ATLAS set — the entire pair was discarded, so
//	even the format selection was lost.
//
// Refusing is the safe side, and shared with `migrate apply`. Tracked in #990.
func TestCompatMigrateIntegritySemicolonQuery_FailurePath(t *testing.T) {
	tests := []struct {
		name  string
		verb  string
		query string
	}{
		{name: "hash with a trailing pair", verb: "hash", query: "?format=flyway;x=1"},
		{name: "hash with a bare semicolon", verb: "hash", query: "?format=flyway;"},
		{name: "validate with a trailing pair", verb: "validate", query: "?format=flyway;x=1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			dir := writeIntegrityFixture(c.TB)

			_, _, err := runCompatExit("migrate", tt.verb, "--dir", "file://"+dir+tt.query)

			c.Assert(err, qt.ErrorMatches,
				"atlas migrate "+tt.verb+" --dir: parse migration directory URL query: .*semicolon.*")
			c.Assert(exitcode.Code(err, 0), qt.Equals, 1)
			_, statErr := os.Stat(filepath.Join(dir, migratesum.AtlasFileName))
			c.Assert(os.IsNotExist(statErr), qt.IsTrue)
		})
	}
}
