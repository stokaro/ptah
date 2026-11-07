package generate_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/cli/generate"
)

// mySQLOptionsSchema declares the four table options the MySQL family owns and
// an AUTO_INCREMENT key, which a MySQL-family target keeps and the others do
// not.
const mySQLOptionsSchema = `CREATE TABLE users (
    id INT NOT NULL AUTO_INCREMENT,
    name VARCHAR(255) NOT NULL,
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin AUTO_INCREMENT=100;
`

// renderNotes are the lines of a render's stderr that report a declaration
// the render went on without.
func renderNotes(stderr string) []string {
	var notes []string
	for line := range strings.SplitSeq(stderr, "\n") {
		if strings.HasPrefix(line, "note: ") {
			notes = append(notes, line)
		}
	}
	return notes
}

// TestGenerateCommand_NamesWhatTheRenderSkipped pins the note a render writes
// on stderr for each declaration the target goes on without. PostgreSQL also
// writes a comment beside the statement; SQLite writes none, so before the
// note its render dropped the four table options without a word. MySQL keeps
// every declaration and writes no note.
func TestGenerateCommand_NamesWhatTheRenderSkipped(t *testing.T) {
	tests := []struct {
		dialect string
		want    []string
	}{
		{dialect: "postgres", want: []string{
			"note: postgres: column users.id: auto-increment was skipped (unsupported); " +
				"declare the column type as SERIAL or BIGSERIAL, or give it an identity clause with identity_generation",
			"note: postgres: table users: table option AUTO_INCREMENT=100 was skipped (unsupported); " +
				"declare the start on the key column with identity_start",
			"note: postgres: table users: table option CHARSET=utf8mb4 was skipped (unsupported)",
			"note: postgres: table users: table option COLLATE=utf8mb4_bin was skipped (unsupported)",
			"note: postgres: table users: table option ENGINE=InnoDB was skipped (unsupported)",
		}},
		{dialect: "sqlite", want: []string{
			"note: sqlite: table users: table option AUTO_INCREMENT=100 was skipped (unsupported)",
			"note: sqlite: table users: table option CHARSET=utf8mb4 was skipped (unsupported)",
			"note: sqlite: table users: table option COLLATE=utf8mb4_bin was skipped (unsupported)",
			"note: sqlite: table users: table option ENGINE=InnoDB was skipped (unsupported)",
		}},
		{dialect: "mysql"},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)
			path := filepath.Join(t.TempDir(), "schema.sql")
			c.Assert(os.WriteFile(path, []byte(mySQLOptionsSchema), 0o600), qt.IsNil)
			cmd := generate.NewGenerateCommand()
			cmd.SetArgs([]string{"--schema-file", path, "--dialect", test.dialect})

			stdout, stderr, err := executeGenerate(c, cmd)

			c.Assert(err, qt.IsNil, qt.Commentf("generate stderr:\n%s", stderr))
			c.Assert(stdout, qt.Contains, "CREATE TABLE")
			c.Assert(stdout, qt.Not(qt.Contains), "note:")
			c.Assert(renderNotes(stderr), qt.DeepEquals, test.want)
		})
	}
}
