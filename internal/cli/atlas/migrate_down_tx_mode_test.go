package atlas_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// `migrate down --dev-url` replays the applied history in the mode
// PTAH_TX_MODE names, the environment twin of the native --tx-mode. The
// compatibility surface takes no --tx-mode flag on this verb, and the --format
// path, which builds its own rehearsal, refuses a value that names no mode
// before it reads the directory, as the forwarded rollback does.
func TestAtlasMigrateDownTxMode_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name       string
		env        string
		args       []string
		wantStderr string
	}{
		{
			name:       "the native flag",
			env:        "file",
			args:       []string{"--tx-mode", "none"},
			wantStderr: "Error: atlas migrate down does not accept native Ptah flag --tx-mode\n",
		},
		{
			name:       "a variable that names no mode, with --format",
			env:        "each",
			args:       []string{"--format", "{{ .Current }}"},
			wantStderr: "Error: invalid tx-mode \"each\": expected file, all, or none\n",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Setenv("PTAH_TX_MODE", test.env)

			stdout, stderr, code := runCompat(c, append([]string{"migrate", "down", "--url", "sqlite://dev.db", "--dir", "file://migrations"}, test.args...)...)

			c.Assert(code, qt.Equals, 1)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, test.wantStderr)
		})
	}
}
