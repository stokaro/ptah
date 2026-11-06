package root_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestNativeFlagValuesRefuseInTheStandardLibrarysWords drives the native root
// with a value its flag cannot parse. The refusal carries the strconv or time
// error, as ptah-compat's does, rather than pflag's own sentence (`must be a
// duration like "30s" or "5m"`, `must be true or false`).
func TestNativeFlagValuesRefuseInTheStandardLibrarysWords(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{
			name:       "a duration",
			args:       []string{"schema", "serve", "--refresh=soon"},
			wantStderr: "error: invalid argument \"soon\" for \"--refresh\" flag: time: invalid duration \"soon\"\n",
		},
		{
			name:       "a boolean",
			args:       []string{"schema", "apply", "--dry-run=maybe"},
			wantStderr: "error: invalid argument \"maybe\" for \"--dry-run\" flag: strconv.ParseBool: parsing \"maybe\": invalid syntax\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, code := runRoot(c, tt.args...)

			c.Assert(code, qt.Equals, 2)
			c.Assert(stdout, qt.Equals, "")
			c.Assert(stderr, qt.Equals, tt.wantStderr)
		})
	}
}

// TestNativeHelpPrintsBooleanFlagsWithoutAValueSuffix covers the flags cobra
// adds when it executes a command, --help and --version, which exist only
// after the pass over the assembled tree. pflag v1.1.0 writes each boolean
// flag as `--name[=true|false]`; the help writes `--name` alone.
func TestNativeHelpPrintsBooleanFlagsWithoutAValueSuffix(t *testing.T) {
	tests := []struct {
		name     string
		args     []string
		wantLine string
	}{
		{name: "a verb's --help", args: []string{"schema", "apply", "--help"}, wantLine: "  -h, --help "},
		{name: "the help command", args: []string{"help", "schema", "apply"}, wantLine: "  -h, --help "},
		{name: "the root's --version", args: []string{"--help"}, wantLine: "  -v, --version "},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			stdout, stderr, code := runRoot(c, tt.args...)

			c.Assert(code, qt.Equals, 0)
			c.Assert(stderr, qt.Equals, "")
			c.Assert(stdout, qt.Contains, tt.wantLine)
			c.Assert(stdout, qt.Not(qt.Contains), "[=true|false]")
		})
	}
}
