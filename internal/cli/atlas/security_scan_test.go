package atlas_test

import (
	"bytes"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/atlas"
	"ptah.run/internal/cli/atlas/internal/atlastest"
	"ptah.run/internal/cli/internal/exitcode"
	"ptah.run/internal/envbool/envbooltest"
)

const securityScanVar = "PTAH_ATLAS_SECURITY_SCAN"

// compatRun is one invocation's observable outcome.
type compatRun struct {
	stdout, stderr string
	code           int
}

// runTree runs args through root and returns what it wrote and the exit code
// it asked for.
func runTree(root *cobra.Command, args ...string) compatRun {
	var out, errOut bytes.Buffer
	root.SetOut(&out)
	root.SetErr(&errOut)
	root.SetArgs(args)
	err := atlastest.ExecuteAtlasTestCommand(root)
	return compatRun{stdout: out.String(), stderr: errOut.String(), code: exitcode.Code(err, 0)}
}

// withoutSecurity returns the full compatibility tree with the security group
// taken out: the tree as the root answers without it.
func withoutSecurity(c *qt.C) *cobra.Command {
	c.Helper()
	root := atlas.NewCompatCommand("atlas")
	commands := root.Commands()
	index := slices.IndexFunc(commands, func(cmd *cobra.Command) bool { return cmd.Name() == "security" })
	c.Assert(index >= 0, qt.IsTrue, qt.Commentf("the full tree registers no security group"))
	root.RemoveCommand(commands[index])
	return root
}

// TestCompatSecurity_DisabledAnswersAsTheTreeWithoutIt runs every spelling of
// the group, with the variable unset and set to each false spelling, through
// the full tree and through the same tree with the group taken out. The
// outcomes match byte for byte, exit code included: an invocation is the
// unknown-command error the community binary gives, a help request is the
// root's help, and shell completion offers what the tree without the group
// offers. Without the run-time gate the group would print its own help at exit
// 0, without the help gate `help security` would print the group's help, and
// without the completion gate `__complete security sc` would offer `scan`.
func TestCompatSecurity_DisabledAnswersAsTheTreeWithoutIt(t *testing.T) {
	// A regression that reaches the scan opens sqlite://x.db; it does so in a
	// scratch directory rather than in this package.
	t.Chdir(t.TempDir())
	values := []struct {
		name  string
		setup func(testing.TB)
	}{
		{name: "unset", setup: envbooltest.Unset(securityScanVar)},
		{name: "false", setup: envbooltest.Set(securityScanVar, "false")},
		{name: "0", setup: envbooltest.Set(securityScanVar, "0")},
		{name: "FALSE", setup: envbooltest.Set(securityScanVar, "FALSE")},
	}
	invocations := [][]string{
		{"security"},
		{"security", "--help"},
		{"security", "-h"},
		{"security", "scan"},
		{"security", "scan", "--help"},
		{"security", "scan", "-u", "sqlite://x.db"},
		{"security", "scan", "--url", "sqlite://x.db", "--format", "{{ json . }}"},
		{"--url", "sqlite://x.db", "security", "scan"},
		{"security", "nosuch"},
		{"--help", "security"},
		{"-h", "security"},
		{"help", "security"},
		{"help", "security", "scan"},
		{"__complete", "sec"},
		{"__complete", "security", "sc"},
		{"__complete", "security", ""},
		{"__complete", "security", "scan", "--"},
		{"__complete", "security", "scan", "-"},
		{"__completeNoDesc", "security", "sc"},
		{"__completeNoDesc", "security", "scan", "--"},
	}
	for _, value := range values {
		for _, args := range invocations {
			t.Run(value.name+"/"+strings.Join(args, " "), func(t *testing.T) {
				c := qt.New(t)
				value.setup(t)

				got := runTree(atlas.NewCompatCommand("atlas"), args...)
				want := runTree(withoutSecurity(c), args...)

				c.Assert(got, qt.Equals, want)
			})
		}
	}
}

// TestCompatSecurity_DisabledIsTheCommunityDiagnostic pins what the
// comparison above compares against for an invocation: the community
// binary's unknown-command error at exit 1, with nothing on stdout.
func TestCompatSecurity_DisabledIsTheCommunityDiagnostic(t *testing.T) {
	c := qt.New(t)
	envbooltest.Unset(securityScanVar)(t)

	got := runTree(atlas.NewCompatCommand("atlas"), "security", "scan", "--help")

	c.Assert(got, qt.Equals, compatRun{
		stderr: "Error: unknown command \"security\" for \"atlas\"\nRun 'atlas --help' for usage.\n",
		code:   1,
	})
}

// TestCompatSecurity_EnabledServesTheGroup is the control for the tests
// above: with the variable enabled the group and its verb answer for
// themselves, so the matching outcomes come from the gate and not from a
// group that is never reached.
func TestCompatSecurity_EnabledServesTheGroup(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStdout string
	}{
		{name: "the group's help lists the verb", args: []string{"security", "--help"},
			wantStdout: "  scan        Report known server vulnerabilities of connected databases\n"},
		{name: "help names the group", args: []string{"help", "security"},
			wantStdout: "  scan        Report known server vulnerabilities of connected databases\n"},
		{name: "the verb's help names the native command", args: []string{"security", "scan", "--help"},
			wantStdout: "Forwards to `ptah db vulnerabilities`.\n"},
		{name: "the verb's help names the native variables", args: []string{"security", "scan", "--help"},
			wantStdout: "PTAH_FAIL_SEVERITY, PTAH_MIN_SEVERITY and PTAH_DEPLOYMENT.\n"},
		{name: "the verb takes -u, one database per occurrence", args: []string{"help", "security", "scan"},
			wantStdout: "  -u, --url stringArray   URL of a database to scan; repeat it to scan several [env: PTAH_URL]\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(securityScanVar, "1")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
			c.Assert(got.stdout, qt.Contains, tt.wantStdout)
			c.Assert(got.stderr, qt.Equals, "")
		})
	}
}

// TestCompatSecurity_EnabledCompletesTheGroup is the control for the
// completion rows above: with the variable enabled, completion offers the
// group's verb and the verb's flags.
func TestCompatSecurity_EnabledCompletesTheGroup(t *testing.T) {
	tests := []struct {
		name       string
		args       []string
		wantStdout string
	}{
		{name: "the verb", args: []string{"__complete", "security", "sc"},
			wantStdout: "scan\tReport known server vulnerabilities of connected databases\n"},
		{name: "the verb's flags", args: []string{"__complete", "security", "scan", "--"},
			wantStdout: "--url\tURL of a database to scan; repeat it to scan several [env: PTAH_URL]\n"},
		{name: "the verb without descriptions", args: []string{"__completeNoDesc", "security", "sc"},
			wantStdout: "scan\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(securityScanVar, "1")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
			c.Assert(got.stdout, qt.Contains, tt.wantStdout)
		})
	}
}

// TestCompatSecurityScan_FailurePath runs the enabled verb on invocations it
// refuses before it connects: a --format other than the JSON template, a
// native flag this surface does not register, and no database at all.
func TestCompatSecurityScan_FailurePath(t *testing.T) {
	t.Chdir(t.TempDir())
	tests := []struct {
		name       string
		args       []string
		wantStderr string
	}{
		{name: "a native --format value", args: []string{"security", "scan", "-u", "sqlite://x.db", "--format", "json"},
			wantStderr: "Error: atlas security scan --format: security scan reads --format only as '{{ json . }}', got \"json\"; " +
				"omit it for the table report\n"},
		{name: "a template other than JSON", args: []string{"security", "scan", "-u", "sqlite://x.db", "--format", "{{ sql . }}"},
			wantStderr: "Error: atlas security scan --format: security scan reads --format only as '{{ json . }}', got \"{{ sql . }}\"; " +
				"omit it for the table report\n"},
		{name: "the native database flag", args: []string{"security", "scan", "--db-url", "sqlite://x.db"},
			wantStderr: "Error: atlas security scan does not accept native Ptah flag --db-url\n"},
		{name: "a native scan option", args: []string{"security", "scan", "-u", "sqlite://x.db", "--fail-on", "none"},
			wantStderr: "Error: atlas security scan does not accept native Ptah flag --fail-on\n"},
		{name: "the native advisory source", args: []string{"security", "scan", "-u", "sqlite://x.db", "--advisory-source", "bundled"},
			wantStderr: "Error: atlas security scan does not accept native Ptah flag --advisory-source\n"},
		{name: "-u with no value", args: []string{"security", "scan", "-u"},
			wantStderr: "Error: flag needs an argument: 'u' in -u\n"},
		{name: "--url with no value", args: []string{"security", "scan", "-u", "sqlite://x.db", "--url"},
			wantStderr: "Error: flag needs an argument: --url\n"},
		{name: "arguments after the separator", args: []string{"security", "scan", "-u", "sqlite://x.db", "--", "--format", "y"},
			wantStderr: "Error: unexpected positional arguments [\"--format\" \"y\"]\n"},
		{name: "no database", args: []string{"security", "scan"},
			wantStderr: "Error: required flag(s) \"url\" not set\n"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(securityScanVar, "1")(t)
			envbooltest.Unset("PTAH_URL")(t)
			envbooltest.Unset("PTAH_DB_URL")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(got, qt.Equals, compatRun{stderr: tt.wantStderr, code: 1})
		})
	}
}

// TestCompatSecurityScan_DatabaseFromTheEnvironment is the control for the
// "no database" row above: a variable names the database, the Atlas
// spelling's or the native one's, so the refusal is about a missing database
// and not about the verb.
func TestCompatSecurityScan_DatabaseFromTheEnvironment(t *testing.T) {
	tests := []struct {
		name     string
		variable string
		unset    string
	}{
		{name: "the Atlas flag's variable", variable: "PTAH_URL", unset: "PTAH_DB_URL"},
		{name: "the native flag's variable", variable: "PTAH_DB_URL", unset: "PTAH_URL"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(securityScanVar, "1")(t)
			envbooltest.Unset(tt.unset)(t)
			envbooltest.Set(tt.variable, "sqlite://"+c.TempDir()+"/scan.db")(t)
			envbooltest.Set("PTAH_FAIL_ON", "none")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), "security", "scan")

			c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
			c.Assert(got.stdout, qt.Contains, "Product: sqlite\n")
		})
	}
}

// TestCompatSecurity_MalformedValue_FailurePath refuses a value that is not a
// boolean on every invocation of the group, before it reads a flag, the help
// command included: a help function cannot fail the run, so without the gate
// in root's pre-run `help security` would print the root's help at exit 0.
func TestCompatSecurity_MalformedValue_FailurePath(t *testing.T) {
	t.Chdir(t.TempDir())
	tests := []struct {
		name string
		args []string
	}{
		{name: "the group", args: []string{"security"}},
		{name: "the verb", args: []string{"security", "scan", "-u", "sqlite://x.db"}},
		{name: "the verb's help flag", args: []string{"security", "scan", "--help"}},
		{name: "the help command on the group", args: []string{"help", "security"}},
		{name: "the help command on the verb", args: []string{"help", "security", "scan"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(securityScanVar, "maybe")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), tt.args...)

			c.Assert(got, qt.Equals, compatRun{
				stderr: "Error: invalid boolean value \"maybe\" for PTAH_ATLAS_SECURITY_SCAN\n",
				code:   1,
			})
		})
	}
}

// TestCompatSecurity_MalformedValueCompletesAsTheTreeWithoutIt completes
// with a malformed value: completion cannot fail, so it offers what the tree
// without the group offers, as for a disabled value.
func TestCompatSecurity_MalformedValueCompletesAsTheTreeWithoutIt(t *testing.T) {
	invocations := [][]string{
		{"__complete", "security", "sc"},
		{"__complete", "security", "scan", "--"},
	}
	for _, args := range invocations {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(securityScanVar, "maybe")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), args...)
			want := runTree(withoutSecurity(c), args...)

			c.Assert(got, qt.Equals, want)
		})
	}
}

// TestCompatSecurity_CompletionKeepsTheGroup runs a disabled completion and
// then, on the same tree, the enabled group: the group the completion took out
// is back, so a tree that serves more than one run keeps it.
func TestCompatSecurity_CompletionKeepsTheGroup(t *testing.T) {
	c := qt.New(t)
	root := atlas.NewCompatCommand("atlas")
	envbooltest.Unset(securityScanVar)(t)
	runTree(root, "__complete", "security", "sc")
	envbooltest.Set(securityScanVar, "1")(t)

	got := runTree(root, "security", "--help")

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	c.Assert(got.stdout, qt.Contains, "  scan        Report known server vulnerabilities of connected databases\n")
}

// TestCompatSecurity_MalformedValueLeavesOtherCommands is the control for the
// refusals above: a command that does not own the variable runs as usual, and
// so does the help command on another group.
func TestCompatSecurity_MalformedValueLeavesOtherCommands(t *testing.T) {
	invocations := [][]string{
		{"license"},
		{"help", "schema"},
	}
	for _, args := range invocations {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			c := qt.New(t)
			envbooltest.Set(securityScanVar, "maybe")(t)

			got := runTree(atlas.NewCompatCommand("atlas"), args...)

			c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
			c.Assert(got.stderr, qt.Equals, "")
		})
	}
}

// TestCompatSecurityScan_ScansEveryURL repeats -u: each URL is its own
// target. Joined into one --db-url value, the two would be one target that
// names neither database.
func TestCompatSecurityScan_ScansEveryURL(t *testing.T) {
	c := qt.New(t)
	t.Chdir(t.TempDir())
	envbooltest.Set(securityScanVar, "1")(t)
	envbooltest.Set("PTAH_FAIL_ON", "none")(t)
	envbooltest.Unset("PTAH_URL")(t)
	envbooltest.Unset("PTAH_DB_URL")(t)

	got := runTree(atlas.NewCompatCommand("atlas"), "security", "scan", "-u", "sqlite://a.db", "--url", "sqlite://b.db")

	c.Assert(got.code, qt.Equals, 0, qt.Commentf("stderr:\n%s", got.stderr))
	c.Assert(strings.Count(got.stdout, "Target: "), qt.Equals, 2)
	c.Assert(got.stdout, qt.Contains, "Target: sqlite://a.db\n")
	c.Assert(got.stdout, qt.Contains, "Target: sqlite://b.db\n")
}

// TestCompatSecurity_StrictRefusesTheVariable resolves the strict profile
// with the variable enabled: it is refused by name before any command runs,
// because the community binary has no command it could select.
func TestCompatSecurity_StrictRefusesTheVariable(t *testing.T) {
	c := qt.New(t)
	envbooltest.Set(atlascompatpolicy.StrictCompatEnvVar, "1")(t)
	envbooltest.Set(securityScanVar, "1")(t)

	policy, err := atlascompatpolicy.Resolve()

	c.Assert(err, qt.ErrorMatches, `PTAH_ATLAS_STRICT_COMPAT does not allow PTAH_ATLAS_SECURITY_SCAN`)
	c.Assert(policy, qt.Equals, atlascompatpolicy.Policy{})
}

// TestCompatSecurity_StrictTreeHasNoGroup runs the strict tree, as a disabled
// value leaves it: the group is not registered there at all, so the root
// reports the word as it reports any unknown command.
func TestCompatSecurity_StrictTreeHasNoGroup(t *testing.T) {
	t.Chdir(t.TempDir())
	c := qt.New(t)
	envbooltest.Set(securityScanVar, "0")(t)
	root := atlas.NewCompatCommandWithPolicy("atlas", atlascompatpolicy.StrictCE())

	got := runTree(root, "security", "scan", "-u", "sqlite://x.db")

	c.Assert(slices.ContainsFunc(root.Commands(), func(cmd *cobra.Command) bool { return cmd.Name() == "security" }), qt.IsFalse)
	c.Assert(got, qt.Equals, compatRun{
		stderr: "Error: unknown command \"security\" for \"atlas\"\nRun 'atlas --help' for usage.\n",
		code:   1,
	})
}
