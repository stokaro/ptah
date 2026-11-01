package doccommands_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/doccommands"
)

// fixture is a small tree with the shapes the real ones have: a root that
// runs, a group that does not, an alias, a persistent flag, a shorthand, a
// boolean, and a forwarding verb that parses no flags of its own.
func fixture() *cobra.Command {
	run := func(*cobra.Command, []string) error { return nil }
	root := &cobra.Command{Use: "ptah", Version: "v0.0.0", RunE: run}
	root.PersistentFlags().String("log-level", "info", "")
	migrations := &cobra.Command{Use: "migrations", Aliases: []string{"migration"}}
	up := &cobra.Command{Use: "up", RunE: run}
	up.Flags().String("migrations-dir", "", "")
	up.Flags().Bool("verify-sum", false, "")
	up.Flags().StringP("db-url", "u", "", "")
	down := &cobra.Command{Use: "down", RunE: run}
	migrations.AddCommand(up, down)
	forward := &cobra.Command{Use: "forward", DisableFlagParsing: true, RunE: run}
	root.AddCommand(migrations, forward)
	return doccommands.Prepare(root)
}

func TestCheck_HappyPath(t *testing.T) {
	for _, line := range [][]string{
		{"migrations", "up", "--migrations-dir", "./migrations", "--verify-sum"},
		{"migrations", "up", "--migrations-dir=./migrations"},
		{"migration", "up"},
		{"--log-level", "debug", "migrations", "up"},
		{"migrations", "up", "--log-level", "debug"},
		{"migrations", "up", "-u", "sqlite://app.db"},
		{"migrations", "up", "--verify-sum", "positional"},
		{"migrations", "up", "--help"},
		{"migrations", "-h"},
		{"--version"},
		{"migrations", "<verb>", "--anything"},
		{"forward", "--whatever", "the", "target", "parses"},
		{"help", "migrations", "up"},
		{"completion", "zsh"},
		{cobra.ShellCompRequestCmd, "migrations", "u"},
		{"migrations", "up", "--", "--not-a-flag"},
	} {
		t.Run(fmt.Sprint(line), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(doccommands.Check(fixture(), line), qt.IsNil)
		})
	}
}

func TestCheck_FailurePath(t *testing.T) {
	for _, test := range []struct {
		line []string
		want string
	}{
		{[]string{"generate"}, `"ptah" has no subcommand "generate"`},
		{[]string{"migrations", "bogus"}, `"ptah migrations" has no subcommand "bogus"`},
		{[]string{"migrations", "up", "--from", "x"}, `"ptah migrations up" has no flag --from`},
		{[]string{"migrations", "up", "--from=x"}, `"ptah migrations up" has no flag --from`},
		{[]string{"migrations", "up", "-x"}, `"ptah migrations up" has no flag -x`},
		// A sibling's local flag is not the resolved command's.
		{[]string{"migrations", "down", "--migrations-dir", "x"}, `"ptah migrations down" has no flag --migrations-dir`},
		{[]string{"help", "bogus"}, `"ptah" has no subcommand "bogus"`},
		// --version belongs to the root alone.
		{[]string{"migrations", "up", "--version"}, `"ptah migrations up" has no flag --version`},
	} {
		t.Run(fmt.Sprint(test.line), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(doccommands.Check(fixture(), test.line), qt.ErrorMatches, test.want)
		})
	}
}

// summary is what a row compares: where a line starts, which program it runs,
// its words, and whether it came from an inline span.
func summary(lines []doccommands.Line) []string {
	var out []string
	for _, line := range lines {
		out = append(out, fmt.Sprintf("%d %s %q inline=%v", line.Line, line.Program, line.Args, line.Inline))
	}
	return out
}

func TestExtract_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
		want   []string
	}{
		{
			name:   "a continued line starts where it was written",
			source: "```bash\n# a comment\nptah migrations up \\\n  --migrations-dir ./migrations\n```\n",
			want:   []string{`3 ptah ["migrations" "up" "--migrations-dir" "./migrations"] inline=false`},
		},
		{
			name:   "a console block runs only its prompted lines",
			source: "```console\n$ ptah version --short\nptah version v1\n```\n",
			want:   []string{`2 ptah ["version" "--short"] inline=false`},
		},
		{
			name:   "a YAML run step and a run block scalar",
			source: "```yaml\n- run: ptah migrations lint\n  run: |\n    ptah migrations up\n    echo done\n```\n",
			want:   []string{`2 ptah ["migrations" "lint"] inline=false`, `4 ptah ["migrations" "up"] inline=false`},
		},
		{
			name:   "control operators, substitution and a pipe",
			source: "```sh\nDIGEST=$(ptah oci resolve ref) && ptah migrations up --migrations-dir \"$DIGEST\" | tee log\n```\n",
			want:   []string{`2 ptah ["oci" "resolve" "ref"] inline=false`, `2 ptah ["migrations" "up" "--migrations-dir" "$DIGEST"] inline=false`},
		},
		{
			name:   "an assignment and a path to the binary",
			source: "```bash\nPTAH_X=1 ./bin/ptah-compat migrate apply\n```\n",
			want:   []string{`2 ptah-compat ["migrate" "apply"] inline=false`},
		},
		{
			name:   "redirections drop with what they name",
			source: "```bash\nptah schema render > out.sql 2>&1\n```\n",
			want:   []string{`2 ptah ["schema" "render"] inline=false`},
		},
		{
			name:   "quotes group a word and are removed",
			source: "```bash\nptah db read --url 'postgres://localhost/db?x=1 y'\n```\n",
			want:   []string{`2 ptah ["db" "read" "--url" "postgres://localhost/db?x=1 y"] inline=false`},
		},
		{
			name:   "a heredoc body is data",
			source: "```bash\ncat <<'EOF' > notes\nptah is not run here\nEOF\nptah version\n```\n",
			want:   []string{`5 ptah ["version"] inline=false`},
		},
		{
			name:   "inline spans that begin with a program",
			source: "Run `ptah migrations up` or `ptah-compat migrate apply`, not `ptah` or `echo ptah`.\n",
			want:   []string{`1 ptah ["migrations" "up"] inline=true`, `1 ptah-compat ["migrate" "apply"] inline=true`},
		},
		{
			name:   "Windows line endings",
			source: "```bash\r\nptah version\r\n```\r\n",
			want:   []string{`2 ptah ["version"] inline=false`},
		},
		{
			name:   "a tilde fence and a longer backtick fence",
			source: "~~~bash\nptah version\n~~~\n````sh\nptah schema render\n````\n",
			want:   []string{`2 ptah ["version"] inline=false`, `5 ptah ["schema" "render"] inline=false`},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(summary(doccommands.Extract("page.md", test.source)), qt.DeepEquals, test.want)
		})
	}
}

// Each source holds a program name somewhere no command line is: output,
// another language, a span naming the bare binary, and a word that only
// begins with the name.
func TestExtract_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		source string
	}{
		{"a text block", "```text\nptah migrations up --from x\n```\n"},
		{"a Go block", "```go\n// ptah migrations up\n```\n"},
		{"console output", "```console\nptah: unknown command\n```\n"},
		{"a longer name", "Run `ptahx migrations up`.\n"},
		{"the bare binary", "The `ptah` binary.\n"},
		{"a program as an argument", "```bash\necho ptah migrations up\n```\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(summary(doccommands.Extract("page.md", test.source)), qt.HasLen, 0)
		})
	}
}

func TestExtractHelp_HappyPath(t *testing.T) {
	c := qt.New(t)
	root := &cobra.Command{Use: "ptah"}
	child := &cobra.Command{
		Use:     "resolve",
		Example: "  ptah resolve ref",
		Long: "Resolve a reference.\n\nCapture it with `ptah resolve --json`:\n\n" +
			"    DIGEST=$(ptah resolve ref)\n    ptah migrations up --migrations-dir \"$DIGEST\"\n\nThe ptah resolve prose is not a command.",
	}
	root.AddCommand(child)
	c.Assert(summary(doccommands.ExtractHelp(root)), qt.DeepEquals, []string{
		`0 ptah ["resolve" "ref"] inline=false`,
		`0 ptah ["resolve" "--json"] inline=true`,
		`0 ptah ["resolve" "ref"] inline=false`,
		`0 ptah ["migrations" "up" "--migrations-dir" "$DIGEST"] inline=false`,
	})
	c.Assert(doccommands.ExtractHelp(root)[0].File, qt.Equals, "ptah resolve")
}
