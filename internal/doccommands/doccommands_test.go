package doccommands_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/spf13/cobra"

	"ptah.run/internal/cmdforward"
	"ptah.run/internal/doccommands"
)

// fixture is a pair of small trees with the shapes the real ones have. The
// native tree has a root that runs, a group that runs (schema) and one that
// does not (migrations), an alias, persistent flags, shorthands, and booleans.
// The compatibility tree has a forwarding verb that registers a flag of its
// own, builds a detached target, and refuses one of the target's flags, and a
// forwarding verb that registers and builds nothing.
func fixture() doccommands.Trees {
	run := func(*cobra.Command, []string) error { return nil }
	native := &cobra.Command{Use: "ptah", Version: "v0.0.0", RunE: run}
	native.PersistentFlags().String("log-level", "info", "")
	native.PersistentFlags().Bool("verbose", false, "")
	migrations := &cobra.Command{Use: "migrations", Aliases: []string{"migration"}}
	up := &cobra.Command{Use: "up", RunE: run}
	up.Flags().String("migrations-dir", "", "")
	up.Flags().Bool("verify-sum", false, "")
	up.Flags().BoolP("dry-run", "n", false, "")
	up.Flags().StringP("db-url", "u", "", "")
	down := &cobra.Command{Use: "down", RunE: run}
	down.Flags().String("target", "", "")
	migrations.AddCommand(up, down)
	schema := &cobra.Command{Use: "schema", RunE: run}
	schema.AddCommand(&cobra.Command{Use: "render", RunE: run})
	native.AddCommand(migrations, schema)

	compat := &cobra.Command{Use: "ptah-compat", RunE: run}
	migrate := &cobra.Command{Use: "migrate", RunE: run}
	forward := &cobra.Command{Use: "down", DisableFlagParsing: true, RunE: run}
	forward.Flags().String("dir", "", "")
	cmdforward.Register(forward, "migrations down", func() *cobra.Command {
		target := &cobra.Command{Use: "down", RunE: run}
		target.Flags().String("target", "", "")
		target.Flags().String("log-level", "info", "")
		target.Flags().Bool("verify-sum", false, "")
		return target
	})
	cmdforward.Refuse(forward, "verify-sum")
	raw := &cobra.Command{Use: "raw", DisableFlagParsing: true, RunE: run}
	edit := &cobra.Command{Use: "edit", DisableFlagParsing: true, RunE: run}
	cmdforward.Register(edit, "migrations edit", func() *cobra.Command {
		target := &cobra.Command{Use: "edit", RunE: run}
		target.Flags().String("engine", "", "")
		return target
	})
	cmdforward.ReadsPositionals(edit)
	migrate.AddCommand(forward, raw, edit)
	compat.AddCommand(migrate)
	return doccommands.NewTrees(native, compat)
}

func native(args ...string) doccommands.Line {
	return doccommands.Line{Program: doccommands.Native, Args: args}
}

func compat(args ...string) doccommands.Line {
	return doccommands.Line{Program: doccommands.Compat, Args: args}
}

func TestCheck_HappyPath(t *testing.T) {
	for _, line := range []doccommands.Line{
		native("migrations", "up", "--migrations-dir", "./migrations", "--verify-sum"),
		native("migrations", "up", "--migrations-dir=./migrations"),
		native("migration", "up"),
		native("--log-level", "debug", "migrations", "up"),
		// A boolean the root knows takes no value, so "migrations" is the verb.
		native("--verbose", "migrations", "up"),
		// cobra takes the word after a flag the root does not know as its value
		// while it looks for the command, and up knows the flag.
		native("--db-url", "sqlite://app.db", "migrations", "up"),
		native("migrations", "up", "--log-level", "debug"),
		native("migrations", "up", "-u", "sqlite://app.db"),
		native("migrations", "up", "-nu", "sqlite://app.db"),
		native("migrations", "up", "--verify-sum", "positional"),
		native("migrations", "up", "-"),
		native("migrations", "up", "--help"),
		native("migrations", "-h"),
		native("schema", "render"),
		native("--version"),
		native("-v"),
		// cobra takes "migrations" as the value of a flag it has not added yet,
		// then prints the version.
		native("--version", "migrations"),
		native("migrations", "<verb>", "--anything"),
		native("help", "migrations", "up"),
		native("help", "--", "migrations"),
		native("completion", "zsh"),
		native(cobra.ShellCompRequestCmd, "migrations", "u"),
		native("migrations", "up", "--", "--not-a-flag"),
		compat("migrate", "down", "--dir", "file://migrations"),
		// A forwarded verb hands its target the flags the target declares.
		compat("migrate", "down", "--target", "5", "--log-level", "info"),
		compat("migrate", "down", "--help"),
		compat("migrate", "raw", "--whatever", "the", "target", "parses"),
		// A verb that reads positionals takes a target flag's value with "=".
		compat("migrate", "edit", "1", "--engine=x"),
	} {
		t.Run(strings.Join(append([]string{line.Program}, line.Args...), " "), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(fixture().Check(line), qt.IsNil)
		})
	}
}

func TestCheck_FailurePath(t *testing.T) {
	for _, test := range []struct {
		line doccommands.Line
		want string
	}{
		{native("generate"), `"ptah" has no subcommand "generate"`},
		{native("migrations", "bogus"), `"ptah migrations" has no subcommand "bogus"`},
		// A group that runs refuses an unknown word too.
		{native("schema", "generate"), `"ptah schema" has no subcommand "generate"`},
		{native("migrations", "up", "--from", "x"), `"ptah migrations up" has no flag --from`},
		{native("migrations", "up", "--from=x"), `"ptah migrations up" has no flag --from`},
		{native("migrations", "up", "-x"), `"ptah migrations up" has no flag -x`},
		{native("migrations", "up", "-nx"), `"ptah migrations up" has no flag -x`},
		// A sibling's local flag is not the resolved command's.
		{native("migrations", "down", "--migrations-dir", "x"), `"ptah migrations down" has no flag --migrations-dir`},
		// The group does not know the flag, so it takes "up" as its value.
		{native("migrations", "--dry-run", "up"), `"ptah migrations" has no flag --dry-run`},
		{native("migrations", "up", "--version"), `"ptah migrations up" has no flag --version`},
		{native("help", "bogus"), `"ptah" has no subcommand "bogus"`},
		{native("help", "--", "bogus"), `"ptah" has no subcommand "bogus"`},
		{native("help", "migrations", "up", "--bogus"), `"ptah help" has no flag --bogus`},
		{native("--", "migrations", "up"), `"ptah" has no subcommand "migrations"`},
		// cobra takes "migrations" as the value of --help before it adds the
		// flag, and the root has no verb "up".
		{native("--help", "migrations", "up"), `"ptah" has no subcommand "up"`},
		{native("--zzflag", "v", "help"), `"ptah help" has no flag --zzflag`},
		{compat("migrate", "down", "--bogus", "x"), `"ptah-compat migrate down" has no flag --bogus`},
		// The forwarder refuses this flag before forwarding, though its target
		// declares it.
		{compat("migrate", "down", "--verify-sum"), `"ptah-compat migrate down" has no flag --verify-sum`},
		// The target runs detached, so the native root's persistent flags do
		// not reach it.
		{compat("migrate", "down", "--verbose"), `"ptah-compat migrate down" has no flag --verbose`},
		{compat("migrate", "bogus"), `"ptah-compat migrate" has no subcommand "bogus"`},
		// The verb would read x as a second positional argument.
		{compat("migrate", "edit", "1", "--engine", "x"), `"ptah-compat migrate edit" reads the word after --engine as an argument; write --engine=VALUE`},
	} {
		t.Run(strings.Join(append([]string{test.line.Program}, test.line.Args...), " "), func(t *testing.T) {
			c := qt.New(t)
			c.Assert(fixture().Check(test.line), qt.ErrorMatches, test.want)
		})
	}
}

// A check leaves the trees as it found them, so a verdict does not depend on
// which lines were checked before it. cobra adds --help to the command it
// executes, after it has found it, so --help before a verb still takes the
// verb as its value.
func TestCheck_VerdictsDoNotDependOnOrder(t *testing.T) {
	c := qt.New(t)
	trees := fixture()
	c.Assert(trees.Check(native("--help")), qt.IsNil)
	c.Assert(trees.Check(native("migrations", "up", "--help")), qt.IsNil)
	c.Assert(trees.Check(native("--help", "migrations", "up")), qt.ErrorMatches, `"ptah" has no subcommand "up"`)
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
			name:   "a console transcript runs only its prompted lines",
			source: "```console\n$ ptah version --short\nptah version v1\n```\n",
			want:   []string{`2 ptah ["version" "--short"] inline=false`},
		},
		{
			name:   "a console block with no prompt is commands",
			source: "```console\nptah migrations up\nptah migrations status\n```\n",
			want:   []string{`2 ptah ["migrations" "up"] inline=false`, `3 ptah ["migrations" "status"] inline=false`},
		},
		{
			name:   "a YAML run step and a run block scalar",
			source: "```yaml\n- run: ptah migrations lint\n  run: |\n    ptah migrations up\n    echo done\n```\n",
			want:   []string{`2 ptah ["migrations" "lint"] inline=false`, `4 ptah ["migrations" "up"] inline=false`},
		},
		{
			name:   "YAML command and script keys, quoted and folded",
			source: "```yaml\ncommand: \"ptah migrations lint\"\n- script: ptah schema render\nrun: >\n  ptah migrations up\n  --verify-sum\n```\n",
			want:   []string{`2 ptah ["migrations" "lint"] inline=false`, `3 ptah ["schema" "render"] inline=false`, `5 ptah ["migrations" "up" "--verify-sum"] inline=false`},
		},
		{
			name:   "a literal block continues a command with a backslash",
			source: "```yaml\n- script: |\n    ptah migrations lint --dir ./m \\\n      --format sarif > out.sarif\n  displayName: Lint\n```\n",
			want:   []string{`3 ptah ["migrations" "lint" "--dir" "./m" "--format" "sarif"] inline=false`},
		},
		{
			name: "script list items, folded and plain over several lines",
			source: "```yaml\nscript:\n  - >\n    ptah migrations lint --dir ./m\n    --format json\n  - ptah migrations lint --dir ./m\n      --format gitlab > report.json\n" +
				"  - echo done\n```\n",
			want: []string{`4 ptah ["migrations" "lint" "--dir" "./m" "--format" "json"] inline=false`, `6 ptah ["migrations" "lint" "--dir" "./m" "--format" "gitlab"] inline=false`},
		},
		{
			name:   "control operators, substitution and a pipe",
			source: "```sh\nDIGEST=$(ptah oci resolve ref) && ptah migrations up --migrations-dir \"$DIGEST\" | tee log\n```\n",
			want:   []string{`2 ptah ["oci" "resolve" "ref"] inline=false`, `2 ptah ["migrations" "up" "--migrations-dir" "$DIGEST"] inline=false`},
		},
		{
			name:   "a substitution inside double quotes",
			source: "```bash\necho \"digest: $(ptah oci resolve ref)\"\n```\n",
			want:   []string{`2 ptah ["oci" "resolve" "ref"] inline=false`},
		},
		{
			name:   "an assignment and a path to the binary",
			source: "```bash\nPTAH_X=1 ./bin/ptah-compat migrate apply\n```\n",
			want:   []string{`2 ptah-compat ["migrate" "apply"] inline=false`},
		},
		{
			name:   "go run of the command package",
			source: "```bash\ngo run ./cmd/ptah schema render\ngo run -tags x ./cmd/ptah-compat migrate apply\n```\n",
			want:   []string{`2 ptah ["schema" "render"] inline=false`, `3 ptah-compat ["migrate" "apply"] inline=false`},
		},
		{
			name:   "keywords and launchers before the program",
			source: "```bash\nuntil ptah migrations status; do sleep 1; done\nsudo -E ptah version\nenv -u HOME ptah version\n```\n",
			want:   []string{`2 ptah ["migrations" "status"] inline=false`, `3 ptah ["version"] inline=false`, `4 ptah ["version"] inline=false`},
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
			name:   "a heredoc body is data, and its delimiter may carry a dash",
			source: "```bash\ncat <<'END-OF-FILE' > notes\nptah is not run here\nEND-OF-FILE\nptah version\n```\n",
			want:   []string{`5 ptah ["version"] inline=false`},
		},
		{
			name:   "a here-string, a shift and a commented heredoc open no body",
			source: "```bash\nptah sql lint <<< 'SELECT 1'\necho $((1<<2))\n# cat <<EOF\nptah version\n```\n",
			want:   []string{`2 ptah ["sql" "lint"] inline=false`, `5 ptah ["version"] inline=false`},
		},
		{
			name:   "a comment ending in a backslash does not continue",
			source: "```bash\n# C:\\\nptah version\n```\n",
			want:   []string{`3 ptah ["version"] inline=false`},
		},
		{
			name:   "PowerShell continues with a backtick and skips a here-string",
			source: "```powershell\n@'\nptah not a command\n'@ | Set-Content x\nptah migrations down `\n  --target 5\n& .\\ptah.exe version\n```\n",
			want:   []string{`5 ptah ["migrations" "down" "--target" "5"] inline=false`, `7 ptah ["version"] inline=false`},
		},
		{
			name:   "a placeholder is a word, and a redirection is not",
			source: "```bash\nptah migrations up --db-url <url> --bogus\nptah schema render < in.sql\n```\n",
			want:   []string{`2 ptah ["migrations" "up" "--db-url" "<url>" "--bogus"] inline=false`, `3 ptah ["schema" "render"] inline=false`},
		},
		{
			name:   "a placeholder in an inline span",
			source: "Run `ptah migrations up --db-url <url> --bogus` now.\n",
			want:   []string{`1 ptah ["migrations" "up" "--db-url" "<url>" "--bogus"] inline=true`},
		},
		{
			name:   "inline spans that begin with a program",
			source: "Run `ptah migrations up` or `ptah-compat migrate apply`, not `ptah` or `echo ptah`.\n",
			want:   []string{`1 ptah ["migrations" "up"] inline=true`, `1 ptah-compat ["migrate" "apply"] inline=true`},
		},
		{
			name:   "an inline span that wraps onto the next line",
			source: "Some prose, then `ptah migrations\nup --verify-sum` closes it.\n",
			want:   []string{`1 ptah ["migrations" "up" "--verify-sum"] inline=true`},
		},
		{
			name:   "a double-backtick span",
			source: "Run ``ptah schema render`` here.\n",
			want:   []string{`1 ptah ["schema" "render"] inline=true`},
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
		{"transcript output", "```console\n$ echo hi\nptah: unknown command\n```\n"},
		{"a longer name", "Run `ptahx migrations up`.\n"},
		{"the bare binary", "The `ptah` binary.\n"},
		{"a program as an argument", "```bash\necho ptah migrations up\n```\n"},
		{"go run of something else", "```bash\ngo run ./tools/ptah-lint check\n```\n"},
		{"a span that never closes", "An open `ptah migrations up span.\n"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(summary(doccommands.Extract("page.md", test.source)), qt.HasLen, 0)
		})
	}
}

func TestTranscript_HappyPath(t *testing.T) {
	c := qt.New(t)
	c.Assert(doccommands.Transcript([]string{"$ ptah version", "ptah v1"}), qt.IsTrue)
}

func TestTranscript_FailurePath(t *testing.T) {
	c := qt.New(t)
	c.Assert(doccommands.Transcript([]string{"ptah version", "ptah migrations up"}), qt.IsFalse)
}

func TestExtractHelp_HappyPath(t *testing.T) {
	c := qt.New(t)
	root := &cobra.Command{Use: "ptah"}
	root.CompletionOptions.DisableDefaultCmd = true
	child := &cobra.Command{
		Use:     "resolve",
		Example: "  ptah resolve ref",
		Long: "Resolve a reference.\n\nCapture it with `ptah resolve --json`:\n\n" +
			"  DIGEST=$(ptah resolve ref)\n  ptah migrations up \\\n    --migrations-dir \"$DIGEST\"\n\n" +
			"Verbs:\n\n  ptah resolve   resolve a reference\n\nThe ptah resolve prose is not a command, and \"ptah resolve --plain\" is.",
	}
	forward := &cobra.Command{Use: "down", Long: "Forwards to `ptah migrations down`.",
		Annotations: map[string]string{cmdforward.Annotation: "migrations down"}}
	root.AddCommand(child, forward)
	lines := doccommands.ExtractHelp(root)
	// cobra lists commands by name, so the forwarder's help comes first.
	c.Assert(summary(lines), qt.DeepEquals, []string{
		`0 ptah ["migrations" "down"] inline=true`,
		`0 ptah ["resolve" "ref"] inline=false`,
		`0 ptah ["resolve" "--json"] inline=true`,
		`0 ptah ["resolve" "ref"] inline=false`,
		`0 ptah ["migrations" "up" "--migrations-dir" "$DIGEST"] inline=false`,
		`0 ptah ["resolve"] inline=false`,
		`0 ptah ["resolve" "--plain"] inline=true`,
	})
	c.Assert(lines[0].File, qt.Equals, "ptah down")
	c.Assert(lines[0].Generated, qt.IsTrue)
	c.Assert(lines[1].File, qt.Equals, "ptah resolve")
	c.Assert(lines[1].Generated, qt.IsFalse)
}
