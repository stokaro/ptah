package atlas

import (
	"errors"
	"fmt"
	"io"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlascompatpolicy"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// atlasNativeEnvPrefix is the environment prefix cmdadapter installs on every
// forwarded native target (installForwardedTargetEnv). It is restated here
// because the resolver below has to read the same variables the target will.
const atlasNativeEnvPrefix = "PTAH"

// atlasMigrateIntegrityRunner reads or writes the Atlas integrity file of a
// migration directory laid out in a foreign tool's convention.
type atlasMigrateIntegrityRunner func(
	*cobra.Command,
	atlasMigrateSource,
) error

// atlasMigrateSource is the migration directory an integrity verb reads,
// resolved once from both spellings Atlas accepts for the source layout.
type atlasMigrateSource struct {
	// dir is the local directory path, the same value the forwarded native
	// command receives for a native Atlas directory.
	dir string
	// format is the resolved source layout, which decides both the set of
	// source files the integrity file covers and the order they are hashed in.
	format atlasmigrateimport.Format
	// devURL is the resolved --dev-url, empty when none was selected. A verb
	// that registers no --dev-url rejects the flag as unknown before this is
	// read, so it is always empty there.
	devURL string
	// forwardArgs are the Atlas-form arguments to forward when format selects a
	// native Atlas directory.
	forwardArgs []string
	// localDir is dir with the URL query it was named by and the root it is
	// confined to, so a caller that opens the directory itself opens it through
	// the same boundary the forwarded native command uses.
	localDir atlasargs.LocalDir
	// writeDir is the rooted backing directory a writing verb changes. It is
	// equal to localDir for ordinary paths and points at the template source for
	// a data.template_dir whose rendered filesystem remains localDir's read view.
	writeDir atlasargs.LocalDir
	// project is the loaded atlas.hcl, zero when none was selected. It owns the
	// directory handle localDir may be confined to.
	project atlasProject
	// projectArgs are the Atlas-form arguments with the project selection flags
	// removed and the selected env's values merged in: what a caller that
	// executes directly, rather than forwarding, has to validate itself.
	projectArgs []string
}

// newAtlasMigrateIntegrityCommand wraps the table-driven adapter command for an
// Atlas integrity verb with a converted-source-format path.
//
// Atlas CE accepts a foreign tool's directory layout on `migrate hash` and
// `migrate validate` under two spellings that mean the same thing — the
// `?format=` query on --dir and the --dir-format flag — and produces a
// byte-identical atlas.sum from either. Both are refused today, with two
// different messages (stokaro/ptah#983).
//
// A native Atlas directory keeps forwarding to `ptah migrations hash|validate`
// unchanged, including when the atlas format was spelled as a query: the query
// is stripped and --dir-format normalized so exactly the same native
// invocation runs. Every other layout is covered by a different set of source
// files, which no value of the native --dir-format axis ({auto, ptah, atlas})
// can express, so those run here instead of being forwarded.
//
// The source format is resolved with atlasmigrate.ResolveApplyDirFormat, the
// same resolver `migrate apply` uses, so the three verbs cannot drift on which
// spelling wins.
func newAtlasMigrateIntegrityCommand(
	policy atlascompatpolicy.Policy,
	verb atlasVerb,
	run atlasMigrateIntegrityRunner,
) *cobra.Command {
	cmd := newAtlasAdapterCommand("migrate", verb)
	forward := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) (runErr error) {
		if atlasArgsHaveHelp(args) {
			return forward(cmd, args)
		}
		cleanup := &cmdadapter.CleanupScope{}
		defer func() {
			runErr = errors.Join(runErr, cleanup.Close())
		}()
		// The unknown-format adaptation used to be an errors.As block here,
		// which is why it reached `migrate hash` and `migrate validate` and no
		// other verb. It now lives on the refusal itself, inside
		// resolveAtlasMigrateSource, so `migrate new` -- which shares that
		// resolver but not this wrapper -- gets it too.
		source, err := resolveAtlasMigrateSource(cmd, verb, args, cleanup)
		if err != nil {
			return err
		}
		// Both verbs reaching this wrapper -- `migrate hash` and
		// `migrate validate` -- take no positional, so the stray-argument
		// refusal runs here on BOTH branches rather than only on the one that
		// executes directly.
		//
		// It used to run only on the direct branch, leaving the forwarding
		// branch to the native command's own validator. That answered with the
		// native wording, which does not say where the value belongs. Refusing
		// a trailing positional is a deliberate divergence from the pinned
		// community binary v1.3.0, which accepts one and silently discards it,
		// and a divergence is only defensible when the refusal is more useful
		// than the acceptance (stokaro/ptah#1241 item 13).
		//
		// `migrate new`, which does take a migration name, is built by
		// newAtlasAdapterCommand directly and never reaches this wrapper; it
		// shares only atlasVerbFlagSet, which registers flags and no
		// positionals.
		if err := checkAtlasMigrateSourceArgs(cmd, verb, source.projectArgs); err != nil {
			return err
		}
		directStrictReplay := policy.IsStrictCE() &&
			verb.use == atlasMigrateValidateVerb().use &&
			source.devURL != ""
		// A --dev-url this surface refuses is answered on the direct branch too,
		// and the reason is ordering rather than wording. Measured on the pinned
		// community binary v1.3.0 on 2026-08-13, `migrate validate` against an
		// UNHASHED directory with `--dev-url notadriver://x` prints `checksum
		// file not found`, so the URL must be settled after the integrity gate.
		// Only the direct branch runs that gate before it reaches the URL; a
		// refusal placed in this wrapper, in front of the forward, would answer
		// the driver on a directory the community binary answers the checksum on.
		// The forwarded native command owns its own, clearer wording for the same
		// value, which is why this cannot simply be left to it.
		directRefusedDevURL := verb.use == atlasMigrateValidateVerb().use &&
			atlasDevURLDriverDiagnostic(source.devURL) != nil
		virtualSource := source.project.isVirtualMigrationDir(source.localDir)
		if atlasmigrate.ReadsNativeAtlasDir(source.format) &&
			!directStrictReplay &&
			!directRefusedDevURL &&
			!virtualSource {
			return forward(cmd, source.forwardArgs)
		}
		// Only the directly executed path can report what the project file
		// declares and this run ignores; the forwarded native command reports
		// its own.
		if err := dbcli.ReportIgnoredAtlasConstructs(cmd.ErrOrStderr(), source.project.Config); err != nil {
			return err
		}
		return run(cmd, source)
	}
	return cmd
}

// resolveAtlasMigrateSource resolves the migration directory and its source
// layout from every layer the forwarded native command would see: explicit
// flags, PTAH_* environment values, the selected atlas.hcl environment, and
// the Atlas flag defaults.
func resolveAtlasMigrateSource(
	cmd *cobra.Command,
	verb atlasVerb,
	args []string,
	cleanup *cmdadapter.CleanupScope,
) (atlasMigrateSource, error) {
	project, err := resolveAtlasVerbProject(cmd, "migrate", verb, args, cleanup)
	if err != nil {
		return atlasMigrateSource{}, err
	}
	// atlasargs.Map applies the environment and default layers on top of the
	// atlas.hcl values resolveAtlasVerbProject already merged in. Running the
	// real mapper over a copy of the flag set whose --dir and --dir-format keep
	// their raw values is what makes this one resolution instead of a second
	// one that has to agree with the mapper by inspection.
	mapped, err := atlasargs.Map("migrate", verb.use, atlasMigrateSourceFlags(verb.flags), project.args)
	if err != nil {
		return atlasMigrateSource{}, err
	}

	rawDir, spelledByAtlas := resolveAtlasMigrateSourceDir(verb, mapped)
	localDir, err := atlasargs.ParseLocalDir(rawDir)
	if err != nil {
		return atlasMigrateSource{}, fmt.Errorf("atlas migrate %s --dir: %w", verb.use, err)
	}
	// A directory named through atlas.hcl is confined to the project root, and
	// the raw value the mapper carries is the path that resolution already
	// produced. Re-attaching the root here is what lets a caller reading the
	// directory read it through the same boundary rather than reopening the
	// resolved path unbounded.
	if project.project.migrationDirResolved && localDir.Path == project.project.migrationDir.Path {
		localDir.AllowedRoot = project.project.migrationDir.AllowedRoot
	}
	// The scheme requirement runs before the layout is resolved, and the order
	// is measured: `migrate new a --dir mig --dir-format nosuchformat` prints
	// the scheme refusal, not `unknown dir format`, on the pinned community
	// binary v1.3.0. Cobra's arity check still wins over both there, and does
	// here too, because it runs before RunE.
	//
	// The message is that binary's own and carries no `atlas migrate new --dir: `
	// prefix, which would cost the byte parity that is the point of reproducing
	// it. It goes out through cmdutil.Fail rather than being returned bare so
	// the `Error: ` line reaches stderr from the command itself, the way the
	// #1086 checksum refusal on the same two verbs already does.
	if spelledByAtlas && atlasDirSchemeIsAnswerable(project.project, rawDir) {
		if err := atlasargs.RequireDirScheme(rawDir); err != nil {
			return atlasMigrateSource{}, cmdutil.Fail(cmd, err)
		}
	}
	configured, _ := atlasNativeArgValue(mapped, atlasVerbNativeName(verb, "dir-format"))
	format, err := atlasmigrate.ResolveApplyDirFormat(configured, localDir.Query)
	if err != nil {
		return atlasMigrateSource{}, atlasDirFormatError(
			verb.use,
			atlasDirFormatSpelling(localDir.Query),
			err,
		)
	}

	// Positioned after the format value has been accepted and before this verb's
	// atlas.sum gate; see [reportIgnoredDirQuery] for the two rules that fix the
	// position.
	if err := reportIgnoredDirQuery(cmd.ErrOrStderr(), verb.use, localDir.Query); err != nil {
		return atlasMigrateSource{}, err
	}

	devURL, _ := atlasNativeArgValue(mapped, atlasVerbNativeName(verb, "dev-url"))
	source := atlasMigrateSource{
		dir:         localDir.Path,
		format:      format,
		devURL:      devURL,
		forwardArgs: args,
		localDir:    localDir,
		writeDir:    project.project.writeLocalDir(localDir),
		project:     project.project,
		projectArgs: project.args,
	}
	// Any query at all is dropped before forwarding, not just one carrying
	// ?format=. The forwarded native command's --dir mapper is query-free by
	// construction, so leaving an ignored key such as ?nonsense=1 on the URL
	// would resurrect the blanket refusal one layer down — exit 1 where the
	// community binary exits 0 (stokaro/ptah#1013 section 2).
	rewriteDir := atlasDirWithoutQuery(rawDir)
	rewriteSource := len(localDir.Query) > 0
	if verb.writesDir && project.project.isVirtualMigrationDir(localDir) {
		rewriteDir = source.writeDir.Path
		rewriteSource = true
	}
	if rewriteSource {
		source.forwardArgs = rewriteAtlasMigrateSourceArgs(verb, args, rewriteDir, string(format))
	}
	return source, nil
}

// resolveAtlasMigrateSourceDir returns the migration directory the forwarded
// native command will actually read, from every layer that can supply one.
//
// The layers are the forwarded command's own precedence, in its order:
//
//  1. the mapped native flag, which already carries an explicit --dir, the
//     Atlas-facing PTAH_DIR that [atlasargs.Map] folds in, and any value the
//     selected atlas.hcl env contributed;
//  2. the NATIVE flag's own PTAH_<NATIVE_FLAG> environment twin, which
//     cmdadapter installs on the target for every forwarded execution
//     (installForwardedTargetEnv) and which pflag applies to any flag the
//     mapper left unset;
//  3. the native flag's default.
//
// Layer 2 is the one that is easy to miss and the reason this is a named
// function. On `migrate new` the Atlas --dir maps to the native
// --migrations-dir, so PTAH_DIR and PTAH_MIGRATIONS_DIR are different
// variables naming the same directory, and only the first reaches
// atlasargs.Map. Reading layer 1 alone would leave the atlas.sum gate
// verifying `migrations` while the forwarded command wrote into
// $PTAH_MIGRATIONS_DIR -- a gate that verifies a directory nobody touches is
// not a gate. Verbs whose Atlas and native spellings coincide (`migrate hash`,
// `migrate validate`) see the same value from layers 1 and 2 and are
// unaffected.
//
// It also reports whether the value came from layer 1, the only layer an Atlas
// spelling reaches. Layers 2 and 3 are the NATIVE flag under its own name and
// its own default, which take a plain path and which the community binary has
// no spelling for at all, so a rule read off that binary — the scheme
// requirement the writing verbs enforce — must not be applied to them.
func resolveAtlasMigrateSourceDir(verb atlasVerb, mapped []string) (dir string, spelledByAtlas bool) {
	nativeName := atlasVerbNativeName(verb, "dir")
	if dir, found := atlasNativeArgValue(mapped, nativeName); found {
		return dir, true
	}
	if dir, found := os.LookupEnv(cmdflags.EnvName(atlasNativeEnvPrefix, nativeName)); found && dir != "" {
		return dir, false
	}
	// The Atlas --dir flag registers no default on these verbs, so an omitted
	// directory is whatever the forwarded native command defaults to. Reading
	// it from the native command keeps one default rather than a copy that can
	// drift.
	return atlasNativeFlagDefault(verb, "dir"), false
}

// atlasDirSchemeIsAnswerable reports whether the scheme the operator spelled on
// the migration directory has survived to this point.
//
// It has not when the directory came from atlas.hcl. `migration.dir` is
// normalized by config/projectconfig.normalizeAtlasMigrationDir, which strips
// `file://` at parse time, and the result is re-injected into the args as a
// bare `--dir <path>` — so by the time any verb sees it, `file://mig` and `mig`
// are the same string and requiring the scheme would refuse both. The community
// binary refuses only the second, so this layer stays as it is
// (stokaro/ptah#1186) rather than being closed by a check that cannot tell the
// two apart.
//
// The --dir flag and its PTAH_DIR environment twin arrive verbatim and are
// answerable.
func atlasDirSchemeIsAnswerable(project atlasProject, rawDir string) bool {
	return !project.migrationDirResolved || rawDir != project.migrationDir.Path
}

// atlasMigrateSourceFlags returns flags with the two flags the source
// resolution reads left unmapped, so the resolver observes the raw Atlas values
// — a --dir URL with its query intact, and a --dir-format naming a foreign
// tool — that the forwarding mappers reject outright.
func atlasMigrateSourceFlags(flags []atlasargs.Flag) []atlasargs.Flag {
	out := slices.Clone(flags)
	for i := range out {
		if out[i].Name == "dir" || out[i].Name == "dir-format" {
			out[i].MapValue = nil
		}
	}
	return out
}

// atlasVerbNativeName returns the native flag name an Atlas flag forwards to,
// so the resolver reads mapped arguments by the name they actually carry.
func atlasVerbNativeName(verb atlasVerb, name string) string {
	for _, flag := range verb.flags {
		if flag.Name != name {
			continue
		}
		if flag.NativeName != "" {
			return flag.NativeName
		}
		return flag.Name
	}
	return name
}

// atlasNativeFlagDefault returns the default the forwarded native command
// applies for one of its flags.
func atlasNativeFlagDefault(verb atlasVerb, name string) string {
	if verb.factory == nil {
		return ""
	}
	flag := verb.factory().Flags().Lookup(atlasVerbNativeName(verb, name))
	if flag == nil {
		return ""
	}
	return flag.DefValue
}

// atlasNativeArgValue returns the last value mapped native arguments give the
// named flag, matching pflag's last-one-wins parsing.
func atlasNativeArgValue(args []string, name string) (string, bool) {
	long := "--" + name
	value := ""
	found := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			break
		}
		if rest, inline := strings.CutPrefix(arg, long+"="); inline {
			value, found = rest, true
			continue
		}
		if arg == long && i+1 < len(args) {
			i++
			value, found = args[i], true
		}
	}
	return value, found
}

// atlasDirWithoutQuery drops the URL query from a migration directory value.
// The format selection it carried has already been resolved and is forwarded as
// --dir-format instead; every other key is one the community binary ignores, so
// dropping it here is what makes the forwarded invocation identical to the one
// the same directory gets with no query at all.
func atlasDirWithoutQuery(raw string) string {
	before, _, _ := strings.Cut(raw, "?")
	return before
}

// rewriteAtlasMigrateSourceArgs returns args with the --dir query removed and
// --dir-format set to the resolved format, so a native Atlas directory named
// through ?format= forwards exactly as the same directory named without one.
//
// The rewrite replaces the existing tokens rather than appending new ones,
// because atlasargs.Map maps every occurrence of a flag: an appended
// --dir-format=atlas beside a user's own --dir-format=goose would still run
// that goose value through the rejecting mapper. When a value came from the
// environment or from atlas.hcl instead of the command line, there is no token
// to replace and an appended one takes precedence over both.
func rewriteAtlasMigrateSourceArgs(verb atlasVerb, args []string, dir, dirFormat string) []string {
	valueFlags := atlasValueFlagNames(verb.flags)
	for _, name := range []string{atlasConfigFlagName, "c", dbcli.EnvFlagName, atlasVarFlagName} {
		valueFlags[name] = struct{}{}
	}

	out := make([]string, 0, len(args)+4)
	var tail []string
	dirSeen, formatSeen := false, false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			tail = args[i:]
			break
		}
		name, inlineValue, ok := atlasFlagName(arg)
		if !ok {
			out = append(out, arg)
			continue
		}
		rewritten := true
		switch name {
		case "dir":
			dirSeen = true
			out = append(out, "--dir="+dir)
		case "dir-format":
			formatSeen = true
			out = append(out, "--dir-format="+dirFormat)
		default:
			rewritten = false
			out = append(out, arg)
		}
		if inlineValue {
			continue
		}
		if _, valued := valueFlags[name]; !valued || i+1 >= len(args) {
			continue
		}
		i++
		if !rewritten {
			out = append(out, args[i])
		}
	}
	if !dirSeen {
		out = append(out, "--dir="+dir)
	}
	if !formatSeen {
		out = append(out, "--dir-format="+dirFormat)
	}
	return append(out, tail...)
}

// checkAtlasMigrateSourceArgs rejects arguments the verb does not accept.
//
// The diagnostics are deliberately the ones the forwarded native command
// produces for the same input — pflag's own "unknown flag" text and
// cmdutil.NoPositionalArgs's wording — so naming a directory's layout does not
// change how the verb reports a typo. Both are exit-1 refusals where Atlas CE
// exits 0 on a stray positional; that predates this path and holds on the
// forwarding path too.
func checkAtlasMigrateSourceArgs(cmd *cobra.Command, verb atlasVerb, args []string) error {
	flagSet := atlasVerbFlagSet(verb)
	if err := flagSet.Parse(args); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return cmdutil.NoPositionalArgsHint("name the migration directory with --dir")(cmd, flagSet.Args())
}

// atlasVerbFlagSet builds the pflag set for one Atlas verb's own flags, so a
// path that executes directly instead of forwarding sees the same flags the
// forwarded native command would have been given.
//
// It is shared with the converted `migrate new` path, which reads --edit and
// the positional migration name out of it. Two copies of this loop would be two
// answers to "which flags does this verb accept", and the second one is always
// the one that misses a newly registered flag.
func atlasVerbFlagSet(verb atlasVerb) *pflag.FlagSet {
	flagSet := pflag.NewFlagSet("atlas migrate "+verb.use, pflag.ContinueOnError)
	flagSet.SetOutput(io.Discard)
	for _, flag := range verb.flags {
		switch flag.Kind {
		case atlasargs.BoolFlag:
			flagSet.BoolP(flag.Name, flag.Shorthand, false, flag.Usage)
		case atlasargs.UintFlag:
			flagSet.UintP(flag.Name, flag.Shorthand, 0, flag.Usage)
		case atlasargs.StringFlag:
			flagSet.StringP(flag.Name, flag.Shorthand, flag.Default, flag.Usage)
		case atlasargs.StringArrayFlag:
			flagSet.StringSliceP(flag.Name, flag.Shorthand, nil, flag.Usage)
		}
	}
	return flagSet
}
