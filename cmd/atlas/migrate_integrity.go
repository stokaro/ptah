package atlas

import (
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
	"go.5x5.cz/ptah/internal/atlasmigrateimport"
)

// atlasMigrateIntegrityRunner reads or writes the Atlas integrity file of a
// migration directory laid out in a foreign tool's convention.
type atlasMigrateIntegrityRunner func(*cobra.Command, atlasMigrateSource) error

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
		source, err := resolveAtlasMigrateSource(cmd, verb, args, cleanup)
		if err != nil {
			return err
		}
		if atlasmigrate.ReadsNativeAtlasDir(source.format) {
			return forward(cmd, source.forwardArgs)
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

	rawDir, found := atlasNativeArgValue(mapped, atlasVerbNativeName(verb, "dir"))
	if !found {
		// The Atlas --dir flag registers no default on these verbs, so an
		// omitted directory is whatever the forwarded native command defaults
		// to. Reading it from the native command keeps one default rather than
		// a copy that can drift.
		rawDir = atlasNativeFlagDefault(verb, "dir")
	}
	localDir, err := atlasargs.ParseLocalDir(rawDir)
	if err != nil {
		return atlasMigrateSource{}, fmt.Errorf("atlas migrate %s --dir: %w", verb.use, err)
	}
	configured, _ := atlasNativeArgValue(mapped, atlasVerbNativeName(verb, "dir-format"))
	format, err := atlasmigrate.ResolveApplyDirFormat(configured, localDir.Query)
	if err != nil {
		// A ?format= query is the only thing that can carry a format value other
		// than the configured one, so it is the only thing that can be blamed
		// for a rejected one. A query carrying only ignored keys selects
		// nothing, and the blame stays on --dir-format.
		spelling := "--dir-format"
		if atlasmigrate.DirFormatFromQuery(localDir.Query) {
			spelling = "--dir"
		}
		return atlasMigrateSource{}, fmt.Errorf("atlas migrate %s %s: %w", verb.use, spelling, err)
	}

	devURL, _ := atlasNativeArgValue(mapped, atlasVerbNativeName(verb, "dev-url"))
	source := atlasMigrateSource{
		dir:         localDir.Path,
		format:      format,
		devURL:      devURL,
		forwardArgs: args,
	}
	// Any query at all is dropped before forwarding, not just one carrying
	// ?format=. The forwarded native command's --dir mapper is query-free by
	// construction, so leaving an ignored key such as ?nonsense=1 on the URL
	// would resurrect the blanket refusal one layer down — exit 1 where the
	// community binary exits 0 (stokaro/ptah#1013 section 2).
	if len(localDir.Query) > 0 {
		source.forwardArgs = rewriteAtlasMigrateSourceArgs(verb, args, atlasDirWithoutQuery(rawDir), string(format))
	}
	if atlasmigrate.ReadsNativeAtlasDir(format) {
		return source, nil
	}
	// The forwarding path lets the native command reject an unknown flag or a
	// stray positional. This path executes directly, so it has to reject them
	// itself instead of silently dropping them.
	if err := checkAtlasMigrateSourceArgs(cmd, verb, project.args); err != nil {
		return atlasMigrateSource{}, err
	}
	return source, nil
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
	if err := flagSet.Parse(args); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	return cmdutil.NoPositionalArgs(cmd, flagSet.Args())
}
