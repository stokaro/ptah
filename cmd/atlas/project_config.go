package atlas

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"go.5x5.cz/ptah/cmd/internal/cmdflags"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/internal/dbcli"
	"go.5x5.cz/ptah/cmd/internal/migrationsource"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasprojectpath"
	"go.5x5.cz/ptah/internal/atlasschema"
	"go.5x5.cz/ptah/internal/atlassource"
	"go.5x5.cz/ptah/internal/pathguard"
)

const (
	atlasConfigFlagName = "config"
	atlasVarFlagName    = "var"
)

type atlasProjectRequirement int

const (
	optionalAtlasProject atlasProjectRequirement = iota
	requiredAtlasProject
)

type atlasProjectFlagValues struct {
	configPath string
	envName    string
	vars       []string
}

type atlasProjectArgValues struct {
	flags         atlasProjectFlagValues
	changed       bool
	configChanged bool
	envChanged    bool
}

type atlasProject struct {
	projectconfig.Config
	root                 *pathguard.OpenedDirectory
	migrationDir         atlasargs.LocalDir
	migrationDirResolved bool
}

func (p *atlasProject) Close() error {
	if p == nil || p.root == nil {
		return nil
	}
	err := p.root.Close()
	p.root = nil
	return err
}

func closeAtlasProject(project *atlasProject, runErr *error) {
	*runErr = errors.Join(*runErr, project.Close())
}

func closeAtlasProjectOnError(project *atlasProject, runErr *error) {
	if *runErr != nil {
		closeAtlasProject(project, runErr)
	}
}

func (p atlasProject) localDirWithQuery(raw string) (atlasargs.LocalDir, error) {
	if p.root == nil {
		return atlasargs.LocalDir{}, fmt.Errorf("atlas project root is unavailable")
	}
	return atlasProjectConfigLocalDirWithQueryFromBaseDir(raw, p.root.Path())
}

func (p *atlasProject) resolveMigrationDirForArgs(
	flags []atlasargs.Flag,
	args []string,
) error {
	migrationDir := p.StringValue(projectconfig.StringMigrationDir)
	if !migrationDir.Present ||
		!atlasFlagRegistered(flags, "dir") ||
		atlasFlagValueSet(flags, args, "dir") {
		return nil
	}
	dir, err := p.localDirWithQuery(migrationDir.Value)
	if err != nil {
		return fmt.Errorf("atlas.hcl migration.dir: %w", err)
	}
	if len(dir.Query) > 0 {
		return fmt.Errorf("atlas.hcl migration.dir: migration directory URL query parameters are not supported yet")
	}
	p.migrationDir = dir
	p.migrationDirResolved = true
	return nil
}

func (p atlasProject) localOptions(dir atlasargs.LocalDir) migrationsource.LocalOptions {
	if dir.AllowedRoot != "" && p.root != nil {
		return migrationsource.LocalOptions{Root: p.root}
	}
	return migrationsource.LocalOptions{AllowedRoot: dir.AllowedRoot}
}

func (p atlasProject) captureLocal(dir atlasargs.LocalDir) (migrationsource.LocalSource, error) {
	return migrationsource.CaptureLocal(dir.Path, p.localOptions(dir))
}

func (p atlasProject) openLocal(dir atlasargs.LocalDir) (*migrationsource.LocalDirectory, error) {
	return migrationsource.OpenLocal(dir.Path, p.localOptions(dir))
}

func (p atlasProject) statLocalDir(dir atlasargs.LocalDir) (fs.FileInfo, error) {
	if dir.AllowedRoot == "" || p.root == nil {
		return os.Stat(dir.Path)
	}
	opened, err := p.root.OpenDirectory(dir.Path)
	if err != nil {
		return nil, err
	}
	info, statErr := opened.Stat(".")
	closeErr := opened.Close()
	return info, errors.Join(statErr, closeErr)
}

func registerAtlasProjectFlags(flags *pflag.FlagSet, target *atlasProjectFlagValues) {
	if flags.Lookup(atlasConfigFlagName) == nil {
		flags.StringVarP(&target.configPath, atlasConfigFlagName, "c", "file://"+projectconfig.AtlasFileName, "select config (project) file using URL format")
	}
	// --var is registered before --env, and the order is load-bearing:
	// dbcli.RegisterEnvFlag also registers --var (idempotently), so letting it
	// run first would leave this surface with the native flag — unbound to
	// target.vars and carrying the native help text where Atlas's belongs.
	// TestCompatVarFlagKeepsAtlasUsage fails if this order is swapped.
	// Registered as [atlasVarValue] rather than as a plain string array so the
	// help line carries the pinned binary's own placeholder,
	// `--var <name>=<value>`, which the conformance cli-surface tier asserts.
	// The value only renders and collects; the syntax rule is
	// [validateAtlasVarFlagValue].
	if flags.Lookup(atlasVarFlagName) == nil {
		flags.Var(newAtlasVarValue(&target.vars), atlasVarFlagName, "input variables")
	}
	if flags.Lookup(dbcli.EnvFlagName) == nil {
		dbcli.RegisterEnvFlag(flags, &target.envName)
	}
}

func openAtlasProject(
	flags atlasProjectFlagValues,
	requirement atlasProjectRequirement,
) (atlasProject, bool, error) {
	path, err := atlasConfigPathValue(flags.configPath)
	if err != nil {
		return atlasProject{}, false, err
	}
	absolute, err := filepath.Abs(path)
	if err != nil {
		return atlasProject{}, false, fmt.Errorf("resolve atlas config path %s: %w", path, err)
	}
	root, err := pathguard.OpenDirectory(filepath.Dir(absolute))
	if err != nil {
		return atlasProject{}, false, fmt.Errorf("open atlas config directory %s: %w", filepath.Dir(path), err)
	}
	raw, err := fs.ReadFile(root.FS(), filepath.Base(absolute))
	if errors.Is(err, fs.ErrNotExist) && requirement == optionalAtlasProject {
		closeErr := root.Close()
		return atlasProject{}, false, closeErr
	}
	if err != nil {
		closeErr := root.Close()
		return atlasProject{}, false, errors.Join(
			fmt.Errorf("failed to read atlas config %s: %w", path, err),
			closeErr,
		)
	}
	cfg, err := projectconfig.ParseAtlasFSWithOptions(raw, path, root.FS(), projectconfig.AtlasLoadOptions{
		EnvName: flags.envName,
		Vars:    flags.vars,
	})
	if err != nil {
		closeErr := root.Close()
		return atlasProject{}, false, errors.Join(err, closeErr)
	}
	return atlasProject{
		Config: cfg,
		root:   root,
	}, true, nil
}

func openRequiredMergedProjectConfig(
	flags atlasProjectFlagValues,
) (project atlasProject, mergedConfig projectconfig.Config, err error) {
	project, _, err = openAtlasProject(flags, requiredAtlasProject)
	if err != nil {
		return atlasProject{}, projectconfig.Config{}, err
	}
	ptah, err := projectconfig.LoadPtahFile(projectconfig.PtahFileName, flags.envName)
	if err != nil {
		closeErr := project.Close()
		return atlasProject{}, projectconfig.Config{}, errors.Join(err, closeErr)
	}
	return project, projectconfig.Merge(ptah, project.Config), nil
}

func atlasConfigPathValue(value string) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", fmt.Errorf("--config must not be empty")
	}
	if path, found := strings.CutPrefix(value, "file://"); found {
		if strings.TrimSpace(path) == "" {
			return "", fmt.Errorf("--config must not be empty")
		}
		return path, nil
	}
	if strings.Contains(value, "://") {
		return "", fmt.Errorf("unsupported atlas --config URL %q: only local file:// config files are supported", value)
	}
	return value, nil
}

func loadOptionalAtlasProjectConfigForCommand(
	cmd *cobra.Command,
) (projectconfig.Config, bool, error) {
	return loadAtlasProjectConfigForCommand(cmd, ignoreMissingEnvSelection)
}

func atlasProjectConfigSchemaURLs(cmd *cobra.Command, raw []string) ([]string, error) {
	flags, _, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return nil, err
	}
	return atlasProjectConfigSchemaURLsFromFlags(flags, raw)
}

func atlasProjectConfigLocalDirWithQueryFromBaseDir(
	raw string,
	baseDir string,
) (atlasargs.LocalDir, error) {
	path, query, err := atlasprojectpath.LocalDirWithQuery(raw, baseDir)
	if err != nil {
		return atlasargs.LocalDir{}, err
	}
	parsed, err := atlasargs.ParseLocalDir(raw)
	if err != nil {
		return atlasargs.LocalDir{}, err
	}
	allowedRoot := ""
	if !filepath.IsAbs(parsed.Path) {
		relative, relativeErr := filepath.Rel(baseDir, filepath.Join(baseDir, parsed.Path))
		if relativeErr == nil &&
			relative != ".." &&
			!strings.HasPrefix(relative, ".."+string(filepath.Separator)) &&
			!filepath.IsAbs(relative) {
			allowedRoot = baseDir
		}
	}
	return atlasargs.LocalDir{
		Path:        path,
		Query:       query,
		AllowedRoot: allowedRoot,
	}, nil
}

func atlasProjectConfigSchemaURLsFromFlags(flags atlasProjectFlagValues, raw []string) ([]string, error) {
	baseDir, err := atlasProjectConfigBaseDir(flags)
	if err != nil {
		return nil, err
	}
	return atlasprojectpath.SchemaFileURLs(raw, baseDir)
}

func atlasProjectConfigBaseDir(flags atlasProjectFlagValues) (string, error) {
	configPath, err := atlasConfigPathValue(flags.configPath)
	if err != nil {
		return "", err
	}
	return filepath.Dir(configPath), nil
}

type missingAtlasEnvSelectionMode int

const (
	ignoreMissingEnvSelection missingAtlasEnvSelectionMode = iota
	reportMissingEnvSelection
)

func loadAtlasProjectConfigForCommand(
	cmd *cobra.Command,
	mode missingAtlasEnvSelectionMode,
) (projectconfig.Config, bool, error) {
	project, loaded, err := openAtlasProjectForCommand(cmd, mode)
	if err != nil {
		return projectconfig.Config{}, false, err
	}
	closeErr := project.Close()
	return project.Config, loaded, closeErr
}

func openAtlasProjectForCommand(
	cmd *cobra.Command,
	mode missingAtlasEnvSelectionMode,
) (atlasProject, bool, error) {
	flags, changed, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return atlasProject{}, false, err
	}
	requirement := optionalAtlasProject
	if changed {
		requirement = requiredAtlasProject
	}
	project, loaded, err := openAtlasProject(flags, requirement)
	if err != nil {
		if isAtlasEnvSelectionRequired(err) && mode == ignoreMissingEnvSelection {
			return atlasProject{}, false, nil
		}
		return atlasProject{}, false, err
	}
	if loaded {
		if err := dbcli.ReportIgnoredAtlasConstructs(cmd.ErrOrStderr(), project.Config); err != nil {
			return atlasProject{}, false, errors.Join(err, project.Close())
		}
	}
	return project, loaded, nil
}

func atlasProjectFlagsFromCommand(cmd *cobra.Command) (atlasProjectFlagValues, bool, error) {
	if err := refreshAtlasProjectFlagEnvironment(cmd); err != nil {
		return atlasProjectFlagValues{}, false, err
	}
	flags := atlasProjectFlagValues{configPath: "file://" + projectconfig.AtlasFileName}
	changed := false
	if flag := cmd.Flags().Lookup(atlasConfigFlagName); flag != nil {
		flags.configPath = flag.Value.String()
		changed = changed || flag.Changed
	}
	if flag := cmd.Flags().Lookup(dbcli.EnvFlagName); flag != nil {
		flags.envName = flag.Value.String()
		changed = changed || flag.Changed
	}
	// --var deliberately does NOT contribute to `changed`, which is what makes
	// the project file REQUIRED. Measured on the pinned Atlas community binary
	// v1.3.0 in a directory holding no atlas.hcl:
	//
	//	schema diff --from file://empty.hcl --to file://v.hcl --var status=live
	//	  -> exit 0, DEFAULT 'live'
	//
	// Ptah answered `failed to read atlas config atlas.hcl: no such file or
	// directory` at exit 1, because --var reached only config/projectconfig and
	// dragged the project requirement along with it. The values are still
	// carried below, so an atlas.hcl that IS present still gets them.
	values, err := atlasVarFlagValues(cmd)
	if err != nil {
		return atlasProjectFlagValues{}, false, err
	}
	flags.vars = values
	return flags, changed, nil
}

// atlasVarFlagValues reads --var, which the Atlas-compatible commands hand both
// to the project config and to the HCL schema files they load.
//
// It re-checks the syntax rather than trusting the caller. Every value cobra
// parsed has already been through [validateAtlasVarFlagsOnCommand], but a value
// [refreshAtlasProjectFlagEnvironment] lifted out of `PTAH_VAR` arrives here
// first: that runs inside the verb, after every PreRunE has returned.
func atlasVarFlagValues(cmd *cobra.Command) ([]string, error) {
	flag := cmd.Flags().Lookup(atlasVarFlagName)
	if flag == nil {
		return nil, nil
	}
	values, err := atlasVarFlagRawValues(flag)
	if err != nil {
		return nil, err
	}
	for _, value := range values {
		if err := validateAtlasVarFlagValue(value); err != nil {
			return nil, err
		}
	}
	return values, nil
}

// atlasVarFlagSpelling is the flag name the pinned binary quotes in its own
// refusal. pflag wraps a flag value parser's failure as
// `invalid argument %q for %q flag: %v`, and that binary's --var is such a
// value, so the wording below is that wrapper reproduced rather than invented.
const atlasVarFlagSpelling = "--" + atlasVarFlagName

// validateAtlasVarFlagValue refuses a --var value whose syntax the pinned Atlas
// community binary v1.3.0 refuses, at the point the value is READ rather than
// at the point a project file consumes it.
//
// Where the check runs is the whole point. That binary parses --var while
// parsing flags, before it looks for an atlas.hcl, so a malformed value is
// refused with no project file in sight. Ptah checked it only inside
// config/projectconfig and internal/atlashcl, both of which run after a project
// file has been found, so `--var foo` in a directory with no atlas.hcl was
// accepted -- and on `migrate new` it wrote a migration directory, a migration
// file and an atlas.sum on an argv the pinned binary refuses without touching
// the disk (stokaro/ptah#1241).
//
// Measured on 2026-08-08 in a directory holding a hashed ./migrations and no
// atlas.hcl, each cell's exit status read on a line of its own:
//
//	--var foo      exit 1  invalid argument "foo" for "--var" flag: variables must be format as key=value, got: "foo"
//	--var=foo      exit 1  the same message
//	--var a=1      exit 0
//	--var a=1,b    exit 1  ... got: "b"      (the value is CSV, and each field is checked)
//	--var ""       exit 1  invalid argument "" for "--var" flag: EOF
//	--var a="b     exit 1  invalid argument "a=\"b" for "--var" flag: parse error on line 1, column 3: bare " in non-quoted-field
//	--var =v       exit 0
//
// The last row is why this tests for the separator alone and not for a
// non-empty name: that binary accepts `=v`. config/projectconfig still refuses
// an empty name later, where a project file is actually being evaluated, and
// stays stricter there deliberately.
func validateAtlasVarFlagValue(raw string) error {
	err := atlasVarSyntaxError(raw)
	if err == nil {
		return nil
	}
	return fmt.Errorf("invalid argument %q for %q flag: %w", raw, atlasVarFlagSpelling, err)
}

// atlasVarSyntaxError returns why the pinned binary refuses raw, or nil when it
// accepts it. It is the INNER error only: on that binary --var is a pflag value,
// so pflag adds the `invalid argument %q for %q flag: %v` wrapper around
// whatever its value parser returned, and [validateAtlasVarFlagValue] is that
// wrapper reproduced. Keeping the two apart is what lets the wrapped message and
// the raw rule be one rule rather than two strings that have to agree.
//
// The CSV reader is not decoration: that binary splits a --var value as CSV and
// checks each field, so `--var a=1,b` is refused naming `b`, an empty --var
// fails with `EOF`, and a --var carrying an unbalanced quote fails with the
// reader's own parse error. All three are in [validateAtlasVarFlagValue]'s
// measured table.
func atlasVarSyntaxError(raw string) error {
	values, err := csv.NewReader(strings.NewReader(raw)).Read()
	if err != nil {
		return err
	}
	for _, value := range values {
		if strings.Contains(value, "=") {
			continue
		}
		return fmt.Errorf("variables must be format as key=value, got: %q", value)
	}
	return nil
}

// validateAtlasVarFlagsOnCommand refuses every malformed --var value cobra
// parsed for cmd.
//
// It is the tree-wide gate, and it exists because per-consumer checking left
// holes. --var is registered once, on the PersistentFlags of the `schema` and
// `migrate` groups, so EVERY descendant accepts it -- but the syntax was checked
// only where a consumer asked for the values, through [atlasVarFlagValues] or
// through [extractAtlasProjectArgs]. Four commands ask for them nowhere and so
// checked nothing. Measured on 2026-08-08 against the pinned Atlas community
// binary v1.3.0, each cell in its own directory with the exit status read on a
// line of its own after a redirect:
//
//	schema fmt --var foo                     that binary 1, ptah-compat 0 (and it reformatted a.hcl)
//	migrate import --from … --to … --var foo  that binary 1 writing NOTHING,
//	                                         ptah-compat 0 CREATING ./dst, two
//	                                         migration files and an atlas.sum
//	schema --var foo                         that binary 1, ptah-compat 0 (group help)
//	migrate --var foo                        that binary 1, ptah-compat 0 (group help)
//
// The gate runs from the PreRunE that [wrapAtlasProjectFlagReset] installs on
// every descendant of both groups, which is the one hook a verb cannot opt out
// of by not consuming the flag. That binary refuses the value while PARSING
// flags, one step earlier, and the step between the two is cobra's positional
// check: an argv carrying both a bad positional and a malformed --var is
// answered here by the positional and there by the flag. Both exit 1, so the
// order cannot make this surface the looser one.
//
// A pflag value whose Set refuses would land on that binary's exact step, and
// was rejected with a measurement rather than on taste: a registered value that
// does not implement [pflag.SliceValue] drops out of that branch of
// [resetAtlasProjectFlags], whose fallback appends flag.DefValue instead of
// clearing -- measured, `[a=1]` became `[a=1,[]]` -- so `--var` would leak and
// grow across Execute calls on a reused root, with the error discarded. The
// flag IS registered as a value, [atlasVarValue], for the help placeholder the
// cli-surface tier asserts; that value implements [pflag.SliceValue] so the
// reset still clears it, and it deliberately does not refuse in Set, so the
// rule stays here where the CSV semantics live.
func validateAtlasVarFlagsOnCommand(cmd *cobra.Command) error {
	flag := cmd.Flags().Lookup(atlasVarFlagName)
	if flag == nil {
		return nil
	}
	values, err := atlasVarFlagRawValues(flag)
	if err != nil {
		return err
	}
	for _, value := range values {
		if err := validateAtlasVarFlagValue(value); err != nil {
			return err
		}
	}
	return nil
}

func refreshAtlasProjectFlagEnvironment(cmd *cobra.Command) error {
	for _, name := range []string{atlasConfigFlagName, dbcli.EnvFlagName, atlasVarFlagName} {
		flag := cmd.Flags().Lookup(name)
		if flag == nil || flag.Changed {
			continue
		}
		value, ok := os.LookupEnv(cmdflags.EnvName("PTAH", name))
		if !ok || value == "" {
			continue
		}
		if err := cmd.Flags().Set(name, value); err != nil {
			return fmt.Errorf("apply %s: %w", cmdflags.EnvName("PTAH", name), err)
		}
	}
	return nil
}

func installAtlasProjectFlagResetTree(group *cobra.Command) {
	wrapAtlasProjectFlagReset(group, group)
	for _, child := range group.Commands() {
		installAtlasProjectFlagResetSubtree(child, group)
	}
}

func installAtlasProjectFlagResetSubtree(cmd, group *cobra.Command) {
	wrapAtlasProjectFlagReset(cmd, group)
	for _, child := range cmd.Commands() {
		installAtlasProjectFlagResetSubtree(child, group)
	}
}

func wrapAtlasProjectFlagReset(cmd, group *cobra.Command) {
	validateArgs := cmd.Args
	cmd.Args = func(cmd *cobra.Command, args []string) error {
		// Cobra retains a child command's first context when a root command is
		// reused. Refresh it before validation so each ExecuteContext call
		// reaches direct commands as well as adapter-backed commands.
		cmd.SetContext(cmd.Root().Context())
		if validateArgs == nil {
			return nil
		}
		err := validateArgs(cmd, args)
		if err != nil {
			resetAtlasExecutionFlags(cmd, group)
		}
		return err
	}
	if persistentPreRunE := cmd.PersistentPreRunE; persistentPreRunE != nil {
		cmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
			err := persistentPreRunE(cmd, args)
			if err != nil {
				resetAtlasExecutionFlags(cmd, group)
			}
			return err
		}
	}
	preRunE := cmd.PreRunE
	preRun := cmd.PreRun
	cmd.PreRun = nil
	cmd.PreRunE = func(cmd *cobra.Command, args []string) error {
		// Before the verb's own PreRunE, so a malformed --var cannot reach any
		// work the verb does there either. See validateAtlasVarFlagsOnCommand:
		// this PreRunE is installed on every descendant of both groups, which is
		// what makes it the one place the check covers the verbs that never
		// consume the flag.
		if err := validateAtlasVarFlagsOnCommand(cmd); err != nil {
			resetAtlasExecutionFlags(cmd, group)
			// Through cmdutil.Fail, like every other refusal on this surface, so
			// the diagnostic reaches the COMMAND's writer rather than only the
			// process's stderr. A caller embedding the tree and reading
			// cmd.SetErr sees the refusal either way, and the printed bytes stay
			// the one line the pinned binary prints.
			return cmdutil.Fail(cmd, err)
		}
		if preRunE != nil {
			if err := preRunE(cmd, args); err != nil {
				resetAtlasExecutionFlags(cmd, group)
				return err
			}
		} else if preRun != nil {
			preRun(cmd, args)
		}
		if err := cmd.ValidateRequiredFlags(); err != nil {
			resetAtlasExecutionFlags(cmd, group)
			return err
		}
		if err := cmd.ValidateFlagGroups(); err != nil {
			resetAtlasExecutionFlags(cmd, group)
			return err
		}
		return nil
	}
	if runE := cmd.RunE; runE != nil {
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			defer resetAtlasExecutionFlags(cmd, group)
			return runE(cmd, args)
		}
	}
	if run := cmd.Run; run != nil {
		cmd.Run = func(cmd *cobra.Command, args []string) {
			defer resetAtlasExecutionFlags(cmd, group)
			run(cmd, args)
		}
	}
	help := cmd.HelpFunc()
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		defer resetAtlasExecutionFlags(cmd, group)
		help(cmd, args)
	})
	flagError := cmd.FlagErrorFunc()
	cmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		defer resetAtlasExecutionFlags(cmd, group)
		return flagError(cmd, err)
	})
}

func resetAtlasExecutionFlags(cmd, group *cobra.Command) {
	resetAtlasProjectFlags(group)
	cmd.LocalNonPersistentFlags().VisitAll(func(flag *pflag.Flag) {
		if value, ok := flag.Value.(pflag.SliceValue); ok {
			_ = value.Replace(nil)
		} else {
			_ = flag.Value.Set(flag.DefValue)
		}
		flag.Changed = false
	})
}

func installAtlasProjectFlagResetRoot(root *cobra.Command, groups ...*cobra.Command) {
	resetGroups := func() {
		for _, group := range groups {
			resetAtlasProjectFlags(group)
		}
	}
	rootArgs := root.Args
	root.Args = func(cmd *cobra.Command, args []string) error {
		err := rootArgs(cmd, args)
		if err != nil {
			resetGroups()
		}
		return err
	}
	rootHelp := root.HelpFunc()
	root.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		defer resetGroups()
		rootHelp(cmd, args)
	})
	rootFlagError := root.FlagErrorFunc()
	root.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		defer resetGroups()
		return rootFlagError(cmd, err)
	})
	if runE := root.RunE; runE != nil {
		root.RunE = func(cmd *cobra.Command, args []string) error {
			defer resetGroups()
			return runE(cmd, args)
		}
	}
	if run := root.Run; run != nil {
		root.Run = func(cmd *cobra.Command, args []string) {
			defer resetGroups()
			run(cmd, args)
		}
	}
	// Generated commands such as __complete do not pass through the wrapped
	// static tree, so successful child execution needs the same cleanup.
	persistentPostRunE := root.PersistentPostRunE
	persistentPostRun := root.PersistentPostRun
	root.PersistentPostRun = nil
	root.PersistentPostRunE = func(cmd *cobra.Command, args []string) error {
		defer resetGroups()
		if persistentPostRunE != nil {
			return persistentPostRunE(cmd, args)
		}
		if persistentPostRun != nil {
			persistentPostRun(cmd, args)
		}
		return nil
	}
}

func resetAtlasProjectFlags(group *cobra.Command) {
	for _, name := range []string{atlasConfigFlagName, dbcli.EnvFlagName, atlasVarFlagName} {
		flag := group.PersistentFlags().Lookup(name)
		if flag == nil {
			continue
		}
		if value, ok := flag.Value.(pflag.SliceValue); ok {
			_ = value.Replace(nil)
		} else {
			_ = flag.Value.Set(flag.DefValue)
		}
		flag.Changed = false
	}
}

func extractAtlasProjectArgs(args []string) (atlasProjectArgValues, []string, error) {
	project := atlasProjectArgValues{
		flags: atlasProjectFlagValues{configPath: "file://" + projectconfig.AtlasFileName},
	}
	remaining := make([]string, 0, len(args))
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if arg == "--" {
			remaining = append(remaining, args[i:]...)
			break
		}
		if value, ok := strings.CutPrefix(arg, "--config="); ok {
			project.flags.configPath = value
			project.changed = true
			project.configChanged = true
			continue
		}
		if arg == "--config" || arg == "-c" {
			value, next, err := nextAtlasProjectArgValue(args, i, arg)
			if err != nil {
				return atlasProjectArgValues{}, nil, err
			}
			project.flags.configPath = value
			project.changed = true
			project.configChanged = true
			i = next
			continue
		}
		if value, ok := strings.CutPrefix(arg, "-c="); ok {
			project.flags.configPath = value
			project.changed = true
			project.configChanged = true
			continue
		}
		if value, ok := strings.CutPrefix(arg, "-c"); ok && value != "" {
			project.flags.configPath = value
			project.changed = true
			project.configChanged = true
			continue
		}
		if value, ok := strings.CutPrefix(arg, "--env="); ok {
			project.flags.envName = value
			project.changed = true
			project.envChanged = true
			continue
		}
		if arg == "--env" {
			value, next, err := nextAtlasProjectArgValue(args, i, arg)
			if err != nil {
				return atlasProjectArgValues{}, nil, err
			}
			project.flags.envName = value
			project.changed = true
			project.envChanged = true
			i = next
			continue
		}
		// --var collects its value but deliberately does NOT set `changed`,
		// so it cannot make the project file REQUIRED. This is the same rule
		// atlasProjectFlagsFromCommand applies to the parsed-flag surface; the
		// two paths differ only in where the flag arrives, and a flag whose
		// meaning depends on that is a flag with two meanings.
		//
		// Measured on the pinned Atlas community binary v1.3.0, in a directory
		// holding no atlas.hcl:
		//
		//	migrate validate --dir file://migrations --var foo=bar -> exit 0
		//	migrate new nm   --dir file://migrations --var foo=bar -> exit 0
		//
		// Ptah answered `failed to read atlas config atlas.hcl: openat
		// atlas.hcl: no such file or directory` at exit 1 on every verb whose
		// command disables flag parsing and therefore reaches this extractor:
		// migrate hash, validate, new, down, checkpoint, edit, rebase, rm,
		// test and schema test (stokaro/ptah#1241 item 12).
		//
		// The values are still carried, and resolveAtlasVerbProject loads an
		// atlas.hcl that IS present, so `--var` keeps feeding a project file
		// the caller actually has.
		//
		// The SYNTAX is checked here all the same. Not requiring a project file
		// is not the same as not reading the flag: the pinned binary refuses a
		// malformed value while parsing flags, before it looks for an atlas.hcl,
		// and dropping the requirement without keeping the check is what made
		// `migrate new nm --var foo` write a migration directory where that
		// binary writes nothing. See [validateAtlasVarFlagValue].
		if value, ok := strings.CutPrefix(arg, "--var="); ok {
			if err := validateAtlasVarFlagValue(value); err != nil {
				return atlasProjectArgValues{}, nil, err
			}
			project.flags.vars = append(project.flags.vars, value)
			continue
		}
		if arg == "--var" {
			value, next, err := nextAtlasProjectArgValue(args, i, arg)
			if err != nil {
				return atlasProjectArgValues{}, nil, err
			}
			if err := validateAtlasVarFlagValue(value); err != nil {
				return atlasProjectArgValues{}, nil, err
			}
			project.flags.vars = append(project.flags.vars, value)
			i = next
			continue
		}
		remaining = append(remaining, arg)
	}
	return project, remaining, nil
}

func mergeAtlasProjectArgs(
	parent atlasProjectArgValues,
	raw atlasProjectArgValues,
) atlasProjectArgValues {
	// --var marks neither side as changed, so the variables have to be merged
	// BEFORE the two short circuits below. Returning one side whole would drop
	// the other side's variables, which is how `--var` reaching a command
	// through both the parsed flag set and the raw arguments would end up
	// carrying only one of them.
	vars := make([]string, 0, len(parent.flags.vars)+len(raw.flags.vars))
	vars = append(vars, parent.flags.vars...)
	vars = append(vars, raw.flags.vars...)
	if !parent.changed {
		raw.flags.vars = vars
		return raw
	}
	if !raw.changed {
		parent.flags.vars = vars
		return parent
	}
	if !raw.configChanged {
		raw.flags.configPath = parent.flags.configPath
	}
	if !raw.envChanged {
		raw.flags.envName = parent.flags.envName
	}
	raw.flags.vars = vars
	raw.changed = true
	return raw
}

func nextAtlasProjectArgValue(args []string, index int, flagName string) (string, int, error) {
	next := index + 1
	if next >= len(args) || args[next] == "--" {
		return "", index, fmt.Errorf("%s requires a value", flagName)
	}
	return args[next], next, nil
}

// atlasProjectArgsApplier maps loaded atlas.hcl values onto a verb's Atlas
// flags before atlasargs.Map translates them to native flags.
type atlasProjectArgsApplier func(
	flags []atlasargs.Flag,
	args []string,
	project atlasProject,
	projectFlags atlasProjectFlagValues,
) ([]string, error)

func applyAtlasProjectConfigToArgs(
	flags []atlasargs.Flag,
	args []string,
	project atlasProject,
	_ atlasProjectFlagValues,
) ([]string, error) {
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"url",
		project.StringValue(projectconfig.StringDatabaseURL),
	)
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"dev-url",
		project.StringValue(projectconfig.StringDevURL),
	)
	if project.migrationDirResolved {
		args = append(args, "--dir", project.migrationDir.Path)
	}
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"dir-format",
		project.StringValue(projectconfig.StringMigrationFormat),
	)
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"revisions-schema",
		project.StringValue(projectconfig.StringMigrationRevisionsSchema),
	)
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"lock-timeout",
		project.StringValue(projectconfig.StringMigrationLockTimeout),
	)
	cliLatest := atlasFlagValueSet(flags, args, "latest")
	cliGitBase := atlasFlagValueSet(flags, args, "git-base")
	if !cliGitBase {
		args = appendAtlasProjectStringArg(flags, args, "latest", atlasProjectLatest(project.Config))
	}
	gitBase := project.StringValue(projectconfig.StringLintGitBase)
	if !cliLatest && gitBase.Value != "" {
		args = appendAtlasProjectStringArg(flags, args, "git-base", gitBase)
	}
	gitDir := project.StringValue(projectconfig.StringLintGitDir)
	if !cliLatest && gitDir.Value != "" {
		args = appendAtlasProjectStringArg(flags, args, "git-dir", gitDir)
	}
	return args, nil
}

// applyAtlasSchemaTestProjectConfig maps atlas.hcl values onto the schema test
// verb. Unlike the generic applier, the verb's --url flag is the desired
// schema source (env schema.src), not the target database URL (env url), so
// env url must never be injected into it. A single schema.src entry becomes
// the desired-schema --url; multiple sources cannot map onto the native
// single-root scan and are rejected loudly.
func applyAtlasSchemaTestProjectConfig(
	flags []atlasargs.Flag,
	args []string,
	project atlasProject,
	projectFlags atlasProjectFlagValues,
) ([]string, error) {
	cfg := project.Config
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"dev-url",
		cfg.StringValue(projectconfig.StringDevURL),
	)
	if atlasFlagValueSet(flags, args, "url") {
		return args, nil
	}
	// The schema test verb consumes a single local schema file as --url; an
	// external schema program has no file spelling to map onto it.
	if atlasExternalSchemaConfigured(cfg) {
		return nil, fmt.Errorf(
			"atlas schema test does not support atlas.hcl data.external_schema desired state yet; pass --url explicitly",
		)
	}
	sources := cfg.SchemaSourcesValue()
	if !sources.Present {
		return args, nil
	}
	if len(sources.Value) == 0 {
		return nil, fmt.Errorf("atlas.hcl schema.src: desired schema source is required")
	}
	if len(sources.Value) > 1 {
		return nil, fmt.Errorf("atlas schema test supports one atlas.hcl schema source, got %d", len(sources.Value))
	}
	// A database desired-state source has no local path to resolve against the
	// atlas.hcl directory, and the shared local-file resolver refuses it with
	// "only local file:// schema files are supported". That resolver is left
	// exactly as it is because `schema diff`'s env path pins its wording; the
	// schema-test accommodation belongs here, on the one verb that wants it.
	if source, err := atlassource.Classify(sources.Value[0]); err == nil && source.Kind == atlassource.KindDatabase {
		return append(args, "--url", strings.TrimSpace(sources.Value[0])), nil
	}
	urls, err := atlasProjectConfigSchemaURLsFromFlags(projectFlags, sources.Value)
	if err != nil {
		return nil, fmt.Errorf("atlas.hcl schema.src: %w", err)
	}
	return append(args, "--url", urls[0]), nil
}

// atlasExternalSchemaConfigured reports whether the loaded atlas.hcl env's
// desired state is a data.external_schema program. The parser consumed the
// data source marker into ExternalSchema and cleared the schema sources, so
// commands spell the desired state as env://src and let the source resolver
// expand (and gate) the program.
func atlasExternalSchemaConfigured(cfg projectconfig.Config) bool {
	return len(cfg.ExternalSchema.Program) > 0
}

func atlasProjectLatest(cfg projectconfig.Config) projectconfig.Value[string] {
	latest := cfg.LintLatestValue()
	return projectconfig.Value[string]{
		Value:   fmt.Sprint(latest.Value),
		Present: latest.Present,
	}
}

func appendAtlasProjectStringArg(
	flags []atlasargs.Flag,
	args []string,
	name string,
	value projectconfig.Value[string],
) []string {
	if !value.Present || !atlasFlagRegistered(flags, name) || atlasFlagValueSet(flags, args, name) {
		return args
	}
	return append(args, "--"+name, value.Value)
}

func atlasFlagRegistered(flags []atlasargs.Flag, name string) bool {
	for _, flag := range flags {
		if flag.Name == name {
			return true
		}
	}
	return false
}

func atlasFlagPresent(flags []atlasargs.Flag, args []string, name string) bool {
	return atlasFlagPresentByName(args, name, atlasFlagShorthand(flags, name))
}

func atlasFlagValueSet(flags []atlasargs.Flag, args []string, name string) bool {
	return atlasFlagPresent(flags, args, name) || atlasFlagEnvironmentPresent(flags, name)
}

func atlasFlagEnvironmentPresent(flags []atlasargs.Flag, name string) bool {
	for _, flag := range flags {
		if flag.Name != name || flag.EnvDisabled {
			continue
		}
		if nonEmptyEnvironmentVariable(cmdflags.EnvName("PTAH", flag.Name)) {
			return true
		}
		if flag.NativeName != "" &&
			nonEmptyEnvironmentVariable(cmdflags.EnvName("PTAH", flag.NativeName)) {
			return true
		}
	}
	return false
}

func nonEmptyEnvironmentVariable(name string) bool {
	value, ok := os.LookupEnv(name)
	return ok && value != ""
}

func atlasFlagPresentByName(args []string, name string, short string) bool {
	long := "--" + name
	for _, arg := range args {
		if arg == long || strings.HasPrefix(arg, long+"=") {
			return true
		}
		if short != "" && (arg == "-"+short || strings.HasPrefix(arg, "-"+short+"=")) {
			return true
		}
	}
	return false
}

func atlasFlagShorthand(flags []atlasargs.Flag, name string) string {
	for _, flag := range flags {
		if flag.Name == name {
			return flag.Shorthand
		}
	}
	return ""
}

func isAtlasEnvSelectionRequired(err error) bool {
	return strings.Contains(err.Error(), "contains multiple env blocks; pass --env")
}

// atlasSourceProjectEnv packages the evaluated atlas.hcl environment for the
// desired-state URL resolver, so env:// references expand with the selected
// env's values and relative paths resolve against the config directory.
// Callers pass the zero ProjectEnv instead when no configuration was loaded.
func atlasSourceProjectEnv(
	cmd *cobra.Command,
	cfg projectconfig.Config,
) (atlassource.ProjectEnv, error) {
	flags, _, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return atlassource.ProjectEnv{}, err
	}
	baseDir, err := atlasProjectConfigBaseDir(flags)
	if err != nil {
		return atlassource.ProjectEnv{}, err
	}
	return atlassource.ProjectEnv{Loaded: true, Config: cfg, BaseDir: baseDir}, nil
}

func effectiveAtlasExclude(cmd *cobra.Command, flagValues []string, cfg projectconfig.Config) []string {
	values := effectiveStringArray(cmd, "exclude", flagValues, cfg.Exclude)
	return append(slices.Clone(values), cfg.Schema.Mode.ExcludePatterns()...)
}

func atlasDiffPolicy(cfg projectconfig.Config) (atlasschema.DiffPolicy, error) {
	return atlasschema.DiffPolicy{
		SkipDropTable:         cfg.Diff.Skip.DropTable.Set && cfg.Diff.Skip.DropTable.Value,
		ConcurrentIndexCreate: cfg.Diff.ConcurrentIndexCreate(),
		ConcurrentIndexDrop:   cfg.Diff.ConcurrentIndexDrop(),
	}, nil
}
