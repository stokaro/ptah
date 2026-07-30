package atlas

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/cmd/internal/cmdflags"
	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasprojectpath"
	"github.com/stokaro/ptah/internal/atlasschema"
	"github.com/stokaro/ptah/internal/atlassource"
)

const (
	atlasConfigFlagName = "config"
	atlasVarFlagName    = "var"
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

func registerAtlasProjectFlags(flags *pflag.FlagSet, target *atlasProjectFlagValues) {
	if flags.Lookup(atlasConfigFlagName) == nil {
		flags.StringVarP(&target.configPath, atlasConfigFlagName, "c", "file://"+projectconfig.AtlasFileName, "select config (project) file using URL format")
	}
	if flags.Lookup(dbcli.EnvFlagName) == nil {
		dbcli.RegisterEnvFlag(flags, &target.envName)
	}
	if flags.Lookup(atlasVarFlagName) == nil {
		flags.StringArrayVar(&target.vars, atlasVarFlagName, nil, "input variables")
	}
}

func loadAtlasProjectConfig(flags atlasProjectFlagValues) (projectconfig.Config, error) {
	path, err := atlasConfigPathValue(flags.configPath)
	if err != nil {
		return projectconfig.Config{}, err
	}
	return projectconfig.LoadAtlasFileWithOptions(path, projectconfig.AtlasLoadOptions{
		EnvName: flags.envName,
		Vars:    flags.vars,
	})
}

func loadRequiredAtlasProjectConfig(flags atlasProjectFlagValues) (projectconfig.Config, error) {
	path, err := atlasConfigPathValue(flags.configPath)
	if err != nil {
		return projectconfig.Config{}, err
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return projectconfig.Config{}, fmt.Errorf("failed to read atlas config %s: %w", path, err)
	}
	return projectconfig.ParseAtlasWithOptions(raw, path, projectconfig.AtlasLoadOptions{
		EnvName: flags.envName,
		Vars:    flags.vars,
	})
}

func loadRequiredMergedProjectConfig(
	flags atlasProjectFlagValues,
) (atlasConfig, mergedConfig projectconfig.Config, err error) {
	atlas, err := loadRequiredAtlasProjectConfig(flags)
	if err != nil {
		return projectconfig.Config{}, projectconfig.Config{}, err
	}
	ptah, err := projectconfig.LoadPtahFile(projectconfig.PtahFileName, flags.envName)
	if err != nil {
		return projectconfig.Config{}, projectconfig.Config{}, err
	}
	return atlas, projectconfig.Merge(ptah, atlas), nil
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

func loadRequiredAtlasProjectConfigForCommand(
	cmd *cobra.Command,
) (projectconfig.Config, bool, error) {
	return loadAtlasProjectConfigForCommand(cmd, reportMissingEnvSelection)
}

func atlasProjectConfigLocalDir(cmd *cobra.Command, raw string) (string, error) {
	flags, _, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return "", err
	}
	return atlasProjectConfigLocalDirFromFlags(flags, raw)
}

func atlasProjectConfigLocalDirWithQuery(cmd *cobra.Command, raw string) (atlasargs.LocalDir, error) {
	flags, _, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return atlasargs.LocalDir{}, err
	}
	return atlasProjectConfigLocalDirWithQueryFromFlags(flags, raw)
}

func atlasProjectConfigSchemaURLs(cmd *cobra.Command, raw []string) ([]string, error) {
	flags, _, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return nil, err
	}
	return atlasProjectConfigSchemaURLsFromFlags(flags, raw)
}

func atlasProjectConfigLocalDirFromFlags(flags atlasProjectFlagValues, raw string) (string, error) {
	dir, err := atlasProjectConfigLocalDirWithQueryFromFlags(flags, raw)
	if err != nil {
		return "", err
	}
	if len(dir.Query) > 0 {
		return "", fmt.Errorf("migration directory URL query parameters are not supported yet")
	}
	return dir.Path, nil
}

func atlasProjectConfigLocalDirWithQueryFromFlags(
	flags atlasProjectFlagValues,
	raw string,
) (atlasargs.LocalDir, error) {
	baseDir, err := atlasProjectConfigBaseDir(flags)
	if err != nil {
		return atlasargs.LocalDir{}, err
	}
	path, query, err := atlasprojectpath.LocalDirWithQuery(raw, baseDir)
	if err != nil {
		return atlasargs.LocalDir{}, err
	}
	return atlasargs.LocalDir{Path: path, Query: query}, nil
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
	flags, changed, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return projectconfig.Config{}, false, err
	}
	if changed {
		cfg, err := loadRequiredAtlasProjectConfig(flags)
		return cfg, true, err
	}
	path, err := atlasConfigPathValue(flags.configPath)
	if err != nil {
		return projectconfig.Config{}, false, err
	}
	if !atlasProjectConfigExists(path) {
		return projectconfig.Config{}, false, nil
	}
	cfg, err := loadAtlasProjectConfig(flags)
	if err != nil {
		if isAtlasEnvSelectionRequired(err) && mode == ignoreMissingEnvSelection {
			return projectconfig.Config{}, false, nil
		}
		return projectconfig.Config{}, false, err
	}
	return cfg, true, nil
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
	if flag := cmd.Flags().Lookup(atlasVarFlagName); flag != nil {
		values, err := cmd.Flags().GetStringArray(atlasVarFlagName)
		if err != nil {
			return atlasProjectFlagValues{}, false, err
		}
		flags.vars = values
		changed = changed || flag.Changed
	}
	return flags, changed, nil
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
	if runE := cmd.RunE; runE != nil {
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			defer resetAtlasProjectFlags(group)
			return runE(cmd, args)
		}
	}
	if run := cmd.Run; run != nil {
		cmd.Run = func(cmd *cobra.Command, args []string) {
			defer resetAtlasProjectFlags(group)
			run(cmd, args)
		}
	}
	help := cmd.HelpFunc()
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		defer resetAtlasProjectFlags(group)
		help(cmd, args)
	})
	flagError := cmd.FlagErrorFunc()
	cmd.SetFlagErrorFunc(func(cmd *cobra.Command, err error) error {
		defer resetAtlasProjectFlags(group)
		return flagError(cmd, err)
	})
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

func atlasProjectConfigExists(path string) bool {
	info, err := os.Stat(path)
	return err == nil && !info.IsDir()
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
		if value, ok := strings.CutPrefix(arg, "--var="); ok {
			project.flags.vars = append(project.flags.vars, value)
			project.changed = true
			continue
		}
		if arg == "--var" {
			value, next, err := nextAtlasProjectArgValue(args, i, arg)
			if err != nil {
				return atlasProjectArgValues{}, nil, err
			}
			project.flags.vars = append(project.flags.vars, value)
			project.changed = true
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
	if !parent.changed {
		return raw
	}
	if !raw.changed {
		return parent
	}
	if !raw.configChanged {
		raw.flags.configPath = parent.flags.configPath
	}
	if !raw.envChanged {
		raw.flags.envName = parent.flags.envName
	}
	raw.flags.vars = append(parent.flags.vars, raw.flags.vars...)
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
	cfg projectconfig.Config,
	projectFlags atlasProjectFlagValues,
) ([]string, error)

func applyAtlasProjectConfigToArgs(
	flags []atlasargs.Flag,
	args []string,
	cfg projectconfig.Config,
	projectFlags atlasProjectFlagValues,
) ([]string, error) {
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"url",
		cfg.StringValue(projectconfig.StringDatabaseURL),
	)
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"dev-url",
		cfg.StringValue(projectconfig.StringDevURL),
	)
	migrationDir := cfg.StringValue(projectconfig.StringMigrationDir)
	if migrationDir.Present &&
		atlasFlagRegistered(flags, "dir") &&
		!atlasFlagValueSet(flags, args, "dir") {
		dir, err := atlasProjectConfigLocalDirFromFlags(projectFlags, migrationDir.Value)
		if err != nil {
			return nil, fmt.Errorf("atlas.hcl migration.dir: %w", err)
		}
		args = append(args, "--dir", dir)
	}
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"dir-format",
		cfg.StringValue(projectconfig.StringMigrationFormat),
	)
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"revisions-schema",
		cfg.StringValue(projectconfig.StringMigrationRevisionsSchema),
	)
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"lock-timeout",
		cfg.StringValue(projectconfig.StringMigrationLockTimeout),
	)
	cliLatest := atlasFlagValueSet(flags, args, "latest")
	cliGitBase := atlasFlagValueSet(flags, args, "git-base")
	if !cliGitBase {
		args = appendAtlasProjectStringArg(flags, args, "latest", atlasProjectLatest(cfg))
	}
	gitBase := cfg.StringValue(projectconfig.StringLintGitBase)
	if !cliLatest && gitBase.Value != "" {
		args = appendAtlasProjectStringArg(flags, args, "git-base", gitBase)
	}
	gitDir := cfg.StringValue(projectconfig.StringLintGitDir)
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
	cfg projectconfig.Config,
	projectFlags atlasProjectFlagValues,
) ([]string, error) {
	args = appendAtlasProjectStringArg(
		flags,
		args,
		"dev-url",
		cfg.StringValue(projectconfig.StringDevURL),
	)
	if atlasFlagValueSet(flags, args, "url") {
		return args, nil
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
	urls, err := atlasProjectConfigSchemaURLsFromFlags(projectFlags, sources.Value)
	if err != nil {
		return nil, fmt.Errorf("atlas.hcl schema.src: %w", err)
	}
	return append(args, "--url", urls[0]), nil
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
	if cfg.Diff.ConcurrentIndex.Drop.Set && cfg.Diff.ConcurrentIndex.Drop.Value {
		return atlasschema.DiffPolicy{}, fmt.Errorf("atlas.hcl diff.concurrent_index.drop is not supported yet")
	}
	return atlasschema.DiffPolicy{
		SkipDropTable:         cfg.Diff.Skip.DropTable.Set && cfg.Diff.Skip.DropTable.Value,
		ConcurrentIndexCreate: cfg.Diff.ConcurrentIndex.Create.Set && cfg.Diff.ConcurrentIndex.Create.Value,
	}, nil
}
