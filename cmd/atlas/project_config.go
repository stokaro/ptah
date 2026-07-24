package atlas

import (
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/atlasargs"
	"github.com/stokaro/ptah/internal/atlasprojectpath"
	"github.com/stokaro/ptah/internal/atlasschema"
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
	if _, err := os.Stat(path); err != nil {
		return projectconfig.Config{}, fmt.Errorf("failed to read atlas config %s: %w", path, err)
	}
	return loadAtlasProjectConfig(flags)
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

func atlasProjectConfigSchemaURLs(cmd *cobra.Command, raw []string) ([]string, error) {
	flags, _, err := atlasProjectFlagsFromCommand(cmd)
	if err != nil {
		return nil, err
	}
	return atlasProjectConfigSchemaURLsFromFlags(flags, raw)
}

func atlasProjectConfigLocalDirFromFlags(flags atlasProjectFlagValues, raw string) (string, error) {
	baseDir, err := atlasProjectConfigBaseDir(flags)
	if err != nil {
		return "", err
	}
	return atlasprojectpath.LocalDir(raw, baseDir)
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

func applyAtlasProjectConfigToArgs(
	flags []atlasargs.Flag,
	args []string,
	cfg projectconfig.Config,
	projectFlags atlasProjectFlagValues,
) ([]string, error) {
	args = appendAtlasProjectStringArg(flags, args, "url", cfg.DatabaseURL)
	args = appendAtlasProjectStringArg(flags, args, "dev-url", cfg.DevURL)
	if cfg.Migration.Dir != "" && atlasFlagRegistered(flags, "dir") && !atlasFlagPresent(flags, args, "dir") {
		dir, err := atlasProjectConfigLocalDirFromFlags(projectFlags, cfg.Migration.Dir)
		if err != nil {
			return nil, fmt.Errorf("atlas.hcl migration.dir: %w", err)
		}
		args = append(args, "--dir", dir)
	}
	args = appendAtlasProjectStringArg(flags, args, "dir-format", cfg.Migration.Format)
	args = appendAtlasProjectStringArg(flags, args, "revisions-schema", cfg.Migration.RevisionsSchema)
	args = appendAtlasProjectStringArg(flags, args, "lock-timeout", cfg.Migration.LockTimeout)
	args = appendAtlasProjectStringArg(flags, args, "latest", atlasProjectLatest(cfg))
	args = appendAtlasProjectStringArg(flags, args, "git-base", cfg.Lint.GitBase)
	args = appendAtlasProjectStringArg(flags, args, "git-dir", cfg.Lint.GitDir)
	return args, nil
}

func applyAtlasProjectConfigToNativeArgs(args []string, flags atlasProjectFlagValues) ([]string, error) {
	path, err := atlasConfigPathValue(flags.configPath)
	if err != nil {
		return nil, err
	}
	args = append(args, "--"+dbcli.AtlasProjectConfigFlagName, path)
	if strings.TrimSpace(flags.envName) != "" && !atlasFlagPresentByName(args, dbcli.EnvFlagName, "") {
		args = append(args, "--"+dbcli.EnvFlagName, flags.envName)
	}
	for _, value := range flags.vars {
		args = append(args, "--"+dbcli.AtlasProjectVarFlagName, value)
	}
	return args, nil
}

func atlasProjectLatest(cfg projectconfig.Config) string {
	if cfg.Lint.Latest == nil {
		return ""
	}
	return fmt.Sprint(*cfg.Lint.Latest)
}

func appendAtlasProjectStringArg(flags []atlasargs.Flag, args []string, name string, value string) []string {
	if strings.TrimSpace(value) == "" || !atlasFlagRegistered(flags, name) || atlasFlagPresent(flags, args, name) {
		return args
	}
	return append(args, "--"+name, value)
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
