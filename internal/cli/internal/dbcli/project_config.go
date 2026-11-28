package dbcli

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

	"ptah.run/config/projectconfig"
	"ptah.run/core/schemasource"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/migrationsource"
	"ptah.run/internal/execpolicy"
	"ptah.run/internal/pathguard"
	"ptah.run/internal/rundiag"
)

const (
	// EnvFlagName selects an env block from project config.
	EnvFlagName = "env"
	// ProjectVarFlagName supplies a value for a variable block that declares no
	// default, in an atlas.hcl or in a schema file. It is the flag both
	// evaluators' diagnostics name, so it is registered wherever EnvFlagName
	// is: an atlas.hcl reached through --env can require a variable on any of
	// those commands, and a command that can print the advice but cannot honor
	// it is the defect this flag exists to close.
	//
	// [DeclaredVars] resolves it for both files. A schema file declaring a
	// variable without a default was unusable through the native commands
	// while the compatibility surface honored the same flag against the same
	// file, which put a capability behind the adapter (stokaro/ptah#3113).
	ProjectVarFlagName = "var"
	// AllowExternalSchemaFlagName explicitly permits executing the
	// external_schema program loaded from ptah.yaml or from an atlas.hcl
	// data.external_schema source.
	AllowExternalSchemaFlagName = "allow-external-schema"
	// AtlasProjectConfigFlagName passes an Atlas project config path through
	// internal command adapters without exposing Atlas flags on native commands.
	AtlasProjectConfigFlagName = "atlas-project-config"
	// AtlasProjectVarFlagName passes Atlas project variable overrides through
	// internal command adapters without exposing Atlas flags on native commands.
	AtlasProjectVarFlagName = "atlas-project-var"
	schemaCommandFlagName   = "schema-cmd"
)

// ProjectConfigEnvAnnotation marks the --env flag that selects a ptah.yaml or
// atlas.hcl env block. The flag name alone does not identify it: `ptah seed`
// registers an unrelated --env naming the seed environment to apply, reads no
// project config, and must not grow a --var it would never honor. The
// annotation is what lets a test tell the two apart from the assembled command
// tree instead of from a hand-kept list of packages.
const ProjectConfigEnvAnnotation = "ptah_project_config_env"

const projectEnvFlagUsage = "Project env to read from ptah.yaml or atlas.hcl; without it no env applies"

type projectConfigContextKey struct{}

type projectConfigContextValue struct {
	configs []projectconfig.Config
}

// WithProjectConfig returns a context carrying an immutable project-config
// snapshot for a forwarded native command.
func WithProjectConfig(ctx context.Context, config projectconfig.Config) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return WithProjectConfigs(ctx, []projectconfig.Config{config})
}

// WithProjectConfigs returns a context carrying an immutable snapshot of every
// config an env expands to, in the order given, for a forwarded native command
// that runs once per instance. [LoadProjectConfigs] returns the snapshot, and
// [LoadProjectConfig] refuses one that holds more than one config.
func WithProjectConfigs(ctx context.Context, configs []projectconfig.Config) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	snapshot := make([]projectconfig.Config, 0, len(configs))
	for _, config := range configs {
		snapshot = append(snapshot, cloneProjectConfig(config))
	}
	return context.WithValue(ctx, projectConfigContextKey{}, projectConfigContextValue{configs: snapshot})
}

// RegisterEnvFlag registers the shared project env selection flag, bound to
// target, together with the variable-override flag its evaluator advises. The
// two are registered by one call because every command that can select an env
// can also hit an atlas.hcl variable with no default, and registering them
// separately is how eleven of the fourteen commands came to advise a flag they
// rejected.
func RegisterEnvFlag(flags *pflag.FlagSet, target *string) {
	flags.StringVar(target, EnvFlagName, "", projectEnvFlagUsage)
	markProjectEnvFlag(flags)
}

// RegisterProjectEnvFlag registers the same pair as [RegisterEnvFlag] without
// binding --env to a variable, for commands that read the value back off the
// flag set. It exists so no command has to spell EnvFlagName inline: five did,
// which is why a search for the helper found nine of the fourteen commands
// that can reach the variable diagnostic.
func RegisterProjectEnvFlag(flags *pflag.FlagSet) {
	flags.String(EnvFlagName, "", projectEnvFlagUsage)
	markProjectEnvFlag(flags)
}

func markProjectEnvFlag(flags *pflag.FlagSet) {
	RegisterProjectVarFlag(flags)
	// No env has a blank name, so PTAH_ENV="  " is the empty variable, and
	// [RefuseEmptyProjectEnv] refuses it as one.
	cmdflags.ReadBlankAsEmpty(flags, EnvFlagName)
	if err := flags.SetAnnotation(EnvFlagName, ProjectConfigEnvAnnotation, []string{"true"}); err != nil {
		panic(err)
	}
}

// RegisterProjectVarFlag registers the atlas.hcl variable override flag. It is
// idempotent so a command that already carries the flag — from
// [RegisterEnvFlag] or from an Atlas-compatible surface that spells it the
// same way — keeps the binding it already has.
func RegisterProjectVarFlag(flags *pflag.FlagSet) {
	if flags.Lookup(ProjectVarFlagName) != nil {
		return
	}
	flags.StringArray(
		ProjectVarFlagName,
		nil,
		"Value for a variable with no default, in an atlas.hcl or a schema file, as name=value (repeatable)",
	)
}

// RegisterExternalSchemaOptInFlag registers --allow-external-schema, the older
// spelling of the root's --allow-config-exec, on a command that reads an
// external_schema program. The root reads it into the same program policy, and
// it allows no more than --allow-config-exec does. An explicit --schema-cmd
// needs neither, because supplying the command is already the permission.
func RegisterExternalSchemaOptInFlag(flags *pflag.FlagSet) {
	flags.Bool(
		AllowExternalSchemaFlagName,
		false,
		"Older spelling of --allow-config-exec",
	)
}

// RegisterAtlasProjectInternalFlags registers hidden adapter-only flags used to
// forward Atlas project config selection into native command implementations.
func RegisterAtlasProjectInternalFlags(flags *pflag.FlagSet) {
	if flags.Lookup(AtlasProjectConfigFlagName) == nil {
		flags.String(AtlasProjectConfigFlagName, "", "Internal Atlas project config path")
		if err := flags.MarkHidden(AtlasProjectConfigFlagName); err != nil {
			panic(err)
		}
	}
	if flags.Lookup(AtlasProjectVarFlagName) == nil {
		flags.StringArray(AtlasProjectVarFlagName, nil, "Internal Atlas project variable override")
		if err := flags.MarkHidden(AtlasProjectVarFlagName); err != nil {
			panic(err)
		}
	}
}

// AtlasProjectConfigExtension is the file extension that routes an explicit
// --config path to the Atlas project loader rather than the ptah.yaml one.
const AtlasProjectConfigExtension = ".hcl"

// splitExplicitConfigPath decides which loader an explicit --config path is
// meant for, by extension.
//
// # Why --config takes both
//
// The Atlas project loader has always accepted an explicit path, but the only
// way to reach it from the native binary was discovery, which looks for
// ./atlas.hcl and nothing else. A project that keeps its Atlas config anywhere
// else -- a subdirectory, a second file for staging, any name but that one --
// was reachable from ptah-compat and unreachable from ptah, which is the
// opposite of the adoption direction this is meant to run in
// (stokaro/ptah#1215).
//
// # Why by extension and not by reading the file
//
// A path the operator typed should resolve the same way every time, whatever
// is inside the file today. Sniffing the contents would make --config's
// meaning depend on them, so a file mid-edit could change which loader claims
// it, and a syntax error in one language would surface as a parse error in the
// other.
func splitExplicitConfigPath(path string) (ptahPath, atlasPath string) {
	if path == "" {
		return "", ""
	}
	if strings.EqualFold(filepath.Ext(path), AtlasProjectConfigExtension) {
		return "", path
	}
	return path, ""
}

// LoadProjectConfig loads project-level configuration for a command. The
// explicit config path controls ptah.yaml, or an Atlas project config when it
// ends in .hcl; Atlas-compatible adapters
// can pass an internal atlas.hcl path and variable overrides through hidden
// flags. A project-config snapshot supplied by an adapter takes precedence and
// avoids reopening project files during the forwarded execution.
//
// It binds the migration directory the config names to cmd's context, as
// [WithProjectMigrationDirectory] does, so a caller reads cmd.Context() after
// the load. An env that expands to several instances is refused; a command
// that runs once per instance loads them with [LoadProjectConfigs].
func LoadProjectConfig(cmd *cobra.Command, ptahConfigPath string) (projectconfig.Config, error) {
	if configs, ok := projectConfigsFromContext(cmd.Context()); ok {
		if len(configs) != 1 {
			return projectconfig.Config{}, fmt.Errorf(
				"%s runs against one database, and the forwarded project config names %d", cmd.CommandPath(), len(configs))
		}
		config := cloneProjectConfig(configs[0])
		cmd.SetContext(WithProjectMigrationDirectory(cmd.Context(), config))
		return config, nil
	}
	opts, err := projectLoadOptions(cmd, ptahConfigPath)
	if err != nil {
		return projectconfig.Config{}, err
	}
	config, err := projectconfig.Load(opts)
	if err != nil {
		return projectconfig.Config{}, err
	}
	if err := RefuseEmptyProjectEnv(cmd.Flags(), config); err != nil {
		return projectconfig.Config{}, err
	}
	if err := ReportIgnoredAtlasConstructs(cmd.Context(), cmd.ErrOrStderr(), config); err != nil {
		return projectconfig.Config{}, err
	}
	cmd.SetContext(WithProjectMigrationDirectory(cmd.Context(), config))
	return config, nil
}

// LoadProjectConfigs is [LoadProjectConfig] for a command that runs once per
// instance of an env: it returns every config the selected env expands to, in
// the order the expansion gives, where LoadProjectConfig refuses more than
// one. The slice is never empty. An adapter's snapshot is returned as it was
// handed over, see [WithProjectConfigs].
//
// It binds no migration directory to cmd, because each instance may name its
// own: the caller runs each instance under
// [WithProjectMigrationDirectory](cmd.Context(), config).
func LoadProjectConfigs(cmd *cobra.Command, ptahConfigPath string) ([]projectconfig.Config, error) {
	if configs, ok := projectConfigsFromContext(cmd.Context()); ok {
		if len(configs) == 0 {
			return nil, fmt.Errorf("%s: the forwarded project config names no instance", cmd.CommandPath())
		}
		clones := make([]projectconfig.Config, 0, len(configs))
		for _, config := range configs {
			clones = append(clones, cloneProjectConfig(config))
		}
		return clones, nil
	}
	opts, err := projectLoadOptions(cmd, ptahConfigPath)
	if err != nil {
		return nil, err
	}
	configs, err := projectconfig.LoadCollection(opts)
	if err != nil {
		return nil, err
	}
	// Every instance carries the envs the files declare, so the first one
	// answers for the run.
	if err := RefuseEmptyProjectEnv(cmd.Flags(), configs[0]); err != nil {
		return nil, err
	}
	// Every instance carries the file's ignored names, so one report covers
	// the run.
	if err := ReportIgnoredAtlasConstructs(cmd.Context(), cmd.ErrOrStderr(), configs[0]); err != nil {
		return nil, err
	}
	return configs, nil
}

// projectLoadOptions reads the project files and the env a command names off
// its flags.
func projectLoadOptions(cmd *cobra.Command, ptahConfigPath string) (projectconfig.LoadOptions, error) {
	envName, err := stringFlag(cmd, EnvFlagName)
	if err != nil {
		return projectconfig.LoadOptions{}, err
	}
	atlasPath, err := stringFlag(cmd, AtlasProjectConfigFlagName)
	if err != nil {
		return projectconfig.LoadOptions{}, err
	}
	ptahConfigPath, explicitAtlasPath := splitExplicitConfigPath(ptahConfigPath)
	if explicitAtlasPath != "" {
		// A compat adapter forwarding its own project config and an operator
		// naming a different one on --config cannot both be honored, and
		// picking either silently would apply a config the caller did not ask
		// for. Name both paths instead.
		if atlasPath != "" && atlasPath != explicitAtlasPath {
			return projectconfig.LoadOptions{}, fmt.Errorf(
				"--%s names %s and the forwarded Atlas project config is %s; pass one Atlas project config",
				ConfigFlagName, explicitAtlasPath, atlasPath)
		}
		atlasPath = explicitAtlasPath
	}
	atlasVars, err := projectVars(cmd)
	if err != nil {
		return projectconfig.LoadOptions{}, err
	}
	return projectconfig.LoadOptions{
		Context:   cmd.Context(),
		PtahPath:  ptahConfigPath,
		AtlasPath: atlasPath,
		EnvName:   envName,
		AtlasVars: atlasVars,
		// So a refusal about a for_each env names the command the operator
		// ran rather than an internal API (stokaro/ptah#1696).
		Verb: cmd.CommandPath(),
	}, nil
}

// WithProjectMigrationDirectory returns ctx with the migration directory
// config names bound to the filesystem the project file rendered for it, such
// as an atlas.hcl data.template_dir, so a directory read under the returned
// context opens that filesystem rather than a path on disk. It returns ctx as
// it is when config names no directory, or one the project file did not
// render.
func WithProjectMigrationDirectory(ctx context.Context, config projectconfig.Config) context.Context {
	dir := config.StringValue(projectconfig.StringMigrationDir)
	if !dir.Present {
		return ctx
	}
	fsys, ok := config.MigrationDirectoryFS(dir.Value)
	if !ok {
		return ctx
	}
	return migrationsource.WithVirtual(ctx, dir.Value, fsys, dir.Value)
}

// ReportIgnoredAtlasConstructs raises a warning for every atlas.hcl name that
// Atlas CE accepts without acting on. Ptah preserves this compatibility
// behavior, but makes the resulting no-op visible instead of silently hiding
// a likely typo or unsupported policy. A run reports them together; see
// [rundiag.IgnoredProjectSettings].
func ReportIgnoredAtlasConstructs(ctx context.Context, out io.Writer, config projectconfig.Config) error {
	for _, construct := range config.IgnoredConstructs {
		line := fmt.Sprintf(
			"atlas.hcl %s %q at %s:%d is ignored for Atlas compatibility and has no effect",
			construct.Kind,
			construct.Name,
			construct.Filename,
			construct.Line,
		)
		if err := rundiag.Warn(ctx, out, rundiag.IgnoredProjectSettings, rundiag.Record{Key: line, Line: line, Attrs: []slog.Attr{
			slog.String("kind", construct.Kind), slog.String("name", construct.Name),
			slog.String("file", construct.Filename), slog.Int("line", construct.Line),
		}}); err != nil {
			return fmt.Errorf("write ignored atlas.hcl construct warning: %w", err)
		}
	}
	return nil
}

// DeclaredVars resolves the variable overrides a command carries, for whichever
// file declares the variable.
//
// One resolver, because one flag supplies both. `--var` names a value, and the
// file that needs it may be the atlas.hcl an --env selects or the schema file
// --schema-file names; a second reader for the second file would agree with
// this one until the day either moved.
//
// The public --var wins when the user passed it; otherwise the hidden
// adapter-only flag supplies whatever a forwarding command already routed. The
// two are not concatenated: a repeated --var for one name is how a list(string)
// variable is built, so merging both sources would turn a scalar override into
// a two-element list.
//
// A command that registers neither flag gets no values and no error, so a
// caller may ask unconditionally.
func DeclaredVars(cmd *cobra.Command) ([]string, error) {
	return projectVars(cmd)
}

func projectVars(cmd *cobra.Command) ([]string, error) {
	if flagChanged(cmd, ProjectVarFlagName) {
		return cmd.Flags().GetStringArray(ProjectVarFlagName)
	}
	adapterVars, err := stringArrayFlag(cmd, AtlasProjectVarFlagName)
	if err != nil {
		return nil, err
	}
	if len(adapterVars) > 0 {
		return adapterVars, nil
	}
	return stringArrayFlag(cmd, ProjectVarFlagName)
}

func projectConfigsFromContext(ctx context.Context) ([]projectconfig.Config, bool) {
	if ctx == nil {
		return nil, false
	}
	value, ok := ctx.Value(projectConfigContextKey{}).(projectConfigContextValue)
	return value.configs, ok
}

func cloneProjectConfig(config projectconfig.Config) projectconfig.Config {
	return projectconfig.Merge(projectconfig.Config{}, config)
}

// EffectiveString returns an explicit CLI value first, then a present project
// config value (including an empty value), then the flag's built-in default.
func EffectiveString(
	cmd *cobra.Command,
	flagName,
	flagValue string,
	configValue projectconfig.Value[string],
) string {
	if flagChanged(cmd, flagName) || !configValue.Present {
		return flagValue
	}
	return configValue.Value
}

// ChecksUnusedDevURL reports whether a run that needs nothing from its dev or
// shadow database opens the URL in flagName anyway, only to refuse one nothing
// answers on (see [ptah.run/internal/devreach]).
//
// It does when the operator gave the URL for this run, on the command line or
// through the flag's PTAH_* variable, which is what pflag's Changed bit says on
// a Ptah surface. A URL [EffectiveString] took from the project file's `dev:`
// key does not count: it is a default for every command in the project, and a
// command that does not use it has no reason to fail on it.
//
// A run ptah-compat forwards here always counts as given, wherever its URL came
// from, as the Atlas CLI opens every dev URL it is given: the adapter writes an
// atlas.hcl `dev` onto the forwarded command line as the native flag, so the
// flag carries the Changed bit like any other.
func ChecksUnusedDevURL(cmd *cobra.Command, flagName string) bool {
	return flagChanged(cmd, flagName)
}

// EffectiveSchemas is the schema list a command reads: the --schemas value
// when the flag or its variable set it, else the project's list, parsed by
// [ParseSchemas]. The project's names go through [JoinSchemas], so a name with
// spaces around it or a comma in it reaches the command as the project wrote
// it. A malformed --schemas value is the error. The live URL is checked
// against that effective selection before connecting, as [ParseSchemasForURLs]
// checks a flag value on a verb that reads no project configuration.
func EffectiveSchemas(cmd *cobra.Command, flagValue string, project projectconfig.Value[[]string], address string) ([]string, error) {
	joined := projectconfig.Value[string]{Value: JoinSchemas(project.Value), Present: project.Present}
	return ParseSchemasForURLs(EffectiveString(cmd, SchemasFlagName, flagValue, joined), address)
}

// ResolveExternalSchemaCommands resolves the external-command schema source for
// a command, preferring an explicit --schema-cmd. A ptah.yaml external_schema
// block or an atlas.hcl data.external_schema source requires
// --allow-external-schema because the conventional config files are
// auto-discovered and executing repository-controlled code must be explicit.
func ResolveExternalSchemaCommands(
	cmd *cobra.Command,
	schemaCmd,
	schemaFormat string,
	cfg projectconfig.Config,
) ([]schemasource.Command, error) {
	if commands := externalSchemaCommandsFromCLI(schemaCmd, schemaFormat); commands != nil {
		return commands, nil
	}
	if flagChanged(cmd, schemaCommandFlagName) {
		return nil, nil
	}
	if len(cfg.ExternalSchema.Program) == 0 {
		return nil, nil
	}
	// Asked here, before any database is contacted, and asked again where the
	// program starts; both ask the binary's one program policy.
	origin := externalSchemaSourceLabel(cfg.ExternalSchema.Origin)
	if err := execpolicy.Authorize(cmd.Context(), origin, cfg.ExternalSchema.Program); err != nil {
		return nil, err
	}
	return externalSchemaCommandsFromConfig(
		origin,
		cfg.ExternalSchema.Program,
		cfg.ExternalSchema.Format,
		cfg.ExternalSchema.WorkingDir,
		cfg.ExternalSchema.Env,
		cfg.ExternalSchema.Secrets,
	)
}

// externalSchemaSourceLabel names the config construct that supplied the
// external schema program, so the safety-gate error points the user at the
// file they need to review before opting in.
func externalSchemaSourceLabel(origin string) string {
	if origin == projectconfig.AtlasFileName {
		return `atlas.hcl data "external_schema"`
	}
	return "ptah.yaml external_schema"
}

func externalSchemaCommandsFromCLI(commandLine, format string) []schemasource.Command {
	if strings.TrimSpace(commandLine) == "" {
		return nil
	}
	return []schemasource.Command{{
		Args:   strings.Fields(commandLine),
		Format: format,
	}}
}

func externalSchemaCommandsFromConfig(
	origin string,
	program []string,
	format,
	dir string,
	env,
	secrets []string,
) ([]schemasource.Command, error) {
	if len(program) == 0 {
		return nil, nil
	}
	if strings.TrimSpace(dir) != "" {
		// No boundary, and the reason is worth stating because this call site
		// looks like the one that should keep one: it names the working
		// directory of a program Ptah EXECUTES.
		//
		// Two arguments say otherwise. The control on this surface is the opt-in
		// flag above -- once an operator has authorized running an arbitrary
		// program, restricting which directory it starts in prevents nothing
		// the program cannot undo with one chdir. And by the time the value
		// arrives here the config layer has already joined a relative
		// working_dir onto the atlas.hcl directory, so a root anchored at the
		// process working directory would refuse an ordinary configuration
		// whose config file lives somewhere else (stokaro/ptah#1622).
		resolvedDir, err := pathguard.ResolveCLIPath(dir)
		if err != nil {
			return nil, fmt.Errorf("resolve external_schema working_dir: %w", err)
		}
		dir = resolvedDir
	}
	return []schemasource.Command{{
		Origin:  origin,
		Args:    slices.Clone(program),
		Format:  format,
		Dir:     dir,
		Env:     slices.Clone(env),
		Secrets: slices.Clone(secrets),
	}}, nil
}

func stringFlag(cmd *cobra.Command, name string) (string, error) {
	flag := cmd.Flags().Lookup(name)
	if flag == nil {
		return "", nil
	}
	return flag.Value.String(), nil
}

func stringArrayFlag(cmd *cobra.Command, name string) ([]string, error) {
	if cmd.Flags().Lookup(name) == nil {
		return nil, nil
	}
	return cmd.Flags().GetStringArray(name)
}

func flagChanged(cmd *cobra.Command, name string) bool {
	flag := cmd.Flags().Lookup(name)
	return flag != nil && flag.Changed
}

// SchemaSourceProjectEnv describes the project environment an `env://` schema
// source is expanded through, or the zero value when the run selected none.
//
// A native command reaches the same atlas.hcl the adapter does: the config is
// discovered under its default name when no path was forwarded, so `--env`
// selects a real environment either way. What the two sides did not share was a
// route from that environment to a desired-state source, which is why
// --schema-file refused every `env://` reference while --to accepted it
// (stokaro/ptah#1760).
//
// The zero value is returned when no environment was selected. That is not a
// degraded mode: with no environment there is no `src` attribute to read, and
// the caller's refusal names both the reference and the alternative.
func SchemaSourceProjectEnv(
	cmd *cobra.Command,
	config projectconfig.Config,
) (atlassource.ProjectEnv, error) {
	envName, err := stringFlag(cmd, EnvFlagName)
	if err != nil {
		return atlassource.ProjectEnv{}, err
	}
	if strings.TrimSpace(envName) == "" {
		return atlassource.ProjectEnv{}, nil
	}
	atlasPath, err := stringFlag(cmd, AtlasProjectConfigFlagName)
	if err != nil {
		return atlassource.ProjectEnv{}, err
	}
	if strings.TrimSpace(atlasPath) == "" {
		atlasPath = projectconfig.AtlasFileName
	}
	return atlassource.ProjectEnv{
		Loaded:   true,
		Config:   config,
		BaseDir:  filepath.Dir(atlasPath),
		Programs: execpolicy.From(cmd.Context()),
	}, nil
}

// SchemaSourceEnvSelectorFlag names the flag a command offers for selecting a
// project environment, or is empty when it offers none.
//
// It is read off the flag set rather than assumed, so a command that stops
// registering the flag stops promising it in the refusal too.
func SchemaSourceEnvSelectorFlag(cmd *cobra.Command) string {
	if cmd.Flags().Lookup(EnvFlagName) == nil {
		return ""
	}
	return EnvFlagName
}
