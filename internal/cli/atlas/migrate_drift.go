package atlas

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/atlasargs"
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/internal/cmdadapter"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/cli/migrationsdrift"
	"ptah.run/internal/envbool"
)

// atlasMigrateDriftEnvVar routes `migrate drift` on this surface to `ptah
// migrations drift` under the Atlas flag names. The pinned community binary has
// no such verb: `atlas migrate drift` prints the migrate group help at exit 0,
// and a flag after it is refused as unknown. Unset, this surface answers the
// same way.
//
// It is a variable rather than a flag because the compatibility surface
// registers no flag the community binary does not have.
const atlasMigrateDriftEnvVar = "PTAH_ATLAS_MIGRATE_DRIFT"

// atlasMigrateDrift is the declaration of the variable. It is
// [ptah.run/internal/envbool.Gated]: the drift report is a verb the pinned
// community binary lacks, so strict mode refuses the variable.
var atlasMigrateDrift = envbool.New(atlasMigrateDriftEnvVar, false, envbool.Gated)

// atlasNoCacheFlag is the Atlas flag that bypasses an expected-state cache.
// Ptah keeps no such cache and computes the expected state on every run, so
// the flag asks for what every run does; [atlasMigrateDriftNativeArgs] drops
// it rather than forwarding it.
const atlasNoCacheFlag = "no-cache"

// atlasDriftFormatReason and atlasDriftSkipLockReason say why the drift report
// refuses --format and --skip-lock, and the atlas.hcl settings that ask for
// the same.
const (
	atlasDriftFormatReason   = "Ptah prints its own drift report, and has no template data in the shape Atlas documents for it"
	atlasDriftSkipLockReason = "the report reads the revision table under the migration lock, so that a migration " +
		"being applied is not reported as drift"
)

// atlasDriftNativeOnlyFlags are the native flags of `ptah migrations drift`
// this surface does not accept. Their environment bindings are disabled on
// the target too ([atlasMigrateDriftTarget]): the adapter decides each value
// the native command reads, and a variable the target read itself would
// replace that decision for every target, as PTAH_SHADOW_DB would replace the
// dev database of each instance.
var atlasDriftNativeOnlyFlags = []string{
	"atlas-env",
	"connect-timeout",
	"db-url",
	"json",
	"migration-lock-name",
	"migration-lock-timeout",
	"migrations-dir",
	"migrations-engine",
	"migrations-schema",
	"migrations-table",
	dbcli.RevisionTableFormatFlagName,
	"plain-http",
	"shadow-db",
}

// atlasDriftProjectRefusals are the atlas.hcl settings the Atlas drift
// documentation gives an effect and this surface does not carry out, by
// [projectconfig.IgnoredAtlasConstruct.Path]. The parser tolerates each, as
// the community binary does, and every other verb warns that it has no
// effect. The drift report refuses them instead, as it refuses the flags that
// ask for the same: a report that took the lock while skip_lock asked it not
// to, or printed text while a template asked for JSON, would look like an
// answer to the request.
var atlasDriftProjectRefusals = []struct {
	path string
	// flag takes precedence over the setting, as an Atlas flag overrides the
	// env attribute of the same meaning: given, the setting is not refused,
	// and the flag's value is the one the report runs with.
	flag string
	// carriedOut is the one value the report carries out as it stands, which
	// is not refused: skip_lock = false asks for the lock the report takes.
	carriedOut string
	refusal    string
}{
	{path: "format.migrate.drift", refusal: "Ptah does not implement its behavior: " + atlasDriftFormatReason},
	{
		path: "migration.skip_lock", carriedOut: "false",
		refusal: "Ptah does not implement its behavior: " + atlasDriftSkipLockReason,
	},
	{
		path: "migration.exclude", flag: "exclude",
		refusal: "Ptah does not read it yet; pass the patterns with --exclude, which takes precedence",
	},
	{
		path: "migration.lock_name", flag: atlasLockNameFlag,
		refusal: "Ptah does not read it yet; pass the name with --lock-name, which takes precedence",
	},
}

// refuseUnreadDriftSettings refuses a selected env that sets one of
// [atlasDriftProjectRefusals], unless given reports the flag that takes
// precedence over it, or the env sets the value the report carries out. A
// value the instances of the env disagree on is no value, and is refused.
//
// A setting the run carries out, `skip_lock = false`, is taken out of the
// config's tolerated constructs, so the warning that a tolerated attribute has
// no effect does not name it.
func refuseUnreadDriftSettings(configs []*projectconfig.Config, given func(flag string) bool) error {
	for _, cfg := range configs {
		kept := make([]projectconfig.IgnoredAtlasConstruct, 0, len(cfg.IgnoredConstructs))
		for _, construct := range cfg.IgnoredConstructs {
			carriedOut, err := driftSettingCarriedOut(construct, given)
			if err != nil {
				return err
			}
			if !carriedOut {
				kept = append(kept, construct)
			}
		}
		cfg.IgnoredConstructs = kept
	}
	return nil
}

// driftSettingCarriedOut refuses construct where it is one of
// [atlasDriftProjectRefusals] in the selected env that the run does not carry
// out, and reports whether the run carries out what it asks.
func driftSettingCarriedOut(construct projectconfig.IgnoredAtlasConstruct, given func(flag string) bool) (bool, error) {
	if !construct.InSelectedEnv {
		return false, nil
	}
	for _, refusal := range atlasDriftProjectRefusals {
		if construct.Path != refusal.path {
			continue
		}
		if refusal.flag != "" && given(refusal.flag) {
			return false, nil
		}
		if refusal.carriedOut != "" && construct.Value == refusal.carriedOut {
			return true, nil
		}
		reason := refusal.refusal
		if construct.Disagrees {
			reason = "the instances of the env give it different values; give it one value in every instance"
		}
		return false, fmt.Errorf("atlas migrate drift reads the env's %s, set at %s:%d, but %s",
			construct.Path, construct.Filename, construct.Line, reason)
	}
	return false, nil
}

// newAtlasMigrateDriftCommand returns `migrate drift`, registered outside the
// strict profile only and hidden. Until [atlasMigrateDriftEnvVar] is enabled,
// every spelling of it answers as the group does in a tree without the verb:
// the group reads the arguments, refuses a flag it does not register, and
// prints its help otherwise. A value that is not a boolean is refused on every
// run of the verb.
//
// The decision is taken when the verb runs, not when the tree is built, so the
// tree and the command reference generated from it do not depend on the
// environment of whoever built them, as for `security`
// ([newAtlasSecurityCommand]). The help command and shell completion run no
// function of the verb, so [gateAtlasMigrateDriftHelp] and
// [gateAtlasRequests] decide for them.
func newAtlasMigrateDriftCommand() *cobra.Command {
	cmd := newAtlasAdapterCommand("migrate", atlasMigrateDriftVerb())
	cmd.Hidden = true
	run := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		enabled, err := atlasMigrateDrift.Resolve()
		if err != nil {
			return failAtlasCommand(cmd, err)
		}
		if !enabled {
			return runAtlasWordAsGroupArgument(cmd, args)
		}
		return run(cmd, args)
	}
	return cmd
}

// runAtlasWordAsGroupArgument answers a run of cmd as its group answers the
// same words in a tree without cmd: the group parses what follows the word,
// so a flag it does not register is refused, and any other spelling,
// including a help flag, prints the group help.
func runAtlasWordAsGroupArgument(cmd *cobra.Command, args []string) error {
	group := cmd.Parent()
	group.InitDefaultHelpFlag()
	if err := group.ParseFlags(args); err != nil {
		return group.FlagErrorFunc()(group, err)
	}
	return runAtlasGroupHelp(group, nil)
}

// gateAtlasMigrateDriftHelp makes the help of `migrate drift` the migrate
// group help while [atlasMigrateDriftEnvVar] is not enabled, which is what
// `help migrate drift` prints in a tree without the verb. A help function
// cannot fail the run, so the help command refuses a malformed value before it
// gets here ([gateAtlasRequests]), and here a malformed value counts as not
// enabled.
//
// It wraps the help function the usage tree installed, so it runs after
// installAtlasUsageTree.
func gateAtlasMigrateDriftHelp(drift *cobra.Command) {
	help := drift.HelpFunc()
	drift.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		if enabled, err := atlasMigrateDrift.Resolve(); err != nil || !enabled {
			group := cmd.Parent()
			group.InitDefaultHelpFlag()
			group.HelpFunc()(group, args)
			return
		}
		help(cmd, args)
	})
}

// atlasMigrateDriftVerb is `migrate drift`, forwarded to `ptah migrations
// drift`. The flag set is the one the Atlas drift detection documentation
// lists (https://atlasgo.io/versioned/drift-detection, read 2026-11-24); the
// verb was never run, since it needs an Atlas Pro login. An env with for_each
// is handed over whole, so the native command reports every target and exits
// with the most serious status of any.
func atlasMigrateDriftVerb() atlasVerb {
	return atlasVerb{
		use:          "drift",
		displayUse:   "drift [flags]",
		short:        "Report drift between a database and its applied migration history",
		native:       "migrations drift",
		factory:      atlasMigrateDriftTarget,
		eachInstance: true,
		// The Atlas revision table and directory layout, as on `migrate down`:
		// left to the native defaults, the report reads ptah_schema_migrations
		// and finds no history where `migrate apply` wrote one.
		prefixArgs:            []string{"--" + dbcli.RevisionTableFormatFlagName, "atlas", "--dir-format", atlasDirFormatDefault},
		finishNativeArgs:      atlasMigrateDriftNativeArgs,
		refuseProjectSettings: refuseUnreadDriftSettings,
		// --trust-replay-sql is refused beside them, but its environment
		// binding stays live on the target: this surface trusts replayed SQL
		// through PTAH_TRUST_REPLAY_SQL alone.
		nativeOnlyFlags: append(slices.Clone(atlasDriftNativeOnlyFlags), dbcli.TrustReplaySQLFlagName),
		flags: atlasTargetEnvDisabled(atlasDriftNativeOnlyFlags, []atlasargs.Flag{
			atlasargs.NativeString("url", "u", "URL of the database to check", "db-url"),
			atlasargs.NativeString("dev-url", "",
				"URL of the dev database the applied migrations are replayed on", "shadow-db"),
			atlasMigrationsDirFlag(),
			atlasMigrateDirFormatFlag("dir-format"),
			atlasRevisionsSchemaFlag("Schema of the revision table"),
			atlasDriftExcludeFlag(),
			atlasargs.UnsupportedStringReason("format", "", "Go template to use to format the output",
				atlasDriftFormatReason),
			atlasargs.NativeString("lock-timeout", "", "How long to wait for the migration lock",
				"migration-lock-timeout"),
			atlasForwardedLockNameFlag("Name of the migration lock"),
			// --skip-lock=false asks for the lock the report takes anyway, as
			// skip_lock = false does, so only true is refused.
			atlasargs.UnsupportedTrueBoolReason(atlasSkipLockFlag, "", "Skip taking the migration lock",
				atlasDriftSkipLockReason),
			atlasargs.NativeBool(atlasNoCacheFlag, "", "Compute the expected state again; Ptah does on every run",
				atlasNoCacheFlag),
		}),
	}
}

// atlasTargetEnvDisabled marks each flag whose native flag is one of
// unbound [atlasargs.Flag.TargetEnvDisabled], for a target whose bindings
// of those names are disabled.
func atlasTargetEnvDisabled(unbound []string, flags []atlasargs.Flag) []atlasargs.Flag {
	for i := range flags {
		flags[i].TargetEnvDisabled = slices.Contains(unbound, flags[i].NativeName)
	}
	return flags
}

// atlasDriftExcludeFlag is the repeatable --exclude, forwarded one value at a
// time because the native --exclude is repeatable too.
func atlasDriftExcludeFlag() atlasargs.Flag {
	flag := atlasargs.NativeStringArray("exclude", "", "Glob patterns of schema objects to leave out of the comparison", "exclude")
	flag.ForwardEach = true
	return flag
}

// atlasMigrateDriftTarget is `ptah migrations drift` with the environment
// binding of every flag of [atlasDriftNativeOnlyFlags] disabled. The adapter
// decides which database each target reads: --url or PTAH_URL for one
// database, or each instance's url. Left live, PTAH_DB_URL would mark --db-url
// as set and replace the url of every instance with one database, the
// decision the adapter refuses; PTAH_JSON would print the native document in
// place of the report this surface prints.
func atlasMigrateDriftTarget() *cobra.Command {
	target := migrationsdrift.NewMigrationsDriftCommand()
	for _, name := range atlasDriftNativeOnlyFlags {
		if err := cmdflags.DisableEnvBinding(target.Flags(), name); err != nil {
			panic(err)
		}
	}
	return target
}

// atlasMigrateDriftNativeArgs drops --no-cache from the native arguments,
// after checking a value given to it reads as a boolean, as pflag would. A
// blank --lock-name is refused as `migrate apply` refuses it: read as unset it
// would take the default lock, and a runner holding the named one would not
// exclude this report.
func atlasMigrateDriftNativeArgs(native []string) ([]string, error) {
	for i, arg := range native {
		name, inline := strings.CutPrefix(arg, "--migration-lock-name=")
		if !inline && arg == "--migration-lock-name" && i+1 < len(native) {
			name, inline = native[i+1], true
		}
		if inline && strings.TrimSpace(name) == "" {
			return nil, fmt.Errorf("--%s must not be empty", atlasLockNameFlag)
		}
	}
	for _, arg := range native {
		value, found := strings.CutPrefix(arg, "--"+atlasNoCacheFlag+"=")
		if !found {
			continue
		}
		if _, err := strconv.ParseBool(value); err != nil {
			return nil, fmt.Errorf("invalid argument %q for \"--%s\" flag: %w", value, atlasNoCacheFlag, err)
		}
	}
	return slices.DeleteFunc(native, func(arg string) bool {
		return arg == "--"+atlasNoCacheFlag || strings.HasPrefix(arg, "--"+atlasNoCacheFlag+"=")
	}), nil
}

// atlasInstanceAgreement is a value the native command takes once for every
// instance, named by the Atlas flag that carries it.
type atlasInstanceAgreement struct {
	flag  string
	field projectconfig.StringField
}

// atlasInstanceAgreements are the instance values [resolveAtlasVerbInstances]
// forwards as one flag. The url, the dev database and the migration directory
// are not among them: the native command reads those out of each instance's
// config.
var atlasInstanceAgreements = []atlasInstanceAgreement{
	{flag: "dir-format", field: projectconfig.StringMigrationFormat},
	{flag: "revisions-schema", field: projectconfig.StringMigrationRevisionsSchema},
	{flag: "lock-timeout", field: projectconfig.StringMigrationLockTimeout},
}

// resolveAtlasVerbInstances is [resolveAtlasVerbProject] for a verb marked
// [atlasVerb.eachInstance]. It loads every instance of the selected env, as
// `migrate apply` does, and hands them over to the native command as one
// snapshot, so the native command reads each instance's database and dev
// database and reports each one. The values of [atlasInstanceAgreements] must
// agree across the instances; they are forwarded as a single env's values are.
// A migration directory every instance shares is forwarded the same way, as
// one --dir. Instances that name different directories each get their own in
// the snapshot ([atlasInstancesNameTheirDirectories]). Either way a relative
// directory is read against the project file's directory and stays inside it.
func resolveAtlasVerbInstances(
	resolved atlasVerbArgs,
	verb atlasVerb,
	flags atlasProjectFlagValues,
	policy atlascompatpolicy.Policy,
	cleanup *cmdadapter.CleanupScope,
) (atlasVerbArgs, error) {
	if !flags.readsProject() {
		return resolved, nil
	}
	set, loaded, err := openAtlasProjectsWithPolicy(resolved.context, flags, policy)
	if err != nil || !loaded {
		return resolved, err
	}
	cleanup.Add(set.Close)
	if verb.refuseProjectSettings != nil {
		// A variable the flag refuses empty is refused first, as the mapping
		// would refuse it, so an empty PTAH_LOCK_NAME is not read as no flag.
		for _, flag := range verb.flags {
			if err := atlasargs.CheckEmptyEnv(flag); err != nil {
				return atlasVerbArgs{}, err
			}
		}
		loadedConfigs := make([]*projectconfig.Config, 0, len(set.projects))
		for i := range set.projects {
			loadedConfigs = append(loadedConfigs, &set.projects[i].Config)
		}
		given := func(flag string) bool { return atlasFlagValueSet(verb.flags, resolved.args, flag) }
		if err := verb.refuseProjectSettings(loadedConfigs, given); err != nil {
			return atlasVerbArgs{}, err
		}
	}
	if err := refuseDisagreeingInstances(verb, resolved.args, flags.envName, set.projects); err != nil {
		return atlasVerbArgs{}, err
	}
	ownDirs := atlasInstancesNameTheirDirectories(verb, resolved.args, set.projects)
	// A native command given an env merges the ptah.yaml env of that name
	// under the atlas.hcl one, as [openMergedProjectConfigWithPolicy] does for
	// a single env.
	var ptah projectconfig.Config
	if flags.envName != "" {
		if ptah, err = projectconfig.LoadPtahFile(projectconfig.PtahFileName, flags.envName); err != nil {
			return atlasVerbArgs{}, err
		}
	}
	configs := make([]projectconfig.Config, 0, len(set.projects))
	for _, project := range set.projects {
		if err := policy.ValidateProjectConfig(project.Config); err != nil {
			return atlasVerbArgs{}, err
		}
		configs = append(configs, projectconfig.Merge(ptah, project.Config))
	}
	dirs := set.projects[:1]
	if ownDirs {
		dirs = set.projects
	}
	for i := range dirs {
		if err := dirs[i].resolveMigrationDirForArgs(verb.flags, resolved.args); err != nil {
			return atlasVerbArgs{}, err
		}
		resolved.context = withAtlasProjectMigrationRoot(resolved.context, "migrate", dirs[i])
		if ownDirs {
			configs[i].Migration.Dir = dirs[i].migrationDir.Path
		}
	}
	first := set.projects[0]
	resolved.project = first
	resolved.projectLoaded = true
	resolved.instanceDirs = ownDirs
	if !ownDirs && first.migrationDirResolved {
		resolved.args = append(resolved.args, "--dir", first.migrationDir.Path)
	}
	resolved.args = applyAtlasInstanceValues(verb.flags, resolved.args, first)
	resolved.context = dbcli.WithProjectConfigs(resolved.context, configs)
	resolved.instances = configs
	return resolved, nil
}

// refuseDisagreeingInstances refuses an env whose instances give a value of
// [atlasInstanceAgreements] more than one way, unless the command line names
// it. One flag cannot carry two values, and taking the first instance's for
// every instance would read the others against the wrong directory or table.
func refuseDisagreeingInstances(verb atlasVerb, args []string, envName string, projects []atlasProject) error {
	for _, agreement := range atlasInstanceAgreements {
		if atlasFlagValueSet(verb.flags, args, agreement.flag) {
			continue
		}
		first := projects[0].StringValue(agreement.field)
		for _, project := range projects[1:] {
			if project.StringValue(agreement.field) != first {
				return fmt.Errorf(
					"the targets of env %q do not agree on --%s, which this surface forwards once for every target; "+
						"give them one value or pass --%s", envName, agreement.flag, agreement.flag)
			}
		}
	}
	return nil
}

// atlasInstancesNameTheirDirectories reports whether each instance's
// migration directory goes into its own config rather than into one --dir:
// the command line and PTAH_DIR name none, and the instances name different
// ones. The instances of an env come from one block, so when one names a
// directory every one does: the parser refuses a `dir` that evaluates to null.
func atlasInstancesNameTheirDirectories(verb atlasVerb, args []string, projects []atlasProject) bool {
	if atlasFlagValueSet(verb.flags, args, "dir") {
		return false
	}
	first := projects[0].StringValue(projectconfig.StringMigrationDir)
	return slices.ContainsFunc(projects[1:], func(project atlasProject) bool {
		return project.StringValue(projectconfig.StringMigrationDir) != first
	})
}

// atlasInstanceDirFlags drops the default of --dir when the instances name
// their own directories: forwarded, the default would be the one directory
// every target reads.
func atlasInstanceDirFlags(flags []atlasargs.Flag, project atlasVerbArgs) []atlasargs.Flag {
	if !project.instanceDirs {
		return flags
	}
	out := slices.Clone(flags)
	for i := range out {
		if out[i].Name == "dir" {
			out[i].Default = ""
		}
	}
	return out
}

// applyAtlasInstanceValues appends the values of [atlasInstanceAgreements]
// the first instance gives, as [applyAtlasProjectConfigToArgs] appends a
// single env's.
func applyAtlasInstanceValues(flags []atlasargs.Flag, args []string, project atlasProject) []string {
	for _, agreement := range atlasInstanceAgreements {
		args = appendAtlasProjectStringArg(flags, args, agreement.flag, project.StringValue(agreement.field))
	}
	return args
}

// refuseOneDatabaseForEveryInstance refuses --url, or the PTAH_URL it reads,
// over an env that names several targets. The native command would read the
// one database for every target and report each of them as that database, so
// a drifted or unreachable target would read as clean.
func refuseOneDatabaseForEveryInstance(project atlasVerbArgs, native []string) error {
	if !atlasInstancesCollection(project.instances) {
		return nil
	}
	index := slices.IndexFunc(native, isNativeDatabaseURLArg)
	if index < 0 {
		return nil
	}
	every, named, each := "every target", fmt.Sprintf("names %d targets with a url each", len(project.instances)), "each target"
	if len(project.instances) == 1 {
		every, named, each = "the target", "names one target with a url of its own", "that target"
	}
	set := "--url or PTAH_URL sets one database for " + every + ","
	if nativeDatabaseURL(native, index) == "" {
		set = "--url or PTAH_URL is set, though empty,"
	}
	return fmt.Errorf("%s and env %q %s; leave both unset to report %s", set, project.projectFlags.envName, named, each)
}

// atlasInstancesCollection reports whether configs are the instances of an env
// with for_each, which the native command reports as targets, however many
// there are.
func atlasInstancesCollection(configs []projectconfig.Config) bool {
	return len(configs) > 1 || (len(configs) == 1 && configs[0].EnvInstance > 0)
}

// nativeDatabaseURL is the value of the --db-url at native[index], written
// inline or as the next argument.
func nativeDatabaseURL(native []string, index int) string {
	if value, inline := strings.CutPrefix(native[index], "--db-url="); inline {
		return value
	}
	if index+1 < len(native) {
		return native[index+1]
	}
	return ""
}

func isNativeDatabaseURLArg(arg string) bool {
	return arg == "--db-url" || strings.HasPrefix(arg, "--db-url=")
}
