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
		prefixArgs:       []string{"--" + dbcli.RevisionTableFormatFlagName, "atlas", "--dir-format", atlasDirFormatDefault},
		finishNativeArgs: atlasMigrateDriftNativeArgs,
		nativeOnlyFlags: []string{
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
		},
		flags: []atlasargs.Flag{
			atlasargs.NativeString("url", "u", "URL of the database to check", "db-url"),
			atlasargs.NativeString("dev-url", "",
				"URL of the dev database the applied migrations are replayed on", "shadow-db"),
			atlasMigrationsDirFlag(),
			atlasMigrateDirFormatFlag("dir-format"),
			atlasRevisionsSchemaFlag("Schema of the revision table"),
			atlasDriftExcludeFlag(),
			atlasargs.UnsupportedStringReason("format", "", "Go template to use to format the output",
				"Ptah prints its own drift report, and has no template data in the shape Atlas documents for it"),
			atlasargs.NativeString("lock-timeout", "", "How long to wait for the migration lock",
				"migration-lock-timeout"),
			atlasargs.NativeString(atlasLockNameFlag, "", "Name of the migration lock", "migration-lock-name"),
			atlasargs.UnsupportedBoolReason(atlasSkipLockFlag, "", "Skip taking the migration lock",
				"the report reads the revision table under the migration lock, so that a migration "+
					"being applied is not reported as drift"),
			atlasargs.NativeBool(atlasNoCacheFlag, "", "Compute the expected state again; Ptah does on every run",
				atlasNoCacheFlag),
		},
	}
}

// atlasDriftExcludeFlag is the repeatable --exclude, forwarded one value at a
// time because the native --exclude is repeatable too.
func atlasDriftExcludeFlag() atlasargs.Flag {
	flag := atlasargs.NativeStringArray("exclude", "", "Glob patterns of schema objects to leave out of the comparison", "exclude")
	flag.ForwardEach = true
	return flag
}

// atlasMigrateDriftTarget is `ptah migrations drift` with the binding of its
// --db-url to PTAH_DB_URL disabled. The adapter decides which database each
// target reads: --url or PTAH_URL for one database, or each instance's url.
// Left live, the binding would mark --db-url as set and replace the url of
// every instance with one database, the decision the adapter refuses.
func atlasMigrateDriftTarget() *cobra.Command {
	target := migrationsdrift.NewMigrationsDriftCommand()
	if err := cmdflags.DisableEnvBinding(target.Flags(), "db-url"); err != nil {
		panic(err)
	}
	return target
}

// atlasMigrateDriftNativeArgs drops --no-cache from the native arguments,
// after checking a value given to it reads as a boolean, as pflag would.
func atlasMigrateDriftNativeArgs(native []string) ([]string, error) {
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
// forwards as one flag. The url and the dev database are not among them: the
// native command reads those out of each instance's config.
var atlasInstanceAgreements = []atlasInstanceAgreement{
	{flag: "dir", field: projectconfig.StringMigrationDir},
	{flag: "dir-format", field: projectconfig.StringMigrationFormat},
	{flag: "revisions-schema", field: projectconfig.StringMigrationRevisionsSchema},
	{flag: "lock-timeout", field: projectconfig.StringMigrationLockTimeout},
}

// resolveAtlasVerbInstances is [resolveAtlasVerbProject] for a verb marked
// [atlasVerb.eachInstance]. It loads every instance of the selected env, as
// `migrate apply` does, and hands them over to the native command as one
// snapshot, so the native command reads each instance's database and dev
// database and reports each one. The values the native command takes as one
// flag for every instance must agree across the instances; they are forwarded
// as a single env's values are, so a relative migration directory is read
// against the project file's directory and stays inside it.
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
	if err := refuseDisagreeingInstances(verb, resolved.args, flags.envName, set.projects); err != nil {
		return atlasVerbArgs{}, err
	}
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
	first := set.projects[0]
	if err := first.resolveMigrationDirForArgs(verb.flags, resolved.args); err != nil {
		return atlasVerbArgs{}, err
	}
	resolved.project = first
	resolved.projectLoaded = true
	resolved.args = applyAtlasInstanceValues(verb.flags, resolved.args, first)
	resolved.context = withAtlasProjectMigrationRoot(resolved.context, "migrate", first)
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
					"the targets of env %q do not agree on --%s, which the drift report takes once for every target; "+
						"give them one value or pass --%s", envName, agreement.flag, agreement.flag)
			}
		}
	}
	return nil
}

// applyAtlasInstanceValues appends the values of [atlasInstanceAgreements]
// the first instance gives, as [applyAtlasProjectConfigToArgs] appends a
// single env's.
func applyAtlasInstanceValues(flags []atlasargs.Flag, args []string, project atlasProject) []string {
	if project.migrationDirResolved {
		args = append(args, "--dir", project.migrationDir.Path)
	}
	for _, agreement := range atlasInstanceAgreements[1:] {
		args = appendAtlasProjectStringArg(flags, args, agreement.flag, project.StringValue(agreement.field))
	}
	return args
}

// refuseOneDatabaseForEveryInstance refuses --url, or the PTAH_URL it reads,
// over an env that names several targets. The native command would read the
// one database for every target and report each of them as that database, so
// a drifted or unreachable target would read as clean.
func refuseOneDatabaseForEveryInstance(project atlasVerbArgs, native []string) error {
	if !atlasInstancesCollection(project.instances) || !slices.ContainsFunc(native, isNativeDatabaseURLArg) {
		return nil
	}
	noun := "targets"
	if len(project.instances) == 1 {
		noun = "target"
	}
	return fmt.Errorf("--url or PTAH_URL sets one database for every target, and env %q names %d %s with a url each; "+
		"leave both unset to report each target",
		project.projectFlags.envName, len(project.instances), noun)
}

// atlasInstancesCollection reports whether configs are the instances of an env
// with for_each, which the native command reports as targets, however many
// there are.
func atlasInstancesCollection(configs []projectconfig.Config) bool {
	return len(configs) > 1 || (len(configs) == 1 && configs[0].EnvInstance > 0)
}

func isNativeDatabaseURLArg(arg string) bool {
	return arg == "--db-url" || strings.HasPrefix(arg, "--db-url=")
}
