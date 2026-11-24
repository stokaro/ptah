package atlas

import (
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/internal/atlasargs"
	"ptah.run/internal/cli/dbvulnerabilities"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/envbool"
)

// atlasSecurityScanEnvVar routes `ptah-compat security scan` to the native
// vulnerability scan, `ptah db vulnerabilities`. Unset, every spelling of
// `security` answers as the unknown command the community binary reports.
//
// It is a variable rather than a flag because the compatibility surface
// registers no flag the community binary does not have.
const atlasSecurityScanEnvVar = "PTAH_ATLAS_SECURITY_SCAN"

// atlasSecurityScan is the declaration of the variable. See
// [ptah.run/internal/envbool].
//
// It is [ptah.run/internal/envbool.Gated]: the pinned community binary
// registers no security command and refuses `security`, `security scan` and
// their help flags as an unknown command, so strict compatibility refuses an
// enabled value rather than run a scan the oracle has no command for.
var atlasSecurityScan = envbool.New(atlasSecurityScanEnvVar, false, envbool.Gated)

// atlasSecurityJSONFormat is the one --format value `security scan` reads, with
// its whitespace removed: the template that renders the report as JSON on
// every Atlas verb that takes one.
const atlasSecurityJSONFormat = "{{json.}}"

// newAtlasSecurityCommand returns the `security` group, registered outside the
// strict profile only, as `script` is. It is hidden, and until
// PTAH_ATLAS_SECURITY_SCAN is enabled every spelling of it answers as the tree
// without it does: running it is the community binary's unknown-command error,
// and asking for its help prints the root's help.
//
// The decision is taken when the command runs, not when the tree is built, so
// the tree and the command reference generated from it do not depend on the
// environment of whoever built them. The group and its verb both read their
// own flags, which is what lets a help flag reach the decision: a help flag
// cobra parses is answered with help at exit 0 before any run function, and
// the community binary refuses `security --help` at exit 1. The help command
// and shell completion run no function of the group, so [gateAtlasRequests]
// decides for them.
func newAtlasSecurityCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:                "security [command]",
		Short:              "Security checks of connected databases",
		Hidden:             true,
		DisableFlagParsing: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := requireAtlasSecurityScan(cmd); err != nil {
				return err
			}
			return runAtlasGroupHelp(cmd, args)
		},
	}
	// Like the schema and migrate groups, an extra word asks for the group's
	// help.
	cmdutil.ConfigureCommandArgs(cmd, nil)
	registerAtlasProjectFlags(cmd.PersistentFlags(), &atlasProjectFlagValues{})
	scan := newAtlasAdapterCommand("security", atlasSecurityScanVerb())
	scan.Long = atlasSecurityScanLong
	run := scan.RunE
	scan.RunE = func(cmd *cobra.Command, args []string) error {
		if err := requireAtlasSecurityScan(cmd); err != nil {
			return err
		}
		return run(cmd, args)
	}
	cmd.AddCommand(scan)
	return cmd
}

// requireAtlasSecurityScan resolves PTAH_ATLAS_SECURITY_SCAN for a run of the
// group or its verb. A malformed value is refused on every such run; an unset
// or false one is the unknown-command error the root gives a word it does not
// register, so the default surface answers exactly as the community binary.
func requireAtlasSecurityScan(cmd *cobra.Command) error {
	enabled, err := atlasSecurityScan.Resolve()
	if err != nil {
		return failAtlasCommand(cmd, err)
	}
	if !enabled {
		return unknownAtlasRootCommand(cmd.Root(), "security")
	}
	return nil
}

// gateAtlasSecurityHelp makes the help of the security group and its verb the
// root's help while PTAH_ATLAS_SECURITY_SCAN is not enabled, which is what
// `help security` prints in a tree without the group. A help function cannot
// fail the run, so the help command refuses a malformed value before it gets
// here ([gateAtlasRequests]), and here a malformed value counts as not
// enabled.
//
// It wraps the help function the usage tree installed, so it runs after
// installAtlasUsageTree.
func gateAtlasSecurityHelp(cmd *cobra.Command) {
	help := cmd.HelpFunc()
	cmd.SetHelpFunc(func(cmd *cobra.Command, args []string) {
		if enabled, err := atlasSecurityScan.Resolve(); err != nil || !enabled {
			// The help command initializes the help flag of the command it
			// found, which is this one; the root's help lists the root's.
			root := cmd.Root()
			root.InitDefaultHelpFlag()
			root.HelpFunc()(root, args)
			return
		}
		help(cmd, args)
	})
	for _, child := range cmd.Commands() {
		gateAtlasSecurityHelp(child)
	}
}

// atlasCommandWithin reports whether the command args name under root is
// within, or is, group.
func atlasCommandWithin(root *cobra.Command, args []string, group *cobra.Command) bool {
	target, _, err := root.Find(args)
	for ; err == nil && target != nil; target = target.Parent() {
		if target == group {
			return true
		}
	}
	return false
}

// atlasSecurityScanLong is the help text of `security scan`. It names the
// native variables because they are the only way this surface takes the scan's
// other options.
const atlasSecurityScanLong = `Forwards to ` + "`ptah db vulnerabilities`" + `.

The command matches the version of each database server -u/--url names
against reviewed vendor advisories and reports what affects it. --format
'{{ json . }}' prints the report as JSON; without it the report is a table.

The scan's other options are the native command's environment variables:
PTAH_ADVISORIES, PTAH_ADVISORY_SOURCE, PTAH_SUPPRESSIONS, PTAH_IGNORE,
PTAH_FAIL_ON, PTAH_FAIL_SEVERITY, PTAH_MIN_SEVERITY and PTAH_DEPLOYMENT.`

// atlasSecurityScanVerb forwards `ptah-compat security scan` to the native
// `ptah db vulnerabilities`. -u/--url names a database and repeats, and an
// atlas.hcl environment's url fills it under --env. The report is the native
// table, or the native JSON document under --format '{{ json . }}'. Every
// other scan option is a native flag this surface does not register: it
// reaches the scan through the native command's own PTAH_* variable, such as
// PTAH_FAIL_ON or PTAH_ADVISORIES.
func atlasSecurityScanVerb() atlasVerb {
	url := atlasargs.NativeStringArray("url", "u", "URL of a database to scan; repeat it to scan several", "db-url")
	url.MapValue = atlasSecurityScanURL
	format := atlasargs.NativeString("format", "", "Go template to use to format the output", "format")
	format.MapValue = atlasSecurityScanFormat
	return atlasVerb{
		use:        "scan",
		displayUse: "scan [flags]",
		short:      "Report known server vulnerabilities of connected databases",
		native:     "db vulnerabilities",
		factory:    dbvulnerabilities.NewCommand,
		// Every invocation needs a database, and the native refusal names a
		// flag this surface refuses; projectConfig runs on every invocation,
		// with or without a project file.
		projectConfig: applyAtlasSecurityScanProject,
		nativeOnlyFlags: []string{
			"advisories",
			"advisory-source",
			"db-url",
			"deployment",
			"fail-on",
			"fail-severity",
			"ignore",
			"min-severity",
			"provider-config",
			"provider-engine",
			"provider-exchanges",
			"suppressions",
		},
		flags: []atlasargs.Flag{url, format},
	}
}

// applyAtlasSecurityScanProject applies the selected atlas.hcl environment as
// every verb does, then refuses a scan that names no database: no -u/--url, no
// environment url, and neither PTAH_URL nor the native PTAH_DB_URL, both of
// which atlasFlagValueSet reads.
//
// A native flag is refused first, as the mapper refuses it after this step:
// `--db-url x` names a database, just not in a spelling this surface reads,
// and the refusal that says so is the one to give.
func applyAtlasSecurityScanProject(
	flags []atlasargs.Flag,
	args []string,
	project atlasProject,
	projectFlags atlasProjectFlagValues,
) (mappedArgs, nativeArgs []string, err error) {
	mappedArgs, nativeArgs, err = applyAtlasProjectConfigToArgs(flags, args, project, projectFlags)
	if err != nil {
		return nil, nil, err
	}
	if err := rejectNativeOnlyAtlasFlags("security", atlasSecurityScanVerb(), mappedArgs); err != nil {
		return nil, nil, err
	}
	if !atlasFlagValueSet(flags, mappedArgs, "url") {
		return nil, nil, errors.New(`required flag(s) "url" not set`)
	}
	return mappedArgs, nativeArgs, nil
}

// atlasSecurityScanURL refuses an empty -u/--url value. Refused here, the
// error names the flag as the caller wrote it; forwarded, the native command
// would name --db-url, which this surface does not accept.
func atlasSecurityScanURL(value string) (string, error) {
	if value == "" {
		return "", errors.New("a database URL must not be empty")
	}
	return value, nil
}

// atlasSecurityScanFormat maps the Atlas --format template onto the native
// --format. The report has no Atlas shape to render a template against, so the
// JSON template is the one value it reads, spaced any way, and anything else is
// refused rather than printed as the table.
func atlasSecurityScanFormat(value string) (string, error) {
	if strings.Join(strings.Fields(value), "") == atlasSecurityJSONFormat {
		return "json", nil
	}
	return "", fmt.Errorf("security scan reads --format only as '{{ json . }}', got %q; omit it for the table report", value)
}
