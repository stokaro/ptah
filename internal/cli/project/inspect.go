package project

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/dbcli"
	"ptah.run/internal/dburldisplay"
)

const (
	inspectFormatText = "text"
	inspectFormatJSON = "json"
)

type inspectOptions struct {
	atlasPath string
	ptahPath  string
	envName   string
	format    string
}

// Setting is one project setting and where Ptah stands on it.
type Setting struct {
	// Name is the setting as a reader of the project file would name it.
	Name string `json:"name"`
	// Value is what Ptah resolved, empty when the file sets nothing.
	Value string `json:"value,omitempty"`
	// Carried reports whether Ptah acts on it.
	Carried bool `json:"carried"`
}

// DeclaredEnv is an env a project file declares.
type DeclaredEnv struct {
	// File is the declaring file, as the loader was given it.
	File string `json:"file"`
	// Name is the env's name, empty for an atlas.hcl env block with no label.
	Name string `json:"name,omitempty"`
}

// Report is what `project inspect` answers.
type Report struct {
	// Env is the selected environment, empty when the run named none.
	Env string `json:"env,omitempty"`
	// UnselectedEnvs lists the envs the project files declare when the run
	// named none. No env applies to such a run, so its settings may all read
	// "not set" while the files hold values; this says where they are.
	UnselectedEnvs []DeclaredEnv `json:"unselected_envs,omitempty"`
	// Carried are the settings Ptah acts on.
	Carried []Setting `json:"carried"`
	// Ignored are the names the file declares that Ptah read and did nothing
	// with, each with the position it was written at.
	//
	// This is the half that cannot be obtained any other way: Atlas CE reports
	// nothing for a name it does not know, so a setting that silently does
	// nothing is indistinguishable from one that works.
	Ignored []projectconfig.IgnoredAtlasConstruct `json:"ignored"`
}

func newProjectInspectCommand() *cobra.Command {
	opts := inspectOptions{}
	cmd := &cobra.Command{
		Use:   "inspect",
		Short: "Report what Ptah reads from a project file, and what it ignores",
		Long: `Report what Ptah reads from a project file, and what it ignores.

The carried list is what Ptah acts on: the target database, the dev database,
the desired schema sources, the schemas and exclusions, and the migration
directory.

The ignored list is every name the file declares that Ptah read and did nothing
with, with the file and line it was written at. A name in that list is not an
error -- Atlas CE accepts unknown names too -- but it is a setting whose author
believes it does something.

This command reads files and contacts no database.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runInspect(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.atlasPath, "atlas-config", "", "Path to atlas.hcl (default: ./atlas.hcl when present)")
	flags.StringVar(&opts.ptahPath, "config", "", "Path to ptah.yaml (default: ./ptah.yaml when present)")
	flags.StringVar(&opts.envName, "env", "", "Project environment to resolve; without it no env applies")
	flags.StringVar(&opts.format, "format", inspectFormatText, "Output format: text or json")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runInspect(cmd *cobra.Command, opts inspectOptions) error {
	if opts.format != inspectFormatText && opts.format != inspectFormatJSON {
		return cmdutil.Fail(cmd, fmt.Errorf("unsupported --format %q: use text or json", opts.format))
	}

	config, err := projectconfig.Load(projectconfig.LoadOptions{
		Context:   cmd.Context(),
		PtahPath:  opts.ptahPath,
		AtlasPath: opts.atlasPath,
		EnvName:   opts.envName,
		Verb:      "project inspect",
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	report := reportFor(config)
	if opts.format == inspectFormatJSON {
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		return encoder.Encode(report)
	}
	writeText(cmd.OutOrStdout(), report)
	return nil
}

// reportFor turns a resolved config into the report.
//
// The carried list names every setting whether or not the file sets it, and
// says so with Carried. A list of only the settings that happen to be present
// answers "what did I write"; the operator's question is "what does Ptah do
// with this project", and an absent target database is part of that answer.
func reportFor(config projectconfig.Config) Report {
	report := Report{
		Env:            config.EnvName,
		UnselectedEnvs: unselectedEnvs(config),
		Ignored:        slices.Clone(config.IgnoredConstructs),
		Carried: []Setting{
			{Name: "database url", Value: dburldisplay.Format(config.DatabaseURL)},
			{Name: "dev url", Value: dburldisplay.Format(config.DevURL)},
			{Name: "schema sources", Value: strings.Join(displayedSources(config.SchemaSources), ", ")},
			{Name: "schemas", Value: strings.Join(config.Schemas, ", ")},
			{Name: "exclude", Value: strings.Join(config.Exclude, ", ")},
			{Name: "exporter", Value: config.ExporterName},
		},
	}
	for index := range report.Carried {
		report.Carried[index].Carried = report.Carried[index].Value != ""
	}
	// Sorted by position rather than left in read order: two runs over one file
	// must produce the same report, and a diff between them must mean the file
	// changed.
	slices.SortFunc(report.Ignored, func(a, b projectconfig.IgnoredAtlasConstruct) int {
		if a.Filename != b.Filename {
			return strings.Compare(a.Filename, b.Filename)
		}
		if a.Line != b.Line {
			return a.Line - b.Line
		}
		return strings.Compare(a.Name, b.Name)
	})
	return report
}

// unselectedEnvs returns the envs the project files declare when the run named
// none, and nil when it named one.
func unselectedEnvs(config projectconfig.Config) []DeclaredEnv {
	if config.EnvName != "" || len(config.DeclaredEnvs) == 0 {
		return nil
	}
	envs := make([]DeclaredEnv, 0, len(config.DeclaredEnvs))
	for _, env := range config.DeclaredEnvs {
		envs = append(envs, DeclaredEnv{File: env.File, Name: env.Name})
	}
	return envs
}

// describeUnselected names the envs as a refusal does.
func describeUnselected(envs []DeclaredEnv) string {
	declared := make([]projectconfig.DeclaredEnv, 0, len(envs))
	for _, env := range envs {
		declared = append(declared, projectconfig.DeclaredEnv{File: env.File, Name: env.Name})
	}
	return dbcli.DescribeDeclaredEnvs(declared)
}

// displayedSources are schema sources as a report shows them: a source may be a
// database URL.
func displayedSources(sources []string) []string {
	displayed := make([]string, 0, len(sources))
	for _, source := range sources {
		displayed = append(displayed, dburldisplay.Format(source))
	}
	return displayed
}

func writeText(out io.Writer, report Report) {
	if report.Env != "" {
		fmt.Fprintf(out, "Environment: %s\n\n", report.Env)
	}
	if len(report.UnselectedEnvs) > 0 {
		fmt.Fprintf(out, "Environment: none selected; %s. Pass --env to resolve one.\n\n",
			describeUnselected(report.UnselectedEnvs))
	}

	fmt.Fprintln(out, "Carried by Ptah:")
	for _, setting := range report.Carried {
		if !setting.Carried {
			fmt.Fprintf(out, "  %-16s (not set)\n", setting.Name)
			continue
		}
		fmt.Fprintf(out, "  %-16s %s\n", setting.Name, setting.Value)
	}

	if len(report.Ignored) == 0 {
		fmt.Fprintln(out, "\nRead and ignored: nothing.")
		return
	}
	fmt.Fprintf(out, "\nRead and ignored (%d):\n", len(report.Ignored))
	for _, ignored := range report.Ignored {
		fmt.Fprintf(out, "  %s:%d  %s %q\n", ignored.Filename, ignored.Line, ignored.Kind, ignored.Name)
	}
	fmt.Fprintln(out, "\nThese names are accepted and acted on by nothing. That is not an error,")
	fmt.Fprintln(out, "but a setting whose author believes it does something is worth knowing about.")
}
