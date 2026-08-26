package project

import (
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasregistry"
)

// The adoption classes.
//
// stokaro/ptah#1215 asks for "exact/native-equivalent/compat-only/manual/
// unsupported or an equivalent explicit taxonomy". This is the equivalent one,
// and the fold is stated rather than left to be inferred: `exact` and
// `native-equivalent` are one class here because Ptah has ONE project language
// -- a construct Ptah acts on with no Atlas-only spelling needs no change and
// there is no second native form for it to be equivalent to. `manual` has no
// members yet and is therefore absent: a class that can never be populated
// would be a promise the analysis does not keep.
const (
	// classExact is a construct Ptah acts on that means the same thing in a
	// native Ptah project file. Nothing to do.
	classExact = "exact"
	// classCompatOnly is a construct Ptah acts on whose SPELLING exists for
	// Atlas compatibility and has a native equivalent. Adoption rewrites the
	// spelling; the meaning survives.
	classCompatOnly = "compat-only"
	// classUnsupported is a name the file declares that Ptah read and acts on
	// nothing. Adoption cannot carry it, and it is the reason a project is not
	// native-ready.
	classUnsupported = "unsupported"
)

type adoptOptions struct {
	atlasPath string
	ptahPath  string
	envName   string
	format    string
	check     bool
}

// Construct is one thing the project file declares and where adoption stands
// on it.
type Construct struct {
	// Name is the construct as a reader of the project file would name it.
	Name string `json:"name"`
	// Class is one of the constants above.
	Class string `json:"class"`
	// Detail says what the class means for this construct: the native
	// equivalent for a compat-only one, the position for an unsupported one.
	Detail string `json:"detail,omitempty"`
}

// AdoptionReport is what `project adopt --check` answers.
type AdoptionReport struct {
	// Env is the selected environment, empty when the file has none.
	Env string `json:"env,omitempty"`
	// NativeReady reports whether the project can be operated by native Ptah
	// with no semantic conversion and no rewriting of its file.
	//
	// It is the answer to #1215's "a project that needs no semantic conversion
	// can be identified as native-ready without rewriting its file", and it is
	// false when anything is compat-only or unsupported.
	NativeReady bool `json:"native_ready"`
	// Constructs are everything the analysis classified, in a stable order.
	Constructs []Construct `json:"constructs"`
}

func newProjectAdoptCommand() *cobra.Command {
	opts := adoptOptions{}
	cmd := &cobra.Command{
		Use:   "adopt",
		Short: "Report what it would take to operate this project with native Ptah",
		Long: `Report what it would take to operate this project with native Ptah.

Every construct the project file declares is put in one of three classes:

  exact         Ptah acts on it and it means the same in a native Ptah project.
  compat-only   Ptah acts on it, but the spelling is Atlas's and a native
                equivalent exists. Adoption rewrites the spelling.
  unsupported   Ptah read the name and acts on nothing. Adoption cannot carry
                it, and it is why a project is not native-ready.

A project with nothing in the last two classes is native-ready: it can be
operated by native Ptah as it stands, without its file being rewritten.

Only the read-only analysis exists. --check is required, and this command
contacts no database and writes no file.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runAdopt(cmd, opts)
		},
	}
	flags := cmd.Flags()
	flags.StringVar(&opts.atlasPath, "atlas-config", "", "Path to atlas.hcl (default: ./atlas.hcl when present)")
	flags.StringVar(&opts.ptahPath, "config", "", "Path to ptah.yaml (default: ./ptah.yaml when present)")
	flags.StringVar(&opts.envName, "env", "", "Project environment to resolve")
	flags.StringVar(&opts.format, "format", inspectFormatText, "Output format: text or json")
	flags.BoolVar(&opts.check, "check", false,
		"Report the analysis without changing anything (currently required)")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAdopt(cmd *cobra.Command, opts adoptOptions) error {
	if !opts.check {
		// Refused rather than defaulted to the analysis: `adopt` without a flag
		// will one day rewrite the project file, and a reader who learns today
		// that it is safe would be wrong then. Naming what is missing is what
		// keeps an unsupported construct from disappearing into a conversion
		// nobody wrote (stokaro/ptah#1215).
		return cmdutil.Fail(cmd, fmt.Errorf(
			"adoption rewrites the project file and is not implemented: "+
				"run with --check for the read-only analysis of what it would have to change"))
	}
	if opts.format != inspectFormatText && opts.format != inspectFormatJSON {
		return cmdutil.Fail(cmd, fmt.Errorf("unsupported --format %q: use text or json", opts.format))
	}

	config, err := projectconfig.Load(projectconfig.LoadOptions{
		Context:   cmd.Context(),
		PtahPath:  opts.ptahPath,
		AtlasPath: opts.atlasPath,
		EnvName:   opts.envName,
		Verb:      "project adopt",
	})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	report := adoptionReportFor(config)
	if opts.format == inspectFormatJSON {
		encoder := json.NewEncoder(cmd.OutOrStdout())
		encoder.SetIndent("", "  ")
		if err := encoder.Encode(report); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	} else {
		writeAdoptionText(cmd.OutOrStdout(), report)
	}
	if !report.NativeReady {
		// A --check that reported a gap and exited 0 could not gate anything.
		return cmdutil.Fail(cmd, fmt.Errorf(
			"this project is not native-ready: %d of %d constructs need adoption",
			needingAdoption(report), len(report.Constructs)))
	}
	return nil
}

// adoptionReportFor classifies everything the resolved config holds.
func adoptionReportFor(config projectconfig.Config) AdoptionReport {
	report := AdoptionReport{Env: config.EnvName}
	report.Constructs = append(report.Constructs, carriedConstructs(config)...)
	report.Constructs = append(report.Constructs, unsupportedConstructs(config)...)
	// Sorted by name so two runs over one file produce the same report and a
	// diff between them means the file changed.
	slices.SortFunc(report.Constructs, func(a, b Construct) int {
		if a.Class != b.Class {
			return strings.Compare(a.Class, b.Class)
		}
		return strings.Compare(a.Name, b.Name)
	})
	report.NativeReady = needingAdoption(report) == 0
	return report
}

// carriedConstructs classifies the settings Ptah acts on.
//
// A setting the file does not set is not a construct: adoption is about what
// the project declares, and an absent target database is not something to
// carry, rewrite or refuse.
func carriedConstructs(config projectconfig.Config) []Construct {
	settings := []struct {
		name  string
		value string
	}{
		{name: "database url", value: config.DatabaseURL},
		{name: "dev url", value: config.DevURL},
		{name: "schema sources", value: strings.Join(config.SchemaSources, ", ")},
		{name: "schemas", value: strings.Join(config.Schemas, ", ")},
		{name: "exclude", value: strings.Join(config.Exclude, ", ")},
		{name: "exporter", value: config.ExporterName},
	}
	constructs := make([]Construct, 0, len(settings)+1)
	for _, setting := range settings {
		if setting.value == "" {
			continue
		}
		constructs = append(constructs, Construct{Name: setting.name, Class: classExact})
	}
	if migration, ok := migrationDirConstruct(config); ok {
		constructs = append(constructs, migration)
	}
	return constructs
}

// migrationDirConstruct classifies `migration.dir`, which is the one carried
// setting that can be compat-only.
//
// An `atlas://` reference names a directory in a registry, and native Ptah
// addresses the same artifact through `oci://`. The reference the PROJECT wrote
// is read back from the registered source rather than from Migration.Dir, which
// by this point holds the in-memory URL the rest of the tree opens.
func migrationDirConstruct(config projectconfig.Config) (Construct, bool) {
	if strings.TrimSpace(config.Migration.Dir) == "" {
		return Construct{}, false
	}
	source, registered := config.MigrationDirectorySource(config.Migration.Dir)
	if !registered || !atlasregistry.IsReference(source.Path) {
		return Construct{Name: "migration dir", Class: classExact}, true
	}
	return Construct{
		Name:   "migration dir",
		Class:  classCompatOnly,
		Detail: nativeReferenceFor(source.Path),
	}, true
}

// nativeReferenceFor says what an `atlas://` reference becomes natively, or why
// it cannot be said yet.
//
// #1215 asks that such a reference be normalizable "where mapping is
// unambiguous". The mapping is the configured OCI namespace, so with none
// configured the analysis reports the gap instead of inventing a repository.
func nativeReferenceFor(reference string) string {
	resolved, err := atlasregistry.Resolve(reference)
	if err != nil {
		return fmt.Sprintf("%s: no unambiguous native reference (%v)", reference, err)
	}
	return fmt.Sprintf("%s becomes %s", reference, resolved.OCI)
}

// unsupportedConstructs are the names the file declares that nothing acts on.
func unsupportedConstructs(config projectconfig.Config) []Construct {
	constructs := make([]Construct, 0, len(config.IgnoredConstructs))
	for _, ignored := range config.IgnoredConstructs {
		constructs = append(constructs, Construct{
			Name:   fmt.Sprintf("%s %q", ignored.Kind, ignored.Name),
			Class:  classUnsupported,
			Detail: fmt.Sprintf("%s:%d", ignored.Filename, ignored.Line),
		})
	}
	return constructs
}

// needingAdoption counts what stands between this project and native Ptah.
func needingAdoption(report AdoptionReport) int {
	needing := 0
	for _, construct := range report.Constructs {
		if construct.Class != classExact {
			needing++
		}
	}
	return needing
}

func writeAdoptionText(out io.Writer, report AdoptionReport) {
	if report.Env != "" {
		fmt.Fprintf(out, "Environment: %s\n\n", report.Env)
	}
	if len(report.Constructs) == 0 {
		fmt.Fprintln(out, "The project file declares nothing this analysis can classify.")
		return
	}
	for _, class := range []string{classUnsupported, classCompatOnly, classExact} {
		writeAdoptionClass(out, report, class)
	}
	if report.NativeReady {
		fmt.Fprintln(out, "\nNative-ready: every construct means the same in a native Ptah project,")
		fmt.Fprintln(out, "so this file can be operated by ptah as it stands.")
		return
	}
	fmt.Fprintf(out, "\nNot native-ready: %d of %d constructs need adoption.\n",
		needingAdoption(report), len(report.Constructs))
}

// writeAdoptionClass prints one class, and prints nothing for an empty one.
func writeAdoptionClass(out io.Writer, report AdoptionReport, class string) {
	members := make([]Construct, 0, len(report.Constructs))
	for _, construct := range report.Constructs {
		if construct.Class == class {
			members = append(members, construct)
		}
	}
	if len(members) == 0 {
		return
	}
	fmt.Fprintf(out, "%s (%d):\n", class, len(members))
	for _, construct := range members {
		if construct.Detail == "" {
			fmt.Fprintf(out, "  %s\n", construct.Name)
			continue
		}
		fmt.Fprintf(out, "  %-24s %s\n", construct.Name, construct.Detail)
	}
	fmt.Fprintln(out)
}
