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
	"go.5x5.cz/ptah/internal/adoptpreflight"
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
	preflight bool
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
	// Current and Native are the compat-only spelling and what it becomes.
	//
	// They are separate from Detail because the rewriting half needs the two
	// values, and reading them back out of a sentence written for a human is
	// how a normalizer comes to write whatever the prose happened to say. Both
	// are empty unless Class is compat-only AND the native spelling could be
	// named.
	Current string `json:"current,omitempty"`
	Native  string `json:"native,omitempty"`
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
	// Database is the persisted revision state, present only when --preflight
	// asked for it.
	//
	// Absent means NOT ASKED, and never "nothing wrong": the text report says
	// so in words for the same reason, because a clean project-file verdict
	// with no database section reads as a finished adoption, and the database
	// is where re-running applied SQL actually happens (stokaro/ptah#1215).
	Database *adoptpreflight.Report `json:"database,omitempty"`
}

func newProjectAdoptCommand() *cobra.Command {
	opts := adoptOptions{}
	cmd := &cobra.Command{
		Use:   "adopt",
		Short: "Adopt this project into native Ptah, or report what that would take",
		Long: `Report what it would take to operate this project with native Ptah.

Every construct the project file declares is put in one of three classes:

  exact         Ptah acts on it and it means the same in a native Ptah project.
  compat-only   Ptah acts on it, but the spelling is Atlas's and a native
                equivalent exists. Adoption rewrites the spelling.
  unsupported   Ptah read the name and acts on nothing. Adoption cannot carry
                it, and it is why a project is not native-ready.

A project with nothing in the last two classes is native-ready: it can be
operated by native Ptah as it stands, without its file being rewritten.

--check reports the analysis and writes nothing. Without it, the compat-only
spellings are rewritten in place; a project declaring anything unsupported is
refused rather than half-converted. Neither form contacts a database.

--preflight adds the database half: it reads the revision history the project's
own database holds and reports whether native Ptah may take it over. It writes
nothing there either -- not the revision table it looks for, and not the layout
of one it finds. A history it cannot adopt from is named and explained rather
than converted.`,
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
		"Report the analysis without changing anything")
	flags.BoolVar(&opts.preflight, "preflight", false,
		"Also inspect the revision history in the project's database, without writing to it")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}

func runAdopt(cmd *cobra.Command, opts adoptOptions) error {
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
	if opts.preflight {
		database, err := runDatabasePreflight(cmd.Context(), config)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		report.Database = database
	}
	if !opts.check {
		return runAdoptWrite(cmd, opts, report)
	}
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
	if report.Database != nil && report.Database.Refused {
		// A refusal is not a step the operator can take on the way to
		// adoption; it is a state adoption cannot start from, and calling it
		// an action would tell them to do something the report never named.
		return cmdutil.Fail(cmd, fmt.Errorf(
			"this project's database cannot be adopted in the state it is in: "+
				"the refusal above says what has to be true first"))
	}
	if report.Database != nil && !report.Database.Ready {
		// The file is adoptable and the database is not, which is the case
		// #1215 exists for: exiting 0 here would report a finished adoption
		// over a history native Ptah would re-run or misread.
		return cmdutil.Fail(cmd, fmt.Errorf(
			"this project's file is native-ready and its database is not: "+
				"%d revision-state action(s) remain before the writer can be switched",
			report.Database.Actions()))
	}
	return nil
}

// runAdoptWrite normalizes the project file, or says why it will not.
//
// Three answers, and the refusal is the one that matters. An unsupported
// construct is a name Ptah read and acts on nothing, so a rewrite would produce
// a file that LOOKS adopted while the behaviour that name asked for is still
// missing -- which is exactly how such a construct disappears into a conversion
// nobody wrote. Adoption refuses and names it instead (stokaro/ptah#1215).
func runAdoptWrite(cmd *cobra.Command, opts adoptOptions, report AdoptionReport) error {
	if unsupported := constructsInClass(report, classUnsupported); len(unsupported) > 0 {
		writeAdoptionText(cmd.OutOrStdout(), report)
		return cmdutil.Fail(cmd, fmt.Errorf(
			"this project declares %d construct(s) Ptah acts on nothing for, so adoption cannot carry it: "+
				"remove them from the file, or keep operating it through ptah-compat",
			len(unsupported)))
	}
	if report.NativeReady {
		fmt.Fprintln(cmd.OutOrStdout(),
			"Nothing to rewrite: every construct already means the same thing in a native Ptah project.")
		writeDatabaseAdoption(cmd.OutOrStdout(), report.Database)
		return nil
	}

	path := opts.atlasPath
	if path == "" {
		path = projectconfig.AtlasFileName
	}
	changed, err := normalizeProject(path, report)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	if len(changed) == 0 {
		// Compat-only, and no native spelling could be named -- the namespace
		// is unset, so the analysis reported the gap rather than inventing a
		// repository. Writing nothing and saying so beats writing a guess.
		writeAdoptionText(cmd.OutOrStdout(), report)
		return cmdutil.Fail(cmd, fmt.Errorf(
			"no compat-only construct has an unambiguous native spelling, so nothing was rewritten"))
	}
	writeNormalizationText(cmd.OutOrStdout(), path, changed)
	// The rewrite changed the file. Whether native Ptah may take over the
	// database is a separate answer, and printing it here is what keeps a
	// successful rewrite from reading as the whole adoption.
	writeDatabaseAdoption(cmd.OutOrStdout(), report.Database)
	return nil
}

// constructsInClass is the members of one class, in report order.
func constructsInClass(report AdoptionReport, class string) []Construct {
	members := make([]Construct, 0, len(report.Constructs))
	for _, construct := range report.Constructs {
		if construct.Class == class {
			members = append(members, construct)
		}
	}
	return members
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
	construct := Construct{
		Name:   "migration dir",
		Class:  classCompatOnly,
		Detail: nativeReferenceFor(source.Path),
	}
	if resolved, err := atlasregistry.Resolve(source.Path); err == nil {
		construct.Current = source.Path
		construct.Native = resolved.OCI
	}
	return construct, true
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
	writeDatabaseAdoption(out, report.Database)
	if report.NativeReady {
		fmt.Fprintln(out, "\nNative-ready: every construct means the same in a native Ptah project,")
		fmt.Fprintln(out, "so this file can be operated by ptah as it stands.")
	} else {
		fmt.Fprintf(out, "\nNot native-ready: %d of %d constructs need adoption.\n",
			needingAdoption(report), len(report.Constructs))
	}
	// A separate line rather than a term in the sentence above, because the two
	// verdicts are about different things: that one is about the file, and a
	// file can be adoptable while the database in front of it is not.
	if report.Database != nil && report.Database.Refused {
		fmt.Fprintln(out, "This database cannot be adopted in the state it is in.")
	} else if report.Database != nil && !report.Database.Ready {
		fmt.Fprintf(out, "%d database-state action(s) remain before the writer can be switched.\n",
			report.Database.Actions())
	}
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
