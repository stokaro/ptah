package atlas

import (
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/config/projectconfig"
	"go.5x5.cz/ptah/internal/atlasargs"
)

// The UI-bound schema flags, and why they are registered refusals rather than
// implementations or absences.
//
// Both have an Atlas-side source: Atlas's published CLI reference
// (atlasgo.io/cli-reference) lists `-w, --web  open the schema ERD in the
// browser` on `schema inspect` and `--export  use exporter defined in env` on
// `schema diff`. Neither is registered by the pinned community binary, measured
// by running the spelling: both answer `unknown flag` there, with `--format`
// present and `--frobnicate-nonsense` missing as controls. So the reference is
// the source, and the spelling below matches it.
//
// Neither is implemented, and the reasons differ:
//
//   - --web names a browser action. Its payload is not the problem: Ptah renders
//     the same ERD locally, as `schema inspect --format '{{ mermaid . }}'` and as
//     `ptah viz`. What Ptah has no counterpart for is opening a viewer, and a
//     CLI that launches a browser is the wrong shape for the pipelines this
//     surface exists to serve. The refusal names the local spelling that
//     produces the diagram, so the capability stays reachable.
//   - --export selects an exporter declared by an atlas.hcl `exporter` block.
//     Ptah's project-config evaluator tolerates that block and evaluates nothing
//     from it, so there is no exporter for the flag to select. Accepting the
//     flag would silently emit the default output and call it an export.
//
// Accepting either silently is the failure this issue exists to prevent, and
// leaving both unregistered would leave a script no way to learn why its
// spelling did nothing. A registered refusal answers the question in one line
// and cannot be mistaken for success: it exits non-zero before any database is
// contacted.
//
// The same verdict and the same source cover the twins this batch did not touch
// — `schema inspect --export`, `schema diff --web`, `migrate lint --web` — left
// out to stay inside the batch rather than because they differ.
const (
	atlasSchemaWebFlagName    = "web"
	atlasSchemaExportFlagName = "export"
)

func atlasSchemaWebFlag() atlasargs.Flag {
	return atlasargs.UnsupportedBoolReason(
		atlasSchemaWebFlagName, "w", "Open the schema ERD in the browser",
		"opening a viewer is a UI action with no local counterpart; the ERD itself is local — render it with --format '{{ mermaid . }}' or `ptah viz`",
	)
}

func atlasSchemaExportFlag() atlasargs.Flag {
	return atlasargs.NativeBool(
		atlasSchemaExportFlagName, "", "Use the exporter defined in the atlas.hcl env",
		atlasSchemaExportFlagName,
	)
}

// atlasExportProject is the project config an export resolves against, with
// whether one was found at all.
//
// The two travel together because "no config" and "a config selecting nothing"
// need different sentences, and a bare bool parameter beside the config reads
// as a mode switch rather than as part of the same answer.
type atlasExportProject struct {
	config projectconfig.Config
	loaded bool
}

// resolveAtlasExporter turns `--export` into the template it selects.
//
// # What an exporter is
//
// A Go text/template over the same report `--format` renders, declared once by
// a top-level `exporter` block and chosen by an env's `exporter` attribute:
//
//	exporter "markdown" {
//	  template = "# Changes\n{{ range .Changes }}- {{ .Cmd }}\n{{ end }}"
//	}
//
//	env "local" {
//	  url      = "sqlite://app.db"
//	  exporter = "markdown"
//	}
//
// So `--export` is `--format` with the template kept in the project instead of
// in every invocation, which is why it needs no evaluator of its own. The
// alternative was a declarative description of output structure: a second
// language to learn, document and version, doing what the template surface
// already does (stokaro/ptah#1620).
//
// # Why each refusal exists
//
// Every failure here is a case where emitting the ordinary report would let an
// operator believe their exporter ran. That is the failure the flag was a
// registered refusal to avoid, and implementing it must not reintroduce it.
func resolveAtlasExporter(cmd *cobra.Command, project atlasExportProject) (string, error) {
	if !cmd.Flags().Changed(atlasSchemaExportFlagName) {
		return "", nil
	}
	if !project.loaded {
		return "", fmt.Errorf(
			"--%s needs a project config: an exporter is declared by an atlas.hcl `exporter` block",
			atlasSchemaExportFlagName)
	}
	if cmd.Flags().Changed("format") {
		return "", fmt.Errorf(
			"--%s and --format both choose the output; pass one", atlasSchemaExportFlagName)
	}
	if project.config.ExporterName == "" {
		return "", fmt.Errorf(
			"--%s: this env selects no exporter; name one with an `exporter` attribute",
			atlasSchemaExportFlagName)
	}
	exporter, err := project.config.Exporter(project.config.ExporterName)
	if err != nil {
		return "", fmt.Errorf("--%s: %w", atlasSchemaExportFlagName, err)
	}
	return exporter.Template, nil
}

// refuseAtlasUIFlag rejects a registered UI-bound flag that was actually passed.
// Registration alone is help parity; the refusal is what keeps the flag from
// being accepted and ignored.
func refuseAtlasUIFlag(cmd *cobra.Command, group, use string, flag atlasargs.Flag) error {
	if !cmd.Flags().Changed(flag.Name) {
		return nil
	}
	return atlasargs.UnsupportedFlagError(group, use, flag, "--"+flag.Name)
}

func registerAtlasUIFlag(cmd *cobra.Command, flag atlasargs.Flag) {
	cmd.Flags().BoolP(flag.Name, flag.Shorthand, false, flag.Usage)
}
