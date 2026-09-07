package atlas

import (
	"fmt"

	"github.com/spf13/cobra"

	"ptah.run/config/projectconfig"
	"ptah.run/internal/atlasargs"
)

// The UI-bound schema flags: where their spelling comes from, and what each
// one does here.
//
// Both have an Atlas-side source: Atlas's published CLI reference
// (atlasgo.io/cli-reference) lists `-w, --web  open the schema ERD in the
// browser` on `schema inspect` and `--export  use exporter defined in env` on
// `schema diff`. Neither is registered by the pinned community binary, measured
// by running the spelling: both answer `unknown flag` there, with `--format`
// present and `--frobnicate-nonsense` missing as controls. So the reference is
// the source, and the spelling below matches it.
//
// Both are implemented, and each was a registered refusal first. The refusals
// are worth remembering because both were wrong in the same way -- each named a
// real difficulty and then declined more than the difficulty required:
//
//   - --export was refused on the grounds that Ptah's project-config evaluator
//     tolerated an `exporter` block and evaluated nothing from it, so there was
//     nothing for the flag to select. stokaro/ptah#1620 closed that by reading
//     the block: an exporter is a Go text/template over the same report
//     --format renders, so it needed no evaluator of its own.
//   - --web was refused on the grounds that opening a viewer has no local
//     counterpart. stokaro/ptah#3011 closed that: the ERD is local and Ptah
//     already draws it, so what the refusal declined was handing the operator
//     the file, not opening one.
//
// What remains refused is narrower and belongs to --export alone: an invocation
// that selects no exporter. No project config, an env naming none, a name the
// project does not declare, or --format passed beside it -- each is a case where
// emitting the ordinary report would let an operator believe their exporter ran.
// That is the failure the whole flag was once a refusal to avoid, and
// implementing it did not license reintroducing it. See resolveAtlasExporter.
//
// `migrate lint --web` is the one member of the documented group that stays
// unregistered; stokaro/ptah#3011 left it out of scope deliberately rather than
// by accident, so the group is not split without a word.
const (
	atlasSchemaWebFlagName    = "web"
	atlasSchemaExportFlagName = "export"
)

func atlasSchemaWebFlag() atlasargs.Flag {
	// The usage line says local, because the vendor description does not and a
	// reader is entitled to assume the flag publishes the schema somewhere.
	//
	// It does not name the suppressing variable. The `[env: PTAH_...]` marker
	// on a flag line means that flag's own binding, and this flag's is
	// PTAH_WEB; naming a different variable there would make the marker say
	// something it does not mean everywhere else.
	return atlasargs.Bool(
		atlasSchemaWebFlagName, "w",
		"Write the schema ERD to a local HTML file and open it",
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
func resolveAtlasExporter(cmd *cobra.Command, project atlasExportProject) (string, bool, error) {
	// The VALUE, not just Changed. Cobra marks a boolean flag changed for
	// `--export=false` too, so testing Changed alone would apply the exporter
	// on an invocation that explicitly asked for ordinary output -- and error
	// on a project that declares none. Generated command lines pass explicit
	// booleans, so this is a spelling real callers use.
	export, err := cmd.Flags().GetBool(atlasSchemaExportFlagName)
	if err != nil || !export {
		return "", false, nil
	}
	if !project.loaded {
		return "", false, fmt.Errorf(
			"--%s needs a project config: an exporter is declared by an atlas.hcl `exporter` block",
			atlasSchemaExportFlagName)
	}
	if cmd.Flags().Changed("format") {
		return "", false, fmt.Errorf(
			"--%s and --format both choose the output; pass one", atlasSchemaExportFlagName)
	}
	if project.config.ExporterName == "" {
		return "", false, fmt.Errorf(
			"--%s: this env selects no exporter; name one with an `exporter` attribute",
			atlasSchemaExportFlagName)
	}
	exporter, err := project.config.Exporter(project.config.ExporterName)
	if err != nil {
		return "", false, fmt.Errorf("--%s: %w", atlasSchemaExportFlagName, err)
	}
	// The selection is reported separately from the template. A caller reading
	// "selected" off a non-empty string cannot tell an exporter that renders
	// nothing from one that was never chosen, and would quietly print the
	// default report for the first.
	return exporter.Template, true, nil
}

func registerAtlasUIFlag(cmd *cobra.Command, flag atlasargs.Flag) {
	cmd.Flags().BoolP(flag.Name, flag.Shorthand, false, flag.Usage)
}
