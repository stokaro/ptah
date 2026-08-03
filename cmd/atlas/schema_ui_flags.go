package atlas

import (
	"github.com/spf13/cobra"

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
	return atlasargs.UnsupportedBoolReason(
		atlasSchemaExportFlagName, "", "Use the exporter defined in the atlas.hcl env",
		"an exporter is declared by an atlas.hcl `exporter` block, which Ptah does not evaluate; there is no exporter to select, and emitting the default output instead would report an export that never happened",
	)
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
