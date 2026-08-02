package atlas

import (
	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/internal/atlasschema"
)

// newAtlasSchemaPlanNewCommand implements `atlas schema plan new`.
//
// # Evidence
//
// Flag set, per the published Atlas CLI reference
// (https://atlasgo.io/cli-reference, entry "atlas schema plan new"): the
// transition set plus --edit, --name, --name-format and -o/--output, and
// notably WITHOUT --save, --dry-run, --push, --pending, --skip-lint and
// --directive. Usage line `atlas schema plan new [flags]`, so no positional
// argument.
//
// Description: DOCUMENTED — "Create a new plan file for the schema transition"
// (https://atlasgo.io/cli-reference).
//
// Behavior: INFERRED. Only the flag surface above is established. CE aborts
// the whole `schema plan` path (measured on the pinned CE v1.2.0 binary,
// 2026-08-02: `atlas schema plan new` is byte-identical to
// `atlas schema plan frobnicate-nonsense`), so it settles nothing here either.
// The chain "the verb creates a plan FILE, and it has neither --save nor
// --dry-run, therefore it always writes one" is the only reading that leaves
// the verb with a function, but it is a reading, not a measurement. The stderr
// note says so on every run.
//
// # Implementation
//
// `new` is `schema plan` restricted to the flag set Atlas registers for it,
// with saving forced on. Delegating to runAtlasSchemaPlan rather than
// re-implementing it is deliberate: the plan document, the naming rules, the
// editor round trip, the collision refusal and the format selection then
// cannot differ between the two spellings, because there is only one of each.
func newAtlasSchemaPlanNewCommand() *cobra.Command {
	opts := atlasSchemaPlanOptions{verb: atlasSchemaPlanNewVerb}
	cmd := &cobra.Command{
		Use:   "new",
		Short: "Create a new plan file for the schema transition",
		Long: `Atlas ` + "`atlas schema plan new`" + ` command path.

Computes the declarative migration plan from the --from target database to the
local --to schema files and writes it to a plan file. It is ` + "`schema plan`" + `
restricted to the flags Atlas registers on this sub-verb, with saving always
on: Atlas gives ` + "`new`" + ` neither --save nor --dry-run, and its documented purpose
is to create the plan file.

The plan file is written to --output when given, and to <name>` + atlasschema.PlanFileSuffixHCL + `
in the working directory otherwise; a default-named plan file is never
overwritten. An --output path ending in .json writes Ptah's native
fingerprinted JSON plan instead of the Atlas ` + "`.plan.hcl`" + ` shape.

--edit, --name and --name-format behave exactly as they do on ` + "`schema plan`" + `.
Registry-bound planning (--repo), --format and --lock-timeout are declared for
CLI-surface parity and refused.`,
		RunE: func(cmd *cobra.Command, _ []string) error {
			writeAtlasSchemaPlanDocsDerivedNote(cmd,
				"the behavior of `"+atlasSchemaPlanNewVerb+
					"` (writing a plan file with no --save flag to request it)")
			// Atlas registers neither --save nor --dry-run here, so there is no
			// operator input that could select the print-to-stdout path; the
			// verb's whole documented function is to produce the file.
			opts.save = true
			return runAtlasSchemaPlan(cmd, opts)
		},
	}
	flags := cmd.Flags()
	registerAtlasSchemaPlanTransitionFlags(flags, &opts.atlasSchemaPlanTransitionFlags)
	flags.StringVar(&opts.name, "name", "", "Plan name recorded in the plan file")
	flags.StringVar(&opts.nameFormat, "name-format", "",
		"Go template used to compute the plan name (standard Base64 may render '/', which requires --output)")
	flags.StringVarP(&opts.output, "output", "o", "",
		"Plan file output path (default <name>"+atlasschema.PlanFileSuffixHCL+"; a .json path writes the native JSON plan format)")
	flags.BoolVar(&opts.edit, "edit", false, "Edit the plan in the terminal editor")
	// Both spell the same field, and Atlas's precedence between them is
	// unmeasured; refusing the combination beats silently picking a winner.
	cmd.MarkFlagsMutuallyExclusive("name", "name-format")
	cmdutil.ConfigureCommandArgs(cmd, cmdutil.NoPositionalArgs)
	return cmd
}
