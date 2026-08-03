package atlas

import (
	"errors"
	"fmt"

	"github.com/spf13/cobra"

	"go.5x5.cz/ptah/cmd/internal/cmdadapter"
	"go.5x5.cz/ptah/cmd/internal/cmdutil"
	"go.5x5.cz/ptah/cmd/migrate"
	"go.5x5.cz/ptah/internal/atlasargs"
	"go.5x5.cz/ptah/internal/atlasmigrate"
)

// newAtlasMigrateNewCommand returns `atlas migrate new`.
//
// The verb is still a plain forward into `ptah migrations create`; what this
// wrapper adds is the atlas.sum integrity gate in front of it
// (stokaro/ptah#1086). It cannot live in the forwarded command: the compat
// surface is where the Atlas-form --dir, its URL query, the atlas.hcl env and
// both environment spellings of the directory are resolved into the one
// directory the native command will write into, and the refusal has to happen
// before that command runs at all.
//
// It is deliberately the same resolution the `migrate hash` / `migrate
// validate` wrapper uses (resolveAtlasMigrateSource), not a second one:
// "which directory does this verb touch" is one question, and answering it
// twice is how a gate ends up verifying a directory the writer never opens.
func newAtlasMigrateNewCommand() *cobra.Command {
	verb := atlasMigrateNewVerb()
	cmd := newAtlasAdapterCommand("migrate", verb)
	forward := cmd.RunE
	cmd.RunE = func(cmd *cobra.Command, args []string) (runErr error) {
		if atlasArgsHaveHelp(args) {
			return forward(cmd, args)
		}
		cleanup := &cmdadapter.CleanupScope{}
		defer func() {
			runErr = errors.Join(runErr, cleanup.Close())
		}()
		source, err := resolveAtlasMigrateSource(cmd, verb, args, cleanup)
		if err != nil {
			return err
		}
		if err := checkAtlasMigrateWriteFormat(cmd, verb, source); err != nil {
			return err
		}
		if source.dir == "" {
			// No layer named a directory. `ptah migrations create` registers no
			// default for it and refuses with its own diagnostic, which is the
			// one this surface has always printed; inventing a directory to
			// verify here would change that message and verify nothing.
			return forward(cmd, source.forwardArgs)
		}
		if err := verifyAtlasWriteDirChecksum(cmd, source.project, source.localDir); err != nil {
			return err
		}
		return forward(cmd, source.forwardArgs)
	}
	return cmd
}

func atlasMigrateNewVerb() atlasVerb {
	return atlasVerb{
		use:        "new",
		displayUse: "new [flags] [name]",
		short:      "Create a new migration file",
		native:     "migrations create",
		factory:    migrate.NewMigrateCreateCommand,
		flags: []atlasargs.Flag{
			atlasargs.NativeLocalDir("dir", "", "Migration directory", "migrations-dir"),
			atlasMigrateDirFormatFlag("dir-format"),
			atlasargs.NativeBool("edit", "", "Edit the created migration files", "edit"),
		},
	}
}

// checkAtlasMigrateWriteFormat refuses a source layout a writing verb cannot
// write, naming the spelling that selected it.
//
// The community binary honors both spellings here — measured, `migrate new
// --dir 'file://goosedir?format=goose'` refuses an unhashed goose directory
// over goose's own covered file set and writes into a hashed one — so this is a
// divergence in the strict direction and nothing more. Writing a foreign layout
// is tracked by stokaro/ptah#1013 section 1 and stokaro/ptah#1002; refusing is
// what keeps this verb from writing into a directory whose covered file set
// this surface does not compute.
//
// The blame follows the spelling because the two arrive by different routes:
// the --dir-format value reaches the forwarding mapper, which has its own
// refusal for it, while a ?format= is resolved only here.
func checkAtlasMigrateWriteFormat(cmd *cobra.Command, verb atlasVerb, source atlasMigrateSource) error {
	if atlasmigrate.ReadsNativeAtlasDir(source.format) {
		return nil
	}
	spelling, detail := "--dir-format", fmt.Sprintf(
		"Atlas accepts --dir-format=%s, but Ptah does not implement that directory format yet",
		source.format,
	)
	if atlasmigrate.DirFormatFromQuery(source.localDir.Query) {
		spelling, detail = "--dir", fmt.Sprintf(
			"Atlas accepts ?format=%s, but Ptah does not implement that directory format for this command yet",
			source.format,
		)
	}
	return cmdutil.Fail(cmd, fmt.Errorf("atlas migrate %s %s: %s", verb.use, spelling, detail))
}
