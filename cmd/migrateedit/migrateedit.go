// Package migrateedit implements the migrations edit command: it edits a
// migration's up/down SQL (interactively via $EDITOR, or non-interactively from
// --up-file / --down-file) and rewrites the integrity file, refusing to edit an
// already-applied migration unless forced (#662).
package migrateedit

import (
	"errors"
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
	"github.com/stokaro/ptah/cmd/internal/editor"
	"github.com/stokaro/ptah/cmd/internal/migratemaint"
	"github.com/stokaro/ptah/internal/migrateops"
)

type editInputs struct {
	upFile   string
	downFile string
	editor   string
}

// NewMigrateEditCommand returns the migrations edit command.
func NewMigrateEditCommand() *cobra.Command {
	opts := &migratemaint.Options{}
	inputs := &editInputs{}
	cmd := &cobra.Command{
		Use:   "edit",
		Short: "Edit a migration's SQL and rewrite the integrity file",
		Long: `migrations edit opens a migration's up/down pair for editing and then rewrites
ptah.sum / atlas.sum so the directory still validates. By default it opens the
files in $EDITOR (or $VISUAL, or --editor); for non-interactive use, pass
--up-file and/or --down-file to replace a direction with the contents of that
file. It refuses to edit a migration that is already applied in the database
given by --db-url unless --force is passed; without --db-url it warns that
applied state was not verified.`,
		Args:          cmdutil.NoPositionalArgs,
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runEdit(cmd, opts, inputs)
		},
	}
	migratemaint.RegisterFlags(cmd.Flags(), opts)
	cmd.Flags().StringVar(&inputs.upFile, "up-file", "", "Replace the up migration with the contents of this file (non-interactive)")
	cmd.Flags().StringVar(&inputs.downFile, "down-file", "", "Replace the down migration with the contents of this file (non-interactive)")
	cmd.Flags().StringVar(&inputs.editor, "editor", "", "Editor command to open the pair (defaults to $VISUAL, then $EDITOR)")
	cmd.SetFlagErrorFunc(cmdutil.FlagErrorFunc)
	return cmd
}

func runEdit(cmd *cobra.Command, opts *migratemaint.Options, inputs *editInputs) error {
	r, err := opts.Resolve()
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	dir, version, format := r.Dir, r.Version, r.Format
	if err := opts.Guard(cmd.Context(), cmd.OutOrStdout(), version, format); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	upPath, downPath, err := migrateops.LocatePair(dir, version, format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}

	if inputs.upFile != "" || inputs.downFile != "" {
		if err := replaceFrom(upPath, inputs.upFile, "up"); err != nil {
			return cmdutil.Fail(cmd, err)
		}
		if err := replaceFrom(downPath, inputs.downFile, "down"); err != nil {
			return cmdutil.Fail(cmd, err)
		}
	} else if err := openEditor(inputs.editor, upPath, downPath); err != nil {
		return cmdutil.Fail(cmd, err)
	}

	res, err := migrateops.Rehash(dir, format)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	out := cmd.OutOrStdout()
	fmt.Fprintf(out, "Edited migration %d\n", version)
	fmt.Fprintf(out, "Wrote %s/%s\n", dir, res.SumFile)
	return nil
}

// replaceFrom overwrites target with the contents of src. A nil src (empty
// string) leaves the direction untouched; a request to replace a direction that
// has no file is an error.
func replaceFrom(target, src, direction string) error {
	if src == "" {
		return nil
	}
	if target == "" {
		return fmt.Errorf("cannot replace the %s migration: it does not exist", direction)
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("read %s: %w", src, err)
	}
	if err := os.WriteFile(target, data, 0o600); err != nil { //nolint:gosec // migration files are shared, 0600 is fine here
		return fmt.Errorf("write %s: %w", target, err)
	}
	return nil
}

// openEditor launches the resolved editor on the pair's existing files, wired to
// the current terminal for interactive editing.
func openEditor(editorCmd string, paths ...string) error {
	err := editor.Open(editorCmd, paths...)
	if errors.Is(err, editor.ErrNoEditor) {
		return fmt.Errorf("%w, or pass --editor, --up-file, or --down-file", err)
	}
	return err
}
