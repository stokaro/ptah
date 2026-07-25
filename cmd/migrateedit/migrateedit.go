// Package migrateedit implements the migrations edit command: it edits a
// migration's up/down SQL (interactively via $EDITOR, or non-interactively from
// --up-file / --down-file) and rewrites the integrity file, refusing to edit an
// already-applied migration unless forced (#662).
package migrateedit

import (
	"fmt"
	"os"
	"os/exec"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
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
func openEditor(editor string, paths ...string) error {
	if editor == "" {
		editor = firstNonEmpty(os.Getenv("VISUAL"), os.Getenv("EDITOR"))
	}
	if strings.TrimSpace(editor) == "" {
		return fmt.Errorf("no editor configured: set $EDITOR or $VISUAL, or pass --editor, --up-file, or --down-file")
	}
	fields := strings.Fields(editor)
	args := append(append([]string{}, fields[1:]...), nonEmpty(paths)...)
	c := exec.Command(fields[0], args...) //nolint:gosec // the editor is operator-provided, like git's core.editor
	c.Stdin, c.Stdout, c.Stderr = os.Stdin, os.Stdout, os.Stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("editor %q failed: %w", editor, err)
	}
	return nil
}

func nonEmpty(values []string) []string {
	out := make([]string, 0, len(values))
	for _, v := range values {
		if v != "" {
			out = append(out, v)
		}
	}
	return out
}

func firstNonEmpty(values ...string) string {
	for _, v := range values {
		if strings.TrimSpace(v) != "" {
			return v
		}
	}
	return ""
}
