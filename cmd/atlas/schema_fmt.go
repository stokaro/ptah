package atlas

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"

	"github.com/hashicorp/hcl/v2"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/cmdutil"
)

func newAtlasSchemaFmtCommand() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "fmt [path ...]",
		Short: "Format schema files",
		Long: `Atlas OSS ` + "`atlas schema fmt`" + ` command path.

Formats .hcl files using HashiCorp HCL's canonical layout. Directory arguments
are walked recursively. When no path is provided, the current directory is used.
Only files whose content changes are printed.`,
		RunE: runAtlasSchemaFmt,
	}
	cmdutil.ConfigureCommandArgs(cmd, cobra.ArbitraryArgs)
	return cmd
}

func runAtlasSchemaFmt(cmd *cobra.Command, args []string) error {
	paths := args
	if len(paths) == 0 {
		paths = []string{"."}
	}

	for _, path := range paths {
		changed, err := formatAtlasSchemaPath(path)
		if err != nil {
			return err
		}
		for _, file := range changed {
			fmt.Fprintln(cmd.OutOrStdout(), file)
		}
	}
	return nil
}

func formatAtlasSchemaPath(path string) ([]string, error) {
	info, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if !info.IsDir() {
		changed, err := formatAtlasSchemaFile(path)
		if err != nil || !changed {
			return nil, err
		}
		return []string{path}, nil
	}

	var changed []string
	err = filepath.WalkDir(path, func(file string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || filepath.Ext(file) != ".hcl" {
			return nil
		}
		fileChanged, err := formatAtlasSchemaFile(file)
		if err != nil {
			return err
		}
		if fileChanged {
			changed = append(changed, file)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	return changed, nil
}

func formatAtlasSchemaFile(path string) (bool, error) {
	if filepath.Ext(path) != ".hcl" {
		return false, nil
	}

	info, err := os.Stat(path)
	if err != nil {
		return false, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if info.IsDir() {
		return false, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return false, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	if _, diags := hclwrite.ParseConfig(data, path, hcl.Pos{Line: 1, Column: 1}); diags.HasErrors() {
		return false, fmt.Errorf("schema fmt %s: %s", path, diags.Error())
	}

	formatted := hclwrite.Format(data)
	if bytes.Equal(data, formatted) {
		return false, nil
	}
	//nolint:gosec // schema fmt intentionally rewrites the user-selected local HCL file after stat/read validation.
	if err := os.WriteFile(path, formatted, info.Mode().Perm()); err != nil {
		return false, fmt.Errorf("schema fmt %s: %w", path, err)
	}
	return true, nil
}
