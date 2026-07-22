package atlas

import (
	"fmt"
	"os"
	"slices"
	"strings"

	"github.com/spf13/cobra"

	"github.com/stokaro/ptah/cmd/internal/dbcli"
	"github.com/stokaro/ptah/config/projectconfig"
	"github.com/stokaro/ptah/internal/atlasschema"
)

func loadAtlasProjectConfig(envName string) (projectconfig.Config, error) {
	return projectconfig.LoadAtlasFile(projectconfig.AtlasFileName, envName)
}

func loadOptionalAtlasProjectConfigForCommand(
	cmd *cobra.Command,
	envName string,
) (projectconfig.Config, bool, error) {
	return loadAtlasProjectConfigForCommand(cmd, envName, ignoreMissingEnvSelection)
}

func loadRequiredAtlasProjectConfigForCommand(
	cmd *cobra.Command,
	envName string,
) (projectconfig.Config, bool, error) {
	return loadAtlasProjectConfigForCommand(cmd, envName, reportMissingEnvSelection)
}

type missingAtlasEnvSelectionMode int

const (
	ignoreMissingEnvSelection missingAtlasEnvSelectionMode = iota
	reportMissingEnvSelection
)

func loadAtlasProjectConfigForCommand(
	cmd *cobra.Command,
	envName string,
	mode missingAtlasEnvSelectionMode,
) (projectconfig.Config, bool, error) {
	if cmd.Flags().Changed(dbcli.EnvFlagName) {
		cfg, err := loadAtlasProjectConfig(envName)
		return cfg, true, err
	}
	if !atlasProjectConfigExists() {
		return projectconfig.Config{}, false, nil
	}
	cfg, err := loadAtlasProjectConfig("")
	if err != nil {
		if isAtlasEnvSelectionRequired(err) && mode == ignoreMissingEnvSelection {
			return projectconfig.Config{}, false, nil
		}
		return projectconfig.Config{}, false, err
	}
	return cfg, true, nil
}

func atlasProjectConfigExists() bool {
	info, err := os.Stat(projectconfig.AtlasFileName)
	return err == nil && !info.IsDir()
}

func isAtlasEnvSelectionRequired(err error) bool {
	return strings.Contains(err.Error(), "contains multiple env blocks; pass --env")
}

func effectiveAtlasExclude(cmd *cobra.Command, flagValues []string, cfg projectconfig.Config) []string {
	values := effectiveStringArray(cmd, "exclude", flagValues, cfg.Exclude)
	return append(slices.Clone(values), cfg.Schema.Mode.ExcludePatterns()...)
}

func atlasDiffPolicy(cfg projectconfig.Config) (atlasschema.DiffPolicy, error) {
	if cfg.Diff.ConcurrentIndex.Drop.Set && cfg.Diff.ConcurrentIndex.Drop.Value {
		return atlasschema.DiffPolicy{}, fmt.Errorf("atlas.hcl diff.concurrent_index.drop is not supported yet")
	}
	return atlasschema.DiffPolicy{
		SkipDropTable:         cfg.Diff.Skip.DropTable.Set && cfg.Diff.Skip.DropTable.Value,
		ConcurrentIndexCreate: cfg.Diff.ConcurrentIndex.Create.Set && cfg.Diff.ConcurrentIndex.Create.Value,
	}, nil
}
