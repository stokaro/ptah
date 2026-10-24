package providers

import (
	"fmt"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/providerinstall"
	"ptah.run/internal/providerrelease"
)

type installOptions struct {
	baseURL string
	version string
	json    bool
}

type installReport struct {
	ContractVersion int                      `json:"contract_version"`
	Status          string                   `json:"status"`
	EngineID        string                   `json:"engine_id"`
	ProviderID      string                   `json:"provider_id"`
	Version         string                   `json:"version"`
	MinContract     providerrelease.Contract `json:"min_contract"`
	Platform        string                   `json:"platform"`
	Executable      string                   `json:"executable"`
	SHA256          string                   `json:"sha256"`
}

// newInstallCommand builds `providers install`. base carries the installer
// settings the command line does not: tests set the release API, the platform
// and the host contract, and the shipped command passes the zero value. The
// store location, the version and the download base URL are filled in per run.
func newInstallCommand(base providerinstall.Installer) *cobra.Command {
	opts := new(installOptions)
	command := &cobra.Command{
		Use:   "install <engine>",
		Short: "Download, verify and store an official provider release without running it",
		Long: "Download the newest release of the engine's official provider whose minimum contract version this Ptah meets, " +
			"or the release --version names, and store its executable in the user store. " +
			"The release's ptah-provider.json and archive are checked against the release's checksums.txt before anything is extracted, " +
			"and the executable's SHA-256 is recorded so that every start can check it. " +
			"The command never runs the downloaded file and does not edit the provider manifest. " +
			"Installing the release the store already holds downloads no archive.",
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		installer := base
		installer.BaseURL = opts.baseURL
		root, err := providerinstall.DefaultRoot()
		if err != nil {
			return failJSON(cmd, opts.json, err)
		}
		installer.Root = root
		result, err := installer.Install(cmd.Context(), args[0], opts.version)
		if err != nil {
			return failJSON(cmd, opts.json, err)
		}
		report := installReport{ContractVersion: 1, Status: "installed", EngineID: result.EngineID, ProviderID: result.ProviderID,
			Version: result.Version, MinContract: result.MinContract, Platform: result.Platform.String(),
			Executable: result.Executable, SHA256: result.SHA256}
		if !result.Installed {
			report.Status = "already_installed"
		}
		if opts.json {
			return writeJSON(cmd, report)
		}
		verb := map[bool]string{true: "Installed", false: "Already installed"}[result.Installed]
		fmt.Fprintf(cmd.OutOrStdout(), "%s %s %s for %s (%s), minimum contract %s\nexecutable: %s\nsha256: %s\n",
			verb, report.ProviderID, report.Version, report.Platform, report.EngineID, report.MinContract, report.Executable, report.SHA256)
		return nil
	}
	command.Flags().StringVar(&opts.version, "version", "",
		"Release to install, such as 0.2.0; refused when this Ptah does not meet its minimum contract version. Required with --download-base-url")
	command.Flags().StringVar(&opts.baseURL, "download-base-url", "",
		"Mirror that replaces https://github.com and serves the GitHub release download layout; the release's checksums.txt still decides what is accepted")
	command.Flags().BoolVar(&opts.json, "json", false, "Print one JSON document")
	for _, name := range []string{"version", "json"} {
		if err := cmdflags.DisableEnvBinding(command.Flags(), name); err != nil {
			panic(err)
		}
	}
	cmdutil.ConfigureCommandArgs(command, cobra.ExactArgs(1))
	return command
}

func failJSON(command *cobra.Command, asJSON bool, err error) error {
	return (&options{json: asJSON}).fail(command, err)
}
