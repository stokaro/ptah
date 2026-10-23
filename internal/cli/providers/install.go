package providers

import (
	"fmt"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/providerinstall"
)

type installOptions struct {
	baseURL string
	json    bool
}

type installReport struct {
	ContractVersion int    `json:"contract_version"`
	Status          string `json:"status"`
	EngineID        string `json:"engine_id"`
	ProviderID      string `json:"provider_id"`
	Version         string `json:"version"`
	Platform        string `json:"platform"`
	Executable      string `json:"executable"`
	SHA256          string `json:"sha256"`
}

// newInstallCommand builds `providers install`. base carries the installer
// settings the command line does not: tests set the pins and platform, and the
// shipped command passes the zero value. The store location and the download
// base URL are filled in per run.
func newInstallCommand(base providerinstall.Installer) *cobra.Command {
	opts := new(installOptions)
	command := &cobra.Command{
		Use:   "install <engine>",
		Short: "Download, verify and store the pinned official provider of an engine without running it",
		Long: "Download the provider release this Ptah release pins for the engine, check the archive and the executable " +
			"against the pinned SHA-256 digests, and store the executable in the user store. " +
			"The command takes no version, never runs the downloaded file and does not edit the provider manifest. " +
			"Installing a verified provider again changes nothing.",
	}
	command.RunE = func(cmd *cobra.Command, args []string) error {
		installer := base
		installer.BaseURL = opts.baseURL
		root, err := providerinstall.DefaultRoot()
		if err != nil {
			return failJSON(cmd, opts.json, err)
		}
		installer.Root = root
		result, err := installer.Install(cmd.Context(), args[0])
		if err != nil {
			return failJSON(cmd, opts.json, err)
		}
		report := installReport{ContractVersion: 1, Status: "installed", EngineID: result.EngineID, ProviderID: result.ProviderID,
			Version: result.Version, Platform: result.Platform.String(), Executable: result.Executable, SHA256: result.SHA256}
		if !result.Installed {
			report.Status = "already_installed"
		}
		if opts.json {
			return writeJSON(cmd, report)
		}
		verb := map[bool]string{true: "Installed", false: "Already installed"}[result.Installed]
		fmt.Fprintf(cmd.OutOrStdout(), "%s %s %s for %s (%s)\nexecutable: %s\nsha256: %s\n",
			verb, report.ProviderID, report.Version, report.Platform, report.EngineID, report.Executable, report.SHA256)
		return nil
	}
	command.Flags().StringVar(&opts.baseURL, "download-base-url", "",
		"Directory URL to fetch the pinned archive from instead of the official release; the pinned digests still decide what is accepted")
	command.Flags().BoolVar(&opts.json, "json", false, "Print one JSON document")
	if err := cmdflags.DisableEnvBinding(command.Flags(), "json"); err != nil {
		panic(err)
	}
	cmdutil.ConfigureCommandArgs(command, cobra.ExactArgs(1))
	return command
}

func failJSON(command *cobra.Command, asJSON bool, err error) error {
	return (&options{json: asJSON}).fail(command, err)
}
