// Package providers exposes provider catalog, installation, and negotiation
// diagnostics, and installs the pinned official provider of an engine. Listing
// and installing never start an executable or connect to a database.
package providers

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerdiagnostic"
	"ptah.run/internal/providerhost"
	"ptah.run/internal/providerinstall"
)

// NewCommand returns the native provider diagnostics namespace.
func NewCommand() *cobra.Command {
	command := &cobra.Command{Use: "providers", Short: "Inspect provider configuration, verify trusted executables and install pinned official providers"}
	command.AddCommand(newListCommand(), newCheckCommand(), newInstallCommand(providerinstall.Installer{}))
	cmdutil.ConfigureCommandArgs(command, cmdutil.NoPositionalArgs)
	return command
}

type options struct {
	path   string
	engine string
	json   bool
}

func flags(command *cobra.Command, opts *options) {
	command.Flags().StringVar(&opts.path, "provider-config", "", "Explicit trusted provider manifest")
	command.Flags().BoolVar(&opts.json, "json", false, "Print one JSON document")
	names := []string{"provider-config", "json"}
	for _, name := range names {
		if err := cmdflags.DisableEnvBinding(command.Flags(), name); err != nil {
			panic(err)
		}
	}
	cmdutil.ConfigureCommandArgs(command, cmdutil.NoPositionalArgs)
}

func load(command *cobra.Command, path string) (providerconfig.Configuration, error) {
	if !command.Flags().Changed("provider-config") {
		return providerconfig.Configuration{}, nil
	}
	if strings.TrimSpace(path) == "" {
		return providerconfig.Configuration{}, fmt.Errorf("--provider-config requires a manifest path")
	}
	return providerconfig.Load(path)
}

type installationReport struct {
	ProviderID     string                         `json:"provider_id"`
	Executable     string                         `json:"executable"`
	SHA256         string                         `json:"sha256"`
	Engines        []providerconfig.Engine        `json:"engines"`
	Status         string                         `json:"status"`
	ProtocolStatus string                         `json:"protocol_status"`
	Diagnostic     *providerdiagnostic.Diagnostic `json:"diagnostic,omitempty"`
}

type listReport struct {
	ContractVersion int                     `json:"contract_version"`
	Catalog         []providercatalog.Entry `json:"catalog"`
	Installations   []installationReport    `json:"installations"`
}

func newListCommand() *cobra.Command {
	opts := new(options)
	command := &cobra.Command{Use: "list", Short: "List official providers and configured installations without executing them"}
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		config, err := load(cmd, opts.path)
		if err != nil {
			return opts.fail(cmd, err)
		}
		report := listReport{ContractVersion: 1, Catalog: providercatalog.Entries(), Installations: make([]installationReport, 0, len(config.Providers))}
		for _, installation := range config.Providers {
			row := installationReport{ProviderID: installation.ProviderID, Executable: installation.Executable, SHA256: installation.SHA256,
				Engines: installation.Engines, Status: "integrity_verified", ProtocolStatus: "not_checked"}
			if err := installation.Verify(installation.Engines[0].ID); err != nil {
				row.Diagnostic, _ = errors.AsType[*providerdiagnostic.Diagnostic](err)
				row.Status = row.Diagnostic.Code
			}
			report.Installations = append(report.Installations, row)
		}
		if opts.json {
			return writeJSON(cmd, report)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Official provider catalog (does not imply installation):")
		for _, entry := range report.Catalog {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", entry.DisplayName, entry.ProviderID, entry.SetupURL)
		}
		fmt.Fprintln(cmd.OutOrStdout(), "Configured installations (protocol not checked):")
		for _, row := range report.Installations {
			fmt.Fprintf(cmd.OutOrStdout(), "%s\t%s\t%s\n", row.ProviderID, row.Status, row.Executable)
			if row.Diagnostic != nil {
				fmt.Fprintln(cmd.OutOrStdout(), row.Diagnostic.Error())
			}
		}
		return nil
	}
	flags(command, opts)
	return command
}

type checkReport struct {
	ContractVersion int                        `json:"contract_version"`
	Status          string                     `json:"status"`
	Executable      string                     `json:"executable"`
	EngineID        string                     `json:"engine_id"`
	Provider        providerhost.Metadata      `json:"provider"`
	ReleaseLines    []providerhost.ReleaseLine `json:"release_lines,omitempty"`
}

func newCheckCommand() *cobra.Command {
	opts := new(options)
	command := &cobra.Command{Use: "check", Short: "Start one trusted provider and verify its identity and protocol without opening a database"}
	command.RunE = func(cmd *cobra.Command, _ []string) error {
		if strings.TrimSpace(opts.engine) == "" {
			return opts.fail(cmd, fmt.Errorf("--provider-engine is required"))
		}
		config, err := load(cmd, opts.path)
		if err != nil {
			return opts.fail(cmd, err)
		}
		installation, target, err := config.Select(opts.engine)
		if err != nil {
			return opts.fail(cmd, err)
		}
		host, err := providerconfig.Start(cmd.Context(), installation, target)
		if err != nil {
			return opts.fail(cmd, err)
		}
		defer host.Close()
		report := checkReport{ContractVersion: 1, Status: "compatible", Executable: installation.Executable, EngineID: target, Provider: host.Metadata(), ReleaseLines: host.ReleaseLines(target)}
		if opts.json {
			return writeJSON(cmd, report)
		}
		fmt.Fprintf(cmd.OutOrStdout(), "%s %s: compatible protocol revision %d (%s)\n", report.Provider.ProviderID, report.Provider.Version, report.Provider.ProtocolRevision, report.Executable)
		return nil
	}
	flags(command, opts)
	command.Flags().StringVar(&opts.engine, "provider-engine", "", "Engine whose configured provider to verify")
	if err := cmdflags.DisableEnvBinding(command.Flags(), "provider-engine"); err != nil {
		panic(err)
	}
	return command
}

func writeJSON(command *cobra.Command, value any) error {
	encoder := json.NewEncoder(command.OutOrStdout())
	encoder.SetIndent("", "  ")
	return encoder.Encode(value)
}

func (opts *options) fail(command *cobra.Command, err error) error {
	if opts.json {
		diagnostic, _ := errors.AsType[*providerdiagnostic.Diagnostic](err)
		report := struct {
			ContractVersion int                            `json:"contract_version"`
			Status          string                         `json:"status"`
			Error           string                         `json:"error"`
			Diagnostic      *providerdiagnostic.Diagnostic `json:"diagnostic,omitempty"`
		}{1, "failed", err.Error(), diagnostic}
		if writeErr := writeJSON(command, report); writeErr != nil {
			return cmdutil.Fail(command, writeErr)
		}
	}
	return cmdutil.Fail(command, err)
}
