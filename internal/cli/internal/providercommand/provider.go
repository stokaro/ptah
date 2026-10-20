// Package providercommand owns explicitly configured providers for one native
// command, including every error and panic exit from that command.
package providercommand

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/providerhost"
)

const flag = "provider-config"

// Configure adds an explicit-only manifest flag and binds provider lifetime to
// RunE. Help and argument errors start no processes. Nothing discovers or starts
// an executable from PATH, the working directory, or an environment variable.
func Configure(command *cobra.Command) *cobra.Command {
	var path string
	command.Flags().StringVar(&path, flag, "", "Explicit trusted provider manifest (experimental JSON format)")
	if err := cmdflags.DisableEnvBinding(command.Flags(), flag); err != nil {
		panic(err)
	}
	run := command.RunE
	command.RunE = func(cmd *cobra.Command, args []string) error {
		if !cmd.Flags().Changed(flag) {
			return run(cmd, args)
		}
		if strings.TrimSpace(path) == "" {
			return cmdutil.Fail(cmd, fmt.Errorf("--%s requires a manifest path", flag))
		}
		specs, err := load(path)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		selected, closeProviders, err := selectProviders(cmd.Context(), specs)
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		defer closeProviders()
		previous := cmd.Context()
		cmd.SetContext(selected)
		defer cmd.SetContext(previous)
		return run(cmd, args)
	}
	return command
}

type manifest struct {
	FormatVersion int                 `json:"format_version"`
	Providers     []providerhost.Spec `json:"providers"`
}

func load(path string) ([]providerhost.Spec, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read provider manifest: %w", err)
	}
	defer func() { _ = file.Close() }()
	const limit = 1024 * 1024
	data, err := io.ReadAll(io.LimitReader(file, limit+1))
	if err != nil {
		return nil, fmt.Errorf("read provider manifest: %w", err)
	}
	if len(data) > limit {
		return nil, fmt.Errorf("provider manifest exceeds 1 MiB")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var config manifest
	if err := decoder.Decode(&config); err != nil {
		return nil, fmt.Errorf("parse provider manifest: %w", err)
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return nil, fmt.Errorf("provider manifest must contain one JSON document")
	}
	if config.FormatVersion != 1 || len(config.Providers) == 0 {
		return nil, fmt.Errorf("provider manifest requires format_version 1 and at least one provider")
	}
	return config.Providers, nil
}

func selectProviders(ctx context.Context, specs []providerhost.Spec) (context.Context, func(), error) {
	hosts := make([]*providerhost.Host, 0, len(specs))
	closeProviders := func() {
		for _, host := range hosts {
			host.Close()
		}
	}
	providers := make([]engine.Provider, 0, len(specs))
	bindings := make(map[string]dbschema.NativeRegistration)
	for _, spec := range specs {
		host, err := providerhost.Start(ctx, spec)
		if err != nil {
			closeProviders()
			return nil, nil, err
		}
		hosts = append(hosts, host)
		providers = append(providers, host.Provider())
		for name, binding := range host.Connections() {
			if atlassource.IsReservedScheme(name) {
				closeProviders()
				return nil, nil, fmt.Errorf("provider URL scheme %q is reserved for schema sources", name)
			}
			if _, exists := bindings[name]; exists {
				closeProviders()
				return nil, nil, fmt.Errorf("provider URL scheme %q has multiple owners", name)
			}
			bindings[name] = binding
		}
	}
	runtime, err := builtin.New(providers...)
	if err != nil {
		closeProviders()
		return nil, nil, err
	}
	selected, err := dbschema.WithNativeConnections(ctx, bindings)
	if err != nil {
		closeProviders()
		return nil, nil, err
	}
	selected, err = engineruntime.With(selected, runtime)
	if err != nil {
		closeProviders()
		return nil, nil, err
	}
	return selected, closeProviders, nil
}
