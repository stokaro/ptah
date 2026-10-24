// Package providercommand owns explicitly configured providers for one native
// command, including every error and panic exit from that command. It reads the
// command's flags and hands them to [providerselect], which decides, starts and
// describes the provider.
package providercommand

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/dbschema"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerselect"
)

const (
	configFlag = "provider-config"
	engineFlag = "provider-engine"
)

// Configure selects one explicitly trusted provider before the command runs.
// fail owns the command's error presentation, including machine-readable output.
// Help and argument errors start no processes. Unselected installations remain
// data: nothing executes from PATH, the working directory, or environment.
func Configure(command *cobra.Command, fail func(*cobra.Command, error) error) *cobra.Command {
	var path, selectedEngine string
	command.Flags().StringVar(&path, configFlag, "", "Explicit trusted provider manifest (experimental JSON format)")
	command.Flags().StringVar(&selectedEngine, engineFlag, "", "Engine required from an executable provider; never use a built-in implementation")
	for _, name := range []string{configFlag, engineFlag} {
		if err := cmdflags.DisableEnvBinding(command.Flags(), name); err != nil {
			panic(err)
		}
	}
	run := command.RunE
	command.RunE = func(cmd *cobra.Command, args []string) error {
		ctx, closeProvider, err := prepare(cmd, path, selectedEngine)
		if err != nil {
			return fail(cmd, err)
		}
		if closeProvider == nil {
			return run(cmd, args)
		}
		defer closeProvider()
		previous := cmd.Context()
		cmd.SetContext(ctx)
		defer cmd.SetContext(previous)
		return run(cmd, args)
	}
	return command
}

func prepare(cmd *cobra.Command, path, selectedEngine string) (context.Context, func(), error) {
	configured, explicit := cmd.Flags().Changed(configFlag), cmd.Flags().Changed(engineFlag)
	if configured && strings.TrimSpace(path) == "" {
		return nil, nil, fmt.Errorf("--%s requires a manifest path", configFlag)
	}
	if explicit && strings.TrimSpace(selectedEngine) == "" {
		return nil, nil, fmt.Errorf("--%s requires an engine ID", engineFlag)
	}
	request := providerselect.Request{Configured: configured, Engine: selectedEngine, Explicit: explicit, Targets: targets(cmd)}
	if configured {
		var err error
		request.Config, err = providerconfig.Load(path)
		if err != nil {
			return nil, nil, err
		}
	}
	choice, err := providerselect.Choose(request)
	if err != nil {
		return nil, nil, err
	}
	if choice == nil {
		return cmd.Context(), nil, nil
	}
	// A selected executable owns this runtime completely. In particular an
	// official provider never inherits a built-in implementation on failure.
	started, err := choice.Start(cmd.Context(), providerselect.Exclusive)
	if err != nil {
		return nil, nil, err
	}
	ctx, err := apply(cmd.Context(), started)
	if err != nil {
		started.Close()
		return nil, nil, err
	}
	return ctx, started.Close, nil
}

// targets lists the connection flags the command defines and the values they
// carry, in the order selection reads them.
func targets(cmd *cobra.Command) []providerselect.Target {
	var result []providerselect.Target
	for _, name := range []string{providerselect.FlagDatabaseURL, providerselect.FlagDialect, providerselect.FlagDevURL} {
		if cmd.Flags().Lookup(name) == nil {
			continue
		}
		value, _ := cmd.Flags().GetString(name)
		result = append(result, providerselect.Target{Flag: name, Value: value})
	}
	return result
}

func apply(ctx context.Context, started *providerselect.Started) (context.Context, error) {
	selected, err := dbschema.WithNativeConnections(ctx, started.Connections)
	if err != nil {
		return nil, err
	}
	selected, err = engineruntime.With(selected, started.Runtime)
	if err != nil {
		return nil, err
	}
	if started.Dev != nil {
		selected = devdocker.WithExternal(selected, started.Dev)
	}
	return selected, nil
}
