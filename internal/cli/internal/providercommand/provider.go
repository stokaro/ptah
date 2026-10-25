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

// The flags every command that selects a provider declares, named here for the
// commands that quote them in a refusal of their own.
const (
	ConfigFlag = "provider-config"
	EngineFlag = "provider-engine"
)

// Configure selects one explicitly trusted provider before the command runs.
// fail owns the command's error presentation, including machine-readable output.
// Help and argument errors start no processes. Unselected installations remain
// data: nothing executes from PATH, the working directory, or environment.
//
// The targets are the command's --db-url, --dialect and --dev-url flags, those
// of them it defines. A command whose connection flags carry other names uses
// [Register] and [Selection.Start] instead.
func Configure(command *cobra.Command, fail func(*cobra.Command, error) error) *cobra.Command {
	selection := Register(command)
	run := command.RunE
	command.RunE = func(cmd *cobra.Command, args []string) error {
		ctx, stop, err := selection.Start(cmd, targets(cmd))
		if err != nil {
			return fail(cmd, err)
		}
		defer stop()
		previous := cmd.Context()
		cmd.SetContext(ctx)
		defer cmd.SetContext(previous)
		return run(cmd, args)
	}
	return command
}

// Selection is what one command's provider flags hold.
type Selection struct {
	path, engine string
}

// Register declares --provider-config and --provider-engine on command and
// returns what they hold. Neither flag reads an environment variable: a
// variable exported once is not the operator naming a binary for this run.
func Register(command *cobra.Command) *Selection {
	selection := &Selection{}
	command.Flags().StringVar(&selection.path, ConfigFlag, "", "Explicit trusted provider manifest (experimental JSON format)")
	command.Flags().StringVar(&selection.engine, EngineFlag, "", "Engine required from an executable provider; never use a built-in implementation")
	for _, name := range []string{ConfigFlag, EngineFlag} {
		if err := cmdflags.DisableEnvBinding(command.Flags(), name); err != nil {
			panic(err)
		}
	}
	return selection
}

// Start selects the provider the flags and targets name, starts it, and
// returns the context the command runs under: it binds the provider's URL
// schemes and its runtime, and carries the started provider. stop ends the
// provider process. With no provider selected, Start returns cmd's context and
// a stop that does nothing, and the built-in engines keep the invocation.
//
// targets are the connection values the command names, in the order selection
// reads them. Each target's Flag is the flag that carried it, which a
// provider_target_mismatch refusal names.
func (s *Selection) Start(cmd *cobra.Command, targets []providerselect.Target) (context.Context, func(), error) {
	noop := func() {}
	configured, explicit := cmd.Flags().Changed(ConfigFlag), cmd.Flags().Changed(EngineFlag)
	if configured && strings.TrimSpace(s.path) == "" {
		return nil, noop, fmt.Errorf("--%s requires a manifest path", ConfigFlag)
	}
	if explicit && strings.TrimSpace(s.engine) == "" {
		return nil, noop, fmt.Errorf("--%s requires an engine ID", EngineFlag)
	}
	request := providerselect.Request{Configured: configured, Engine: s.engine, Explicit: explicit, Targets: targets}
	if configured {
		var err error
		request.Config, err = providerconfig.Load(s.path)
		if err != nil {
			return nil, noop, err
		}
	}
	choice, err := providerselect.Choose(request)
	if err != nil {
		return nil, noop, err
	}
	if choice == nil {
		return cmd.Context(), noop, nil
	}
	// A selected executable owns this runtime completely. In particular an
	// official provider never inherits a built-in implementation on failure.
	started, err := choice.Start(cmd.Context(), providerselect.Exclusive)
	if err != nil {
		return nil, noop, err
	}
	ctx, err := apply(cmd.Context(), started)
	if err != nil {
		started.Close()
		return nil, noop, err
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
	return providerselect.WithStarted(selected, started), nil
}
