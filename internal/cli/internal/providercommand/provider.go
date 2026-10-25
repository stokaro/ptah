// Package providercommand owns the provider selection of one native command,
// including every error and panic exit from that command. It reads the
// command's flags and hands them to [providerselect], which decides, starts and
// describes the provider. [Install] gives every native verb that names a
// database or a dialect the same selection, so a verb reaches an engine a
// provider serves however it learns which engine it needs.
package providercommand

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"
	"github.com/spf13/pflag"

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
// provider process. The selection is exclusive: a URL scheme the provider does
// not register is refused.
//
// With no provider selected, Start returns a context that carries a lazy
// provider session over the same manifest, or over the user store when no
// manifest was named, and stop closes it. The built-in engines keep the
// invocation, and a provider starts only for a URL or dialect the command names
// later that a provider serves; that selection is additive. A command whose
// context already carries a lazy session, as ptah-compat installs one before
// it forwards to a native command, keeps that session unless its own flags name
// a manifest: a second session would start a second process for an engine the
// first one serves.
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
	if choice == nil && !configured && providerselect.FromContext(cmd.Context()) != nil {
		return cmd.Context(), noop, nil
	}
	if choice == nil {
		// No flag names an engine a provider serves, so the built-in engines
		// keep what the flags name. A URL the command learns later -- from a
		// project file, or a flag that is not one of the targets -- can still
		// name one: the session starts its provider at the first connection,
		// dev database or runtime that needs it, beside the built-in drivers,
		// and stop ends it with the command.
		session := providerselect.NewLazy(request.Config, configured, nil)
		return session.With(cmd.Context()), session.Close, nil
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
// carry, in the order selection reads them. A repeatable flag, such as the
// --dialect of `schema validate`, contributes each value it holds. A flag
// nothing set names no engine: a default, such as the postgres --dialect of
// `schema lineage`, which a live read ignores for the dialect the server
// reports, is not the operator asking for an engine. A value read from the
// environment counts as set.
func targets(cmd *cobra.Command) []providerselect.Target {
	var result []providerselect.Target
	for _, name := range []string{providerselect.FlagDatabaseURL, providerselect.FlagDialect, providerselect.FlagDevURL} {
		flag := cmd.Flags().Lookup(name)
		if flag == nil || !flag.Changed {
			continue
		}
		values := []string{flag.Value.String()}
		if repeated, ok := flag.Value.(pflag.SliceValue); ok {
			values = repeated.GetSlice()
		}
		for _, value := range values {
			result = append(result, providerselect.Target{Flag: name, Value: value})
		}
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
