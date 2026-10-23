// Package providercommand owns explicitly configured providers for one native
// command, including every error and panic exit from that command.
package providercommand

import (
	"context"
	"fmt"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/core/platform"
	"ptah.run/dbschema"
	"ptah.run/engine"
	"ptah.run/internal/cli/internal/cmdflags"
	"ptah.run/internal/cli/internal/engineruntime"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/providercatalog"
	"ptah.run/internal/providerconfig"
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
	config := providerconfig.Configuration{}
	if configured {
		var err error
		config, err = providerconfig.Load(path)
		if err != nil {
			return nil, nil, err
		}
	}
	selector := strings.ToLower(strings.TrimSpace(selectedEngine))
	if !explicit {
		selector = targetSelector(cmd)
	}
	if selector == "" && configured {
		if len(config.Providers) != 1 || len(config.Providers[0].Engines) != 1 {
			return nil, nil, fmt.Errorf("--%s is required when the command does not identify one configured provider engine", engineFlag)
		}
		selector = config.Providers[0].Engines[0].ID
	}
	if selector == "" {
		return cmd.Context(), nil, nil
	}
	if servedEmbedded(config, selector, explicit) {
		return cmd.Context(), nil, nil
	}
	installation, target, err := providerconfig.DefaultResolver().Select(config, selector)
	if err != nil {
		if !configured && !explicit {
			if _, known := providercatalog.Lookup(selector); !known {
				return cmd.Context(), nil, nil
			}
		}
		return nil, nil, err
	}
	if err := validateTargets(cmd, installation, target); err != nil {
		return nil, nil, err
	}
	return selectProvider(cmd.Context(), installation, target)
}

// servedEmbedded reports whether an engine with a built-in implementation keeps
// that implementation for this invocation. Only an explicit --provider-engine,
// or a manifest entry that claims the engine, asks for a provider instead; the
// bundled and stored candidates of the selection order are not consulted for an
// engine that is still built in.
//
// This is the one place the default for an official engine is decided. Serving
// an engine from its provider by default means returning false here for it.
func servedEmbedded(config providerconfig.Configuration, selector string, explicit bool) bool {
	return !explicit && platform.NormalizeDialect(selector) != "" && !configuredFor(config, selector)
}

func targetSelector(cmd *cobra.Command) string {
	for _, name := range []string{"db-url", "dialect", "dev-url"} {
		if cmd.Flags().Lookup(name) == nil {
			continue
		}
		value, _ := cmd.Flags().GetString(name)
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		if entry, known := providercatalog.Lookup(value); known {
			return entry.EngineID
		}
		if named, found := dockerEngine(name, value); found {
			return named
		}
		if scheme, _, found := strings.Cut(value, ":"); found {
			return strings.ToLower(scheme)
		}
		if name == "dialect" {
			return strings.ToLower(value)
		}
	}
	return ""
}

// dockerEngine reads the engine a `docker://<engine>/...` dev URL names. Only
// --dev-url carries one: the URL asks its owner for a database, so the owner is
// the provider of that engine.
func dockerEngine(flag, value string) (string, bool) {
	rest, found := strings.CutPrefix(value, "docker://")
	if !found || flag != "dev-url" {
		return "", false
	}
	name, _, _ := strings.Cut(rest, "/")
	name, _, _ = strings.Cut(name, "?")
	return strings.ToLower(name), name != ""
}

func configuredFor(config providerconfig.Configuration, selector string) bool {
	if entry, found := providercatalog.Lookup(selector); found {
		selector = entry.EngineID
	}
	for _, installation := range config.Providers {
		if _, found := installation.Claims()[selector]; found {
			return true
		}
	}
	return false
}

func selectProvider(ctx context.Context, installation providerconfig.Installation, target string) (context.Context, func(), error) {
	host, err := providerconfig.Start(ctx, installation, target)
	if err != nil {
		return nil, nil, err
	}
	bindings := host.Connections()
	// A selected executable owns this runtime completely. In particular an
	// official provider never inherits a built-in implementation on failure.
	runtime, err := engine.New(host.Provider())
	if err != nil {
		host.Close()
		return nil, nil, err
	}
	selected, err := dbschema.WithNativeConnections(ctx, bindings)
	if err != nil {
		host.Close()
		return nil, nil, err
	}
	selected, err = engineruntime.With(selected, runtime)
	if err != nil {
		host.Close()
		return nil, nil, err
	}
	if owner := host.DevDatabases(); owner != nil {
		selected = devdocker.WithExternal(selected, owner)
	}
	return selected, host.Close, nil
}

// Check every explicit target before startup. The connection selection also
// refuses unregistered schemes from configuration loaded later by the command.
func validateTargets(cmd *cobra.Command, installation providerconfig.Installation, target string) error {
	for _, name := range []string{"db-url", "dialect", "dev-url"} {
		if cmd.Flags().Lookup(name) == nil {
			continue
		}
		value, _ := cmd.Flags().GetString(name)
		value = strings.TrimSpace(value)
		if value == "" {
			continue
		}
		selector := value
		if named, found := dockerEngine(name, value); found {
			selector = named
		} else if entry, found := providercatalog.Lookup(value); found {
			selector = entry.EngineID
		} else if scheme, _, found := strings.Cut(value, ":"); found {
			selector = strings.ToLower(scheme)
		}
		if installation.Claims()[selector] != target {
			return installation.Diagnostic(target, "provider_target_mismatch", "The --"+name+" target is not owned by the selected provider engine.")
		}
	}
	return nil
}
