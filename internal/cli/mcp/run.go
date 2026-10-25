package mcp

import (
	"fmt"

	"github.com/spf13/cobra"

	"ptah.run/internal/agentaudit"
	"ptah.run/internal/buildinfo"
	"ptah.run/internal/cli/internal/agentflags"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/cli/internal/providercommand"
	"ptah.run/internal/mcpserver"
	"ptah.run/internal/providerselect"
)

// run resolves the operator's configuration and serves until the session ends.
//
// The resolution is agentflags', not this command's: `ptah assist` asks the
// same questions and has to get the same answers, and two copies of it would
// stop agreeing the first time one grew a flag.
//
// The provider is the operator's too. It is selected from the command line
// before the transport opens, for the configured database and nothing else, and
// every tool call reaches that database through it. No tool argument names a
// manifest, an engine or an executable, so a client cannot start a process or
// choose which one serves its call. The provider flags are registered here
// rather than in agentflags, so `ptah assist`, which serves the same tools in
// process, selects no provider.
func run(cmd *cobra.Command, opts *agentflags.Options, provider *providercommand.Selection) error {
	if err := refuseProviderWithoutADatabase(cmd, opts); err != nil {
		return cmdutil.Fail(cmd, err)
	}
	session, cleanup, err := agentflags.Build(cmd, opts, agentaudit.SurfaceMCP)
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer cleanup()

	// Started after every other refusal, so a configuration mistake costs no
	// process, and stopped when the session ends, however it ends.
	ctx, stop, err := provider.Start(cmd, []providerselect.Target{{Flag: agentflags.DatabaseURLFlag, Value: opts.DatabaseURL}})
	if err != nil {
		return cmdutil.Fail(cmd, err)
	}
	defer stop()

	return mcpserver.Run(ctx, mcpserver.Config{
		Version: buildinfo.Resolve().Version,
		Session: session,
	})
}

// refuseProviderWithoutADatabase names the provider flag that would do
// nothing. A provider here serves the configured database, and with none
// configured it would be a process started for no tool to use.
func refuseProviderWithoutADatabase(cmd *cobra.Command, opts *agentflags.Options) error {
	if opts.DatabaseURL != "" {
		return nil
	}
	for _, name := range []string{providercommand.ConfigFlag, providercommand.EngineFlag} {
		if cmd.Flags().Changed(name) {
			return fmt.Errorf("--%s needs --%s: a provider serves the configured database, and none is configured",
				name, agentflags.DatabaseURLFlag)
		}
	}
	return nil
}
