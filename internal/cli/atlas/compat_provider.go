package atlas

import (
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/spf13/cobra"

	"ptah.run/engine"
	"ptah.run/engine/builtin"
	"ptah.run/internal/atlascompatpolicy"
	"ptah.run/internal/cli/internal/cmdutil"
	"ptah.run/internal/providerconfig"
	"ptah.run/internal/providerselect"
)

// providerConfigEnvVar names the provider manifest the compatibility surface
// resolves executable providers from.
//
// WHY AN ENVIRONMENT VARIABLE AND NOT A FLAG. The pinned community binary
// registers no provider flag on any verb, so registering one here would put a
// non-Atlas flag on the compatibility surface and break the conformance
// `cli-surface` tier, which asserts flag parity against that binary. Native
// `ptah` takes the same manifest through `--provider-config`, and that flag has
// no environment binding, so the variable is read by this surface only
// (precedent: PTAH_MIGRATIONS_ENGINE).
//
// WHAT IT AUTHORIZES. Nothing. It identifies one explicit manifest file, as
// `--provider-config` does. The manifest entry (absolute executable path,
// SHA-256, provider ID, engines) and the checks that run before every start
// authorize the binary. Nothing is searched, and a manifest that fails to load
// or an entry that fails verification ends the search.
//
// WHY THE EXTENSION EXISTS. The community binary has no driver for an engine
// that only a provider serves, so there is no Atlas behavior to reproduce for
// it. Reachable only through native `ptah`, the provider path would be no
// migration path for someone porting an Atlas pipeline, which the
// compatibility policy forbids. Strict mode refuses the variable by name (see
// atlascompatpolicy), and installs no provider session at all.
const providerConfigEnvVar = "PTAH_PROVIDER_CONFIG"

// providerSession reads the manifest the compatibility surface was told to use
// and returns the lazy resolver for one command invocation. Nothing starts
// until a connection needs a provider.
//
// An unset variable selects no manifest, which leaves the stored official
// providers of the selection order. A set but blank value is refused,
// because `PTAH_PROVIDER_CONFIG=` and an absent variable are different
// configuration states, and a blank manifest path is a mistake to report rather
// than a default.
func providerSession() (*providerselect.Lazy, error) {
	path, present := os.LookupEnv(providerConfigEnvVar)
	if !present {
		return providerselect.NewLazy(providerconfig.Configuration{}, false, nil), nil
	}
	if strings.TrimSpace(path) == "" {
		return nil, fmt.Errorf("%s requires a manifest path", providerConfigEnvVar)
	}
	config, err := providerconfig.Load(path)
	if err != nil {
		return nil, err
	}
	return providerselect.NewLazy(config, true, nil), nil
}

// providerServesScheme reports whether a provider serves the URL scheme under
// the session installed on ctx. It reads the manifest and the catalog only and
// starts nothing.
func providerServesScheme(ctx context.Context, scheme string) bool {
	return providerselect.FromContext(ctx).Serves(scheme)
}

// installAtlasProviderSession gives every connecting verb a lazy provider
// session that ends with the verb. The strict profile installs none: the
// pinned community binary has no provider concept, and a strict run refuses
// the manifest variable before it reaches here.
//
// The session wraps the verb's own function rather than a pre-run hook. The
// forwarding and project-flag hooks refresh a command's context from the root's
// in the pre-run phase, so a value set earlier would be replaced before the
// verb or the native command it forwards to ran.
func installAtlasProviderSession(root *cobra.Command, policy atlascompatpolicy.Policy) {
	if policy.IsStrictCE() {
		return
	}
	for _, name := range []string{"schema", "migrate", "script"} {
		if group, _, err := root.Find([]string{name}); err == nil && group != root {
			installProviderSessionOnLeaves(group)
		}
	}
}

func installProviderSessionOnLeaves(cmd *cobra.Command) {
	if children := cmd.Commands(); len(children) != 0 {
		for _, child := range children {
			installProviderSessionOnLeaves(child)
		}
		return
	}
	run := cmd.RunE
	if run == nil {
		return
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		session, err := providerSession()
		if err != nil {
			return cmdutil.Fail(cmd, err)
		}
		// The provider process lives exactly as long as the verb: returning,
		// failing, panicking and being interrupted all pass through this defer.
		defer session.Close()
		previous := cmd.Context()
		cmd.SetContext(session.With(contextOrBackground(cmd)))
		defer cmd.SetContext(previous)
		return run(cmd, args)
	}
}

// atlasRuntime returns the runtime a verb plans, renders and lints with. It is
// the bundled one, extended with the provider of every engine the URLs name.
//
// The provider starts here, and not at the first connection, because the runtime
// is frozen when it is built and a provider's targets cannot be added later. A
// verb that names only built-in engines starts no process. The provider the
// runtime started is the one the verb's connections use; the session starts each
// provider once.
func atlasRuntime(ctx context.Context, urls []string, devURL string) (*engine.Runtime, error) {
	lazy := providerselect.FromContext(ctx)
	if lazy == nil {
		return builtin.New()
	}
	targets := make([]providerselect.Target, 0, len(urls)+1)
	for _, raw := range urls {
		targets = append(targets, providerselect.Target{Flag: providerselect.FlagDatabaseURL, Value: raw})
	}
	targets = append(targets, providerselect.Target{Flag: providerselect.FlagDevURL, Value: devURL})
	return lazy.Runtime(ctx, targets...)
}
