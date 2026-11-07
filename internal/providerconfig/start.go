package providerconfig

import (
	"context"
	"errors"
	"maps"

	"github.com/stokaro/ptah/provider-sdk/provider"

	"ptah.run/internal/providerhost"
)

// Start verifies one selected installation, starts it, and checks its engine
// declarations against the manifest. Every failure closes any started process.
func Start(ctx context.Context, installation Installation, target string) (*providerhost.Host, error) {
	if err := installation.Verify(target); err != nil {
		return nil, err
	}
	host, err := providerhost.Start(ctx, installation.Spec)
	if err != nil {
		code, message := "provider_start_failed", err.Error()
		if errors.Is(err, provider.ErrProtocol) {
			code = "provider_incompatible"
		}
		if errors.Is(err, providerhost.ErrStartupTimeout) {
			code, message = "provider_start_timeout", "the "+target+" "+message
		}
		diagnostic := installation.Diagnostic(target, code, message)
		diagnostic.Cause = err
		return nil, diagnostic
	}
	actual := make(map[string]string)
	for name, binding := range host.Connections() {
		actual[name] = binding.Target
	}
	if !maps.Equal(actual, installation.Claims()) {
		host.Close()
		return nil, installation.Diagnostic(target, "provider_identity_mismatch", installation.origin.mismatch())
	}
	return host, nil
}
