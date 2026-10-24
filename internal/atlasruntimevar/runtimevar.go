// Package atlasruntimevar opens the runtime-variable URL schemes exposed by
// Atlas Community Edition through Go CDK.
package atlasruntimevar

import (
	"context"
	"sync"

	"gocloud.dev/runtimevar"
	_ "gocloud.dev/runtimevar/awssecretsmanager" // Atlas OSS awssecretsmanager:// URLs.
	_ "gocloud.dev/runtimevar/constantvar"       // Atlas OSS constant:// URLs.
	_ "gocloud.dev/runtimevar/filevar"           // Atlas OSS file:// URLs.
	_ "gocloud.dev/runtimevar/gcpruntimeconfig"  // Atlas OSS gcpruntimeconfig:// URLs.
	_ "gocloud.dev/runtimevar/gcpsecretmanager"  // Atlas OSS gcpsecretmanager:// URLs.
	_ "gocloud.dev/runtimevar/httpvar"           // Atlas OSS http:// and https:// URLs.

	"ptah.run/internal/atlasruntimevar/awsparamstore"
)

// registerParameterStore puts Ptah's Parameter Store driver on the default
// mux, under the scheme gocloud.dev/runtimevar/awsparamstore would claim.
// That package is not linked: its SSM client is the reason
// internal/atlasruntimevar/awsparamstore exists.
var registerParameterStore = sync.OnceFunc(func() {
	runtimevar.DefaultURLMux().RegisterVariable(awsparamstore.Scheme, &awsparamstore.URLOpener{})
})

// Open opens rawURL using the Atlas OSS runtime-variable driver set.
func Open(ctx context.Context, rawURL string) (*runtimevar.Variable, error) {
	registerParameterStore()
	return runtimevar.OpenVariable(ctx, rawURL)
}
