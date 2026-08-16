// Package atlasruntimevar opens the runtime-variable URL schemes exposed by
// Atlas Community Edition through Go CDK.
package atlasruntimevar

import (
	"context"

	"gocloud.dev/runtimevar"
	_ "gocloud.dev/runtimevar/awsparamstore"     // Atlas OSS awsparamstore:// URLs.
	_ "gocloud.dev/runtimevar/awssecretsmanager" // Atlas OSS awssecretsmanager:// URLs.
	_ "gocloud.dev/runtimevar/constantvar"       // Atlas OSS constant:// URLs.
	_ "gocloud.dev/runtimevar/filevar"           // Atlas OSS file:// URLs.
	_ "gocloud.dev/runtimevar/gcpruntimeconfig"  // Atlas OSS gcpruntimeconfig:// URLs.
	_ "gocloud.dev/runtimevar/gcpsecretmanager"  // Atlas OSS gcpsecretmanager:// URLs.
	_ "gocloud.dev/runtimevar/httpvar"           // Atlas OSS http:// and https:// URLs.
)

// Open opens rawURL using the Atlas OSS runtime-variable driver set.
func Open(ctx context.Context, rawURL string) (*runtimevar.Variable, error) {
	return runtimevar.OpenVariable(ctx, rawURL)
}
