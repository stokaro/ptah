package schemaext

import (
	"context"
	"fmt"
)

// RequireRuntime verifies that context and runtime are present before a pipeline
// starts work. It recognizes typed nils and canceled contexts without calling a
// service. It accepts narrow services as well as composed runtimes; callers
// retain their own capability interface. Registration and target support are
// validated by each runtime method.
func RequireRuntime(ctx context.Context, runtime any) error {
	if ctx == nil || absent(runtime) {
		return fmt.Errorf("%w: a context and selected feature runtime are required", ErrInvalidValue)
	}
	return ctx.Err()
}
