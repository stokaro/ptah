package valuerounding

import (
	"context"

	"ptah.run/migration/migrator"
)

type migrationPolicyKey struct{}

// WithMigrationPolicy carries the calling surface's rounding choice to a
// forwarded migration command. The adapter reads its environment variable
// once and keeps its own remedy in a refusal from the native command.
func WithMigrationPolicy(ctx context.Context, policy migrator.ValueRounding) context.Context {
	return context.WithValue(ctx, migrationPolicyKey{}, policy)
}

// MigrationPolicy returns the calling surface's choice when one was supplied,
// or the native invocation's flag and remedy. It reads no environment variable.
func MigrationPolicy(ctx context.Context, allowed bool) migrator.ValueRounding {
	if policy, ok := ctx.Value(migrationPolicyKey{}).(migrator.ValueRounding); ok {
		return policy
	}
	return migrator.ValueRounding{Allowed: allowed, Request: Request}
}
