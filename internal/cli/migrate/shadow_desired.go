package migrate

import (
	"context"
	"fmt"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/providerurl"
)

// shadowDesired is the desired schema the shadow verification compares with,
// or nil where the desired schema the plan was made with serves the shadow
// too.
//
// A provider binds a declared value to the database it names, and a value
// bound to the database the plan reads cannot be bound again. A shadow
// database that interprets values in another context, such as a dev realm of
// the target, therefore gets the sources loaded once more and bound for it.
// Without this the convergence check compares values bound to two databases,
// and the provider refuses the comparison. Only an executable provider binds
// a value to its database, so the sources load once for a built-in dialect,
// and for a shadow URL the plan's provider does not serve, which the
// verification refuses itself.
func shadowDesired(
	ctx context.Context,
	contexts schemaext.DatabaseContextResolver,
	dialect, plannedURL, shadowURL string,
	loadDesired func(dialect, databaseURL string) (*schemamodel.Database, error),
) (*schemamodel.Database, error) {
	if shadowURL == "" {
		return nil, nil
	}
	if target, served := providerurl.Provided(ctx, shadowURL); !served || target != dialect {
		return nil, nil
	}
	planned, err := contexts.ResolveDatabaseContext(ctx, schemaext.DatabaseContextRequest{Target: dialect, Address: plannedURL})
	if err != nil {
		return nil, fmt.Errorf("resolve the database context of the plan: %w", err)
	}
	shadow, err := contexts.ResolveDatabaseContext(ctx, schemaext.DatabaseContextRequest{Target: dialect, Address: shadowURL})
	if err != nil {
		return nil, fmt.Errorf("resolve the database context of --shadow-db: %w", err)
	}
	if shadow == planned {
		return nil, nil
	}
	return loadDesired(dialect, shadowURL)
}
