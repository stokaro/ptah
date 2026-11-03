package schemaclean

import (
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/dbschema"
	"ptah.run/internal/dbreset"
)

// A database an executable provider serves is cleaned by the provider. Ptah
// has no catalog query for its engine and sees none of the statements the
// provider runs, so the plan is the provider's own list of what its cleanup
// removes, and the cleanup is the provider's: it removes every object of the
// list together, or refuses as a whole and removes nothing.
//
// What the connection is decides this, never its dialect name: a provider can
// serve an engine whose name a built-in descriptor also carries, and that
// descriptor describes the built-in writer, not the provider's cleanup.

// providerCleaner is the writer of a session an executable provider serves;
// see internal/providerhost.
type providerCleaner interface {
	// OffersCleanup reports whether the provider and the session's engine both
	// offer the dev database cleanup.
	OffersCleanup() bool
	// CleanupObjects lists every object DropAllTables removes, or stops at, in
	// the scope DropAllTables cleans.
	CleanupObjects(ctx context.Context) ([]dbreset.Object, error)
}

// servedByProvider reports whether an executable provider serves conn; see
// [catalog.ServerInfo.Provider].
func servedByProvider(conn *dbschema.DatabaseConnection) bool {
	return !conn.Info().Provider.IsZero()
}

// inspectProviderPlan is [InspectWithOptions] on a database a provider serves.
// The plan is the provider's list, so the reader's snapshot is read only for
// the caller's validator: a database the provider cannot describe in full is
// one its cleanup may still empty, or refuse naming the object that stops it.
func inspectProviderPlan(ctx context.Context, conn *dbschema.DatabaseConnection, opts InspectOptions) (Plan, error) {
	if opts.ValidateSchema != nil {
		schema, err := conn.Reader().ReadSchemaContext(ctx)
		if err != nil {
			return Plan{}, fmt.Errorf("inspect schema before cleanup: %w", err)
		}
		if err := opts.ValidateSchema(schema); err != nil {
			return Plan{}, err
		}
	}
	objects, err := inspectProviderObjects(ctx, conn)
	if err != nil {
		return Plan{}, err
	}
	return providerPlan(objects), nil
}

// inspectProviderObjects lists, as plan rows, what the provider's cleanup of
// conn removes. Each row keeps the provider's kind in its own words and
// carries no statement, because the provider drops the object in its own
// way. A provider that offers no cleanup is refused, and so is a list naming
// an object the provider cannot remove: the cleanup would then remove
// nothing, and a plan of the rest would describe a drop that does not
// happen.
func inspectProviderObjects(ctx context.Context, conn *dbschema.DatabaseConnection) ([]Object, error) {
	provider := conn.Info().Provider.ID
	cleaner, ok := conn.SchemaWriter().(providerCleaner)
	if !ok || !cleaner.OffersCleanup() {
		return nil, fmt.Errorf("%w: provider %q serves the database and offers no database cleanup (database.dev-cleanup), "+
			"so Ptah cannot list or drop what the database holds", ptaherr.ErrUnsupportedFeature, provider)
	}
	listed, err := cleaner.CleanupObjects(ctx)
	if err != nil {
		return nil, fmt.Errorf("inspect schema before cleanup: %w", err)
	}
	objects := make([]Object, 0, len(listed))
	var refused []string
	for _, found := range listed {
		object := Object{Type: found.Kind, Schema: found.Schema, Name: found.Name}
		if found.Refusal != "" {
			refused = append(refused, describeObject(object)+": "+found.Refusal)
			continue
		}
		objects = append(objects, object)
	}
	if len(refused) > 0 {
		return nil, fmt.Errorf("inspect schema before cleanup: provider %q would remove nothing, because it cannot remove %s",
			provider, strings.Join(refused, "; "))
	}
	return objects, nil
}

// providerPlan is the plan of the objects a provider's cleanup removes. Its
// changes carry no statement: Ptah does not invent SQL the provider would not
// run.
func providerPlan(objects []Object) Plan {
	objects = slices.Clone(objects)
	sortObjects(objects)
	changes := make([]Change, 0, len(objects))
	for _, object := range objects {
		changes = append(changes, Change{
			Type:       object.Type,
			Schema:     object.Schema,
			Table:      object.Table,
			Name:       object.Name,
			Parameters: object.Parameters,
		})
	}
	return Plan{Objects: objects, Changes: changes, provider: true}
}

// applyProviderPlan hands plan to the provider's cleanup, which removes every
// object of its list together. So it runs only a plan that names exactly what
// the provider lists now: a plan narrowed by a selector, or one the database
// has changed under, would print one set and remove another.
func applyProviderPlan(ctx context.Context, conn *dbschema.DatabaseConnection, plan Plan, opts ApplyPlanOptions) error {
	if opts.ValidateBeforeExecute != nil {
		if err := opts.ValidateBeforeExecute(conn.Writer()); err != nil {
			return err
		}
	}
	current, err := inspectProviderObjects(ctx, conn)
	if err != nil {
		return err
	}
	if difference := planDifference(plan.Objects, providerPlan(current).Objects); difference != "" {
		return fmt.Errorf("provider %q removes everything the database holds at once and cannot run a plan that names part of it: %s",
			conn.Info().Provider.ID, difference)
	}
	conn.SchemaWriter().SetDryRun(false)
	if err := conn.SchemaWriter().DropAllTables(ctx); err != nil {
		return fmt.Errorf("drop schema objects: %w", err)
	}
	return nil
}

// planDifference says how planned differs from listed, or answers the empty
// string when the two name the same objects.
func planDifference(planned, listed []Object) string {
	var parts []string
	for _, object := range listed {
		if !slices.Contains(planned, object) {
			parts = append(parts, "the plan leaves out "+describeObject(object))
		}
	}
	for _, object := range planned {
		if !slices.Contains(listed, object) {
			parts = append(parts, "the provider does not list "+describeObject(object))
		}
	}
	return strings.Join(parts, "; ")
}

// describeObject names an object by its kind and where it is:
// `table "users"`, `topic "events" in schema "app"`,
// `foreign_key "fk" on table "posts" in schema "public"`.
func describeObject(object Object) string {
	return describe(object.Type, object.Schema, object.Table, object.Name)
}

func describe(kind, schema, table, name string) string {
	var described strings.Builder
	fmt.Fprintf(&described, "%s %q", kind, name)
	if table != "" {
		fmt.Fprintf(&described, " on table %q", table)
	}
	if schema != "" {
		fmt.Fprintf(&described, " in schema %q", schema)
	}
	return described.String()
}

// Summary is how a report names what the change removes: its drop statement,
// or, where the change carries none, the object's kind and where it is, as
// for an object a provider removes in its own way.
func (c Change) Summary() string {
	if strings.TrimSpace(c.Cmd) != "" {
		return c.Cmd
	}
	return describe(c.Type, c.Schema, c.Table, c.Name)
}
