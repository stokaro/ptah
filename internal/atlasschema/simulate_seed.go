package atlasschema

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/selectext"
	"ptah.run/dbschema"
	"ptah.run/internal/atlasfilter"
	"ptah.run/internal/devseed"
	"ptah.run/internal/readscope"
	"ptah.run/internal/schemascope"
	"ptah.run/internal/schemaselection"
)

// rehearsalSeed is how a rehearsal reads what the target holds outside the
// state its plan compared, so the dev database recreates the objects there
// that the rehearsed statements need; see [devseed].
//
// The zero value reads nothing: the compared state is the target's whole
// realm, and the dev database is given that state alone.
type rehearsalSeed struct {
	// narrowed reports that the compared state leaves out part of what the
	// target's connection reaches: a selection named objects or schemas, or a
	// URL bound to one schema of an engine that resolves names along a search
	// path left the others unread.
	narrowed bool
	// leftOut are the object kinds the planning read left out whole, which
	// the reads outside the scope leave out too: a selection that names a
	// kind away keeps the target's reader off it.
	leftOut []readscope.Kind
}

// newRehearsalSeed is the seed of a rehearsal of a plan compared over the
// connection info describes, under the selection schemas, include and exclude
// name.
func newRehearsalSeed(info catalog.ServerInfo, selections selectext.Set, schemas, include, exclude []string) rehearsalSeed {
	selected := len(schemascope.UniqueNames(schemas)) > 0 ||
		slices.ContainsFunc(include, atlasfilter.NamesSelector) ||
		slices.ContainsFunc(exclude, atlasfilter.NamesSelector)
	bound := described(info.Dialect).Schemas.Selection == descriptor.SchemaSelectionSearchPath &&
		!ConnectionIsRealmScoped(info)
	if !selected && !bound {
		return rehearsalSeed{}
	}
	return rehearsalSeed{narrowed: true, leftOut: atlasfilter.KindsLeftOut(selections, exclude, include)}
}

// namespaceCarrier is the writer of a provider session, which reports
// whether its engine carries namespaces (schema.common-namespaces). The
// provider then binds each schema selector of a read to the schemas its
// engine names by it, and binds a selector that names none to nothing.
type namespaceCarrier interface {
	CarriesNamespaces() bool
}

// target returns the reader of the target conn reaches outside the compared
// scope, or nil when the compared state was not narrowed.
//
// On an engine that resolves names along a search path, the reader reads the
// schemas of the target's realm that a statement names, which includes the
// search path: a schema nothing names is not read, so a schema a selection
// left out because the reader cannot describe it stays unread unless a
// statement needs it. A provider session whose engine carries namespaces reads
// the same way without a realm list: each word a statement uses as a qualifier
// is a schema selector of the provider's read, beside the session's own
// schema, and the provider decides which schema a word names; see
// [devseed.Selected].
//
// Any other engine reads what its URL reaches, once, and so does a provider
// session whose engine carries no namespaces, whose read of its whole
// database is what its URL reaches. What lies outside it is then not
// recreated, and a statement that needs it fails in the rehearsal.
//
// A word a statement uses as a qualifier is often not a schema, such as a
// table alias. Matched against the realm, a read asked for a new word that
// selects the same schemas returns the state it read before; a provider is
// asked again, and binds a word that names no schema to nothing.
func (s rehearsalSeed) target(ctx context.Context, conn *dbschema.DatabaseConnection) devseed.Target {
	if !s.narrowed || conn == nil {
		return nil
	}
	ctx = readscope.Without(ctx, s.leftOut...)
	info := conn.Info()
	if carrier, ok := conn.SchemaWriter().(namespaceCarrier); ok {
		if carrier.CarriesNamespaces() {
			return providerSchemasTarget(ctx, conn)
		}
		return providerDatabaseTarget(ctx, conn)
	}
	var read *catalog.Database
	var readSchemas []string
	urlOnly := described(info.Dialect).Schemas.Selection != descriptor.SchemaSelectionSearchPath
	return func(named []string) (*catalog.Database, error) {
		var realm []string
		if !urlOnly {
			var err error
			realm, err = schemaselection.RealmSchemas(ctx, info.Dialect, info.Capabilities, conn)
			urlOnly = errors.Is(err, schemaselection.ErrNoRealmProbe)
			if err != nil && !urlOnly {
				return nil, err
			}
		}
		if urlOnly {
			if read != nil {
				return read, nil
			}
			var err error
			read, err = dbschema.ReadSchemaWithSchemasContext(ctx, conn, nil)
			return read, err
		}
		urlScope, err := schemascope.ReadNames(ctx, info, nil, conn)
		if err != nil {
			return nil, err
		}
		schemas := make([]string, 0, len(named))
		for _, schema := range realm {
			if slices.Contains(named, strings.ToLower(schema)) || slices.Contains(urlScope, schema) {
				schemas = append(schemas, schema)
			}
		}
		switch {
		case read != nil && slices.Equal(schemas, readSchemas):
			return read, nil
		case len(schemas) == 0:
			read = &catalog.Database{}
		default:
			if read, err = dbschema.ReadSchemaWithSchemasContext(ctx, conn, schemas); err != nil {
				return nil, err
			}
		}
		readSchemas = schemas
		return read, nil
	}
}

// providerSchemasTarget is [rehearsalSeed.target] over a provider session
// whose engine carries namespaces: it reads the schemas the statements name,
// with the session's own schema, through the provider's filtered inspection.
func providerSchemasTarget(ctx context.Context, conn *dbschema.DatabaseConnection) devseed.Target {
	return devseed.Selected(func(selectors []string) (*catalog.Database, error) {
		read, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, selectors)
		if err != nil {
			return nil, fmt.Errorf("read schemas %s of the target through its provider: %w", quotedList(selectors), err)
		}
		return read, nil
	}, schemascope.UniqueNames([]string{conn.Info().Schema}))
}

// providerDatabaseTarget is [rehearsalSeed.target] over a provider session
// whose engine carries no namespaces: it reads the whole database once, and a
// failed read says so, since a provider that cannot inspect every object of
// that database cannot be read for the few the rehearsal needs.
func providerDatabaseTarget(ctx context.Context, conn *dbschema.DatabaseConnection) devseed.Target {
	var read *catalog.Database
	return func([]string) (*catalog.Database, error) {
		if read != nil {
			return read, nil
		}
		whole, err := dbschema.ReadSchemaWithSchemasContext(ctx, conn, nil)
		if err != nil {
			return nil, fmt.Errorf("the provider of engine %q carries no namespaces (%s), so the rehearsal reads the target's whole database "+
				"to find what the planned statements need outside the compared scope, and that read failed: %w", conn.Info().Dialect, commonNamespacesFeature, err)
		}
		read = whole
		return read, nil
	}
}

// commonNamespacesFeature names the provider feature an engine carries its
// namespaces under, as a message names it.
const commonNamespacesFeature = "schema.common-namespaces"

// quotedList names each of names quoted, comma-separated.
func quotedList(names []string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, strconv.Quote(name))
	}
	return strings.Join(quoted, ", ")
}

// rehearsalOutside is what [recreateCurrentSchema] needs to add to the
// compared state what the rehearsal needs from outside it: the reader of the
// target there, nil when there is nothing outside, and the statements the dev
// database executes after the recreated state.
type rehearsalOutside struct {
	target   devseed.Target
	executed []string
}

// executedSQL is the SQL of the plan's units, in order; a native operation has
// none.
func (p ApplyPlan) executedSQL() []string {
	statements := make([]string, 0, len(p.units))
	for _, unit := range p.units {
		if unit.Native == nil && strings.TrimSpace(unit.SQL) != "" {
			statements = append(statements, unit.SQL)
		}
	}
	return statements
}
