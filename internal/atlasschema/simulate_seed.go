package atlasschema

import (
	"context"
	"errors"
	"slices"
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
	selected := len(schemascope.SplitNames(schemas)) > 0 ||
		slices.ContainsFunc(include, atlasfilter.NamesSelector) ||
		slices.ContainsFunc(exclude, atlasfilter.NamesSelector)
	bound := described(info.Dialect).Schemas.Selection == descriptor.SchemaSelectionSearchPath &&
		!ConnectionIsRealmScoped(info)
	if !selected && !bound {
		return rehearsalSeed{}
	}
	return rehearsalSeed{narrowed: true, leftOut: atlasfilter.KindsLeftOut(selections, exclude, include)}
}

// target returns the reader of the target conn reaches outside the compared
// scope, or nil when the compared state was not narrowed.
//
// On an engine that resolves names along a search path, the reader reads the
// schemas of the target's realm that a statement names, which includes the
// search path: a schema nothing names is not read, so a schema a selection
// left out because the reader cannot describe it stays unread unless a
// statement needs it. Any other engine reads what its URL reaches, once, and so
// does a connection whose writer offers no realm probe, such as a provider
// session serving a search-path engine's name: what lies outside its URL is
// then not recreated, and a statement that needs it fails in the rehearsal.
//
// A word a statement uses as a qualifier is often not a schema, such as a
// table alias, so a read asked for a new word that selects the same schemas
// returns the state it read before.
func (s rehearsalSeed) target(ctx context.Context, conn *dbschema.DatabaseConnection) devseed.Target {
	if !s.narrowed || conn == nil {
		return nil
	}
	ctx = readscope.Without(ctx, s.leftOut...)
	info := conn.Info()
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
