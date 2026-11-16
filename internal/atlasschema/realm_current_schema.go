package atlasschema

import (
	"context"
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/dbschema"
	"ptah.run/internal/devdocker"
	"ptah.run/internal/planrefusal"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/schemaselection"
	"ptah.run/internal/sqlident"
)

// desiredNaming is the schema a realm-scoped run puts a desired name written
// without a schema in, and the schema another reading of the same name gives.
//
// The zero value places nothing: names stay relative to the connection, as on a
// URL that pins its schema or on an engine that resolves no search path.
type desiredNaming struct {
	// resolved records that the naming was decided for this run, so a
	// comparison made on another connection, such as the dev database's,
	// places names as the target's comparison did.
	resolved bool
	// schema is where an unqualified desired name is placed, empty for none.
	schema string
	// other is where the other reading puts it, empty when both readings
	// agree. On a pair whose dev database resolves the name elsewhere, it is
	// the target's current schema. Without a dev database it is the dialect's
	// default schema, which the community binary reaches through a dev
	// database created for the run.
	other string
	// fromDev is set when schema is the dev database's current schema.
	fromDev bool
}

// namingFor decides where a run over target places an unqualified desired name.
// dev is the dev database's connection, nil for a run without one.
//
// Only a realm-scoped connection on an engine that resolves names along a
// search path is placed: there one URL describes every schema of the
// database, and the name could mean any of them. The community binary reads
// the desired state on its dev database, so an unqualified name lands where
// that database resolves it, and it writes every statement of the plan
// qualified. Measured with the pinned community binary v1.3.0 on PostgreSQL 18,
// role `rebase` whose search_path is the default `"$user", public`, a target
// holding schema `rebase` (current_schema() is `rebase`) and a dev database
// without it (`public`): `CREATE TABLE notes` is planned as `CREATE TABLE
// "public"."notes"` by `schema apply`, `schema diff` and `migrate diff`, and with
// `public.notes` already on the target, nothing is planned for the table.
//
// A run with a dev database that pins its schema keeps the target's reading:
// the rehearsal refuses a pin that differs from the target's current schema.
func namingFor(target catalog.ServerInfo, dev *catalog.ServerInfo) desiredNaming {
	naming := desiredNaming{resolved: true}
	if !placesUnqualifiedNames(target) {
		return naming
	}
	current := defaultSchemaOf(target)
	naming.schema = current
	switch {
	case dev == nil:
		if fallback := dialectDefaultSchema(target.Dialect); current != fallback {
			naming.other = fallback
		}
	case ConnectionIsRealmScoped(*dev) && strings.TrimSpace(dev.Schema) != "":
		naming.schema, naming.fromDev = dev.Schema, true
		if dev.Schema != current {
			naming.other = current
		}
	}
	return naming
}

// placesUnqualifiedNames reports whether a run over info places a desired name
// written without a schema; see [namingFor].
func placesUnqualifiedNames(info catalog.ServerInfo) bool {
	return described(info.Dialect).Schemas.Selection == descriptor.SchemaSelectionSearchPath &&
		ConnectionIsRealmScoped(info)
}

// resolveDesiredNaming is [namingFor] for a run whose dev database is named by
// devURL and not yet connected, empty for a run without one.
//
// The plan is computed before the rehearsal connects to the dev database, so a
// dev URL that addresses a database directly is connected here to ask its
// current schema. A dev database the run provisions, such as a docker:// one,
// is created empty and owned by the server's own role, so it resolves a name
// to the dialect's default schema; the rehearsal checks that answer once it
// connects. A dev URL that pins search_path keeps the target's reading, as
// namingFor does.
func resolveDesiredNaming(ctx context.Context, target catalog.ServerInfo, devURL string) (desiredNaming, error) {
	devURL = strings.TrimSpace(devURL)
	if devURL == "" || !placesUnqualifiedNames(target) {
		return namingFor(target, nil), nil
	}
	if _, pinned := schemaselection.URLScope(devURL); pinned {
		return namingFor(target, &catalog.ServerInfo{Dialect: target.Dialect, URL: devURL}), nil
	}
	if devdocker.ResolvedPerRun(ctx, devURL) {
		return namingFor(target, &catalog.ServerInfo{Dialect: target.Dialect, Schema: dialectDefaultSchema(target.Dialect)}), nil
	}
	devConn, err := dbschema.ConnectToDatabase(ctx, devURL)
	if err != nil {
		return desiredNaming{}, fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(devConn)
	info := devConn.Info()
	return namingFor(target, &info), nil
}

// PlaceUnqualifiedNames returns desired with every name it writes without a
// schema placed in the current schema of the connection info describes, when
// that connection describes every schema of its database and the engine
// resolves a name along a search path, and desired as it is otherwise. A
// comparison on that connection then reads each name as the schema it names,
// and the statements it plans write the schema, as `schema apply` writes them;
// see [namingFor]. desired is not modified.
func PlaceUnqualifiedNames(info catalog.ServerInfo, desired *schemamodel.Database) (*schemamodel.Database, error) {
	return namingFor(info, nil).place(desired, info.Dialect)
}

// place returns desired with every name it writes without a schema placed where
// naming says; see [schemaprep.PlaceUnqualified].
func (n desiredNaming) place(desired *schemamodel.Database, dialect string) (*schemamodel.Database, error) {
	return schemaprep.PlaceUnqualified(desired, n.schema, dialect)
}

// checkDevNaming refuses a rehearsal on a dev database that resolves an
// unqualified name elsewhere than the plan placed it, which can happen only
// when the plan read the dev database's current schema before the rehearsal
// connected to it; see [resolveDesiredNaming].
func checkDevNaming(planned desiredNaming, target, dev catalog.ServerInfo) error {
	if !planned.resolved || !planned.fromDev {
		return nil
	}
	if found := namingFor(target, &dev); found.schema != planned.schema {
		return fmt.Errorf(
			"the plan places a name the desired schema writes without a schema in %q, where the dev database "+
				"resolved it, and the dev database now resolves it to %q; plan again",
			planned.schema, found.schema,
		)
	}
	return nil
}

// unqualifiedTableNames is the set of tables desired names without a schema,
// read before [desiredNaming.place] gives each one.
func unqualifiedTableNames(desired *schemamodel.Database) map[string]bool {
	if desired == nil {
		return nil
	}
	names := make(map[string]bool)
	for _, table := range desired.Tables {
		if strings.TrimSpace(table.Schema) == "" {
			names[table.Name] = true
		}
	}
	return names
}

// refuseUnqualifiedTableMoves refuses a plan that creates a table the desired
// state names without a schema and drops the table of that name the other
// reading of the name would have kept.
//
// An unqualified desired name has two readings on a realm-scoped connection
// whose current schema differs from where the name is placed; see [namingFor].
// The plan follows one of them. Where the other reading names a table the
// target holds, the plan creates the table in the placed schema and drops the
// one the other reading kept, with its rows. Two cases remain:
//
//   - With a dev database, the name is placed where the dev database resolves
//     it, `public`, and the target holds `rebase.notes` in its current schema.
//     The community binary plans the same move, and drops the whole schema
//     `rebase` with it, since its desired state does not hold the schema.
//   - Without one, the name is placed in the target's current schema,
//     `rebase`, and the target holds `public.notes`, where the community
//     binary, with a dev database created for the run, reads the name.
//
// Either plan loses rows the other reading keeps, so it is refused, and the
// message names the qualified spellings that settle it. A desired table
// written with a schema, a URL that pins its schema, and a run whose two
// readings agree are not affected.
func refuseUnqualifiedTableMoves(
	info catalog.ServerInfo,
	naming desiredNaming,
	unqualified map[string]bool,
	diff *difftypes.SchemaDiff,
) error {
	if diff == nil || naming.other == "" || len(diff.TablesAdded) == 0 || len(diff.TablesRemoved) == 0 {
		return nil
	}
	current := defaultSchemaOf(info)
	removed := make(map[string]bool, len(diff.TablesRemoved))
	for _, removal := range diff.TablesRemoved {
		if !removal.Current.HasTable() {
			continue
		}
		table := removal.Current.Table
		if effectiveSchemaOf(table.Schema, current) == naming.other {
			removed[table.Name] = true
		}
	}
	for _, creation := range diff.TablesAdded {
		name := creation.Table.Name
		if !unqualified[name] || !removed[name] || effectiveSchemaOf(creation.Table.Schema, current) != naming.schema {
			continue
		}
		kept := sqlident.QualifiedIdent(info.Dialect, naming.other, name)
		if !naming.fromDev {
			return &planrefusal.DeclarationError{Err: fmt.Errorf(
				"the desired schema names table %q without a schema, which this connection resolves to its current schema %q, "+
					"and the plan would create it there and drop %s with its rows; write the table as %s, "+
					"or pin search_path in the URL",
				name, naming.schema, kept, kept,
			)}
		}
		return &planrefusal.DeclarationError{Err: fmt.Errorf(
			"the desired schema names table %q without a schema, which the dev database resolves to %q and this connection "+
				"to its current schema %q, and the plan would create it in %q and drop %s with its rows; "+
				"write the table as %s to keep it, or as %s to move it",
			name, naming.schema, naming.other, naming.schema, kept, kept,
			sqlident.QualifiedIdent(info.Dialect, naming.schema, name),
		)}
	}
	return nil
}

// effectiveSchemaOf is schema, or current for an object a read relative to
// current left unqualified.
func effectiveSchemaOf(schema, current string) string {
	if strings.TrimSpace(schema) == "" {
		return current
	}
	return schema
}
