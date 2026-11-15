package atlasschema

import (
	"fmt"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/descriptor"
	"ptah.run/internal/planrefusal"
	"ptah.run/internal/sqlident"
)

// refuseUnqualifiedTableMoves refuses a plan over a realm-scoped connection
// whose current schema is not the dialect's default schema when it creates a
// table the desired state names without a schema and drops a table of that
// name from another schema of the database.
//
// An unqualified desired name has two readings there, and the plan follows
// one of them. The connection resolves it to its current schema: on
// PostgreSQL 18, a role `rebase` with the default search_path `"$user",
// public` resolves to `rebase` in a database that holds that schema. The
// desired state's own loader, and the community binary with a dev database
// whose current schema is `public`, read it as `public`. Measured with a
// target holding `public.notes` and a file declaring `CREATE TABLE notes`:
// the pinned community binary v1.3.0 plans nothing for the table, and the
// plan here would create `notes` in `rebase` and drop `public.notes` with its
// rows. A plan that drops a table in order to create one of the same name
// elsewhere is the case where the two readings part and data is lost, so it is
// refused, and the message names the qualified spelling that settles it.
//
// A desired table written with a schema, even the current one, a connection
// whose URL pins its schema, and a connection whose current schema is the
// default are not affected: each has one reading.
func refuseUnqualifiedTableMoves(info catalog.ServerInfo, diff *difftypes.SchemaDiff) error {
	if diff == nil || len(diff.TablesAdded) == 0 || len(diff.TablesRemoved) == 0 {
		return nil
	}
	if described(info.Dialect).Schemas.Selection != descriptor.SchemaSelectionSearchPath ||
		!ConnectionIsRealmScoped(info) {
		return nil
	}
	current := defaultSchemaOf(info)
	if current == "" || current == dialectDefaultSchema(info.Dialect) {
		return nil
	}
	removed := make(map[string]string, len(diff.TablesRemoved))
	for _, removal := range diff.TablesRemoved {
		if !removal.Current.HasTable() {
			continue
		}
		table := removal.Current.Table
		if table.Schema == "" || table.Schema == current {
			continue
		}
		removed[table.Name] = table.Schema
	}
	for _, creation := range diff.TablesAdded {
		if creation.Table.Schema != "" {
			continue
		}
		schema, dropped := removed[creation.Table.Name]
		if !dropped {
			continue
		}
		return &planrefusal.DeclarationError{Err: fmt.Errorf(
			"the desired schema names table %q without a schema, which this connection resolves to its current schema %q, "+
				"and the plan would create it there and drop %s with its rows; write the table as %s, "+
				"or pin search_path in the URL",
			creation.Table.Name, current,
			sqlident.QualifiedIdent(info.Dialect, schema, creation.Table.Name),
			sqlident.QualifiedIdent(info.Dialect, schema, creation.Table.Name),
		)}
	}
	return nil
}
