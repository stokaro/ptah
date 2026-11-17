package atlasschema

import (
	"context"
	"fmt"
	"slices"
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
// the desired schema on its dev database, so an unqualified name lands where
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

// resolveDevNaming is where a run that compares two documents places an
// unqualified name: the current schema of the dev database devURL names, when
// that URL describes every schema of its database and the engine resolves a
// name along a search path, and nowhere otherwise. A dev database the run
// provisions resolves a name to the dialect's default schema, as in
// [resolveDesiredNaming].
func resolveDevNaming(ctx context.Context, dialect, devURL string) (desiredNaming, error) {
	dev := catalog.ServerInfo{Dialect: dialect, URL: strings.TrimSpace(devURL)}
	if dev.URL == "" || !placesUnqualifiedNames(dev) {
		return namingFor(dev, nil), nil
	}
	if devdocker.ResolvedPerRun(ctx, dev.URL) {
		return desiredNaming{resolved: true, schema: dialectDefaultSchema(dialect)}, nil
	}
	devConn, err := dbschema.ConnectToDatabase(ctx, dev.URL)
	if err != nil {
		return desiredNaming{}, fmt.Errorf("connect to --dev-url: %w", err)
	}
	defer dbschema.CloseAndWarn(devConn)
	return namingFor(devConn.Info(), nil), nil
}

// recordedNaming is the naming a saved plan recorded, placed, and false when
// the plan recorded none or a run over target places no name.
func recordedNaming(target catalog.ServerInfo, placedSchema string) (desiredNaming, bool) {
	placedSchema = strings.TrimSpace(placedSchema)
	if placedSchema == "" || !placesUnqualifiedNames(target) {
		return desiredNaming{}, false
	}
	return desiredNaming{resolved: true, schema: placedSchema}, true
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

// held returns database holding the schema naming places names in, as a
// database read on that connection does, and database as it is when it
// declares that schema already or naming places nothing. database is not
// modified.
func (n desiredNaming) held(database *schemamodel.Database) *schemamodel.Database {
	if database == nil || n.schema == "" ||
		slices.ContainsFunc(database.Schemas, func(schema schemamodel.Schema) bool { return schema.Name == n.schema }) {
		return database
	}
	holding := *database
	holding.Schemas = append(slices.Clone(database.Schemas), schemamodel.Schema{Name: n.schema})
	return &holding
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

// unqualifiedNames are the tables and sequences desired names without a
// schema, read before [desiredNaming.place] gives each one. Those are the
// objects that hold something a drop loses: a table its rows, a sequence its
// current value.
type unqualifiedNames struct {
	tables    map[string]bool
	sequences map[string]bool
}

// unqualifiedNamesOf collects the [unqualifiedNames] of desired.
func unqualifiedNamesOf(desired *schemamodel.Database) unqualifiedNames {
	names := unqualifiedNames{tables: make(map[string]bool), sequences: make(map[string]bool)}
	if desired == nil {
		return names
	}
	for _, table := range desired.Tables {
		if strings.TrimSpace(table.Schema) == "" {
			names.tables[table.Name] = true
		}
	}
	for _, sequence := range desired.Sequences {
		if strings.TrimSpace(sequence.Schema) == "" {
			names.sequences[sequence.Name] = true
		}
	}
	return names
}

// refuseUnqualifiedMoves refuses a plan that creates a table or a sequence the
// desired schema names without a schema and drops the object of that name the
// other reading of the name would have kept.
//
// An unqualified desired name has two readings on a realm-scoped connection
// whose current schema differs from where the name is placed; see [namingFor].
// The plan follows one of them. Where the other reading names an object the
// target holds, the plan creates the object in the placed schema and drops the
// one the other reading kept, with its rows or its current value. Two cases
// remain:
//
//   - With a dev database, the name is placed where the dev database resolves
//     it, `public`, and the target holds `rebase.notes` in its current schema.
//     The community binary plans the same move, and drops the whole schema
//     `rebase` with it, since its desired schema does not hold the schema.
//   - Without one, the name is placed in the target's current schema,
//     `rebase`, and the target holds `public.notes`, where the community
//     binary, with a dev database created for the run, reads the name.
//
// Either plan loses what the other reading keeps, so it is refused, and the
// message names the qualified spellings that settle it. An object written with
// a schema, a URL that pins its schema, and a run whose two readings agree are
// not affected. A view, a function or a type holds nothing a drop loses, and
// its move is planned as the placement reads it.
func refuseUnqualifiedMoves(
	info catalog.ServerInfo,
	naming desiredNaming,
	unqualified unqualifiedNames,
	diff *difftypes.SchemaDiff,
) error {
	if diff == nil || naming.other == "" {
		return nil
	}
	current := defaultSchemaOf(info)
	tablesCreated := make([]schemaName, 0, len(diff.TablesAdded))
	for _, creation := range diff.TablesAdded {
		tablesCreated = append(tablesCreated, schemaName{schema: creation.Table.Schema, name: creation.Table.Name})
	}
	tablesDropped := make([]schemaName, 0, len(diff.TablesRemoved))
	for _, removal := range diff.TablesRemoved {
		if removal.Current.HasTable() {
			tablesDropped = append(tablesDropped, schemaName{schema: removal.Current.Table.Schema, name: removal.Current.Table.Name})
		}
	}
	if name, ok := movedName(naming, current, unqualified.tables, tablesCreated, tablesDropped); ok {
		return unqualifiedMoveError(info, naming, "table", "its rows", name)
	}
	sequencesCreated := make([]schemaName, 0, len(diff.SequencesAdded))
	for _, sequence := range diff.SequencesAdded {
		sequencesCreated = append(sequencesCreated, schemaName{schema: sequence.Schema, name: sequence.Name})
	}
	sequencesDropped := make([]schemaName, 0, len(diff.SequencesRemoved))
	for _, sequence := range diff.SequencesRemoved {
		sequencesDropped = append(sequencesDropped, schemaName{schema: sequence.Schema, name: sequence.Name})
	}
	if name, ok := movedName(naming, current, unqualified.sequences, sequencesCreated, sequencesDropped); ok {
		return unqualifiedMoveError(info, naming, "sequence", "its current value", name)
	}
	return nil
}

// schemaName is an object of a diff by its schema, empty for one a read
// relative to the current schema left unqualified, and its name.
type schemaName struct {
	schema, name string
}

// movedName is the first object of created that unqualified holds, that the
// plan creates in the placed schema, and whose name the plan drops in the
// other reading's schema.
func movedName(naming desiredNaming, current string, unqualified map[string]bool, created, dropped []schemaName) (string, bool) {
	if len(created) == 0 || len(dropped) == 0 {
		return "", false
	}
	removed := make(map[string]bool, len(dropped))
	for _, object := range dropped {
		if effectiveSchemaOf(object.schema, current) == naming.other {
			removed[object.name] = true
		}
	}
	for _, object := range created {
		if unqualified[object.name] && removed[object.name] && effectiveSchemaOf(object.schema, current) == naming.schema {
			return object.name, true
		}
	}
	return "", false
}

// unqualifiedMoveError is the refusal of [refuseUnqualifiedMoves] for an
// object of kind, which loses what lost says when it is dropped.
func unqualifiedMoveError(info catalog.ServerInfo, naming desiredNaming, kind, lost, name string) error {
	kept := sqlident.QualifiedIdent(info.Dialect, naming.other, name)
	if !naming.fromDev {
		return &planrefusal.DeclarationError{Err: fmt.Errorf(
			"the desired schema names %s %q without a schema, which this connection resolves to its current schema %q, "+
				"and the plan would create it there and drop %s with %s; write the %s as %s, "+
				"or pin search_path in the URL",
			kind, name, naming.schema, kept, lost, kind, kept,
		)}
	}
	return &planrefusal.DeclarationError{Err: fmt.Errorf(
		"the desired schema names %s %q without a schema, which the dev database resolves to %q and this connection "+
			"to its current schema %q, and the plan would create it in %q and drop %s with %s; "+
			"write the %s as %s to keep it, or as %s to move it",
		kind, name, naming.schema, naming.other, naming.schema, kept, lost, kind, kept,
		sqlident.QualifiedIdent(info.Dialect, naming.schema, name),
	)}
}

// effectiveSchemaOf is schema, or current for an object a read relative to
// current left unqualified.
func effectiveSchemaOf(schema, current string) string {
	if strings.TrimSpace(schema) == "" {
		return current
	}
	return schema
}
