package sqlschema

import (
	"context"
	"fmt"
	"slices"

	"ptah.run/core/ast"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/deporder"
)

// viewDrop is one DROP VIEW or DROP MATERIALIZED VIEW.
type viewDrop struct {
	statement    string
	written      string
	materialized bool
	ifExists     bool
	cascade      bool
}

// applyDropView removes the view a DROP VIEW names, with what the server removes
// along with it: the triggers on it, the grants on it, and the objects an owner
// attached to it. A schema file is a script the server runs in order, so a view it
// creates and then drops is not in the schema, as with [applyDropTable].
func applyDropView(ctx context.Context, database *schemamodel.Database, document *Document, node *ast.DropViewNode, sourcePlatform string) error {
	return applyViewDrop(ctx, database, document, viewDrop{
		statement: "DROP VIEW", written: node.Name, ifExists: node.IfExists, cascade: node.Cascade,
	}, sourcePlatform)
}

// applyDropMaterializedView is [applyDropView] for a materialized view, whose indexes
// go with it.
func applyDropMaterializedView(ctx context.Context, database *schemamodel.Database, document *Document, node *ast.DropMaterializedViewNode, sourcePlatform string) error {
	return applyViewDrop(ctx, database, document, viewDrop{
		statement: "DROP MATERIALIZED VIEW", written: node.Name, materialized: true, ifExists: node.IfExists, cascade: node.Cascade,
	}, sourcePlatform)
}

// applyViewDrop refuses what the servers refuse, before anything is removed. Each
// answer was measured on PostgreSQL 18, YugabyteDB 2026.1, CockroachDB 26.3, MySQL
// 26.10, MariaDB 12.3, ClickHouse 26.10, SQL Server 2025, SQLite 3.51 and the Spanner
// emulator:
//
//   - a name nothing holds, unless the statement says IF EXISTS;
//   - a name a table or the other kind of view holds. IF EXISTS does not help, except
//     where the descriptor says the server skips the statement;
//   - where a view blocks the drop of what it reads, a view or a materialized view
//     another one still reads.
//
// CASCADE is refused as it is on DROP TABLE: it drops objects the file does not list.
// On ClickHouse, which has no DROP MATERIALIZED VIEW, DROP VIEW reaches a materialized
// view too.
func applyViewDrop(ctx context.Context, database *schemamodel.Database, document *Document, drop viewDrop, sourcePlatform string) error {
	if drop.cascade {
		return fmt.Errorf(
			"%s %s CASCADE also drops the objects that depend on the view, which a schema file does not list; drop them by name",
			drop.statement, drop.written)
	}
	databases := declaredIn(database, document)
	declared := declaredViews(databases, sourcePlatform)
	name := unqualified(normalizeSQLTableReference(sourcePlatform, drop.written), sourcePlatform)
	index := resolveDeclaredName(sourcePlatform, name, declared.names)
	target := drop.reaches(declared, index, sourcePlatform)
	switch {
	case target:
		name = declared.names[index]
	case !drop.skipsHolder(sourcePlatform) && holdsRelation(databases, name, sourcePlatform):
		return fmt.Errorf("%s %s: the schema declares %s as another kind of object", drop.statement, drop.written, drop.written)
	case drop.ifExists:
		return nil
	default:
		return undeclared(fmt.Errorf("%s %s names a view this schema does not declare", drop.statement, drop.written))
	}
	if reader := viewReader(databases, name, sourcePlatform); reader != "" {
		return fmt.Errorf("%s %s: %s still reads the view", drop.statement, drop.written, reader)
	}
	reaches := func(other string) bool {
		return resolveDeclaredName(sourcePlatform, unqualified(normalizeSQLTableReference(sourcePlatform, other), sourcePlatform), []string{name}) == 0
	}
	if err := detachFrom(ctx, document, databases, func(schema, table string) bool {
		return reaches(schemamodel.QualifyTableName(schema, table))
	}, sourcePlatform); err != nil {
		return fmt.Errorf("%s %s: %w", drop.statement, drop.written, err)
	}
	removeView(databases, reaches)
	return nil
}

// reaches reports whether the declared view at index is one this statement drops.
func (d viewDrop) reaches(declared declaredViewNames, index int, sourcePlatform string) bool {
	if index < 0 {
		return false
	}
	if d.materialized == declared.materialized[index] {
		return true
	}
	return !d.materialized && !hasDropForm(sourcePlatform, descriptor.DropKindMaterializedView)
}

// skipsHolder reports that the server skips this statement when it says IF EXISTS and
// another kind of object holds the name.
func (d viewDrop) skipsHolder(sourcePlatform string) bool {
	return d.ifExists && factsOf(sourcePlatform).statements.DropViewIfExistsSkipsOtherKinds
}

// hasDropForm reports whether the grammar of a dialect states DROP of kind.
func hasDropForm(sourcePlatform, kind string) bool {
	return slices.ContainsFunc(factsOf(sourcePlatform).grammarDrops, func(drop descriptor.Drop) bool { return drop.Kind == kind })
}

// declaredViewNames lists the views and the materialized views of a document, each
// name without the default schema.
type declaredViewNames struct {
	names        []string
	materialized []bool
}

func declaredViews(databases []*schemamodel.Database, sourcePlatform string) declaredViewNames {
	var declared declaredViewNames
	for _, database := range databases {
		for _, view := range database.Views {
			declared.names = append(declared.names, unqualified(view.Name, sourcePlatform))
			declared.materialized = append(declared.materialized, false)
		}
		for _, view := range database.MaterializedViews {
			declared.names = append(declared.names, unqualified(view.Name, sourcePlatform))
			declared.materialized = append(declared.materialized, true)
		}
	}
	return declared
}

// viewReader names the first view that keeps the server from dropping the view, or
// returns "". The view does not keep itself.
func viewReader(databases []*schemamodel.Database, name, sourcePlatform string) string {
	if !factsOf(sourcePlatform).statements.ViewsBlockDropTable {
		return ""
	}
	bare := name
	if _, table := normalizeSQLTableIdentifier(sourcePlatform, name); table != "" {
		bare = table
	}
	for _, database := range databases {
		for _, view := range database.Views {
			if unqualified(view.Name, sourcePlatform) != name && deporder.ReferencesIdentifier(view.Body, bare) {
				return fmt.Sprintf("view %s", view.Name)
			}
		}
		for _, view := range database.MaterializedViews {
			if unqualified(view.Name, sourcePlatform) != name && deporder.ReferencesIdentifier(view.Body, bare) {
				return fmt.Sprintf("materialized view %s", view.Name)
			}
		}
	}
	return ""
}

// removeView takes the view and what belongs to it out of every database.
func removeView(databases []*schemamodel.Database, reaches func(string) bool) {
	for _, database := range databases {
		database.Views = slices.DeleteFunc(database.Views, func(view schemamodel.View) bool { return reaches(view.Name) })
		database.MaterializedViews = slices.DeleteFunc(database.MaterializedViews, func(view schemamodel.MaterializedView) bool {
			return reaches(view.Name)
		})
		database.Triggers = slices.DeleteFunc(database.Triggers, func(trigger schemamodel.Trigger) bool {
			return trigger.Table != "" && reaches(trigger.Table)
		})
		database.Indexes = slices.DeleteFunc(database.Indexes, func(index schemamodel.Index) bool {
			return index.TableName != "" && reaches(index.TableName)
		})
		database.Grants = slices.DeleteFunc(database.Grants, func(grant schemamodel.Grant) bool {
			return grant.OnTable != "" && reaches(grant.OnTable)
		})
	}
}
