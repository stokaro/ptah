package sqlschema

import (
	"ptah.run/core/schemamodel"
)

// indexRelation is the table or materialized view an index belongs to: its
// name as the reader stores it, schema included, its schema, empty for the
// default, and the table, nil for a materialized view.
//
// Every statement that names an index or the relation it is on resolves the
// relation by one rule: CREATE INDEX and COMMENT ON INDEX through
// [indexHolder], DROP INDEX and ALTER INDEX through [findNamedIndex], which
// finds a table's index by [indexOwner], the table half of indexHolder, and a
// view's index by indexHolder itself. A second rule for any of them would let
// a statement find an index the others cannot, or land an index on a relation
// the others do not look in.
type indexRelation struct {
	qualified, schema string
	table             *schemamodel.Table
}

// indexHolder answers the relation index belongs to: a table, found by the
// rule [indexOwner] gives, or a materialized view, which PostgreSQL indexes as
// it indexes a table. Each is found in the schema the index's table name
// spells, the default one when the name is bare. An index whose table names
// neither answers false.
func indexHolder(databases []*schemamodel.Database, index schemamodel.Index, sourcePlatform string) (indexRelation, bool) {
	if table := indexOwner(databases, index, sourcePlatform); table != nil {
		return indexRelation{qualified: table.QualifiedName(), schema: table.Schema, table: table}, true
	}
	var views, names []string
	for _, database := range databases {
		for _, view := range database.MaterializedViews {
			views = append(views, view.Name)
			names = append(names, unqualified(view.Name, sourcePlatform))
		}
	}
	at := resolveDeclaredName(sourcePlatform, unqualified(normalizeSQLTableReference("", index.TableName), sourcePlatform), names)
	if at < 0 {
		return indexRelation{}, false
	}
	schema, view := normalizeSQLTableIdentifier("", views[at])
	return indexRelation{qualified: schemamodel.QualifyTableName(schema, view), schema: schema}, true
}

// relationElsewhere answers a table or materialized view whose name, without
// its schema, is the one table carries. It is asked only once [indexHolder]
// found nothing for table, so the relation it answers is in another schema:
// the one a match by name alone would take.
func relationElsewhere(databases []*schemamodel.Database, table, sourcePlatform string) (string, bool) {
	_, name := normalizeSQLTableIdentifier("", table)
	var qualified, names []string
	for _, database := range databases {
		for i := range database.Tables {
			qualified = append(qualified, database.Tables[i].QualifiedName())
			names = append(names, database.Tables[i].Name)
		}
		for _, view := range database.MaterializedViews {
			_, bare := normalizeSQLTableIdentifier("", view.Name)
			qualified = append(qualified, view.Name)
			names = append(names, bare)
		}
	}
	at := resolveDeclaredName(sourcePlatform, name, names)
	if at < 0 {
		return "", false
	}
	return qualified[at], true
}
