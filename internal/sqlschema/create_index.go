package sqlschema

import (
	"fmt"

	"ptah.run/core/ast"
	"ptah.run/core/schemamodel"
)

// appendIndex records the index a CREATE INDEX declares, on the relation the
// statement names as the server resolves it: a table or a materialized view of
// the schema the name spells, the default one when the name is bare; see
// [indexHolder]. Measured on PostgreSQL 18.6, `CREATE INDEX mv_a ON mv (a)`
// beside a table s2.mv and a materialized view mv indexes the view.
//
// The index records the name the reader stores for that relation. Left as
// written, a bare name is matched by its name alone when the read is
// finalized, so it reaches a table of that name in any schema, and the index
// above lands on s2.mv.
//
// A name its schema does not hold is refused when another schema holds it,
// since that relation is the one the match by name would take. PostgreSQL
// 18.6 answers `relation "t" does not exist` to `CREATE INDEX t_a ON t (a)`
// beside only s2.t. A name no schema of the document holds is kept as
// written: the table may come from another source of the load.
func appendIndex(database *schemamodel.Database, document *Document, node *ast.IndexNode, sourcePlatform string) error {
	index := ToIndex(node, sourcePlatform)
	databases := declaringDatabases(database, document.base)
	if holder, found := indexHolder(databases, index, sourcePlatform); found {
		index.TableName = holder.qualified
	} else if other, held := relationElsewhere(databases, index.TableName, sourcePlatform); held {
		return undeclared(fmt.Errorf("CREATE INDEX %s ON %s names a relation its schema does not declare; %s is in another schema",
			node.Name, node.Table, other))
	}
	database.Indexes = append(database.Indexes, index)
	if node.Only {
		document.noteOnlyIndex(index, node, databases)
	}
	return nil
}
