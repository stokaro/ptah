package mysql

import (
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/internal/planner/schemaprecondition"
	"ptah.run/internal/sqlident"
	"ptah.run/migration/schemadiff/difftypes"
)

// planServerSchemas creates and changes the databases a comparison of a whole
// MySQL or MariaDB server added or changed, before anything is created in
// them. The comparison records them only for a connection that selected no
// database (stokaro/ptah#3789); every other diff carries none, and this plans
// nothing.
//
// A database is created with IF NOT EXISTS and the character set and
// collation the declaration gives it, through [schemaprecondition.Node], the
// creation every planner builds. The pinned community binary v1.3.0 writes
// `CREATE DATABASE` without the guard; the two create the same database.
func (p *Planner) planServerSchemas(result []ast.Node, diff *difftypes.SchemaDiff) ([]ast.Node, error) {
	if !p.plansServerSchemas() {
		return result, schemaprecondition.RefuseServerSchemas(p.targetDialect(), diff)
	}
	semantics := diff.EffectiveIdentifierSemantics(p.targetDialect())
	for _, schema := range diff.SchemasAdded {
		result = append(result, schemaprecondition.Node(schema.Name, diff.SchemasAdded, semantics))
	}
	for _, change := range diff.SchemasModified {
		result = append(result, alterDatabaseNode(change))
	}
	return result, nil
}

// removeServerSchemas drops the databases a comparison of a whole server
// removed, after every other removal. The statements that drop their objects
// one by one run first, which is where a foreign key another database keeps
// into one of them is dropped; the pinned community binary v1.3.0 drops the
// databases alone, and its `schema clean` stops at the first database another
// database's foreign key references, measured on MySQL 8.4.11:
// `Error 3730 (HY000): Cannot drop table 't' referenced by a foreign key`.
func (p *Planner) removeServerSchemas(result []ast.Node, diff *difftypes.SchemaDiff) []ast.Node {
	if !p.plansServerSchemas() {
		return result
	}
	for _, name := range diff.SchemasRemoved {
		result = append(result, ast.NewRawSQL("DROP DATABASE "+sqlident.Quote(platform.MySQL, name)))
	}
	return result
}

// plansServerSchemas reports whether this planner writes databases, which
// MySQL and MariaDB do and the other dialects this planner serves do not.
func (p *Planner) plansServerSchemas() bool {
	switch p.targetDialect() {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

// alterDatabaseNode sets the character set and collation change declares on
// its database, the attributes it declares and nothing else.
func alterDatabaseNode(change difftypes.SchemaChange) ast.Node {
	parts := []string{"ALTER DATABASE", sqlident.Quote(platform.MySQL, change.Name)}
	if change.Charset != "" {
		parts = append(parts, "CHARACTER SET", change.Charset)
	}
	if change.Collate != "" {
		parts = append(parts, "COLLATE", change.Collate)
	}
	return ast.NewRawSQL(strings.Join(parts, " "))
}
