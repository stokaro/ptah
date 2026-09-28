package sqlschema

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/pgname"
)

// applyDropIndex removes the index a DROP INDEX names.
//
// Measured on MySQL 8.4.11 and PostgreSQL 18.6, `CREATE INDEX ix ON c (x);
// DROP INDEX ix ...` leaves c without ix, and Atlas CE v1.3.0 reports the file
// synced with the database it builds (stokaro/ptah#3876). The engines differ
// in what the statement may name:
//
//   - MySQL and MariaDB name the table, `DROP INDEX ix ON c`, and the
//     statement is their `ALTER TABLE c DROP INDEX ix`: it drops an index, or
//     a UNIQUE, which is an index there.
//   - The other engines drop an index alone. The index behind a constraint is
//     refused: PostgreSQL answers `cannot drop index uq because constraint uq
//     on table c requires it`, and the model says which constraint instead.
//   - PostgreSQL and SQLite name no table, and look the index up in its
//     schema, the default one when the name is bare: an index of `app.c` is
//     not found by `DROP INDEX ix`, as the server does not find it.
//
// A name nothing holds is refused unless the statement says IF EXISTS, and
// CASCADE is refused as it is on DROP TABLE.
func applyDropIndex(database, base *schemamodel.Database, document *Document, node *ast.DropIndexNode, sourcePlatform string) error {
	if node.Cascade {
		return fmt.Errorf(
			"DROP INDEX %s CASCADE also drops the objects that depend on the index, "+
				"which a schema file does not list; drop them by name",
			node.Name)
	}
	if node.Table != "" {
		return dropIndexOfTable(database, document, node, sourcePlatform)
	}
	databases := []*schemamodel.Database{database}
	if base != nil {
		databases = append(databases, base)
	}
	schema, name := normalizeSQLTableIdentifier(sourcePlatform, node.Name)
	for _, owner := range databases {
		for i, index := range owner.Indexes {
			table := indexTable(databases, index, sourcePlatform)
			if index.Name != name || table == nil || !sameSchema(sourcePlatform, schema, table.Schema) {
				continue
			}
			owner.Indexes = slices.Delete(owner.Indexes, i, i+1)
			return nil
		}
	}
	if constraint := constraintIndexNamed(databases, sourcePlatform, schema, name); constraint != "" {
		return fmt.Errorf("DROP INDEX %s: the index enforces %s; drop the constraint instead", node.Name, constraint)
	}
	if node.IfExists {
		return nil
	}
	return fmt.Errorf("DROP INDEX %s names an index this schema does not declare", node.Name)
}

// dropIndexOfTable is [applyDropIndex] for a statement that names the table.
//
// On MySQL and MariaDB the statement is `ALTER TABLE c DROP INDEX ix`, and it
// is read as that statement, so the two spellings cannot disagree about what
// they drop. Elsewhere it drops an index the table declares, and refuses the
// index behind a constraint.
func dropIndexOfTable(database *schemamodel.Database, document *Document, node *ast.DropIndexNode, sourcePlatform string) error {
	target, declared := findAlterTarget(database, document.base, node.Table, sourcePlatform)
	if !declared {
		return fmt.Errorf("DROP INDEX %s ON %s names a table this schema does not declare", node.Name, node.Table)
	}
	if isMySQLFamily(sourcePlatform) {
		alter := &ast.AlterTableNode{
			Name: node.Table,
			Operations: []ast.AlterOperation{&ast.DropConstraintOperation{
				ConstraintName: node.Name,
				Unique:         true,
				IfExists:       node.IfExists,
			}},
		}
		if err := appendAlterTable(database, document, alter, sourcePlatform); err != nil {
			return fmt.Errorf("DROP INDEX %s ON %s: %w", node.Name, node.Table, err)
		}
		return nil
	}
	name := normalizeSQLIdentifier(sourcePlatform, node.Name)
	for _, owner := range target.databases {
		for i, index := range owner.Indexes {
			if target.ownsIndex(index) && index.Name == name {
				owner.Indexes = slices.Delete(owner.Indexes, i, i+1)
				return nil
			}
		}
	}
	for _, owner := range target.databases {
		for _, constraint := range owner.Constraints {
			if constraint.StructName == target.structName && constraint.Name == name && isIndexBacked(constraint) {
				return fmt.Errorf("DROP INDEX %s ON %s: the index enforces %s constraint %s; drop the constraint instead",
					node.Name, node.Table, strings.ToLower(constraint.Type), constraint.Name)
			}
		}
	}
	if node.IfExists {
		return nil
	}
	return fmt.Errorf("DROP INDEX %s ON %s names an index this schema does not declare", node.Name, node.Table)
}

// isMySQLFamily reports whether a source reads DROP INDEX as MySQL and
// MariaDB do.
func isMySQLFamily(sourcePlatform string) bool {
	switch platform.NormalizeDialect(sourcePlatform) {
	case platform.MySQL, platform.MariaDB:
		return true
	default:
		return false
	}
}

// isIndexBacked reports whether a constraint of this kind is enforced by an
// index of its own name.
func isIndexBacked(constraint schemamodel.Constraint) bool {
	switch strings.ToUpper(constraint.Type) {
	case "UNIQUE", "PRIMARY KEY", "EXCLUDE":
		return true
	default:
		return false
	}
}

// indexTable answers the table an index belongs to, or nil.
func indexTable(databases []*schemamodel.Database, index schemamodel.Index, sourcePlatform string) *schemamodel.Table {
	for _, written := range []string{index.TableName, index.StructName} {
		if written == "" {
			continue
		}
		if table := resolveTable(databases, normalizeSQLTableReference("", written), sourcePlatform); table != nil {
			return table
		}
	}
	return nil
}

// sameSchema reports whether a schema a DROP INDEX writes, empty when the name
// is bare, is the schema of a table. A bare name and a table declared without
// a schema are both in the default one, which PostgreSQL calls public and
// SQLite calls main.
func sameSchema(sourcePlatform, written, actual string) bool {
	if written == actual {
		return true
	}
	isDefault := func(schema string) bool {
		switch {
		case schema == "":
			return true
		case platform.IsPostgresFamily(sourcePlatform):
			return schema == "public"
		case platform.NormalizeDialect(sourcePlatform) == platform.SQLite:
			return strings.EqualFold(schema, "main")
		default:
			return false
		}
	}
	return isDefault(written) && isDefault(actual)
}

// constraintIndexNamed names the constraint whose index is called name in the
// schema, or returns "": a UNIQUE, a primary key or an EXCLUDE, named by its
// declaration or derived as PostgreSQL derives it, a column's own UNIQUE
// included; see [pgname.ColumnKey].
func constraintIndexNamed(databases []*schemamodel.Database, sourcePlatform, schema, name string) string {
	for _, holder := range databases {
		for _, table := range holder.Tables {
			if !sameSchema(sourcePlatform, schema, table.Schema) {
				continue
			}
			if constraint := tableConstraintIndexNamed(databases, table, sourcePlatform, name); constraint != "" {
				return constraint
			}
		}
	}
	return ""
}

// tableConstraintIndexNamed is [constraintIndexNamed] for the constraints of
// one table.
func tableConstraintIndexNamed(databases []*schemamodel.Database, table schemamodel.Table, sourcePlatform, name string) string {
	postgres := platform.IsPostgresFamily(sourcePlatform)
	for _, database := range databases {
		for _, constraint := range database.Constraints {
			if constraint.StructName == table.StructName && constraint.Name == name && isIndexBacked(constraint) {
				return fmt.Sprintf("%s constraint %s", strings.ToLower(constraint.Type), constraint.Name)
			}
		}
		if postgres && slices.ContainsFunc(database.Fields, func(field schemamodel.Field) bool {
			return field.StructName == table.StructName && field.Unique &&
				strings.TrimSpace(field.UniqueExpr) == "" && pgname.ColumnKey(table.Name, field.Name) == name
		}) {
			return "unique constraint " + name
		}
	}
	if hasPrimaryKey(databases, table) && primaryKeyIndexName(table, sourcePlatform) == name {
		return "primary key constraint " + name
	}
	return ""
}

// primaryKeyIndexName answers the name of the index behind the primary key of
// table: the declared name, or on PostgreSQL the name the server derives when
// none is declared.
func primaryKeyIndexName(table schemamodel.Table, sourcePlatform string) string {
	if table.PrimaryKeyName == "" && platform.IsPostgresFamily(sourcePlatform) {
		return pgname.Constraint(table.Name, nil, "pkey", nil)
	}
	return table.PrimaryKeyName
}

// hasPrimaryKey reports whether table declares a primary key, on the table or
// on a column.
func hasPrimaryKey(databases []*schemamodel.Database, table schemamodel.Table) bool {
	if len(table.PrimaryKey) > 0 || table.PrimaryKeyName != "" {
		return true
	}
	for _, database := range databases {
		if slices.ContainsFunc(database.Fields, func(field schemamodel.Field) bool {
			return field.StructName == table.StructName && field.Primary
		}) {
			return true
		}
	}
	return false
}
