package compare

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/systemschema"
	"ptah.run/migration/schemadiff/difftypes"
)

// ServerSchemas records on diff the databases a comparison of a whole MySQL or
// MariaDB server creates, drops and changes: desired is the desired state,
// current the user databases the server holds. A database the desired state
// puts a table or a sequence in is declared, whether or not a schema block
// names it: a database dropped under the tables the plan creates in it would
// take them with it.
//
// The pinned community binary v1.3.0 plans the same three, measured on MySQL
// 8.4.11 and MariaDB 11.8.9 with `schema apply` against a URL naming no
// database:
//
//	a database only the file declares      CREATE DATABASE `r9` COLLATE utf8mb4_bin
//	a database only the server holds       DROP DATABASE `r3`
//	a declared character set or collation  ALTER DATABASE `r1` CHARSET latin1
//	that differs                             COLLATE latin1_swedish_ci
//
// A character set or collation the declaration leaves out is the server's to
// choose, and is not compared: `schema "r1" {}` is synced with any database
// r1. A database the server owns is neither created nor changed, because the
// reader leaves them out and a declaration of one would plan a CREATE for a
// database that exists. Names are compared the way the server compares table
// names, which is how it compares database names too.
func ServerSchemas(
	diff *difftypes.SchemaDiff,
	desired *schemamodel.Database,
	current []catalog.Schema,
	semantics identifier.Semantics,
) {
	held := make(map[string]catalog.Schema, len(current))
	for _, schema := range current {
		held[semantics.TableIdentityKey(schema.Name)] = schema
	}
	declaredSchemas := serverDatabasesOf(desired)
	declared := make(map[string]struct{}, len(declaredSchemas))
	for _, schema := range declaredSchemas {
		key := semantics.TableIdentityKey(schema.Name)
		if strings.TrimSpace(schema.Name) == "" || systemschema.IsMySQLSystemDatabase(schema.Name) {
			continue
		}
		if _, seen := declared[key]; seen {
			continue
		}
		declared[key] = struct{}{}
		existing, exists := held[key]
		if !exists {
			diff.SchemasAdded = append(diff.SchemasAdded, schema)
			continue
		}
		if change, changed := schemaAttributeChange(schema, existing); changed {
			diff.SchemasModified = append(diff.SchemasModified, change)
		}
	}
	for _, schema := range current {
		if _, kept := declared[semantics.TableIdentityKey(schema.Name)]; !kept {
			diff.SchemasRemoved = append(diff.SchemasRemoved, schema.Name)
		}
	}
	slices.SortFunc(diff.SchemasAdded, func(a, b schemamodel.Schema) int { return cmp.Compare(a.Name, b.Name) })
	slices.SortFunc(diff.SchemasModified, func(a, b difftypes.SchemaChange) int { return cmp.Compare(a.Name, b.Name) })
	slices.Sort(diff.SchemasRemoved)
}

// serverDatabasesOf is the databases desired declares, in declaration order:
// each schema block, then each database a table or a sequence names that no
// block does, with no attribute of its own.
func serverDatabasesOf(desired *schemamodel.Database) []schemamodel.Schema {
	if desired == nil {
		return nil
	}
	databases := slices.Clone(desired.Schemas)
	named := func(name string) {
		if strings.TrimSpace(name) == "" {
			return
		}
		if slices.ContainsFunc(databases, func(schema schemamodel.Schema) bool { return schema.Name == name }) {
			return
		}
		databases = append(databases, schemamodel.Schema{Name: name})
	}
	for _, table := range desired.Tables {
		named(table.Schema)
	}
	for _, sequence := range desired.Sequences {
		named(sequence.Schema)
	}
	return databases
}

// schemaAttributeChange answers the change from existing to declared, and
// false when every attribute the declaration sets already holds. The server
// compares character set and collation names without case.
func schemaAttributeChange(declared schemamodel.Schema, existing catalog.Schema) (difftypes.SchemaChange, bool) {
	charsetDiffers := declared.Charset != "" && !strings.EqualFold(declared.Charset, existing.Charset)
	collateDiffers := declared.Collate != "" && !strings.EqualFold(declared.Collate, existing.Collate)
	if !charsetDiffers && !collateDiffers {
		return difftypes.SchemaChange{}, false
	}
	return difftypes.SchemaChange{
		Name:           declared.Name,
		Charset:        declared.Charset,
		Collate:        declared.Collate,
		CurrentCharset: existing.Charset,
		CurrentCollate: existing.Collate,
	}, true
}

// RequireServerDatabases refuses a desired state for a whole MySQL or MariaDB
// server that holds a table or a sequence naming no database. Compared with a
// server, such an object is created where the session selected no database,
// which the server refuses, and the table it stands for in its database is
// dropped. The pinned community binary v1.3.0 refuses an HCL table without a
// schema for the same reason, measured on MySQL 8.4.11: `cannot extract
// schema name for table "t"`.
func RequireServerDatabases(desired *schemamodel.Database) error {
	if desired == nil {
		return nil
	}
	for _, table := range desired.Tables {
		if strings.TrimSpace(table.Schema) == "" {
			return serverObjectWithoutDatabase("table", table.Name)
		}
	}
	for _, sequence := range desired.Sequences {
		if strings.TrimSpace(sequence.Schema) == "" {
			return serverObjectWithoutDatabase("sequence", sequence.Name)
		}
	}
	return nil
}

func serverObjectWithoutDatabase(kind, name string) error {
	return fmt.Errorf("the desired %s %q names no database, and a MySQL or MariaDB URL naming no database "+
		"is compared as a whole server, database by database; declare the database the %s is in, "+
		"or name a database in the URL", kind, name, kind)
}
