package atlasschema

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/atlassource"
)

// OutsideDatabaseError refuses a desired object that a MySQL or MariaDB URL
// naming one database cannot hold: the document qualifies it with another
// database.
//
// A URL naming one database is the scope of the run. Without this refusal, a
// schema file with `CREATE TABLE other.u`, against a URL naming app, is
// planned as CREATE TABLE other.u, and `ptah schema apply --auto-approve`
// creates it in a database the URL does not name. The pinned community binary
// v1.3.0 refuses the same file, measured on MySQL 8.4.11, since it runs it on
// the dev database, which holds no such database: `Error 1049 (42000): Unknown
// database 'other'`. With `CREATE DATABASE other` first, the binary reads the
// file as synced and drops other.u without a word, and an HCL document
// declaring other alone is `mismatched HCL and database schemas: "app" <>
// "other"` (stokaro/ptah#3928).
type OutsideDatabaseError struct {
	// Kind and Name identify the object: "table", "other.u".
	Kind, Name string
	// Database is the database the document puts it in.
	Database string
	// Side names the URL limited to one database, "--from", "--to" or "the
	// target URL", and Limit is that database.
	Side, Limit string
}

// Error implements error.
func (e *OutsideDatabaseError) Error() string {
	const remedy = "name no database in the URL to reach the whole server, or leave the database out of the name"
	if e.Kind == "database" {
		return fmt.Sprintf("the document declares database %q, but %s is limited to database %q; %s",
			e.Database, e.Side, e.Limit, remedy)
	}
	return fmt.Sprintf("%s %q is in database %q, but %s is limited to database %q; %s",
		e.Kind, e.Name, e.Database, e.Side, e.Limit, remedy)
}

// refuseOutsideDatabase answers an [OutsideDatabaseError] for the first object
// of desired that a one-database MySQL-family scope cannot hold, and nil for
// every other dialect, a whole server, and a document that names only the
// database it is limited to. A name the document leaves unqualified is in the
// one database, and so is one qualified with its name.
func refuseOutsideDatabase(dialect, limit, side string, desired *schemamodel.Database) error {
	switch platform.NormalizeDialect(dialect) {
	case platform.MySQL, platform.MariaDB:
	default:
		return nil
	}
	if limit == "" || desired == nil {
		return nil
	}
	outside := func(kind, name, database string) error {
		if database == "" || database == limit {
			return nil
		}
		return &OutsideDatabaseError{Kind: kind, Name: name, Database: database, Side: side, Limit: limit}
	}
	for _, schema := range desired.Schemas {
		if err := outside("database", schema.Name, schema.Name); err != nil {
			return err
		}
	}
	for _, table := range desired.Tables {
		if err := outside("table", qualifiedName(table.Schema, table.Name), table.Schema); err != nil {
			return err
		}
	}
	for _, sequence := range desired.Sequences {
		if err := outside("sequence", qualifiedName(sequence.Schema, sequence.Name), sequence.Schema); err != nil {
			return err
		}
	}
	for _, view := range desired.Views {
		if err := outside("view", view.Name, qualifier(view.Name)); err != nil {
			return err
		}
	}
	for _, function := range desired.Functions {
		if err := outside("routine", function.Name, qualifier(function.Name)); err != nil {
			return err
		}
	}
	for _, trigger := range desired.Triggers {
		if err := outside("trigger table", trigger.Table, qualifier(trigger.Table)); err != nil {
			return err
		}
	}
	return nil
}

// qualifiedName joins a database and a name, or answers the name alone.
func qualifiedName(database, name string) string {
	if database == "" {
		return name
	}
	return database + "." + name
}

// qualifier answers the database a dotted name carries, or "".
func qualifier(name string) string {
	database, _, found := strings.Cut(name, ".")
	if !found {
		return ""
	}
	return database
}

// refuseOutsideTargetDatabase is [refuseOutsideDatabase] for an apply: the
// target connection's database limits the desired schema, unless the target is
// a whole server.
func refuseOutsideTargetDatabase(info catalog.ServerInfo, desired *schemamodel.Database) error {
	if info.WholeServer {
		return nil
	}
	return refuseOutsideDatabase(info.Dialect, info.Schema, "the target URL", desired)
}

// refuseOutsideComparedDatabase is [refuseOutsideDatabase] for a diff: a side
// read from one database limits a document or a replayed directory on the
// other side.
func refuseOutsideComparedDatabase(dialect string, from, to atlassource.State) error {
	if limitsToOneDatabase(from) && to.Kind != atlassource.KindDatabase {
		return refuseOutsideDatabase(dialect, from.DefaultSchema, "--from", to.Schema)
	}
	if limitsToOneDatabase(to) && from.Kind != atlassource.KindDatabase {
		return refuseOutsideDatabase(dialect, to.DefaultSchema, "--to", from.Schema)
	}
	return nil
}

// limitsToOneDatabase reports a side read from one database.
func limitsToOneDatabase(state atlassource.State) bool {
	return state.Kind == atlassource.KindDatabase && !state.WholeServer
}
