package schemascope

import (
	"fmt"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/schemamodel"
)

// OutsideDatabaseError refuses a desired object that a URL naming one database
// cannot hold: the document qualifies it with another database.
//
// A URL naming one database is the scope of the run. Without this refusal, a
// schema file with `CREATE TABLE other.u`, against a URL naming app, is
// planned as CREATE TABLE other.u, and `ptah schema apply --auto-approve`
// creates it in a database the URL does not name. A declaration of other alone
// reports synced, since nothing plans a database. The pinned community binary
// v1.3.0 refuses the same file, measured on MySQL 8.4.11, since it runs it on
// the dev database, which holds no such database: `Error 1049 (42000): Unknown
// database 'other'`. With `CREATE DATABASE other` first, the binary reads the
// file as synced and drops other.u without a word, and an HCL document
// declaring other alone is `mismatched HCL and database schemas: "app" <>
// "other"` (stokaro/ptah#3928).
//
// On ClickHouse a read reaches the URL's database alone, so a table the plan
// writes in another database is planned again by every run, and the plan
// creates no database for it: measured on ClickHouse 26.9, `Code: 81. Database
// other does not exist` without one, and `CREATE TABLE other.t` on every run
// with one.
type OutsideDatabaseError struct {
	// Kind and Name identify the object: "table", "other.u".
	Kind, Name string
	// Database is the database the document puts it in.
	Database string
	// Side names the URL limited to one database, "--from", "--to" or "the
	// target URL", and Limit is that database.
	Side, Limit string
	// NoServerScope reports an engine where no URL reaches a whole server.
	NoServerScope bool
}

// Error implements error.
func (e *OutsideDatabaseError) Error() string {
	remedy := "name no database in the URL to reach the whole server, or leave the database out of the name"
	if e.NoServerScope {
		remedy = "name that database in the URL, or leave the database out of the name"
	}
	if e.Kind == "database" {
		return fmt.Sprintf("the document declares database %q, but %s is limited to database %q; %s",
			e.Database, e.Side, e.Limit, remedy)
	}
	return fmt.Sprintf("%s %q is in database %q, but %s is limited to database %q; %s",
		e.Kind, e.Name, e.Database, e.Side, e.Limit, remedy)
}

// RefuseOutsideDatabase answers an [OutsideDatabaseError] for the first object
// of desired that a one-database scope cannot hold, and nil for every other
// dialect, a whole server, and a document that names only the database it is
// limited to. A one-database scope is a URL that names a database on an
// engine whose URL selects a database as its schema, the MySQL family's, or an
// engine whose run reaches the connected database alone, ClickHouse's. A name
// the document leaves unqualified is in the one database, and so is one
// qualified with its name.
func RefuseOutsideDatabase(dialect, limit, side string, desired *schemamodel.Database) error {
	described, _ := platform.Lookup(dialect)
	noServerScope := described.Schemas.ConnectedDatabaseOnly
	if described.Schemas.Selection != descriptor.SchemaSelectionDatabase && !noServerScope {
		return nil
	}
	if limit == "" || desired == nil {
		return nil
	}
	outside := func(kind, name, database string) error {
		if database == "" || database == limit {
			return nil
		}
		return &OutsideDatabaseError{
			Kind: kind, Name: name, Database: database, Side: side, Limit: limit, NoServerScope: noServerScope,
		}
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

// RefuseOutsideTargetDatabase is [RefuseOutsideDatabase] for a verb that plans
// against a target connection: the connection's database limits the desired
// schema, unless the connection is a whole server.
func RefuseOutsideTargetDatabase(info catalog.ServerInfo, desired *schemamodel.Database) error {
	if info.WholeServer {
		return nil
	}
	return RefuseOutsideDatabase(info.Dialect, info.Schema, "the target URL", desired)
}
