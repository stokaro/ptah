package schemascope

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaselection"
)

// ComparisonReadNames resolves the schemas a live read is made at when the
// read is compared with desired: [ComparisonScope] over what [ReadNames]
// answers for the connection with no selection.
//
// Every verb that compares a desired state with a live database reads through
// it, so two verbs given the same URL and the same document read the same
// schemas. A verb that reads less finds nothing for a declared object in the
// schemas it left out and plans the object as an addition: `schema compare`
// then previews `CREATE TABLE "reporting"."b"` for a table `schema apply`
// alters, and `migrations generate` writes a migration that fails with 42P07
// (stokaro/ptah#5030).
func ComparisonReadNames(
	ctx context.Context,
	info catalog.ServerInfo,
	requested []string,
	q schemaselection.RealmConnection,
	desired *schemamodel.Database,
) ([]string, error) {
	if names := SplitNames(requested); len(names) > 0 {
		return names, nil
	}
	urlScope, err := ReadNames(ctx, info, nil, q)
	if err != nil {
		return nil, err
	}
	return ComparisonScope(info, nil, urlScope, desired)
}

// ComparisonScope resolves the schemas the live side of a comparison with
// desired is read at, where urlScope is what [ReadNames] answered for the
// connection with no selection.
//
// The two sides of a comparison have to be read at the same scope or silence
// on one of them is mistaken for absence. A document `schema inspect` wrote
// for a URL that pins no schema describes every schema of the realm
// (stokaro/ptah#1264); compared with a read of the connection's own schema
// alone, it finds no `extra` there and plans `CREATE SCHEMA extra` and
// `CREATE TABLE "extra"."b"` for objects the database already has. Measured
// on PostgreSQL 17.10: the plan never converges, and executing it fails at
// SQLSTATE 42P07.
//
// The scope therefore starts from what the URL covers. Deriving it from the
// desired state instead, to keep a document describing one schema of a
// multi-schema database from reaching the others, makes a whole schema
// silently unmanaged rather than authoritatively absent. The pinned Atlas
// community binary v1.3.0 plans `DROP SCHEMA "extra" CASCADE` for that
// document, measured on PostgreSQL 17.10, and a desired state that means to
// keep a schema it does not describe says so with a `ptah:not-described
// schema` record, which the comparator honors and no scope can express.
//
// The desired state's own schemas are added on top, because a URL pinned to
// one schema by `search_path` covers less than a document may name, and a
// creation planned for an object that exists fails the run. On YDB the URL
// scope is already every directory, and [Widen] keeps it that way.
//
// An explicit selection outranks both: it is the operator naming the scope.
func ComparisonScope(info catalog.ServerInfo, requested, urlScope []string, desired *schemamodel.Database) ([]string, error) {
	if names := SplitNames(requested); len(names) > 0 {
		return names, nil
	}
	declared, err := DeclaredSchemaNames(desired)
	if err != nil {
		return nil, err
	}
	return Widen(info, urlScope, declared), nil
}
