package schemascope

import (
	"context"

	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemaselection"
)

// ReadNames resolves the schemas one live database read covers, for every read
// whose result becomes a side of a schema comparison.
//
// It exists because the answer was being derived independently at each read,
// and the comparator was never told which one it got. `schema inspect` asked
// [schemaselection.Realm] and described a whole multi-schema database
// (stokaro/ptah#1264); the read behind every `--from`/`--to` database URL asked
// nothing and described the connected schema alone. The two disagreeing is not
// a cosmetic difference, because the comparator reads absence as intent, and
// the absence is then produced by whichever reader was asked. Measured on
// PostgreSQL 17.10 against a database holding `public.a` and `extra.b`,
// inspected on the compatibility surface and compared back against itself:
//
//	schema diff --from file://<that document> --to <plain URL>
//	  -> DROP TABLE IF EXISTS "extra"."b" CASCADE;
//	schema diff --from <plain URL> --to file://<that document>
//	  -> CREATE TABLE "extra"."b" ( ... );
//
// The first drops a table nothing asked to drop, the second creates one that
// already exists, and the pinned Atlas community binary v1.3.0 reports
// `Schemas are synced` for both. stokaro/ptah#1276.
//
// The three answers, in the order they are asked:
//
//   - an explicit `--schema`/`--schemas` selection wins. It is the operator
//     naming what they want, and a name that turns out not to exist stays
//     absent from the result rather than being replaced by a schema they did
//     not ask for.
//   - otherwise the URL decides, through [schemaselection.Realm]: a URL that
//     pins no schema puts the whole realm under the read.
//   - a URL that pins one leaves the read at the connected schema.
//
// The names are always returned explicitly, even when they resolve to the one
// schema the reader would have defaulted to, so the read reports the schemas
// themselves and not only their contents.
//
// q is the connection the realm probe runs on; it is the narrow interface
// [schemaselection.RealmSchemas] takes, so this package stays off the
// connection layer.
func ReadNames(
	ctx context.Context,
	info dbschematypes.DBInfo,
	requested []string,
	q schemaselection.RowsQuerier,
) ([]string, error) {
	if names := SplitNames(requested); len(names) > 0 {
		return names, nil
	}
	if !schemaselection.Realm(info.Dialect, info.URL, info.Schema) {
		return connectedSchemaNames(info), nil
	}
	return schemaselection.RealmSchemas(ctx, info.Dialect, q)
}

// connectedSchemaNames is the read scope of a connection whose URL pinned a
// schema: the one it landed in.
//
// An empty answer is returned as no names rather than as one empty name, which
// leaves the dialect reader on its own default. Only a connection whose dialect
// reports no schema at all reaches that, and inventing a name for it here would
// describe a schema the server never confirmed.
func connectedSchemaNames(info dbschematypes.DBInfo) []string {
	if names := SplitNames([]string{info.Schema}); len(names) > 0 {
		return names
	}
	return nil
}
