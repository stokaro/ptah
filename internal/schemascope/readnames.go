package schemascope

import (
	"context"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/internal/schemaselection"
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
	info catalog.ServerInfo,
	requested []string,
	q schemaselection.RowsQuerier,
) ([]string, error) {
	if names := SplitNames(requested); len(names) > 0 {
		return names, nil
	}
	if !schemaselection.Realm(info.Dialect, info.URL, info.Schema) {
		return connectedSchemaNames(info), nil
	}
	return schemaselection.RealmSchemas(ctx, info.Dialect, info.Capabilities, q)
}

// connectedSchemaNames is the read scope of a connection whose URL pinned a
// schema: the one it landed in.
//
// An empty answer is returned as no names rather than as one empty name, which
// leaves the dialect reader on its own default. Only a connection whose dialect
// reports no schema at all reaches that, and inventing a name for it here would
// describe a schema the server never confirmed.
func connectedSchemaNames(info catalog.ServerInfo) []string {
	if names := SplitNames([]string{info.Schema}); len(names) > 0 {
		return names
	}
	return nil
}

// Union is every name either list carries, blank names dropped, sorted and
// de-duplicated. It is nil when nothing is left, which leaves a dialect reader
// on its own default rather than on an allow-list naming nothing.
//
// The order is part of the answer. Two reads that cover the same schemas have to
// be asked for them the same way, because a saved plan's source fingerprint is
// compared byte for byte with a read made later.
func Union(base, more []string) []string {
	names := make([]string, 0, len(base)+len(more))
	for _, name := range slices.Concat(base, more) {
		if trimmed := strings.TrimSpace(name); trimmed != "" {
			names = append(names, trimmed)
		}
	}
	slices.Sort(names)
	names = slices.Compact(names)
	if len(names) == 0 {
		return nil
	}
	return names
}

// BeyondURL is the part of scope that asking [ReadNames] again later, with no
// explicit selection, may not cover. urlScope is what ReadNames answered for the
// same connection when scope was read.
//
// A connection at realm scope needs nothing recorded. The realm is listed again
// at every read, so it covers a schema that did not exist when scope was read
// and exists now -- which is exactly the change a later read is there to notice.
// A connection limited to one schema covers that schema and no other, so every
// other name in scope has to be carried to the later read or that read never
// looks at it. stokaro/ptah#3285 is the case: a saved plan writing a second
// schema was verified against the connected schema alone, and accepted after the
// table it was about to create had been created behind its back.
func BeyondURL(info catalog.ServerInfo, urlScope, scope []string) []string {
	if schemaselection.Realm(info.Dialect, info.URL, info.Schema) {
		return nil
	}
	covered := Union(urlScope, nil)
	var beyond []string
	for _, name := range Union(scope, nil) {
		if !slices.Contains(covered, name) {
			beyond = append(beyond, name)
		}
	}
	return beyond
}
