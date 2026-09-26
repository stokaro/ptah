// Package hashshard reports the CockroachDB keys and indexes a schema read
// describes without their hash sharding.
//
// A key or index built USING HASH is described over its declared columns, and
// the bucket count is recorded beside it in [catalog.Constraint] and
// [catalog.Index] as HashShardBuckets. No schema source can declare USING HASH,
// so the description carries the key and not the sharding. This package turns
// the recorded counts into the note the read surfaces print.
package hashshard

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"ptah.run/catalog"
)

// ReportUndescribed writes a note naming the hash-sharded keys and indexes in
// the description, and nothing at all when there are none.
//
// It belongs on the read surfaces, `ptah db read` and `schema inspect`, and it
// reads the description they are about to print, so a table left out by a
// selector is not named. Applied to another database, the description builds
// each of these unsharded, and a comparison between the two reports no
// difference, since both are the same key over the same columns; the note is
// the only place the difference shows.
//
// A primary or unique key is named once, although the catalog reports it as a
// constraint and as the index behind it.
//
// w may be nil, which is how the inspect surfaces spell "no diagnostics
// stream"; the note is then dropped. Write errors are dropped too: a diagnostic
// that fails to print must not fail a read that succeeded.
func ReportUndescribed(w io.Writer, schema *catalog.Database) {
	if w == nil || schema == nil {
		return
	}
	named := make(map[string]int)
	for _, constraint := range schema.Constraints {
		if constraint.HashShardBuckets > 0 {
			named[qualified(constraint.Schema, constraint.TableName, constraint.Name)] = constraint.HashShardBuckets
		}
	}
	for _, index := range schema.Indexes {
		if index.HashShardBuckets > 0 {
			named[qualified(index.Schema, index.TableName, index.Name)] = index.HashShardBuckets
		}
	}
	if len(named) == 0 {
		return
	}
	listed := make([]string, 0, len(named))
	for name, buckets := range named {
		listed = append(listed, fmt.Sprintf("%s (%d buckets)", name, buckets))
	}
	sort.Strings(listed)
	subject, verb, object := fmt.Sprintf("%d CockroachDB keys and indexes", len(listed)), "are", "them"
	if len(listed) == 1 {
		subject, verb, object = "1 CockroachDB key or index", "is", "it"
	}
	_, _ = fmt.Fprintf(w,
		"note: %s built USING HASH %s described without it, because no schema source can declare"+
			" hash sharding; a description applied to another database builds %s unsharded, and a"+
			" diff between the two reports no difference: %s.\n",
		subject, verb, object, strings.Join(listed, ", "))
}

// qualified names a key or index by its table, and by its schema too when the
// read recorded one outside the connection's default.
func qualified(schema, table, name string) string {
	if schema == "" {
		return table + "." + name
	}
	return schema + "." + table + "." + name
}
