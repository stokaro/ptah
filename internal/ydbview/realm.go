package ydbview

import (
	"strings"

	"ptah.run/dialect/ydb/ydbsyntax"
)

// RealmPrefix is the pragma a connection adds to every query in a dev realm.
// The connection and view reader share this spelling because YDB stores it
// in a view's query text, where the reader must recognize its own prefix.
func RealmPrefix(root string) string {
	return "PRAGMA TablePathPrefix(" + ydbsyntax.StringLiteral(root) + ");\n"
}

// RealmQueryText removes the connection's leading realm pragma from a stored
// view body. The catalog already resolves relative paths inside that realm.
// Only one exact prefix is removed; user pragmas and other roots stay intact.
func RealmQueryText(body, root string) string {
	return strings.TrimPrefix(body, RealmPrefix(root))
}
