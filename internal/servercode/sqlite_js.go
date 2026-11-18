//go:build js

package servercode

// sqliteUniqueViolation is always false here. The SQLite driver does not
// build for js/wasm, so no error can have come from it on this platform.
func sqliteUniqueViolation(error) bool { return false }
