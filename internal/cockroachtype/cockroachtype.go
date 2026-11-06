// Package cockroachtype answers what CockroachDB builds for a declared column
// type where that differs from the declared spelling.
//
// It is a package rather than a function inside the comparison because two
// callers need the same answer: the comparison, which compares a declaration
// with the column a server built from it, and goschematodb, which describes a
// declared schema the way a read of such a server would. When only the
// comparison knew the rule, a schema file compared with itself on CockroachDB
// planned ALTER COLUMN ... TYPE INTEGER for every INTEGER column. Two copies
// of the rule are how that comes back: they agree when the second is written
// and stop agreeing when the first is extended.
package cockroachtype

import (
	"slices"
	"strings"

	"ptah.run/internal/normalize"
)

// Resolve returns the type CockroachDB builds for a column declared as
// declared.
//
// A column declared INT or INTEGER, with no width, is built with the width the
// session's default_int_size names, 8 unless the session sets it. Measured on
// v26.3.2: `n integer` and `k int` both read back as INT8, and INT4 in a
// session with default_int_size = 4. A declaration that names a width, such
// as `int4` or `bigint`, is returned as written, and so is every other type. An
// array takes the width of its element.
//
// A column declared with a serial type is built as INT8 whatever its width, measured
// on v26.3.5: `smallserial` answers `upgrading the column to INT8` and reads back as
// INT8, as do `serial`, `serial2` and `serial4`.
//
// defaultIntSize is the session's setting in bytes, and 0 where nothing read
// it; 0 is taken as CockroachDB's own default.
func Resolve(declared string, defaultIntSize int) string {
	element, isArray := normalize.ArrayElement(declared)
	name := strings.ToLower(strings.TrimSpace(element))
	if slices.Contains([]string{"smallserial", "serial", "bigserial", "serial2", "serial4", "serial8"}, name) {
		return "int8"
	}
	if name != "int" && name != "integer" {
		return declared
	}
	width := "int8"
	if defaultIntSize == 4 {
		width = "int4"
	}
	if isArray {
		return width + "[]"
	}
	return width
}
