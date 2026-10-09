// Package ydbscheme identifies resources shared by YDB schema object families.
// A scheme path can hold one object even when the objects have different kinds.
package ydbscheme

import (
	"strings"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/internal/tableref"
)

// PathKind identifies a physical scheme slot in planning footprints. It is a
// resource identity, not a desired or observed schema model.
const PathKind objectidentity.Kind = "ptah.run/ydb/scheme-path"

// Path returns the shared slot for separate directory and leaf names. Literal
// dots remain part of their component; paths retain YDB's case sensitivity.
func Path(schema, name string) objectidentity.ID {
	return objectidentity.NewBuilder(identifier.ForDialect("ydb")).SchemaScopedParts(PathKind, schema, name)
}

// ObjectPath resolves a common SQL name to the path YDB receives. Planning and
// rendering share this conversion so SQL quotes and literal dots cannot hide
// a collision between different object families at the same physical path.
func ObjectPath(name string) string {
	ref, ok := tableref.Parse(name)
	switch {
	case !ok:
		return name
	case ref.Schema == "":
		return ref.Name
	default:
		return strings.TrimRight(ref.Schema, "/") + "/" + ref.Name
	}
}
