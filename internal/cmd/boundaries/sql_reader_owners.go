package main

import (
	"slices"

	"golang.org/x/tools/go/packages"
)

// sqlReaderRoots are the shared SQL reader of the stokaro/ptah#4140 design:
// the parser that reads the grammar every target shares, and the package that
// builds the schema model from what it reads. An owner's SQL syntax reaches
// the owner through core/sqlext claims, so neither package needs to link an
// owner to read it.
//
// The rule does not police the internal dialect helpers these packages import,
// such as internal/mysqlname or internal/pgname. Those interpret objects of the
// common model and are not owner families, and telling them apart by name is
// the kind of check this command does not make.
var sqlReaderRoots = []string{"internal/parser", "internal/sqlschema"}

// sqlReaderOwnerLinks reports each concrete owner the shared SQL reader links,
// directly or through any chain of imports, once per owner family.
//
// The count is zero. YQL reaches neither root: the YDB owner parses it and
// hands the shared conversion what it parsed, through
// sqlschema.ConvertScript, and a syntax consumer reads it through
// internal/sqlsyntax.
func sqlReaderOwnerLinks(pkg *packages.Package) []finding {
	self := relative(pkg.PkgPath)
	if !slices.Contains(sqlReaderRoots, self) {
		return nil
	}
	return ownerLinks(pkg, self)
}
