package schemadiff

import (
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff/internal/compare"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func Compare(generated *goschema.Database, database *types.DBSchema) *difftypes.SchemaDiff {
	diff := &difftypes.SchemaDiff{}

	// Compare tables and their column structures
	compare.TablesAndColumns(generated, database, diff)

	// Compare enum type definitions and values
	compare.Enums(generated, database, diff)

	// Compare database index definitions
	compare.Indexes(generated, database, diff)

	return diff
}
