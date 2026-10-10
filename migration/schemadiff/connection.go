package schemadiff

import (
	"context"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// Connection is what [CompareWithDatabase] needs from a connected server: its
// description, how it folds and compares the names a comparison meets, and a
// transaction that is always rolled back, in which the server re-spells
// declared expressions. The DatabaseConnection type of ptah.run/dbschema
// implements it. The comparison takes the interface so that it does not link
// the schema readers that package carries.
//
// A nil Connection is refused with an error. An error from
// ResolveIdentifierSemantics fails the comparison. WithRolledBackTransaction
// follows the [schemaext.ProbeSession] contract.
type Connection interface {
	schemaext.ProbeSession
	Info() catalog.ServerInfo
	ResolveIdentifierSemantics(ctx context.Context, names []string) (identifier.Semantics, error)
}
