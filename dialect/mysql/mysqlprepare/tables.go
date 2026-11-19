// Package mysqlprepare resolves, before the shared comparison, the collations
// a MySQL or MariaDB declaration leaves to the server: the collation of a
// column that names none, and the table's default collation. The character
// sets that decide them are the MySQL owner's settings, so the resolution
// belongs to the owner, and the comparison reads the resolved names only.
package mysqlprepare

import (
	"context"
	"fmt"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemapreparation"
	"ptah.run/internal/mysqlcollation"
)

// Service resolves inherited and default collations. The zero value is usable
// and safe for concurrent calls; no live server is consulted. It reads the
// character set defaults and aliases the request carries from the read
// ([schemapreparation.Request.CharsetDefaultCollations] and
// [schemapreparation.Request.CharsetAliases]), and resolves nothing that needs
// a character set the request does not name.
type Service struct{}

// PrepareTables returns independent captures with
// [schemapreparation.Table.InheritedCollations] and
// [schemapreparation.Table.DefaultCollation] set where the declaration leaves
// the collation to the server and the server's defaults decide it, and
// [schemapreparation.Table.StoredTypes] set for each character column of the
// binary character set, which the server stores as a binary type.
//
// A table's default collation is the one it names, or the default collation of
// the character set it names. A column of a type that holds a collation, which
// names none, takes the default collation of the character set it names, the
// national character set for NCHAR, or else the table's default collation. A
// table that names neither leaves its columns and itself unresolved: the
// database's default decides them, and the comparison does not compare them.
// Only MySQL and MariaDB are accepted; cancellation returns no result.
func (Service) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	if ctx == nil {
		return schemapreparation.Result{}, fmt.Errorf("%w: preparation requires a context", schemapreparation.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	target := platform.NormalizeDialect(request.Target)
	if target != platform.MySQL && target != platform.MariaDB {
		return schemapreparation.Result{}, fmt.Errorf("%w: MySQL preparation target %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request = request.Clone()
	server := mysqlcollation.Server{Defaults: request.CharsetDefaultCollations, Aliases: request.CharsetAliases}
	for i := range request.Tables {
		table := &request.Tables[i]
		resolved, err := mysqlcollation.Resolve(target, table.Desired.Table, table.Desired.Fields, server)
		if err != nil {
			return schemapreparation.Result{}, err
		}
		table.DefaultCollation, table.InheritedCollations, table.StoredTypes = resolved.Default, resolved.Inherited, resolved.Stored
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
}
