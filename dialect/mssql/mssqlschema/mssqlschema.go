// Package mssqlschema owns the SQL Server security policy model: one
// schema-scoped feature object holding the policy's state and every predicate
// it binds to a table, in a desired and an observed representation. It
// contains no provider selection, database access or SQL.
//
// The model keeps SQL Server's semantics rather than a shape shared with other
// engines (ADR 0020). A security policy belongs to a schema, not to a table:
// one policy may bind predicates to several tables, each through an inline
// table-valued function, so it is captured once with no table parent. Its
// state, schema binding and replication behavior are its own, and each binding
// keeps its table, its function invocation, whether it filters reads or
// blocks writes, and the exact operation a block predicate is evaluated for.
package mssqlschema

import (
	"ptah.run/core/schemaext"
)

// Owner is the provider identity that owns every security policy codec and
// service. It is separate from the targets the services are registered for.
const Owner = "ptah.run/mssql"

// SecurityPolicyKind identifies one security policy of one schema.
const SecurityPolicyKind schemaext.Kind = "ptah.run/mssql/security-policy"

// validText refuses text a statement or a catalog cannot carry faithfully.
func validText(field, value string) error {
	return schemaext.ValidText("security policy "+field, value)
}

// modelError names the model and representation a refusal is about, as the
// typed error the codec boundary and the census recognize.
func modelError(representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: SecurityPolicyKind, Representation: representation, Message: err.Error()}
}
