// Package pgpolicy owns the PostgreSQL row-security model: a policy as a
// feature object identified by its table and name, and a table's row-security
// switches as a facet of that table, each in a desired and an observed
// representation. It contains no provider selection, database access or SQL.
//
// The model keeps PostgreSQL's semantics rather than a shape shared with other
// engines (ADR 0020). A policy's command, role selectors, USING and WITH CHECK
// expressions and permissive or restrictive composition decide what it admits,
// and ENABLE and FORCE ROW LEVEL SECURITY are two independent flags of the
// table. Another PostgreSQL-wire target may select this model only when its
// adapter implements the same contract.
package pgpolicy

import (
	"ptah.run/core/schemaext"
)

// Owner is the provider identity that owns every row-security codec and
// service. It is separate from the targets the services are registered for.
const Owner = "ptah.run/pgpolicy"

const (
	// PolicyKind identifies one policy on one table.
	PolicyKind schemaext.Kind = "ptah.run/pgpolicy/policy"
	// TableStateKind identifies a table's row-security switches.
	TableStateKind schemaext.Kind = "ptah.run/pgpolicy/table-state"
)

// validTexts checks field/value pairs in order with [schemaext.ValidText], so
// the first invalid field is the one a refusal names.
func validTexts(pairs ...string) error {
	for i := 0; i+1 < len(pairs); i += 2 {
		if err := schemaext.ValidText("row-security "+pairs[i], pairs[i+1]); err != nil {
			return err
		}
	}
	return nil
}

// modelError names the model and representation a refusal is about, as the
// typed error the codec boundary and the census recognize.
func modelError(kind schemaext.Kind, representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: kind, Representation: representation, Message: err.Error()}
}
