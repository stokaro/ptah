package tableidentity

import (
	"fmt"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// BindCoverage resolves table claims with the same identifiers as table capture.
// Comparison and reverse creation projection share this binding so ForParent
// cannot discard a known claim when a connection supplies a default schema.
// Claims about table-owned feature objects are bound as [BindObjects] binds
// the objects, and index claims are left to the caller, whose index namespace
// decides their shape. Explicit schemas and knowledge limits survive;
// collisions are refused. Namespace domains require owner binding before this
// common adapter can resolve relative table names; it refuses those domains.
func BindCoverage(coverage schemaext.Coverage, target string, semantics identifier.Semantics) (schemaext.Coverage, error) {
	if coverage.IsZero() {
		return coverage, nil
	}
	// A namespace can limit omitted subjects too. Rebinding only explicit
	// subjects could move a table outside an unknown region and grant absence.
	for _, record := range coverage.KindRecords() {
		if record.Knowledge.HasNamespaceScopes() {
			return schemaext.Coverage{}, fmt.Errorf("%w: namespace-scoped coverage requires owner binding before table identity resolution", schemaext.ErrInvalidValue)
		}
	}
	subjects := coverage.SubjectRecords()
	for i, record := range subjects {
		ref := record.Subject
		if ref.Kind == objectidentity.KindIndex {
			continue
		}
		if ref.Kind != objectidentity.KindTable {
			// A claim about a table-owned feature object follows the
			// object, which BindObjects binds the same way.
			subjects[i].Subject = bindOwned(ref, target, semantics)
			continue
		}
		if !ref.Database.Empty() || !ref.Namespace.IsZero() || !ref.Catalog.Empty() || !ref.Parent.Empty() || !ref.Signature.IsZero() {
			return schemaext.Coverage{}, fmt.Errorf("%w: invalid table coverage identity %s", schemaext.ErrInvalidValue, ref)
		}
		schema := ref.Schema.Source
		if ref.Schema.Defaulted {
			schema = ""
		}
		subjects[i].Subject = Subject(schema, ref.Name.Source, target, semantics)
	}
	// Rebuilding refuses claims that collide under the selected semantics,
	// including differently spelled claims with the same knowledge state.
	return schemaext.NewCoverage(coverage.Representation(), coverage.KindRecords(), subjects)
}
