package tableidentity

import (
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
)

// BindObjects resolves the owning table of each feature object with the same
// identifiers as table capture, so an object finds its table when a
// connection supplies the default schema. A source writes a table-owned object
// before any connection is known: a ClickHouse row policy that leaves the
// database to the connection carries no schema at all, while its table is
// captured in the connection's database.
//
// Only the schema and the table part change; the object's own name keeps the
// normalization its kind gave it. An explicit schema survives, and two
// objects that become one identity are refused.
func BindObjects(objects schemaext.Objects, target string, semantics identifier.Semantics) (schemaext.Objects, error) {
	if objects.IsZero() {
		return objects, nil
	}
	all, err := objects.All()
	if err != nil {
		return schemaext.Objects{}, err
	}
	for i, object := range all {
		all[i].Ref = bindOwned(object.Ref, target, semantics)
	}
	return schemaext.NewObjects(all...)
}

// bindOwned rebinds the table part of a table-owned identity and returns any
// other identity as it is.
func bindOwned(ref objectidentity.ID, target string, semantics identifier.Semantics) objectidentity.ID {
	if ref.Parent.Empty() {
		return ref
	}
	schema := ref.Schema.Source
	if ref.Schema.Defaulted {
		schema = ""
	}
	table := Subject(schema, ref.Parent.Source, target, semantics)
	ref.Schema, ref.Parent = table.Schema, table.Name
	return ref
}
