package difftypes_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
)

// removal is a removed table main.name whose capture holds constraints.
func removal(name string, constraints ...catalog.Constraint) difftypes.TableRemoval {
	return schemaRemoval("main", name, constraints...)
}

// schemaRemoval is a removed table schema.name whose capture holds
// constraints.
func schemaRemoval(schema, name string, constraints ...catalog.Constraint) difftypes.TableRemoval {
	return difftypes.TableRemoval{Name: schema + "." + name, Current: schemacapture.TableObservation{
		Table: catalog.Table{Schema: schema, Name: name}, Constraints: constraints}}
}

// foreignKey is a foreign key of table that references schema.referenced.
func foreignKey(table, schema, referenced string) catalog.Constraint {
	return catalog.Constraint{Name: table + "_fk", TableName: table, Schema: "main", Type: "FOREIGN KEY", ForeignTable: &referenced, ForeignSchema: schema}
}

// ObservedReferences reads the foreign keys a removal's capture holds between
// the tables the collection removes, keyed and valued by removal name.
func TestTableRemovalsObservedReferences_HappyPath(t *testing.T) {
	semantics := identifier.Semantics{DefaultSchema: "main", TableNames: identifier.ComparisonASCIIInsensitive,
		ColumnNames: identifier.ComparisonExact, IndexNames: identifier.ComparisonExact, IndexNamespace: identifier.IndexNamespaceTable}
	tests := []struct {
		name     string
		removals difftypes.TableRemovals
		want     map[string][]string
	}{
		{name: "a reference without a schema means the referencing table's",
			removals: difftypes.TableRemovals{removal("accounts"), removal("orders", foreignKey("orders", "", "accounts"))},
			want:     map[string][]string{"main.orders": {"main.accounts"}}},
		{name: "a reference without a schema from a table outside the default schema",
			removals: difftypes.TableRemovals{removal("accounts"), schemaRemoval("audit", "accounts"),
				schemaRemoval("audit", "orders", foreignKey("orders", "", "accounts"))},
			want: map[string][]string{"audit.orders": {"audit.accounts"}}},
		{name: "a schema-qualified reference in another letter case",
			removals: difftypes.TableRemovals{removal("accounts"), removal("orders", foreignKey("orders", "main", "ACCOUNTS"))},
			want:     map[string][]string{"main.orders": {"main.accounts"}}},
		{name: "two keys to one table count once",
			removals: difftypes.TableRemovals{removal("accounts"), removal("orders", foreignKey("orders", "", "accounts"), foreignKey("orders", "", "accounts"))},
			want:     map[string][]string{"main.orders": {"main.accounts"}}},
		{name: "a reference to a table the plan keeps",
			removals: difftypes.TableRemovals{removal("orders", foreignKey("orders", "", "accounts"))}},
		{name: "a reference to the same table",
			removals: difftypes.TableRemovals{removal("nodes", foreignKey("nodes", "", "nodes"))}},
		{name: "a reference to a table of another schema",
			removals: difftypes.TableRemovals{removal("accounts"), removal("orders", foreignKey("orders", "audit", "accounts"))}},
		{name: "a constraint that is not a foreign key",
			removals: difftypes.TableRemovals{removal("accounts"), removal("orders", catalog.Constraint{Name: "orders_check", Type: "CHECK"})}},
		{name: "a removal with no capture", removals: difftypes.TableRemovals{{Name: "main.accounts"}, removal("orders", foreignKey("orders", "", "accounts"))}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			references := test.removals.ObservedReferences(semantics)

			c.Assert(references, qt.DeepEquals, test.want)
		})
	}
}
