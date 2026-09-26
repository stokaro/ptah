package compare

import (
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// unnamedDefinition is what identifies a constraint that carries no name: the
// definition, since there is no name to tell two of them apart.
//
// Keyed by the empty name, every unnamed constraint of one kind on one table is
// one map entry. Two unnamed CHECKs on a table would then compare as one, the
// later written over the earlier, and a plan against a database holding both
// drops the earlier one for good (stokaro/ptah#3729). A PostgreSQL schema file
// names its CHECKs the way the server does, which hides this there; a SQL file
// read for another dialect, or a model an embedder builds, still reaches the
// comparison with unnamed constraints.
//
// The definition is what both sides of a comparison can agree on without a
// name. A desired state compared with another desired state -- `schema diff`
// between two files -- has unnamed constraints on both sides, and the same
// unnamed CHECK in both files pairs with itself by its condition. A live
// catalog names every constraint, so an unnamed declaration never pairs with a
// catalog row: it is planned as an addition, and never merged with another.
type unnamedDefinition struct {
	columns        []string
	check          string
	usingMethod    string
	excludes       string
	where          string
	foreignTable   string
	foreignColumns []string
}

// unnamedDeclaredDefinition reads the definition of a desired constraint.
func unnamedDeclaredDefinition(constraint schemamodel.Constraint) unnamedDefinition {
	return unnamedDefinition{
		columns:        constraint.Columns,
		check:          constraint.CheckExpression,
		usingMethod:    constraint.UsingMethod,
		excludes:       constraint.ExcludeElements,
		where:          constraint.WhereCondition,
		foreignTable:   constraint.ForeignTable,
		foreignColumns: constraint.ForeignColumnsOrDefault(),
	}
}

// unnamedCatalogDefinition reads the definition of a constraint the other side
// holds.
func unnamedCatalogDefinition(constraint catalog.Constraint) unnamedDefinition {
	return unnamedDefinition{
		columns:        constraint.ColumnNamesOrDefault(),
		check:          getStringValue(constraint.CheckClause),
		usingMethod:    getStringValue(constraint.UsingMethod),
		excludes:       getStringValue(constraint.ExcludeElements),
		where:          getStringValue(constraint.WhereCondition),
		foreignTable:   getStringValue(constraint.ForeignTable),
		foreignColumns: constraint.ForeignColumnsOrDefault(),
	}
}

// member folds the definition into the member half of a key. It starts with a
// NUL byte, which no constraint name holds, so it cannot equal the key of a
// named constraint.
//
// Names are folded by the target's rules and a condition by the comparison's
// own normalizer, so two spellings the comparison calls equal share a key. The
// referenced table is compared by its bare name: a schema file and a catalog
// qualify it differently, and a foreign key is still compared in full once it
// is paired.
func (d unnamedDefinition) member(semantics identifier.Semantics) string {
	foreignTable := d.foreignTable
	if ref, ok := tableref.Parse(foreignTable); ok {
		foreignTable = ref.Name
	}
	parts := []string{
		"\x00unnamed",
		foldNames(d.columns, semantics),
		normalizeCheckExpression(d.check),
		strings.ToLower(strings.TrimSpace(d.usingMethod)),
		strings.Join(strings.Fields(d.excludes), " "),
		normalizeCheckExpression(d.where),
		semantics.IndexIdentityKey(foreignTable),
		foldNames(d.foreignColumns, semantics),
	}
	return strings.Join(parts, "\x00")
}

func foldNames(names []string, semantics identifier.Semantics) string {
	folded := make([]string, len(names))
	for i, name := range names {
		folded[i] = semantics.ColumnIdentityKey(name)
	}
	return strings.Join(folded, "\x01")
}

// newDeclaredConstraintKey keys a desired constraint: by its name, or by its
// definition when it has none.
func newDeclaredConstraintKey(constraint schemamodel.Constraint, semantics identifier.Semantics) tableMemberKey {
	key := newConstraintKey(constraint.Table, constraint.Name, constraint.Type, semantics)
	if strings.TrimSpace(constraint.Name) == "" {
		key.member = unnamedDeclaredDefinition(constraint).member(semantics)
	}
	return key
}

// newCatalogConstraintKey keys a constraint the other side holds, by the rule
// [newDeclaredConstraintKey] follows.
func newCatalogConstraintKey(constraint catalog.Constraint, semantics identifier.Semantics) tableMemberKey {
	key := newConstraintKey(constraint.QualifiedTableName(), constraint.Name, constraint.Type, semantics)
	if strings.TrimSpace(constraint.Name) == "" {
		key.member = unnamedCatalogDefinition(constraint).member(semantics)
	}
	return key
}
