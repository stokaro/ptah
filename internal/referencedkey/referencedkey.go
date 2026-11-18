// Package referencedkey holds the referenced-key rule of a foreign key: which
// columns of the referenced table a target accepts as the key the foreign key
// references. The target's capabilities name the policy: a declared unique
// key, the leftmost prefix of an index, or any columns where the server builds
// the index itself.
//
// The built-in renderer's schema validation and the schema-diff check of a
// --from document both ask [Check], so the rule they apply cannot drift apart.
package referencedkey

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/schemaprep"
	"ptah.run/internal/tablelookup"
)

// Check refuses a foreign key that references columns of target the policy in
// caps does not accept as a key, with a [ptaherr.CapabilityError] that
// satisfies errors.Is(err, ptaherr.ErrUnsupportedFeature). database is the
// schema target belongs to, read for its keys and indexes. Under the
// unique-key policy a key of a partition's declared parent serves a reference
// to the partition, and a partition whose parent the schema does not declare
// is accepted, since only the server can say what keys that parent holds.
func Check(
	database schemamodel.Database,
	dialect string,
	caps capability.Capabilities,
	target schemamodel.Table,
	columns []string,
) error {
	switch {
	case caps.Has(capability.ForeignKeysRequireUniqueReference):
		// A partition holds a copy of each key of its parent, so a key declared
		// there serves a reference to the partition. Above a parent no table
		// declares, only the server can say.
		ancestors, complete := schemamodel.FacetAncestors(database.Tables, target)
		if !complete || tableHasUniqueKey(database, dialect, target, columns) || slices.ContainsFunc(ancestors, func(ancestor schemamodel.Table) bool {
			return tableHasUniqueKey(database, dialect, ancestor, columns)
		}) {
			return nil
		}
		return uniqueReferenceError(dialect, target, columns)
	case caps.Has(capability.ForeignKeysRequireIndexedReference):
		if !tableHasIndexedKey(database, target, columns) {
			return indexedReferenceError(dialect, target, columns)
		}
	case caps.Has(capability.ForeignKeysCreateBackingIndex):
		return nil
	}
	return nil
}

func tableHasUniqueKey(database schemamodel.Database, dialect string, table schemamodel.Table, columns []string) bool {
	same := referencedKeyMatcher(dialect)
	return tablePrimaryKeyEquals(table, columns, same) ||
		tableFieldsHaveUniqueKey(allDatabaseFields(database), table, columns, same) ||
		tableConstraintsHaveUniqueKey(database, table, columns, same) ||
		tableIndexesHaveUniqueKey(database, dialect, table, columns, same)
}

// referencedKeyMatcher answers whether a key's columns are the ones a foreign key
// references on dialect: in the same order, or in any order on a server that takes
// that; see [ptah.run/core/platform/descriptor.Comparison.ForeignKeyMatchesKeyInAnyOrder].
func referencedKeyMatcher(dialect string) func(key, referenced []string) bool {
	described, found := platform.Lookup(dialect)
	if !found || !described.Comparison.ForeignKeyMatchesKeyInAnyOrder {
		return slices.Equal[[]string]
	}
	return func(key, referenced []string) bool {
		return len(key) == len(referenced) && !slices.ContainsFunc(referenced, func(column string) bool {
			return !slices.Contains(key, column)
		})
	}
}

func tablePrimaryKeyEquals(table schemamodel.Table, columns []string, same func(key, referenced []string) bool) bool {
	return same(table.PrimaryKey, columns) ||
		(primaryKeyPartsAreFullColumns(table.PrimaryKeyParts) &&
			same(primaryKeyPartNames(table.PrimaryKeyParts), columns))
}

func tableFieldsHaveUniqueKey(fields []schemamodel.Field, table schemamodel.Table, columns []string, same func(key, referenced []string) bool) bool {
	var primaryFields []string
	for _, field := range fields {
		if field.StructName != table.StructName {
			continue
		}
		if field.Primary {
			primaryFields = append(primaryFields, field.Name)
		}
		if len(columns) == 1 && field.Name == columns[0] && field.Unique {
			return true
		}
	}
	return same(primaryFields, columns)
}

func tableConstraintsHaveUniqueKey(
	database schemamodel.Database,
	table schemamodel.Table,
	columns []string,
	same func(key, referenced []string) bool,
) bool {
	for _, constraint := range database.Constraints {
		owner := constraintOwnerTable(database.Tables, constraint)
		if owner != nil && owner.QualifiedName() == table.QualifiedName() &&
			(strings.EqualFold(constraint.Type, "PRIMARY KEY") || strings.EqualFold(constraint.Type, "UNIQUE")) &&
			same(constraint.Columns, columns) {
			return true
		}
	}
	return false
}

func tableIndexesHaveUniqueKey(
	database schemamodel.Database,
	dialect string,
	table schemamodel.Table,
	columns []string,
	same func(key, referenced []string) bool,
) bool {
	// Where the descriptor says ForeignKeyKeyIsInline, accepting a standalone
	// unique index could produce a deferred "foreign key mismatch" at the first
	// write.
	if described, found := platform.Lookup(dialect); found && described.Comparison.ForeignKeyKeyIsInline {
		return false
	}
	indexOwners := schemamodel.ResolveIndexTableNames(database.Indexes, database.Tables)
	for i, index := range database.Indexes {
		indexColumns, valid := fullIndexColumnNames(index)
		if index.Unique && valid && strings.TrimSpace(index.Condition) == "" &&
			indexOwners[i] == table.QualifiedName() && same(indexColumns, columns) {
			return true
		}
	}
	return false
}

func tableHasIndexedKey(database schemamodel.Database, table schemamodel.Table, columns []string) bool {
	if isColumnPrefix(table.PrimaryKey, columns) ||
		(primaryKeyPartsAreFullColumns(table.PrimaryKeyParts) &&
			isColumnPrefix(primaryKeyPartNames(table.PrimaryKeyParts), columns)) {
		return true
	}

	var primaryFields []string
	for _, field := range allDatabaseFields(database) {
		if field.StructName != table.StructName {
			continue
		}
		if field.Primary {
			primaryFields = append(primaryFields, field.Name)
		}
		if len(columns) == 1 && field.Name == columns[0] && field.Unique {
			return true
		}
	}
	if isColumnPrefix(primaryFields, columns) {
		return true
	}

	for _, constraint := range database.Constraints {
		owner := constraintOwnerTable(database.Tables, constraint)
		if owner != nil && owner.QualifiedName() == table.QualifiedName() &&
			(strings.EqualFold(constraint.Type, "PRIMARY KEY") || strings.EqualFold(constraint.Type, "UNIQUE")) &&
			isColumnPrefix(constraint.Columns, columns) {
			return true
		}
	}
	indexOwners := schemamodel.ResolveIndexTableNames(database.Indexes, database.Tables)
	for i, index := range database.Indexes {
		if indexOwners[i] == table.QualifiedName() && indexHasFullColumnPrefix(index, columns) {
			return true
		}
	}
	return false
}

func allDatabaseFields(database schemamodel.Database) []schemamodel.Field {
	return schemamodel.ProcessEmbeddedFields(database.EmbeddedFields, database.Fields)
}

func isColumnPrefix(keyColumns, referencedColumns []string) bool {
	return len(referencedColumns) > 0 && len(keyColumns) >= len(referencedColumns) &&
		slices.Equal(keyColumns[:len(referencedColumns)], referencedColumns)
}

func primaryKeyPartsAreFullColumns(parts []schemamodel.PrimaryKeyPart) bool {
	if len(parts) == 0 {
		return false
	}
	for _, part := range parts {
		if strings.TrimSpace(part.Name) == "" || strings.TrimSpace(part.Prefix) != "" {
			return false
		}
	}
	return true
}

func primaryKeyPartNames(parts []schemamodel.PrimaryKeyPart) []string {
	names := make([]string, 0, len(parts))
	for _, part := range parts {
		names = append(names, part.Name)
	}
	return names
}

func fullIndexColumnNames(index schemamodel.Index) ([]string, bool) {
	if len(index.Parts) == 0 {
		return index.Fields, len(index.Fields) > 0
	}
	names := make([]string, 0, len(index.Parts))
	for _, part := range index.Parts {
		if strings.TrimSpace(part.Name) == "" || strings.TrimSpace(part.Expr) != "" ||
			strings.TrimSpace(part.Prefix) != "" || strings.TrimSpace(part.Operator) != "" {
			return nil, false
		}
		names = append(names, part.Name)
	}
	return names, true
}

func indexHasFullColumnPrefix(index schemamodel.Index, columns []string) bool {
	if strings.TrimSpace(index.Condition) != "" || len(columns) == 0 {
		return false
	}
	indexType := strings.ToUpper(strings.TrimSpace(index.Type))
	if (indexType != "" && indexType != "BTREE") || mysqlIndexParser(index) != "" {
		return false
	}
	if len(index.Parts) == 0 {
		return isColumnPrefix(index.Fields, columns)
	}
	if len(index.Parts) < len(columns) {
		return false
	}
	for i, column := range columns {
		part := index.Parts[i]
		if part.Name != column || strings.TrimSpace(part.Expr) != "" ||
			strings.TrimSpace(part.Prefix) != "" || strings.TrimSpace(part.Operator) != "" {
			return false
		}
	}
	return true
}

func uniqueReferenceError(dialect string, table schemamodel.Table, columns []string) error {
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: "foreign key referenced key uniqueness",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s requires referenced columns %s on table %q to be declared unique",
			platform.NormalizeDialect(dialect),
			strings.Join(columns, ", "),
			table.QualifiedName(),
		),
	}
}

func indexedReferenceError(dialect string, table schemamodel.Table, columns []string) error {
	return &ptaherr.CapabilityError{
		Dialect: dialect,
		Feature: "foreign key referenced key index",
		Err:     ptaherr.ErrUnsupportedFeature,
		Message: fmt.Sprintf(
			"%s requires referenced columns %s on table %q to be the full leftmost prefix of an index",
			platform.NormalizeDialect(dialect),
			strings.Join(columns, ", "),
			table.QualifiedName(),
		),
	}
}

// mysqlIndexParser is the FULLTEXT parser an index declares, the MySQL owner's
// index option, and empty for an index that declares none.
func mysqlIndexParser(index schemamodel.Index) string {
	options, _, err := schemaext.FacetAs[*mysqlschema.DesiredIndex](index.Facets, mysqlschema.IndexKind)
	if err != nil || options == nil {
		return ""
	}
	return options.Parser
}

// Problems reports, in declaration order, every foreign key of database whose
// referenced columns [Check] refuses: the keys columns declare, then the
// FOREIGN KEY constraints. A key whose owner or referenced table database does
// not declare is left out; reporting those is another rule's work.
func Problems(database schemamodel.Database, dialect string, caps capability.Capabilities) []error {
	var problems []error
	for _, field := range schemamodel.WithParentColumns(database.Tables, allDatabaseFields(database)) {
		if field.Foreign == "" {
			continue
		}
		owner := tableByStructName(database.Tables, field.StructName)
		if owner == nil {
			continue
		}
		reference := schemaprep.ParseForeignKeyReference(field.Foreign)
		target := referencedTable(database.Tables, *owner, reference.Table)
		if target == nil {
			continue
		}
		if err := Check(database, dialect, caps, *target, reference.ReferencedColumns()); err != nil {
			problems = append(problems, err)
		}
	}
	for _, constraint := range database.Constraints {
		if !strings.EqualFold(strings.TrimSpace(constraint.Type), "FOREIGN KEY") {
			continue
		}
		owner := constraintOwnerTable(database.Tables, constraint)
		if owner == nil {
			continue
		}
		target := referencedTable(database.Tables, *owner, constraint.ForeignTable)
		if target == nil {
			continue
		}
		if err := Check(database, dialect, caps, *target, constraint.ForeignColumnsOrDefault()); err != nil {
			problems = append(problems, err)
		}
	}
	return problems
}

func tableByStructName(tables []schemamodel.Table, structName string) *schemamodel.Table {
	for i := range tables {
		if tables[i].StructName == structName {
			return &tables[i]
		}
	}
	return nil
}

func constraintOwnerTable(tables []schemamodel.Table, constraint schemamodel.Constraint) *schemamodel.Table {
	for i := range tables {
		if schemaprep.ConstraintBelongsToTable(constraint, tables[i]) {
			return &tables[i]
		}
	}
	return nil
}

func referencedTable(tables []schemamodel.Table, owner schemamodel.Table, reference string) *schemamodel.Table {
	resolved := tablelookup.ResolveReference(tables, owner, reference)
	for i := range tables {
		if tables[i].QualifiedName() == resolved {
			return &tables[i]
		}
	}
	return nil
}
