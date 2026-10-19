// Package mysqlcomparison holds the rules MySQL and MariaDB give the shared
// schema comparison ([schemacomparison.Rules]): how an index definition and a
// generated column's expression read on the two engines, and which index the
// server built for a foreign key. The built-in runtime registers them on its
// MySQL and MariaDB targets, so the shared comparison holds no knowledge of
// either engine of its own.
package mysqlcomparison

import (
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/platform/descriptor"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacomparison"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/descriptortable"
	"ptah.run/internal/mysqlindex"
	"ptah.run/internal/mysqlname"
	"ptah.run/internal/normalize"
)

// Rules returns the comparison rules of dialect, MySQL or MariaDB. The two
// engines differ in the access methods they record and in which index can back
// a foreign key, and the rules ask dialect's descriptor and the helpers they
// share with the reader about both.
func Rules(dialect string) schemacomparison.Rules {
	described, _ := platform.Lookup(dialect)
	return schemacomparison.Rules{
		Indexes:              Indexes{dialect: dialect},
		GeneratedExpressions: GeneratedExpressions{},
		ForeignKeyIndexes: ForeignKeyIndexes{
			numbersKeys:      described.ForeignKeyNaming == descriptor.ForeignKeyNamingTableNumbered,
			nameRules:        described.ForeignKeyNameRules,
			descendingCovers: described.IndexNaming.DescendingCovers,
		},
	}
}

// Indexes compares MySQL and MariaDB index definitions.
//
// Registering the rules has the shared comparison compare the definition the
// reader takes from information_schema.STATISTICS: the uniqueness, and each
// key part's column or expression and its direction, beside the options
// below.
//
// Uniqueness has to be compared, and it is why these engines compare more
// than an index's name. A UNIQUE constraint is its index here, so the shared
// comparison lets a declared identity through even where a UNIQUE constraint
// of that name owns it, and without the uniqueness a desired plain `index
// "uq_users_email"` would match a database `UNIQUE KEY uq_users_email` and
// report synced (stokaro/ptah#1245). Measured on MySQL 9.7.1, the pinned
// community binary v1.3.0 plans `ALTER TABLE users DROP INDEX uq_users_email`
// followed by `ALTER TABLE users ADD INDEX uq_users_email (email)` for that
// pair. Key parts are compared for the same reason: `UNIQUE KEY uq (email)`
// against a desired unique `uq (name)` is another index, and comparing them is
// what keeps the pair from planning a CREATE and a DROP of one name that the
// server answers with `Error 1061 (42000): Duplicate key name`.
//
// MySQL 26.7 and MariaDB 12.3 change no part of an index definition in place,
// so a changed definition is a rebuild, which MySQL 8.4.11 and MariaDB 11.8.9
// take as the drop and the add in one statement (stokaro/ptah#3853).
type Indexes struct {
	dialect string
}

// OptionsEqual reports whether a MySQL or MariaDB index has the access method,
// the comment, and each key part the prefix length, declared for it.
//
// The access method is compared through [mysqlindex.Kind], the classification
// the renderer writes the DDL prefix from. A desired SPATIAL index and a BTREE
// index of one name over one column are two schema states, and comparing
// neither reported `InSync` for a table without the requested method
// (stokaro/ptah#2721). Which direction is compared, and why only one, is at
// [mysqlindex.Kind.SatisfiedBy].
//
// The comment is part of the definition on these engines, kept in
// STATISTICS.INDEX_COMMENT, and no statement changes it in place.
//
// A prefix is the number the reader reports in SUB_PART. A key the reader
// could not read whole ([catalog.Index.KeyPartsIncomplete]) has no prefix to
// line up with the declaration, and the shared comparison compares the names
// it was read with.
func (i Indexes) OptionsEqual(desired schemamodel.Index, current catalog.Index) bool {
	if methodChanged(desired, current, i.dialect) || desired.Comment != current.Comment {
		return false
	}
	if current.KeyPartsIncomplete {
		return true
	}
	desiredParts := declaredParts(desired)
	currentParts := reportedParts(current)
	if len(desiredParts) != len(currentParts) {
		// A different number of key parts is the shared comparison's to report.
		return true
	}
	for position, part := range desiredParts {
		if strings.TrimSpace(part.Prefix) != strings.TrimSpace(currentParts[position].Prefix) {
			return false
		}
	}
	return true
}

// RenameCarries reports that a MySQL or MariaDB index carries no facet through
// a rename, so an index with a facet is dropped and built again rather than
// renamed.
func (Indexes) RenameCarries(schemaext.Kind) bool {
	return false
}

// methodChanged answers whether the server's index fails to satisfy the
// access method the desired index asks for.
//
// Both sides go through one classifier, which is what keeps this from becoming
// a string comparison that agrees with the renderer today and stops agreeing
// the moment either spelling moves.
func methodChanged(desired schemamodel.Index, current catalog.Index, dialect string) bool {
	if !mysqlindex.KindOf(desired.Type).SatisfiedBy(mysqlindex.KindOf(current.Method)) {
		return true
	}
	// The USING clause is a second question about the same field, and it needs
	// the dialect because only one of the two engines records what was asked
	// for. [mysqlindex.MethodSatisfiedBy] carries the per-engine measurement.
	return !mysqlindex.MethodSatisfiedBy(dialect, desired.Type, current.Method)
}

// declaredParts are the key parts of a declared index, or its fields as
// ascending parts where it declares no parts.
func declaredParts(index schemamodel.Index) []schemamodel.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]schemamodel.IndexPart, len(index.Fields))
	for position, field := range index.Fields {
		parts[position] = schemamodel.IndexPart{Name: field}
	}
	return parts
}

// reportedParts are the key parts of a reported index, or its columns as
// ascending parts where the reader reported no parts.
func reportedParts(index catalog.Index) []catalog.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]catalog.IndexPart, len(index.Columns))
	for position, column := range index.Columns {
		parts[position] = catalog.IndexPart{Name: column}
	}
	return parts
}

// GeneratedExpressions reads the expression of a MySQL or MariaDB generated
// column.
//
// The catalog prints the expression back in its own spelling: names in
// backticks, without spaces, and `lcase(` for a declared `lower(`. Both sides
// are reduced to that spelling with the quoting and the case folded.
type GeneratedExpressions struct{}

// Spelling returns expression folded the way a catalog reprints an expression
// ([normalize.CatalogExpression]), with `lcase(` read as `lower(`. A
// double-quoted token is a string under the default SQL mode of both engines,
// so a double quote opens a string as a single quote does.
func (GeneratedExpressions) Spelling(expression string) string {
	return normalize.RenameFunction(normalize.CatalogExpression(expression, `'"`), "lcase", "lower")
}

// ForeignKeyIndexes finds the index MySQL or MariaDB built for a foreign key.
type ForeignKeyIndexes struct {
	// numbersKeys is whether the descriptor's ForeignKeyNaming is
	// descriptor.ForeignKeyNamingTableNumbered: the server names a key
	// written without a name by the rules of [mysqlname], and the index it
	// builds for the key after the key's first column.
	numbersKeys bool
	// nameRules is the descriptor's ForeignKeyNameRules: which other names
	// the server gives a key written without one.
	nameRules descriptor.ForeignKeyNameRules
	// descendingCovers is the descriptor's IndexNaming.DescendingCovers: an
	// index whose leading parts descend can still back a key.
	descendingCovers bool
}

// BackingIndexes returns the index the server built for key, where it built
// one at all.
//
// The name alone cannot decide it, because a name is not evidence. Measured on
// MySQL 8.4.11 and MariaDB 11.8.9, both engines accept this table:
//
//	CREATE TABLE children (
//	    a INT, b INT,
//	    KEY cover (a), KEY f (b),
//	    CONSTRAINT f FOREIGN KEY (a) REFERENCES parents(id));
//
// `cover(a)` backs the constraint and `f(b)` is an ordinary performance index
// that happens to share the symbol. Claiming it for the foreign key suppresses
// it from the comparison, so removing it from the desired schema reports
// `InSync` with no plan and leaves a live index nothing will ever manage
// (stokaro/ptah#2782).
//
// Both signals are required: the NAME says the engine could have made it, and
// the LEADING COLUMNS say it could serve the key. Neither is sufficient -- a
// covering index under the author's own name is droppable, and a same-named
// index over different columns is a different object.
//
// A third condition joins them, and it is what the reader recording key-part
// direction unblocked (stokaro/ptah#2816). The engine builds this index only
// where nothing else covers the key, so a same-named index is still the
// author's where another index backs the key too, and dropping it is what the
// author asked for.
//
// Covering differs between the engines, measured on MySQL 8.4.11 and MariaDB
// 11.8.9 by declaring one index and then adding the foreign key, and reading
// back whether the engine built a second index of its own:
//
//	cover           MySQL              MariaDB
//	(a)             reuses cover       reuses cover
//	(a DESC)        builds its own     reuses cover
//	(a, b DESC)     reuses cover       reuses cover
//	(a DESC, b)     builds its own     reuses cover
//
// So MySQL requires the LEADING parts -- the ones the key uses -- to be
// ascending, and MariaDB does not care. The third row is what makes that
// precise rather than "no descending part anywhere".
func (f ForeignKeyIndexes) BackingIndexes(
	current *catalog.Database,
	key catalog.Constraint,
	semantics identifier.Semantics,
) []catalog.Index {
	columns := uniqueStringsPreserveOrder(key.ColumnNamesOrDefault())
	if len(columns) == 0 {
		return nil
	}
	table := key.QualifiedTableName()
	var backing []catalog.Index
	for _, index := range current.Indexes {
		if !f.engineCouldNameIndex(index, key, columns[0], semantics) {
			continue
		}
		if !indexBacksForeignKey(index, table, columns, semantics) {
			continue
		}
		if f.anotherIndexCoversForeignKey(current, index, table, columns, semantics) {
			continue
		}
		backing = append(backing, index)
	}
	return backing
}

// engineCouldNameIndex is the NAME signal of [ForeignKeyIndexes.BackingIndexes]:
// whether the engine could have given the index this name when it built one
// for the key.
//
// A key with a name gets an index under the key's name, on both engines. A key
// written without one is named `<table>_ibfk_<n>`, or `<n>` on MariaDB 12.1
// and later, and its index is named after the key's first column instead, `_2`
// and on when that is taken: measured on MySQL 8.4.11, 26.7.0 and MariaDB
// 11.8.9 and 12.3.3, `FOREIGN KEY (p_id) REFERENCES p(id)` holds `c_ibfk_1`,
// or `1`, over an index called `p_id`. Without the second shape the index
// reads as one nobody declared, and a schema file compared with the database
// it built plans `DROP INDEX p_id`, which the server refuses while the key
// needs it (stokaro/ptah#3725, stokaro/ptah#3743). The constraint's name is
// what keeps a column-named index of the author's out: a key whose name the
// engine could not have chosen was written with one, and its index carries
// that name.
func (f ForeignKeyIndexes) engineCouldNameIndex(
	index catalog.Index,
	key catalog.Constraint,
	firstColumn string,
	semantics identifier.Semantics,
) bool {
	if identifiersEqual(semantics, index.Name, key.Name) {
		return true
	}
	if !f.numbersKeys || !mysqlname.IsServerForeignKeyName(f.nameRules, key.TableName, key.Name) {
		return false
	}
	return mysqlname.IsUnnamedKeyIndexName(familyBaseBytes.Get(), firstColumn, index.Name, func(a, b string) bool {
		return identifiersEqual(semantics, a, b)
	})
}

// familyBaseBytes are the IndexNaming.BaseBytes the registered dialects of
// the table-numbered foreign key naming state, each once and none zero, in
// registry order. The index name is accepted cut as any of them would cut it,
// because a database one engine of the family built can be read on another:
// a MySQL dump loaded into MariaDB keeps the index MySQL named, and the
// comparison would otherwise plan to drop an index the key needs.
var familyBaseBytes = descriptortable.NewIndex(func(dialects []descriptor.Dialect) []int {
	var cuts []int
	for _, described := range dialects {
		bytes := described.IndexNaming.BaseBytes
		if described.ForeignKeyNaming != descriptor.ForeignKeyNamingTableNumbered || bytes == 0 || slices.Contains(cuts, bytes) {
			continue
		}
		cuts = append(cuts, bytes)
	}
	return cuts
})

// anotherIndexCoversForeignKey reports whether some index other than the
// candidate already serves the key, which is what makes the candidate the
// author's rather than the engine's.
//
// Identity is compared through the target's own semantics rather than by
// string, so the candidate is not mistaken for a second index under a spelling
// the engine folds onto the same name.
func (f ForeignKeyIndexes) anotherIndexCoversForeignKey(
	current *catalog.Database,
	candidate catalog.Index,
	table string,
	columns []string,
	semantics identifier.Semantics,
) bool {
	for _, other := range current.Indexes {
		if identifiersEqual(semantics, other.Name, candidate.Name) &&
			strings.EqualFold(other.QualifiedTableName(), candidate.QualifiedTableName()) {
			continue
		}
		if f.indexCoversForeignKey(other, table, columns, semantics) {
			return true
		}
	}
	return false
}

// indexCoversForeignKey reports whether an index can serve as the engine's
// backing index for a foreign key over these columns.
//
// Leading columns decide it on both engines. Direction decides it on MySQL
// alone, and only for the parts the key uses: see the table on
// [ForeignKeyIndexes.BackingIndexes] for the measurement, including the row
// that separates "the leading parts are ascending" from "no part is
// descending".
//
// An index whose Parts the reader dropped is ascending throughout -- the MySQL
// reader clears a parts list that says nothing its column names do not, and a
// descending part is one of the two reasons it is kept.
func (f ForeignKeyIndexes) indexCoversForeignKey(
	index catalog.Index,
	table string,
	columns []string,
	semantics identifier.Semantics,
) bool {
	if !indexBacksForeignKey(index, table, columns, semantics) {
		return false
	}
	if f.descendingCovers {
		return true
	}
	return !leadingPartsDescend(index, len(columns))
}

// leadingPartsDescend reports whether any of an index's first count parts is
// descending.
func leadingPartsDescend(index catalog.Index, count int) bool {
	for position, part := range index.Parts {
		if position >= count {
			return false
		}
		if part.Desc {
			return true
		}
	}
	return false
}

// indexBacksForeignKey reports whether one index could serve a foreign key on
// table: its LEADING columns are the key's, which is the engines' own rule.
func indexBacksForeignKey(
	index catalog.Index,
	table string,
	columns []string,
	semantics identifier.Semantics,
) bool {
	if !strings.EqualFold(strings.TrimSpace(index.QualifiedTableName()), strings.TrimSpace(table)) {
		return false
	}
	if len(index.Columns) < len(columns) {
		return false
	}
	return sameColumnNames(semantics, columns, index.Columns[:len(columns)])
}

// identifiersEqual compares two index names under the target's own rules.
func identifiersEqual(semantics identifier.Semantics, left, right string) bool {
	return semantics.IndexIdentityKey(strings.TrimSpace(left)) ==
		semantics.IndexIdentityKey(strings.TrimSpace(right))
}

// sameColumnNames compares two column lists in order, under the target's
// column-name comparison.
func sameColumnNames(semantics identifier.Semantics, left, right []string) bool {
	if len(left) != len(right) {
		return false
	}
	for position, name := range left {
		if semantics.ColumnIdentityKey(name) != semantics.ColumnIdentityKey(right[position]) {
			return false
		}
	}
	return true
}

// uniqueStringsPreserveOrder drops the repeats of values, keeping the first.
func uniqueStringsPreserveOrder(values []string) []string {
	result := make([]string, 0, len(values))
	seen := make(map[string]struct{}, len(values))
	for _, value := range values {
		if _, ok := seen[value]; ok {
			continue
		}
		seen[value] = struct{}{}
		result = append(result, value)
	}
	return result
}
