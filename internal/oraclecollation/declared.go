package oraclecollation

import (
	"fmt"
	"log/slog"
	"slices"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemaprep"
)

// RefuseDeclared refuses a declaration whose collations Oracle would refuse
// partway through a plan, before the comparison plans anything.
//
// Oracle commits each DDL statement on its own, so a statement the server
// refuses leaves every statement before it applied. Two declarations reach the
// server that way and are decided here, from the declaration and the database
// read:
//
//   - A collation name Oracle does not know answers ORA-12746. A YAML or HCL
//     schema shared with MySQL (`utf8mb4_bin`) is the usual source, and the
//     tables created before the column commit. The names come from sorts, the
//     server's V$NLS_VALID_VALUES list of sorts. When sorts is empty the
//     connector could not read it, and the check is left to the server, which
//     the log says.
//   - A DESC key part over a collated column answers ORA-54034 when another
//     index or key already holds the column ([refuseDescendingCollatedKeys]).
//     A column holds a collation when the declaration names one other than
//     USING_NLS_COMP, or, where it names none, when the column the database read
//     holds one (it is not compared, so the column keeps it).
//
// dialect is the target the comparison plans for; any other than Oracle is left
// alone, so a caller that serves every dialect calls this without naming one.
// observed may be nil, as it is for a comparison of two descriptions.
func RefuseDeclared(dialect string, desired *schemamodel.Database, observed *catalog.Database, sorts []string) error {
	if dialect != platform.Oracle || desired == nil {
		return nil
	}
	declaresAny := false
	for _, field := range desired.Fields {
		collation := declaredCollation(field)
		if collation == "" {
			continue
		}
		declaresAny = true
		if len(sorts) > 0 && !Known(collation, sorts) {
			return fmt.Errorf("%w: oracle: column %q of table %q declares collation %q, which this database does not know "+
				"(ORA-12746); a collation is a sort from V$NLS_VALID_VALUES, with _CI or _AI after it, or USING_NLS_COMP",
				ptaherr.ErrUnsupportedFeature, field.Name, tableNameOf(desired, field.StructName), collation)
		}
	}
	if declaresAny && len(sorts) == 0 {
		slog.Warn("oracle: the collation names this database accepts could not be read from V$NLS_VALID_VALUES, " +
			"so a declared collation name is checked by the server, which answers ORA-12746 after the statements before it have run")
	}
	return refuseDescendingCollatedKeys(desired, observed)
}

// declaredCollation is the collation the declaration gives field on Oracle,
// after the target's platform overrides.
func declaredCollation(field schemamodel.Field) string {
	return strings.TrimSpace(schemaprep.EffectiveFieldForPlatform(field, platform.Oracle).Collate)
}

// tableNameOf is the name of the table the struct declares, or the struct's own
// name where the declaration names no table.
func tableNameOf(desired *schemamodel.Database, structName string) string {
	for _, table := range desired.Tables {
		if table.StructName == structName {
			return table.Name
		}
	}
	return structName
}

// refuseDescendingCollatedKeys refuses a new index whose DESC key part is over a
// collated column that an ascending key holds when the index is created.
//
// Measured on Oracle Free 23.26 with MAX_STRING_SIZE=EXTENDED, over a column c
// that declares BINARY_CI, `CREATE INDEX i ON t (c DESC)` (or `(d, c DESC)`,
// `(c DESC, d)`) answers ORA-54034, "virtual columns not allowed in functional
// index expressions", exactly when an index or key created before it holds c in
// an ascending part: `(c)`, `(c, d)`, `(d, c)`, a UNIQUE or PRIMARY KEY on c.
// It is accepted as the first index over c, and after indexes that hold c only
// as a DESC part, in any of those shapes (DESC, then DESC, then DESC, then an
// ascending index). A function-based index over c, such as UPPER(c), holds
// nothing, and a UNIQUE constraint added after the DESC index is accepted.
//
// The plan creates a table's new indexes in name order (byte order of the name
// as declared; [difftypes.SchemaDiff.SetIndexAdditions]), after the keys of a
// CREATE TABLE or an ADD COLUMN and before a constraint it adds with ALTER
// TABLE, and the check follows that order. The indexes the database already
// holds come first. An index the database already holds is not created again
// and is left alone.
func refuseDescendingCollatedKeys(desired *schemamodel.Database, observed *catalog.Database) error {
	owners := schemamodel.ResolveIndexOwners(desired.Indexes, desired.Tables, desired.MaterializedViews)
	for position, index := range desired.Indexes {
		table := owners[position]
		if observedIndex(observed, table, index.Name) {
			continue
		}
		for _, part := range keyParts(index) {
			if !part.Desc || part.Name == "" {
				continue
			}
			collation := collationOfColumn(desired, observed, table, part.Name)
			if collation == "" {
				continue
			}
			other := ascendingHolder(desired, observed, owners, position, part.Name)
			if other == "" {
				continue
			}
			return fmt.Errorf("%w: oracle: index %q of table %q orders column %q DESC, and the column holds collation %s while %s "+
				"already holds it in an ascending key when the index is created; Oracle answers ORA-54034 to that. A DESC key part over "+
				"a collated column is accepted as the first index over the column and after indexes that hold it only as DESC. "+
				"Drop DESC from the key part, rename the indexes so the DESC one is created first (new indexes are created in name order), "+
				"or apply the ascending index in a later migration",
				ptaherr.ErrUnsupportedFeature, index.Name, table, part.Name, collation, other)
		}
	}
	return nil
}

// observedIndex reports whether the database read holds the named index of the table.
func observedIndex(observed *catalog.Database, table, name string) bool {
	return observed != nil && slices.ContainsFunc(observed.Indexes, func(index catalog.Index) bool {
		return strings.EqualFold(index.Name, name) && strings.EqualFold(index.TableName, table)
	})
}

// observedTable is the table the database read holds under name, if it does.
func observedTable(observed *catalog.Database, name string) (catalog.Table, bool) {
	if observed == nil {
		return catalog.Table{}, false
	}
	at := slices.IndexFunc(observed.Tables, func(table catalog.Table) bool { return strings.EqualFold(table.Name, name) })
	if at < 0 {
		return catalog.Table{}, false
	}
	return observed.Tables[at], true
}

// holdsAscending reports whether a key part names column as a plain column
// ordered ascending.
func holdsAscending(part schemamodel.IndexPart, column string) bool {
	return !part.Desc && part.Expr == "" && strings.EqualFold(part.Name, column)
}

// ascendingHolder names an index or key that holds column in an ascending part
// of the same table by the time the declared index at position is created, or
// returns "" when none does. See [refuseDescendingCollatedKeys] for the order.
func ascendingHolder(desired *schemamodel.Database, observed *catalog.Database, owners []string, position int, column string) string {
	table := owners[position]
	mine := desired.Indexes[position]
	for other, index := range desired.Indexes {
		if other == position || !strings.EqualFold(owners[other], table) {
			continue
		}
		// An index the database holds stands already; a new one is created
		// ahead of this one only when its name sorts first.
		created := observedIndex(observed, table, index.Name) || strings.Compare(index.Name, mine.Name) < 0
		if created && slices.ContainsFunc(keyParts(index), func(part schemamodel.IndexPart) bool { return holdsAscending(part, column) }) {
			return fmt.Sprintf("index %q", index.Name)
		}
	}
	if key := declaredKeyHolder(desired, observed, table, column); key != "" {
		return key
	}
	return observedHolder(observed, table, column)
}

// declaredKeyHolder names a PRIMARY KEY or UNIQUE key the declaration gives
// column of the table that exists before the plan's indexes are created: a key
// written on the column or in the CREATE TABLE of a new table, and one written on
// a column the plan adds. A constraint the plan adds to an existing table comes
// after its indexes and holds nothing yet.
func declaredKeyHolder(desired *schemamodel.Database, observed *catalog.Database, table, column string) string {
	held, tableExists := observedTable(observed, table)
	for _, declared := range desired.Tables {
		if !strings.EqualFold(declared.Name, table) && !strings.EqualFold(declared.QualifiedName(), table) {
			continue
		}
		columnExists := tableExists && slices.ContainsFunc(held.Columns, func(candidate catalog.Column) bool {
			return strings.EqualFold(candidate.Name, column)
		})
		for _, field := range desired.Fields {
			if field.StructName == declared.StructName && strings.EqualFold(field.Name, column) && (field.Primary || field.Unique) && !columnExists {
				return "its own key"
			}
		}
		for _, constraint := range desired.Constraints {
			holds := constraint.Type == "PRIMARY KEY" || constraint.Type == "UNIQUE"
			owned := constraint.StructName == declared.StructName || strings.EqualFold(constraint.Table, declared.Name)
			if holds && owned && !tableExists && slices.ContainsFunc(constraint.Columns, func(name string) bool { return strings.EqualFold(name, column) }) {
				return fmt.Sprintf("the %s constraint %q", strings.ToLower(constraint.Type), constraint.Name)
			}
		}
	}
	return ""
}

// observedHolder names an index or a key of the table the database read holds
// over column in an ascending part. Its keys are on the column flags where
// Oracle named them itself.
func observedHolder(observed *catalog.Database, table, column string) string {
	if observed == nil {
		return ""
	}
	for _, index := range observed.Indexes {
		if !strings.EqualFold(index.TableName, table) {
			continue
		}
		parts := index.Parts
		if len(parts) == 0 {
			for _, name := range index.Columns {
				parts = append(parts, catalog.IndexPart{Name: name})
			}
		}
		if slices.ContainsFunc(parts, func(part catalog.IndexPart) bool {
			return !part.Desc && part.Expr == "" && strings.EqualFold(part.Name, column)
		}) {
			return fmt.Sprintf("index %q", index.Name)
		}
	}
	if held, ok := observedTable(observed, table); ok {
		for _, candidate := range held.Columns {
			if strings.EqualFold(candidate.Name, column) && (candidate.IsPrimaryKey || candidate.IsUnique) {
				return "a key of the table"
			}
		}
	}
	return ""
}

// keyParts is the key of index with each part's direction, read from Parts where
// the declaration structured them and from Fields otherwise, where a trailing
// DESC makes the part descending.
func keyParts(index schemamodel.Index) []schemamodel.IndexPart {
	if len(index.Parts) > 0 {
		return index.Parts
	}
	parts := make([]schemamodel.IndexPart, 0, len(index.Fields))
	for _, field := range index.Fields {
		name := strings.TrimSpace(field)
		desc := strings.HasSuffix(strings.ToUpper(name), " DESC")
		if desc {
			name = strings.TrimSpace(name[:len(name)-len(" DESC")])
		}
		parts = append(parts, schemamodel.IndexPart{Name: name, Desc: desc})
	}
	return parts
}

// collationOfColumn is the collation column of table holds, ignoring
// USING_NLS_COMP, and empty when it holds none or the declaration does not
// name the column.
func collationOfColumn(desired *schemamodel.Database, observed *catalog.Database, table, column string) string {
	for _, declared := range desired.Tables {
		if !strings.EqualFold(declared.Name, table) && !strings.EqualFold(declared.QualifiedName(), table) {
			continue
		}
		for _, field := range desired.Fields {
			if field.StructName != declared.StructName || !strings.EqualFold(field.Name, column) {
				continue
			}
			if collation := declaredCollation(field); collation != "" {
				if strings.EqualFold(collation, Inherited) {
					return ""
				}
				return collation
			}
			return observedCollation(observed, declared.Name, field.Name)
		}
	}
	return ""
}

func observedCollation(observed *catalog.Database, table, column string) string {
	if observed == nil {
		return ""
	}
	for _, candidate := range observed.Tables {
		if !strings.EqualFold(candidate.Name, table) {
			continue
		}
		for _, held := range candidate.Columns {
			if strings.EqualFold(held.Name, column) {
				return held.EffectiveCollate
			}
		}
	}
	return ""
}
