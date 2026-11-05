package compare

import (
	"fmt"
	"regexp"
	"slices"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/difftypes"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/objectlookup"
)

// Sequences compares standalone sequence definitions between the target schema
// (Go annotations) and the current database schema.
//
// Only sequences the database reader classifies as standalone reach
// database.Sequences: implicit sequences that back SERIAL / identity columns are
// excluded by the reader, so declaring a plain SERIAL column never produces a
// spurious sequence diff here.
//
// Modification detection is intentionally asymmetric: only options the target
// explicitly sets are compared. A numeric option left unset in the annotation
// (nil pointer) is treated as "unmanaged" and never flagged, so a sequence that
// relies on PostgreSQL defaults does not churn against the catalog's fully
// populated values.
func Sequences(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
) {
	sequencesWithSemantics(desired, database, diff, cov, identifier.ForDialect(""))
}

// SequencesWithSemantics compares sequence identity using the target
// database's resolved default schema and identifier rules.
func SequencesWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	semantics identifier.Semantics,
) {
	sequencesWithSemantics(desired, database, diff, cov, semantics.Normalize(""))
}

func sequencesWithSemantics(
	desired *schemamodel.Database,
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	cov Coverage,
	semantics identifier.Semantics,
) {
	generatedSequences := make(map[objectIdentity]schemamodel.Sequence, len(desired.Sequences))
	generatedNames := make(map[objectIdentity]string, len(desired.Sequences))
	for _, sequence := range desired.Sequences {
		identity := newObjectIdentity(objectidentity.KindSequence, sequence.Schema, sequence.Name, semantics)
		generatedSequences[identity] = sequence
		generatedNames[identity] = sequence.QualifiedName()
	}

	databaseSequences := make(map[objectIdentity]catalog.Sequence, len(database.Sequences))
	databaseNames := make(map[objectIdentity]string, len(database.Sequences))
	for _, sequence := range database.Sequences {
		identity := newObjectIdentity(objectidentity.KindSequence, sequence.Schema, sequence.Name, semantics)
		databaseSequences[identity] = sequence
		databaseNames[identity] = sequence.QualifiedName()
	}

	for identity, generatedSequence := range generatedSequences {
		databaseSequence, exists := databaseSequences[identity]
		if !exists {
			diff.SequencesAdded = append(diff.SequencesAdded, generatedSequence)
			continue
		}
		diff.Matched.Sequences++
		if changes := sequenceChanges(generatedSequence, databaseSequence); len(changes) > 0 {
			diff.SequencesModified = append(diff.SequencesModified, difftypes.SequenceDiff{
				SequenceName: generatedNames[identity],
				Changes:      changes,
				Desired:      generatedSequence,
			})
		}
	}

	for identity, databaseSequence := range databaseSequences {
		if _, exists := generatedSequences[identity]; !exists {
			diff.SequencesRemoved = append(diff.SequencesRemoved, sequenceFromCatalog(databaseSequence))
		}
	}

	// The Atlas-compatible surface omits `sequence` blocks the binary it stands
	// in for refuses; a document that left one out has said nothing about it,
	// and nothing is not a drop (stokaro/ptah#1276).
	//
	// A declared sequence carrying `if_not_exists` is planned even against a
	// read that never looked, because `CREATE SEQUENCE IF NOT EXISTS` is right
	// either way. One without it is withheld and named, since `CREATE SEQUENCE`
	// against an existing sequence fails the migration.
	sequenceGuards := make(map[string]bool, len(generatedSequences))
	for identity, sequence := range generatedSequences {
		sequenceGuards[generatedNames[identity]] = sequence.IfNotExists
	}
	kept, withheld := keepPlannedAdditions(cov,
		coverage.Sequence, diff.SequencesAdded, sequenceSpelling, sequenceDisplay, guardedCreations(sequenceGuards),
	)
	diff.SequencesAdded = kept
	cov.recordUndecidedAdditions(withheld)
	diff.SequencesRemoved = keepPlannedRemovals(cov, coverage.Sequence, diff.SequencesRemoved, sequenceSpelling)

	sortSequences(diff.SequencesAdded)
	sortSequences(diff.SequencesRemoved)
	sort.Slice(diff.SequencesModified, func(i, j int) bool {
		return diff.SequencesModified[i].SequenceName < diff.SequencesModified[j].SequenceName
	})
}

// nextvalDefault is a column default that draws from one sequence, as a catalog
// prints it.
var nextvalDefault = regexp.MustCompile(`(?i)^nextval\('([^']+)'(?:::regclass)?\)$`)

// OwnedDefaultSequences marks each column change of diff that moves a default off
// a sequence its column owns, where the plan keeps that sequence; see
// [difftypes.ColumnDiff.OwnedDefaultSequence]. It runs after the sequences are
// compared, since a removed sequence is not kept.
func OwnedDefaultSequences(database *catalog.Database, diff *difftypes.SchemaDiff, semantics identifier.Semantics) {
	if database == nil || len(database.Sequences) == 0 {
		return
	}
	for i := range diff.TablesModified {
		tableDiff := &diff.TablesModified[i]
		table := objectlookup.Qualified(database.Tables, tableDiff.TableName, semantics)
		if table == nil {
			continue
		}
		for j := range tableDiff.ColumnsModified {
			columnDiff := &tableDiff.ColumnsModified[j]
			columnDiff.OwnedDefaultSequence = ownedDefaultSequence(database, diff, *table, *columnDiff, semantics)
		}
	}
}

// ownedDefaultSequence is the sequence [OwnedDefaultSequences] marks on change, a
// change of a column of table, or "".
func ownedDefaultSequence(
	database *catalog.Database,
	diff *difftypes.SchemaDiff,
	table catalog.Table,
	change difftypes.ColumnDiff,
	semantics identifier.Semantics,
) string {
	_, plain := change.Changes["default"]
	_, expression := change.Changes["default_expr"]
	if !plain && !expression {
		return ""
	}
	for _, column := range table.Columns {
		if column.ColumnDefault == nil || semantics.ColumnIdentityKey(column.Name) != semantics.ColumnIdentityKey(change.ColumnName) {
			continue
		}
		drawn := nextvalDefault.FindStringSubmatch(strings.TrimSpace(*column.ColumnDefault))
		if drawn == nil {
			return ""
		}
		sequence := objectlookup.Qualified(database.Sequences, drawn[1], semantics)
		if sequence == nil || !sequenceOwnedBy(*sequence, table, column.Name, semantics) ||
			objectlookup.Qualified([]schemamodel.Sequence(diff.SequencesRemoved), sequence.QualifiedName(), semantics) != nil {
			return ""
		}
		return sequence.QualifiedName()
	}
	return ""
}

// sequenceOwnedBy reports whether column of table owns sequence. A reader qualifies
// the owner with its schema only when it differs from the sequence's own.
func sequenceOwnedBy(sequence catalog.Sequence, table catalog.Table, column string, semantics identifier.Semantics) bool {
	cut := strings.LastIndex(sequence.OwnedBy, ".")
	if cut < 0 || semantics.ColumnIdentityKey(sequence.OwnedBy[cut+1:]) != semantics.ColumnIdentityKey(column) {
		return false
	}
	owner := sequence.OwnedBy[:cut]
	if !strings.Contains(owner, ".") {
		owner = catalog.QualifyTableName(sequence.Schema, owner)
	}
	return semantics.QualifiedTableIdentityKey(owner) == semantics.QualifiedTableIdentityKey(table.QualifiedName())
}

// ColumnSequencesLeaving settles the sequences a column owns as part of itself, which
// the reader does not report as sequences of their own. It runs after the sequences are
// compared.
//
// A declaration that names such a sequence does not add it, as CREATE SEQUENCE would
// answer that the relation exists. A sequence nothing declares joins the removed ones
// where the comparison takes the column's default away: a serial column the declaration
// writes as a plain type. With its default gone the reader reports such a sequence as
// one of its own, so a plan that left it would drop it on the next run.
func ColumnSequencesLeaving(database *catalog.Database, diff *difftypes.SchemaDiff, semantics identifier.Semantics) {
	if database == nil {
		return
	}
	declared := func(schema, name string) bool {
		return slices.ContainsFunc(diff.SequencesAdded, func(sequence schemamodel.Sequence) bool {
			return semantics.IndexIdentityKey(sequence.Name) == semantics.IndexIdentityKey(name) &&
				semantics.TableIdentityKey(sequence.Schema) == semantics.TableIdentityKey(schema)
		})
	}
	for _, tableDiff := range diff.TablesModified {
		table := objectlookup.Qualified(database.Tables, tableDiff.TableName, semantics)
		if table == nil {
			continue
		}
		for _, columnDiff := range tableDiff.ColumnsModified {
			for _, column := range table.Columns {
				if semantics.ColumnIdentityKey(column.Name) == semantics.ColumnIdentityKey(columnDiff.ColumnName) &&
					column.OwnedSequence != "" && column.IsAutoIncrement && defaultLeaves(columnDiff) && !declared(table.Schema, column.OwnedSequence) {
					diff.SequencesRemoved = append(diff.SequencesRemoved, schemamodel.Sequence{
						Name: column.OwnedSequence, Schema: table.Schema, OwnedBy: table.Name + "." + column.Name,
					})
				}
			}
		}
	}
	diff.SequencesAdded = slices.DeleteFunc(diff.SequencesAdded, func(sequence schemamodel.Sequence) bool {
		return slices.ContainsFunc(database.Tables, func(table catalog.Table) bool {
			return semantics.TableIdentityKey(table.Schema) == semantics.TableIdentityKey(sequence.Schema) &&
				slices.ContainsFunc(table.Columns, func(column catalog.Column) bool {
					return column.OwnedSequence != "" && semantics.IndexIdentityKey(column.OwnedSequence) == semantics.IndexIdentityKey(sequence.Name)
				})
		})
	})
}

// ColumnSequencesArriving adds to the sequences the plan creates the one each plain
// column takes where the comparison makes it serial; see
// [difftypes.ColumnDiff.SerialSequence]. A sequence of that name the declaration leaves
// out stays and becomes the column's, as dropping it and building it again would
// answer that the name exists. It runs after the sequences are compared.
func ColumnSequencesArriving(database *catalog.Database, diff *difftypes.SchemaDiff, semantics identifier.Semantics) {
	if database == nil {
		return
	}
	for _, tableDiff := range diff.TablesModified {
		table := objectlookup.Qualified(database.Tables, tableDiff.TableName, semantics)
		if table == nil {
			continue
		}
		for _, columnDiff := range tableDiff.ColumnsModified {
			if columnDiff.SerialSequence == "" {
				continue
			}
			owner := table.QualifiedName() + "." + columnDiff.ColumnName
			sequence := schemamodel.Sequence{Name: columnDiff.SerialSequence, Schema: table.Schema, OwnedBy: owner, AsType: serialSequenceType(columnDiff.Desired.Type)}
			position := slices.IndexFunc(diff.SequencesRemoved, func(removed schemamodel.Sequence) bool {
				return semantics.IndexIdentityKey(removed.Name) == semantics.IndexIdentityKey(sequence.Name) &&
					semantics.TableIdentityKey(removed.Schema) == semantics.TableIdentityKey(sequence.Schema)
			})
			if position < 0 {
				diff.SequencesAdded = append(diff.SequencesAdded, sequence)
				continue
			}
			diff.SequencesRemoved = slices.Delete(diff.SequencesRemoved, position, position+1)
			diff.SequencesModified = append(diff.SequencesModified, difftypes.SequenceDiff{
				SequenceName: sequence.QualifiedName(),
				Changes:      map[string]string{"owned_by": " -> " + owner},
				Desired:      sequence,
			})
		}
	}
	sortSequences(diff.SequencesAdded)
}

// serialSequenceType is the type of the sequence a column of the serial type declared
// draws from.
func serialSequenceType(declared string) string {
	switch strings.ToUpper(strings.TrimSpace(declared)) {
	case "SMALLSERIAL", "SERIAL2":
		return "smallint"
	case "SERIAL", "SERIAL4", "AUTO_INCREMENT":
		return "integer"
	default:
		return "bigint"
	}
}

// defaultLeaves reports whether change takes the column's default away.
func defaultLeaves(change difftypes.ColumnDiff) bool {
	for _, key := range []string{"default", "default_expr"} {
		if transition, changed := change.Changes[key]; changed && strings.HasSuffix(transition, " -> ") {
			return true
		}
	}
	return false
}

// sequenceChanges records the option-by-option transitions between a declared
// sequence and its introspected counterpart. Unset (nil) target options are
// skipped so that only explicitly declared options are managed.
func sequenceChanges(target schemamodel.Sequence, current catalog.Sequence) map[string]string {
	changes := make(map[string]string)

	if target.AsType != "" && !strings.EqualFold(target.AsType, current.DataType) {
		changes["as"] = fmt.Sprintf("%s -> %s", current.DataType, target.AsType)
	}
	compareInt64Option(changes, "start", target.Start, current.Start)
	compareInt64Option(changes, "increment", target.Increment, current.Increment)
	compareInt64Option(changes, "minvalue", target.MinValue, current.MinValue)
	compareInt64Option(changes, "maxvalue", target.MaxValue, current.MaxValue)
	compareInt64Option(changes, "cache", target.Cache, current.Cache)
	if target.Cycle != current.Cycle {
		changes["cycle"] = fmt.Sprintf("%t -> %t", current.Cycle, target.Cycle)
	}
	if target.OwnedBy != "" && !strings.EqualFold(target.OwnedBy, current.OwnedBy) {
		changes["owned_by"] = fmt.Sprintf("%s -> %s", current.OwnedBy, target.OwnedBy)
	}

	return changes
}

// compareInt64Option records a change when the target explicitly sets an option
// whose value differs from the current one. A nil target pointer means the
// option is unmanaged and is skipped.
func compareInt64Option(changes map[string]string, key string, target, current *int64) {
	if target == nil {
		return
	}
	if current == nil {
		changes[key] = fmt.Sprintf("<unset> -> %d", *target)
		return
	}
	if *target != *current {
		changes[key] = fmt.Sprintf("%d -> %d", *current, *target)
	}
}

// sequenceFromCatalog carries a sequence the database reported into the shape
// the diff holds. Every property a read establishes has a home in the model;
// the one rename is DataType, which the model spells AsType because that is the
// word a declaration uses.
func sequenceFromCatalog(reported catalog.Sequence) schemamodel.Sequence {
	return schemamodel.Sequence{
		Name:      reported.Name,
		Schema:    reported.Schema,
		AsType:    reported.DataType,
		Start:     reported.Start,
		Increment: reported.Increment,
		MinValue:  reported.MinValue,
		MaxValue:  reported.MaxValue,
		Cache:     reported.Cache,
		Cycle:     reported.Cycle,
		OwnedBy:   reported.OwnedBy,
		Comment:   reported.Comment,
	}
}

// sequenceSpelling is qualifiedName for a change that carries its operand.
func sequenceSpelling(sequence schemamodel.Sequence) (schema string, spellings []string) {
	return qualifiedName(sequence.QualifiedName())
}

// sequenceDisplay names one for a record a person reads.
func sequenceDisplay(sequence schemamodel.Sequence) string { return sequence.QualifiedName() }

// sortSequences orders by the key the name lists were sorted on.
func sortSequences(sequences difftypes.SequenceChanges) {
	sort.Slice(sequences, func(i, j int) bool {
		return sequences[i].QualifiedName() < sequences[j].QualifiedName()
	})
}
