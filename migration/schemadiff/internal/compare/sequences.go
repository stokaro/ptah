package compare

import (
	"fmt"
	"sort"
	"strings"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/coverage"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/internal/objectidentity"
	"go.5x5.cz/ptah/migration/schemadiff/difftypes"
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
		if changes := sequenceChanges(generatedSequence, databaseSequence); len(changes) > 0 {
			diff.SequencesModified = append(diff.SequencesModified, difftypes.SequenceDiff{
				SequenceName: generatedNames[identity],
				Changes:      changes,
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
