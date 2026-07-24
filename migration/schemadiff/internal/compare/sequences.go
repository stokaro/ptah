package compare

import (
	"fmt"
	"sort"
	"strings"

	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/dbschema/types"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

// Sequences compares standalone sequence definitions between the target schema
// (Go annotations) and the current database schema.
//
// Only sequences the database reader classifies as standalone reach
// database.Sequences: implicit sequences that back SERIAL / identity columns are
// excluded upstream, so declaring a plain SERIAL column never produces a
// spurious sequence diff here.
//
// Modification detection is intentionally asymmetric: only options the target
// explicitly sets are compared. A numeric option left unset in the annotation
// (nil pointer) is treated as "unmanaged" and never flagged, so a sequence that
// relies on PostgreSQL defaults does not churn against the catalog's fully
// populated values.
func Sequences(generated *goschema.Database, database *types.DBSchema, diff *difftypes.SchemaDiff) {
	generatedSequences := make(map[string]goschema.Sequence, len(generated.Sequences))
	for _, sequence := range generated.Sequences {
		generatedSequences[sequence.QualifiedName()] = sequence
	}

	databaseSequences := make(map[string]types.DBSequence, len(database.Sequences))
	for _, sequence := range database.Sequences {
		databaseSequences[sequence.QualifiedName()] = sequence
	}

	for name, generatedSequence := range generatedSequences {
		databaseSequence, exists := databaseSequences[name]
		if !exists {
			diff.SequencesAdded = append(diff.SequencesAdded, name)
			continue
		}
		if changes := sequenceChanges(generatedSequence, databaseSequence); len(changes) > 0 {
			diff.SequencesModified = append(diff.SequencesModified, difftypes.SequenceDiff{
				SequenceName: name,
				Changes:      changes,
			})
		}
	}

	for name := range databaseSequences {
		if _, exists := generatedSequences[name]; !exists {
			diff.SequencesRemoved = append(diff.SequencesRemoved, name)
		}
	}

	sort.Strings(diff.SequencesAdded)
	sort.Strings(diff.SequencesRemoved)
	sort.Slice(diff.SequencesModified, func(i, j int) bool {
		return diff.SequencesModified[i].SequenceName < diff.SequencesModified[j].SequenceName
	})
}

// sequenceChanges records the option-by-option transitions between a declared
// sequence and its introspected counterpart. Unset (nil) target options are
// skipped so that only explicitly declared options are managed.
func sequenceChanges(target goschema.Sequence, current types.DBSequence) map[string]string {
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
