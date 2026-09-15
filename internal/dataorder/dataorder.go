// Package dataorder ranks a table against the order a schema's dependency sort
// put its tables in, so a stage that writes rows can put parents first.
//
// Two stages write declared rows -- the migration body datamigrate renders and
// the plan atlasschema prepares -- and both have to obey the same foreign keys:
// a row goes in after the row it references and comes out before it. That is
// one recognition, and this is the one place that carries it. Written twice it
// agrees until one copy learns something the other does not, and neither stage's
// tests can see the other going wrong.
package dataorder

import (
	"math"

	"ptah.run/core/schemamodel"
)

// Unknown ranks a table the schema does not define. It sorts after every known
// table, which leaves a declaration for a table Ptah did not parse at the end of
// the insert order rather than in the middle of it.
const Unknown = math.MaxInt

// Ranker answers where a table sits in the dependency order.
//
// The zero value ranks everything Unknown; build one with [New].
type Ranker struct {
	qualified map[string]int
	bare      map[string]int
	ambiguous map[string]struct{}
}

// New indexes a schema's tables for ranking.
//
// db.Tables is already sorted parents-first, so a table's position in it is the
// insert order and the reverse is the delete order. A nil database indexes
// nothing, and every lookup then answers Unknown.
func New(db *schemamodel.Database) Ranker {
	if db == nil {
		return Ranker{}
	}
	ranker := Ranker{
		qualified: make(map[string]int, len(db.Tables)),
		bare:      make(map[string]int, len(db.Tables)),
		ambiguous: make(map[string]struct{}),
	}
	for position, table := range db.Tables {
		ranker.qualified[table.QualifiedName()] = position
		if _, seen := ranker.bare[table.Name]; seen {
			ranker.ambiguous[table.Name] = struct{}{}
			continue
		}
		ranker.bare[table.Name] = position
	}
	return ranker
}

// Rank returns the table's position in the dependency order, or [Unknown].
//
// The bare name is a fallback for a row declaration that omits the schema while
// the table definition sets one, or the other way round; without it the
// qualified lookup misses and the ordering silently degrades to whatever the
// caller falls back on. A bare name two schemas share resolves to neither, so
// the fallback can never name the wrong table.
func (r Ranker) Rank(schema, table string) int {
	if position, ok := r.qualified[schemamodel.QualifyTableName(schema, table)]; ok {
		return position
	}
	if _, shared := r.ambiguous[table]; shared {
		return Unknown
	}
	if position, ok := r.bare[table]; ok {
		return position
	}
	return Unknown
}
