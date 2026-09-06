// Package pgindexstorage owns which PostgreSQL index storage parameters Ptah
// records, and the one switch that widens the set.
//
// The set is decided by the WEAKEST surface a recorded parameter has to survive,
// not by what the catalog can report. schemadiff treats a difference in the
// recorded set as a reason to rebuild an index, so a parameter that one surface
// carries and another drops makes every such index differ from its own
// inspected document forever -- a permanent rebuild, and a silent one until
// somebody watches an index get rebuilt on every apply.
//
// That is why `pages_per_range` was for a long time the only entry: it is the
// only parameter with a slot in the Atlas-compatible HCL surface, which the
// pinned community binary spells `page_per_range`. Measured on PostgreSQL 17.10,
// `fillfactor`, `deduplicate_items`, `buffering`, `fastupdate`,
// `gin_pending_list_limit` and `autosummarize` have no slot there, and that
// binary drops all of them too: `CREATE INDEX i ON t (name) WITH (fillfactor = 70)`
// comes back from both as `CREATE INDEX "i" ON "t" ("name")`.
//
// So the default behavior is parity, not a divergence, and it stays the default.
// What the switch adds is the capability: with it set, every surface -- reader,
// SQL renderer, SQL parser, HCL writer and HCL parser -- carries every
// parameter, and the HCL document produced is a Ptah document rather than one
// the community binary also reads (stokaro/ptah#2183).
package pgindexstorage

import (
	"slices"

	"ptah.run/internal/envbool"
)

// EnvVar widens the recorded set to every storage parameter the catalog holds.
//
// Gated rather than Retained: it moves the HCL surface off the shape the pinned
// community binary reads, which is exactly what [envbool.Gated] marks. Default
// false, like every other toggle in this tree, so a typo lands on the parity
// behavior rather than silently producing documents that binary cannot parse.
const EnvVar = "PTAH_POSTGRES_INDEX_STORAGE_PARAMS"

var carryAll = envbool.New(EnvVar, false, envbool.Gated)

// compatibleParams are the parameters recorded whatever the switch says.
//
// One entry, and the reason is the HCL surface rather than the engine: it is
// the only index storage parameter the Atlas-compatible document has an
// attribute for.
var compatibleParams = []string{"pages_per_range"}

// CarryAll reports whether every storage parameter is recorded.
func CarryAll() (bool, error) {
	return carryAll.Resolve()
}

// Records reports whether a parameter is recorded under the resolved setting.
func Records(name string, carryEverything bool) bool {
	return carryEverything || slices.Contains(compatibleParams, name)
}

// CompatibleParams returns the parameters recorded by default, in a copy the
// caller may keep.
func CompatibleParams() []string {
	return slices.Clone(compatibleParams)
}

// HasCompatibleSlot reports whether a parameter has an attribute on the
// Atlas-compatible HCL surface.
//
// It is the same question [Records] answers under the default, and a different
// one under the switch: with the switch on, a parameter with no compatible slot
// is still recorded and still written -- as a Ptah attribute. This is what the
// HCL writer uses to decide whether to say so.
func HasCompatibleSlot(name string) bool {
	return slices.Contains(compatibleParams, name)
}
