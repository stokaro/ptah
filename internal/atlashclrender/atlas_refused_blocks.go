package atlashclrender

import (
	"fmt"
	"slices"

	"go.5x5.cz/ptah/core/platform"
)

// Top-level HCL block spellings an inspected render can emit. Only the ones a
// refusal list or a diagnostic names are constants: the token has to be spelled
// identically by the renderer, by [atlasRefusedBlockTypes], and by the
// conformance run that re-measures that list against the binary, and three
// string literals drift where one constant cannot.
const (
	blockExtension = "extension"
	blockPolicy    = "policy"
	blockSequence  = "sequence"
)

// atlasRefusedBlockTypes lists, per dialect, the top-level block types the
// pinned Atlas community binary v1.3.0 refuses AS A FEATURE: not because of how
// Ptah spells an attribute inside them, but because that build does not model
// the construct at all. One such block anywhere in a file costs the WHOLE file,
// so the Atlas-compatible surface omits them rather than emitting a document
// its counterpart cannot read (stokaro/ptah#1251).
//
// Every row is measured, and the message is what makes the row a row. Measured
// on PostgreSQL 17 by starting from a file that binary accepts -- its own
// inspect output for the same database, verified at exit 0 -- and adding one
// block type at a time:
//
//	extension "pgcrypto" {}     exit 1  postgres: extensions are not supported by this version
//	sequence "order_seq" {}     exit 1  postgres: sequences are not supported by this version
//	policy "accounts_all" {}    exit 1  postgres: policies are not supported by this version
//
// The blocks are empty on purpose. A `sequence` carrying the `type = bigint`
// Ptah writes today is refused with `There is no variable named "bigint"`
// instead, which is Ptah's own rendering defect and is fixed rather than
// suppressed. Stripping the block to its label separates the two verdicts: what
// survives is a refusal of the block type itself, which no spelling can lift.
//
// Nothing else Ptah's inspect emits belongs here, and that too is measured. Off
// the same accepted base, one block at a time:
//
//	role, function, view, materialized, trigger, permission, range   exit 0
//	wibble "x" {}                                                    exit 0
//
// The last row is why the others are not evidence of support: that binary drops
// a top-level block whose name it does not model and carries on, so exit 0 says
// only "harmless to the file", which is all the compatibility surface needs.
// Suppressing those would lose description for nothing.
//
// The map is keyed by dialect because the refusal is, and that is measured too:
// on SQLite the same three blocks are accepted and dropped exactly like
// `wibble`, all at exit 0. A dialect absent here suppresses nothing.
//
// A later build may model any of these, at which point suppressing it would
// silently withhold something the reader could have used.
// TestOracleAtlasRefusedBlockTypesMatchTheBinary re-measures the list in both
// directions so that turns the Atlas CE Oracle job red rather than going
// unnoticed.
var atlasRefusedBlockTypes = map[string][]string{
	platform.Postgres: {blockExtension, blockPolicy, blockSequence},
}

// atlasRefusesBlock reports whether the pinned binary refuses a whole file for
// containing this block type on this dialect.
func atlasRefusesBlock(dialect, block string) bool {
	return slices.Contains(atlasRefusedBlockTypes[dialect], block)
}

// omitsBlock reports whether this render drops a top-level block type entirely.
// Only the Atlas-compatible surface does; a native render describes everything
// Ptah models.
func (r *renderer) omitsBlock(block string) bool {
	return r.atlasCompatible && atlasRefusesBlock(r.dialect, block)
}

// omitAtlasRefusedBlock records one omitted object on the render's diagnostics,
// which schema inspect writes to standard error.
//
// Dropping an object silently would make the output lie about the database, so
// the omission is reported through the same loss-diagnostic channel the
// renderer already uses for constructs it cannot represent.
func (r *renderer) omitAtlasRefusedBlock(path, block string) {
	r.warn(path, fmt.Sprintf(
		"omitted from Atlas-compatible schema inspect output: the Atlas community CLI refuses a %s schema file that declares any %s block, and one of them makes the whole document unreadable to it",
		r.dialect, block,
	))
}
