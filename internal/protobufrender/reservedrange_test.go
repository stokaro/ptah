package protobufrender_test

import (
	"runtime"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
)

// wideRangeCase is one previous baseline whose only distinguishing property is
// the width of a single reserved range.
type wideRangeCase struct {
	name string
	// reserved is the range statement as the baseline spells it.
	reserved string
	// widened is the same statement covering one more number, used to damage
	// the baseline without re-stamping its digest.
	widened string
}

// wideRangeCases walk the reserved range from narrower than the retired
// 1,048,576-number expansion cap up to the whole legal field-number space.
// Every row is otherwise byte-identical, so the set measures the width of the
// reservation and nothing else.
func wideRangeCases() []wideRangeCase {
	return []wideRangeCase{{
		name:     "6 numbers, narrower than the retired cap",
		reserved: "20000 to 20005",
		widened:  "20000 to 20006",
	}, {
		name:     "1010001 numbers, just under the retired cap",
		reserved: "20000 to 1030000",
		widened:  "20000 to 1030001",
	}, {
		name:     "2080001 numbers, past the retired cap",
		reserved: "20000 to 2100000",
		widened:  "20000 to 2100001",
	}, {
		name:     "536850912 numbers, the whole legal space above the implementation range",
		reserved: "20000 to 536870911",
		// The whole legal space cannot grow upward, so it grows downward.
		widened: "19999 to 536870911",
	}}
}

func TestReservedRangesSurviveRegardlessOfWidth(t *testing.T) {
	for _, rc := range wideRangeCases() {
		t.Run(rc.name, func(t *testing.T) {
			c := qt.New(t)

			previous := previousExport(
				"message Thing {\n  int64 id = 1;\n\n  reserved " + rc.reserved + ";\n}\n")

			// The range must come back as a range. Expanding it into individual
			// numbers is what forced the loader to cap the width it accepts.
			rewritten := mustRenderText(c, oneTable(column("id", "BIGINT")), withPrevious(previous))
			c.Assert(section(rewritten, "message Thing {"), qt.Equals,
				"message Thing {\n  int64 id = 1;\n\n  reserved "+rc.reserved+";\n}")

			// Regenerating from the file just produced reproduces it exactly, so
			// the range survives a full write-then-read cycle rather than only
			// the first rewrite.
			again := mustRenderText(c, oneTable(column("id", "BIGINT")), withPrevious([]byte(rewritten)))
			c.Assert(again, qt.Equals, rewritten)
		})
	}
}

func TestReservedRangesStillGuardTheirBaseline(t *testing.T) {
	// Non-interference control: widening the range by one number without
	// re-stamping the digest is still refused. Without it, accepting the wide
	// baselines above could equally mean the integrity gate stopped running on
	// them.
	for _, rc := range wideRangeCases() {
		t.Run(rc.name, func(t *testing.T) {
			c := qt.New(t)

			previous := previousExport(
				"message Thing {\n  int64 id = 1;\n\n  reserved " + rc.reserved + ";\n}\n")
			damaged := strings.Replace(string(previous),
				"reserved "+rc.reserved+";", "reserved "+rc.widened+";", 1)
			c.Assert(damaged, qt.Not(qt.Equals), string(previous))

			message := mustFail(c, oneTable(column("id", "BIGINT")), withPrevious([]byte(damaged)))
			c.Assert(message, qt.Contains, "output file was modified since it was generated")
		})
	}
}

func TestReservedEnumRangesKeepTheirInclusiveEnd(t *testing.T) {
	c := qt.New(t)

	// Enum reserved ranges carry an INCLUSIVE end where message ranges carry an
	// exclusive one, so a width past the retired cap has to prove the last
	// number is neither dropped nor invented.
	previous := previousExport(
		"message Thing {\n  ThingState state = 1;\n}\n\n" +
			"enum ThingState {\n  THING_STATE_UNSPECIFIED = 0;\n  THING_STATE_NEW = 1;\n\n" +
			"  reserved 20000 to 2100000;\n}\n")

	rewritten := mustRenderText(c, inlineEnumTable("new"), withPrevious(previous))
	c.Assert(section(rewritten, "enum ThingState {"), qt.Equals,
		"enum ThingState {\n"+
			"  THING_STATE_UNSPECIFIED = 0;\n"+
			"  THING_STATE_NEW = 1;\n"+
			"\n"+
			"  reserved 20000 to 2100000;\n"+
			"}")
}

func TestReservedRangeAllocationClearsTheWholeRange(t *testing.T) {
	c := qt.New(t)

	// Every number inside a reserved range is retired for good, so a new column
	// allocates above the range's end rather than into the hole it leaves.
	previous := previousExport(
		"message Thing {\n  int64 id = 1;\n\n  reserved 20000 to 2100000;\n}\n")

	grown := mustRenderText(c, oneTable(column("id", "BIGINT"), column("extra", "TEXT")),
		withPrevious(previous))
	c.Assert(section(grown, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string extra = 2100001;\n\n  reserved 20000 to 2100000;\n}")
}

func TestReservedRangesMergeWithNewlyRetiredNeighbors(t *testing.T) {
	c := qt.New(t)

	// A number retired by this run that sits immediately above a range the
	// previous file already carried extends that range instead of being written
	// beside it, so a column removed one at a time produces the same statement
	// as the same columns removed together.
	previous := previousExport(
		"message Thing {\n  int64 id = 1;\n  string keep = 8;\n\n  reserved 2 to 7;\n  reserved sku;\n}\n")

	shrunk := mustRenderText(c, oneTable(column("id", "BIGINT")), withPrevious(previous))
	c.Assert(section(shrunk, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n\n  reserved 2 to 8;\n  reserved keep, sku;\n}")
}

func TestReservedRangeCoveringALiveNumberIsRefused(t *testing.T) {
	c := qt.New(t)

	// A baseline that reserves a number one of its own fields still holds is
	// not valid protobuf and must stay refused however wide the range is:
	// loading it would carry the contradiction forward into the next export.
	previous := previousExport(
		"message Thing {\n  int64 id = 5;\n\n  reserved 1 to 2100000;\n}\n")

	message := mustFail(c, oneTable(column("id", "BIGINT")), withPrevious(previous))
	c.Assert(message, qt.Contains, "output file is not valid protobuf")
	c.Assert(message, qt.Contains,
		"message acme.inventory.v1.Thing: field id is using tag 5 which is in reserved range 1 to 2100000")
}

func TestReservedRangeLoadingDoesNotExpandTheRange(t *testing.T) {
	c := qt.New(t)

	// Loading is linear in the number of ranges, not in the count of numbers
	// they cover. The range below covers 40,000,000 numbers, which is 160 MB as
	// a []int32 and 320 MB as a []numberRange. The whole export is held to a
	// 64 MiB budget, measured at 119,344 bytes, so the budget sits far above
	// what the range representation needs and far below any expansion of it.
	//
	// The width is 40,000,000 rather than the whole legal space so that an
	// expanding implementation fails this assertion instead of exhausting the
	// machine before it can be measured.
	previous := previousExport(
		"message Thing {\n  int64 id = 1;\n\n  reserved 20000 to 40019999;\n}\n")

	before := totalAlloc()
	rewritten := mustRenderText(c, oneTable(column("id", "BIGINT")), withPrevious(previous))
	allocated := totalAlloc() - before

	c.Assert(rewritten, qt.Contains, "  reserved 20000 to 40019999;\n")
	c.Assert(allocated < 64<<20, qt.IsTrue,
		qt.Commentf("export allocated %d bytes loading one 40000000-number reserved range", allocated))
}

// totalAlloc reports the process's cumulative allocated bytes. The difference
// across one call is what distinguishes a range kept as a range from a range
// expanded into individual numbers.
func totalAlloc() uint64 {
	var stats runtime.MemStats
	runtime.ReadMemStats(&stats)
	return stats.TotalAlloc
}
