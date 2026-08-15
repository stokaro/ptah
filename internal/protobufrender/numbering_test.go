package protobufrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestNumberingAdditiveChangeAllocatesAboveTheMaximum(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c.TB, oneTable(
		column("id", "BIGINT"),
		column("sku", "TEXT"),
	), baseOptions())

	// The new column is declared first in the source; it must still take the
	// next free number rather than displacing anything.
	grown := mustRenderText(c.TB, oneTable(
		column("added", "TEXT"),
		column("id", "BIGINT"),
		column("sku", "TEXT"),
	), withPrevious(baseline.Data))

	c.Assert(section(grown, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string sku = 2;\n  string added = 3;\n}")
}

func TestNumberingRemovedFieldReservesNumberAndName(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c.TB, oneTable(
		column("id", "BIGINT"),
		column("sku", "TEXT"),
		column("name", "TEXT"),
	), baseOptions())

	shrunk := mustRenderText(c.TB, oneTable(
		column("id", "BIGINT"),
		column("name", "TEXT"),
	), withPrevious(baseline.Data))

	// Reserving the name as well as the number is what keeps the export clean
	// under buf breaking WIRE_JSON.
	c.Assert(section(shrunk, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string name = 3;\n\n  reserved 2;\n  reserved sku;\n}")
}

func TestNumberingRemovedThenReAddedDoesNotReuseTheNumber(t *testing.T) {
	c := qt.New(t)

	full := oneTable(column("id", "BIGINT"), column("sku", "TEXT"))
	baseline := mustRender(c.TB, full, baseOptions())

	shrunk := mustRender(c.TB, oneTable(column("id", "BIGINT")), withPrevious(baseline.Data))
	c.Assert(string(shrunk.Data), qt.Contains, "  reserved 2;\n  reserved sku;\n")

	// Bringing the column back is a name reuse, so it needs the release policy;
	// the retired number 2 is gone for good and 3 is allocated instead.
	opts := withPrevious(shrunk.Data)
	opts.OnNameReuse = releasePolicy
	readded := mustRenderText(c.TB, full, opts)

	c.Assert(section(readded, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string sku = 3;\n\n  reserved 2;\n}")
}

func TestNumberingRemovedEnumValueReservesNumberAndName(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c.TB, inlineEnumTable("new", "done"), baseOptions())

	shrunk := mustRenderText(c.TB, inlineEnumTable("new"), withPrevious(baseline.Data))
	c.Assert(section(shrunk, "enum ThingState {"), qt.Equals,
		"enum ThingState {\n"+
			"  THING_STATE_UNSPECIFIED = 0;\n"+
			"  THING_STATE_NEW = 1;\n"+
			"\n"+
			"  reserved 2;\n"+
			"  reserved THING_STATE_DONE;\n"+
			"}")
}

func TestNumberingSkipsTheProtobufImplementationRange(t *testing.T) {
	c := qt.New(t)

	// 19000..19999 is reserved by protoc itself, so a type whose maximum sits
	// immediately below it must jump the whole block.
	previous := previousExport("message Thing {\n  int64 id = 18999;\n}\n")

	grown := mustRenderText(c.TB, oneTable(
		column("id", "BIGINT"),
		column("extra", "TEXT"),
	), withPrevious(previous))

	c.Assert(section(grown, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 18999;\n  string extra = 20000;\n}")
}

func TestNumberingSkipsTheImplementationRangeForEnums(t *testing.T) {
	c := qt.New(t)

	previous := previousExport(
		"message Thing {\n  ThingState state = 1;\n}\n\n" +
			"enum ThingState {\n  THING_STATE_UNSPECIFIED = 0;\n  THING_STATE_NEW = 18999;\n}\n")

	grown := mustRenderText(c.TB, inlineEnumTable("new", "done"), withPrevious(previous))
	c.Assert(section(grown, "enum ThingState {"), qt.Equals,
		"enum ThingState {\n"+
			"  THING_STATE_UNSPECIFIED = 0;\n"+
			"  THING_STATE_NEW = 18999;\n"+
			"  THING_STATE_DONE = 20000;\n"+
			"}")
}

func TestNumberingRefusesToAllocatePastTheMaximum(t *testing.T) {
	c := qt.New(t)

	// 536870911 is the largest legal field number, so nothing is left.
	previous := previousExport("message Thing {\n  int64 id = 536870911;\n}\n")

	message := mustFail(c.TB, oneTable(
		column("id", "BIGINT"),
		column("extra", "TEXT"),
	), withPrevious(previous))

	c.Assert(message, qt.Equals, `message "Thing" has exhausted the protobuf field number space`)
}

func TestNumberingCountsReservedNumbersAsUsed(t *testing.T) {
	c := qt.New(t)

	// The gap left by a retired field is never filled: allocation must exceed
	// every number the type has ever held, reserved ones included.
	previous := previousExport(
		"message Thing {\n  int64 id = 1;\n\n  reserved 2 to 7;\n  reserved sku, name;\n}\n")

	grown := mustRenderText(c.TB, oneTable(
		column("id", "BIGINT"),
		column("extra", "TEXT"),
	), withPrevious(previous))

	c.Assert(section(grown, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string extra = 8;\n\n  reserved 2 to 7;\n  reserved name, sku;\n}")
}

func TestNumberingCollapsesContiguousReservedRuns(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c.TB, oneTable(
		column("a", "TEXT"),
		column("b", "TEXT"),
		column("c", "TEXT"),
		column("d", "TEXT"),
		column("e", "TEXT"),
	), baseOptions())

	shrunk := mustRenderText(c.TB, oneTable(column("a", "TEXT"), column("e", "TEXT")), withPrevious(baseline.Data))
	c.Assert(section(shrunk, "message Thing {"), qt.Equals,
		"message Thing {\n  string a = 1;\n  string e = 5;\n\n  reserved 2 to 4;\n  reserved b, c, d;\n}")
}

// inlineEnumTable builds a single table whose "state" column carries an inline
// enum, producing the Protobuf enum ThingState.
func inlineEnumTable(values ...string) *goschema.Database {
	return oneTable(goschema.Field{Name: "state", Type: enumCarrierType, Enum: values})
}
