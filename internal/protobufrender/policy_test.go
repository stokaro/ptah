package protobufrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/protobufrender"
)

// twoTypeSchema has one plain table plus a table whose enum column produces a
// second top-level type, so removal policies can be exercised on a message and
// an enum at once.
func twoTypeSchema() *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Thing", Name: "things"},
			{StructName: "Order", Name: "orders"},
		},
		Fields: []goschema.Field{
			{StructName: "Thing", Name: "id", Type: "BIGINT"},
			{StructName: "Order", Name: "id", Type: "BIGINT"},
			{StructName: "Order", Name: "state", Type: enumCarrierType, Enum: []string{"new", "done"}},
		},
	}
}

// survivingSchema is twoTypeSchema with the orders table gone, which removes
// both the Order message and the OrderState enum.
func survivingSchema() *goschema.Database {
	return oneTable(column("id", "BIGINT"))
}

func removalBaseline(tb testing.TB) []byte {
	c := qt.New(tb)
	c.Helper()
	return mustRender(c.TB, twoTypeSchema(), baseOptions()).Data
}

func removalOptions(previous []byte, policy protobufrender.RemovalPolicy) protobufrender.Options {
	opts := withPrevious(previous)
	opts.TypeRemoval = policy
	return opts
}

func TestTypeRemovalErrorIsTheDefault(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c.TB)

	// A table that is dropped and later recreated would otherwise restart
	// numbering at 1 and collide with the numbers old consumers still hold, so
	// refusing is the default.
	message := mustFail(c.TB, survivingSchema(), withPrevious(baseline))
	c.Assert(message, qt.Equals,
		"types removed from the source schema: Order, OrderState; "+
			"protobuf cannot reserve a top-level type name, so choose "+
			"--proto-type-removal=tombstone to retain them for wire compatibility or =drop to abandon it")

	explicit := mustFail(c.TB, survivingSchema(), removalOptions(baseline, protobufrender.RemovalError))
	c.Assert(explicit, qt.Equals, message)
}

func TestTypeRemovalTombstoneRetainsMessageAndEnum(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c.TB)
	res := mustRender(c.TB, survivingSchema(), removalOptions(baseline, protobufrender.RemovalTombstone))
	text := string(res.Data)

	// A message may be emptied completely.
	c.Assert(section(text, "message Order {"), qt.Equals,
		"message Order {\n  reserved 1 to 2;\n  reserved id, state;\n}")

	// An enum may not: protoc rejects "Enums must contain at least one value",
	// so the synthesized zero value survives and everything else is reserved.
	c.Assert(section(text, "enum OrderState {"), qt.Equals,
		"enum OrderState {\n"+
			"  ORDER_STATE_UNSPECIFIED = 0;\n"+
			"\n"+
			"  reserved 1 to 2;\n"+
			"  reserved ORDER_STATE_DONE, ORDER_STATE_NEW;\n"+
			"}")

	c.Assert(text, qt.Contains,
		"// Removed from the source schema; retained for wire compatibility.\nmessage Order {")
	c.Assert(text, qt.Contains,
		"// Removed from the source schema; retained for wire compatibility.\nenum OrderState {")

	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`message "Order" was removed from the source schema and retained as a tombstone`)
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`enum "OrderState" was removed from the source schema and retained as a tombstone`)
}

func TestTypeRemovalTombstoneSurvivesRegeneration(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c.TB)
	first := mustRender(c.TB, survivingSchema(), removalOptions(baseline, protobufrender.RemovalTombstone))

	// Regenerating from unchanged input reproduces the tombstone byte for byte,
	// adding nothing to what it already reserves.
	second := mustRender(c.TB, survivingSchema(), removalOptions(first.Data, protobufrender.RemovalTombstone))
	c.Assert(string(second.Data), qt.Equals, string(first.Data))

	third := mustRender(c.TB, survivingSchema(), removalOptions(second.Data, protobufrender.RemovalTombstone))
	c.Assert(string(third.Data), qt.Equals, string(first.Data))
}

func TestTypeRemovalTombstoneHonorsItsReservationsWhenTheTypeReappears(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c.TB)
	tombstoned := mustRender(c.TB, survivingSchema(), removalOptions(baseline, protobufrender.RemovalTombstone))

	// Recreating the table brings back reserved names, which is exactly what
	// --proto-on-name-reuse governs.
	refused := mustFail(c.TB, twoTypeSchema(), withPrevious(tombstoned.Data))
	c.Assert(refused, qt.Contains, `field "id" on "Order" is reserved because it was previously removed`)

	opts := withPrevious(tombstoned.Data)
	opts.OnNameReuse = protobufrender.NameReuseRelease
	text := mustRenderText(c.TB, twoTypeSchema(), opts)

	// Numbers still allocate above everything the tombstone reserved, so the
	// recreated table can never collide with what old consumers hold.
	c.Assert(section(text, "message Order {"), qt.Equals,
		"message Order {\n  int64 id = 3;\n  OrderState state = 4;\n\n  reserved 1 to 2;\n}")
}

func TestTypeRemovalDropAbandonsCompatibility(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c.TB)
	res := mustRender(c.TB, survivingSchema(), removalOptions(baseline, protobufrender.RemovalDrop))
	text := string(res.Data)

	c.Assert(text, qt.Not(qt.Contains), "message Order {")
	c.Assert(text, qt.Not(qt.Contains), "enum OrderState {")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`type "Order" was removed from the source schema and dropped; `+
			"its field numbers are no longer reserved and wire compatibility for it is abandoned")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`type "OrderState" was removed from the source schema and dropped`)
}

func TestNameReuseErrorIsTheDefaultForFieldsAndEnumValues(t *testing.T) {
	c := qt.New(t)

	full := oneTable(column("id", "BIGINT"), column("sku", "TEXT"))
	shrunk := mustRender(c.TB, oneTable(column("id", "BIGINT")),
		withPrevious(mustRender(c.TB, full, baseOptions()).Data))

	message := mustFail(c.TB, full, withPrevious(shrunk.Data))
	c.Assert(message, qt.Equals,
		`field "sku" on "Thing" is reserved because it was previously removed, `+
			"and protobuf refuses to reuse a reserved name; "+
			"pass --proto-on-name-reuse=release to drop the name reservation (its number stays reserved) "+
			"and abandon JSON-name compatibility for it")

	enumFull := inlineEnumTable("new", "done")
	enumShrunk := mustRender(c.TB, inlineEnumTable("new"),
		withPrevious(mustRender(c.TB, enumFull, baseOptions()).Data))

	message = mustFail(c.TB, enumFull, withPrevious(enumShrunk.Data))
	c.Assert(message, qt.Contains,
		`enum value "THING_STATE_DONE" on "ThingState" is reserved because it was previously removed`)
}

func TestNameReuseReleaseKeepsTheNumberReserved(t *testing.T) {
	c := qt.New(t)

	full := oneTable(column("id", "BIGINT"), column("sku", "TEXT"))
	shrunk := mustRender(c.TB, oneTable(column("id", "BIGINT")),
		withPrevious(mustRender(c.TB, full, baseOptions()).Data))

	opts := withPrevious(shrunk.Data)
	opts.OnNameReuse = protobufrender.NameReuseRelease
	res := mustRender(c.TB, full, opts)

	c.Assert(section(string(res.Data), "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string sku = 3;\n\n  reserved 2;\n}")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`field "sku" reuses a reserved name; the name reservation was released and `+
			"JSON-name compatibility for it is abandoned, which buf breaking WIRE_JSON reports once")
}

func TestNameReuseReleaseForEnumValues(t *testing.T) {
	c := qt.New(t)

	enumFull := inlineEnumTable("new", "done")
	enumShrunk := mustRender(c.TB, inlineEnumTable("new"),
		withPrevious(mustRender(c.TB, enumFull, baseOptions()).Data))

	opts := withPrevious(enumShrunk.Data)
	opts.OnNameReuse = protobufrender.NameReuseRelease
	res := mustRender(c.TB, enumFull, opts)

	c.Assert(section(string(res.Data), "enum ThingState {"), qt.Equals,
		"enum ThingState {\n"+
			"  THING_STATE_UNSPECIFIED = 0;\n"+
			"  THING_STATE_NEW = 1;\n"+
			"  THING_STATE_DONE = 3;\n"+
			"\n"+
			"  reserved 2;\n"+
			"}")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`enum value "THING_STATE_DONE" reuses a reserved name`)
}

// changeCase is one wire-incompatible field change: what the column becomes and
// how the refusal reads.
type changeCase struct {
	name       string
	columnType string
	wantRefuse string
	wantField  string
}

func changeCases() []changeCase {
	return []changeCase{{
		name:       "translated type",
		columnType: "TEXT",
		wantRefuse: `field "sku" on message "Thing" changed from int32 to string, which is not wire compatible; ` +
			"pass --proto-on-incompatible-change=renumber to reserve the old number and allocate a new one",
		wantField: "  string sku = 3;",
	}, {
		name:       "singular to repeated",
		columnType: "INTEGER[]",
		wantRefuse: `field "sku" on message "Thing" changed from int32 to repeated int32, which is not wire compatible; ` +
			"pass --proto-on-incompatible-change=renumber to reserve the old number and allocate a new one",
		wantField: "  repeated int32 sku = 3;",
	}, {
		name:       "well-known type",
		columnType: "TIMESTAMPTZ",
		wantRefuse: `field "sku" on message "Thing" changed from int32 to google.protobuf.Timestamp, ` +
			"which is not wire compatible; " +
			"pass --proto-on-incompatible-change=renumber to reserve the old number and allocate a new one",
		wantField: "  google.protobuf.Timestamp sku = 3;",
	}}
}

func changeBaseline(tb testing.TB) []byte {
	c := qt.New(tb)
	c.Helper()
	return mustRender(c.TB, oneTable(column("id", "BIGINT"), column("sku", "INTEGER")), baseOptions()).Data
}

func assertChangeRefused(tb testing.TB, cc changeCase) {
	c := qt.New(tb)
	c.Helper()
	changed := oneTable(column("id", "BIGINT"), column("sku", cc.columnType))
	c.Assert(mustFail(c.TB, changed, withPrevious(changeBaseline(c.TB))), qt.Equals, cc.wantRefuse)
}

func assertChangeRenumbered(tb testing.TB, cc changeCase) {
	c := qt.New(tb)
	c.Helper()
	changed := oneTable(column("id", "BIGINT"), column("sku", cc.columnType))
	opts := withPrevious(changeBaseline(c.TB))
	opts.OnIncompatibleChange = protobufrender.ChangeRenumber
	res := mustRender(c.TB, changed, opts)

	// The old number is retired and a new one allocated, the wire-safe
	// equivalent of delete plus add.
	c.Assert(section(string(res.Data), "message Thing {"), qt.Contains, cc.wantField)
	c.Assert(section(string(res.Data), "message Thing {"), qt.Contains, "  reserved 2;")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains), `field "sku" changed from int32 to `)
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains), "number 2 is now reserved")
}

func TestIncompatibleChangeErrorIsTheDefault(t *testing.T) {
	for _, cc := range changeCases() {
		t.Run(cc.name, func(t *testing.T) {
			assertChangeRefused(qt.New(t).TB, cc)
		})
	}
}

func TestIncompatibleChangeRenumberReservesTheOldNumber(t *testing.T) {
	for _, cc := range changeCases() {
		t.Run(cc.name, func(t *testing.T) {
			assertChangeRenumbered(qt.New(t).TB, cc)
		})
	}
}

func TestCompatibleTypeSpellingChangeKeepsTheNumber(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c.TB, oneTable(column("id", "BIGINT"), column("sku", "INTEGER")), baseOptions())

	// A different SQL spelling that translates to the same Protobuf type is not
	// a change at all, so nothing moves and no policy is consulted.
	res := mustRender(c.TB, oneTable(column("id", "INT8"), column("sku", "MEDIUMINT")), withPrevious(baseline.Data))
	c.Assert(string(res.Data), qt.Equals, string(baseline.Data))
}

func TestScalarToEnumChangeIsRefused(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c.TB, oneTable(column("state", "TEXT")), baseOptions())

	message := mustFail(c.TB, inlineEnumTable("new", "done"), withPrevious(baseline.Data))
	c.Assert(message, qt.Equals,
		`field "state" on message "Thing" changed from string to ThingState, which is not wire compatible; `+
			"pass --proto-on-incompatible-change=renumber to reserve the old number and allocate a new one")
}
