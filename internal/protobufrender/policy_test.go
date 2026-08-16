package protobufrender_test

import (
	"context"
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

func removalBaseline(c *qt.C) []byte {
	c.Helper()
	return mustRender(c, twoTypeSchema(), baseOptions()).Data
}

func removalOptions(previous []byte, policy protobufrender.RemovalPolicy) protobufrender.Options {
	opts := withPrevious(previous)
	opts.TypeRemoval = policy
	return opts
}

func TestTypeRemovalErrorIsTheDefault(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c)

	// A table that is dropped and later recreated would otherwise restart
	// numbering at 1 and collide with the numbers old consumers still hold, so
	// refusing is the default.
	message := mustFail(c, survivingSchema(), withPrevious(baseline))
	c.Assert(message, qt.Equals,
		"types removed from the source schema: Order, OrderState; "+
			"protobuf cannot reserve a top-level type name, so choose "+
			"--proto-type-removal=tombstone to retain them for wire compatibility or =drop to abandon it")

	explicit := mustFail(c, survivingSchema(), removalOptions(baseline, protobufrender.RemovalError))
	c.Assert(explicit, qt.Equals, message)
}

func TestTypeRemovalTombstoneRetainsMessageAndEnum(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c)
	res := mustRender(c, survivingSchema(), removalOptions(baseline, protobufrender.RemovalTombstone))
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

	baseline := removalBaseline(c)
	first := mustRender(c, survivingSchema(), removalOptions(baseline, protobufrender.RemovalTombstone))

	// Regenerating from unchanged input reproduces the tombstone byte for byte,
	// adding nothing to what it already reserves.
	second := mustRender(c, survivingSchema(), removalOptions(first.Data, protobufrender.RemovalTombstone))
	c.Assert(string(second.Data), qt.Equals, string(first.Data))

	third := mustRender(c, survivingSchema(), removalOptions(second.Data, protobufrender.RemovalTombstone))
	c.Assert(string(third.Data), qt.Equals, string(first.Data))
}

func TestTypeRemovalTombstoneHonorsItsReservationsWhenTheTypeReappears(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c)
	tombstoned := mustRender(c, survivingSchema(), removalOptions(baseline, protobufrender.RemovalTombstone))

	// Recreating the table brings back reserved names, which is exactly what
	// --proto-on-name-reuse governs.
	refused := mustFail(c, twoTypeSchema(), withPrevious(tombstoned.Data))
	c.Assert(refused, qt.Contains, `field "id" on "Order" is reserved because it was previously removed`)

	opts := withPrevious(tombstoned.Data)
	opts.OnNameReuse = protobufrender.NameReuseRelease
	text := mustRenderText(c, twoTypeSchema(), opts)

	// Numbers still allocate above everything the tombstone reserved, so the
	// recreated table can never collide with what old consumers hold.
	c.Assert(section(text, "message Order {"), qt.Equals,
		"message Order {\n  int64 id = 3;\n  OrderState state = 4;\n\n  reserved 1 to 2;\n}")
}

func TestTypeRemovalDropAbandonsCompatibility(t *testing.T) {
	c := qt.New(t)

	baseline := removalBaseline(c)
	res := mustRender(c, survivingSchema(), removalOptions(baseline, protobufrender.RemovalDrop))
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
	shrunk := mustRender(c, oneTable(column("id", "BIGINT")),
		withRetiredFields(mustRender(c, full, baseOptions()).Data))

	message := mustFail(c, full, withRetiredFields(shrunk.Data))
	c.Assert(message, qt.Equals,
		`field "sku" on "Thing" is reserved because it was previously removed, `+
			"and protobuf refuses to reuse a reserved name; "+
			"pass --proto-on-name-reuse=release to drop the name reservation (its number stays reserved) "+
			"and abandon JSON-name compatibility for it")

	enumFull := inlineEnumTable("new", "done")
	enumShrunk := mustRender(c, inlineEnumTable("new"),
		withRetiredFields(mustRender(c, enumFull, baseOptions()).Data))

	message = mustFail(c, enumFull, withRetiredFields(enumShrunk.Data))
	c.Assert(message, qt.Contains,
		`enum value "THING_STATE_DONE" on "ThingState" is reserved because it was previously removed`)
}

func TestNameReuseReleaseKeepsTheNumberReserved(t *testing.T) {
	c := qt.New(t)

	full := oneTable(column("id", "BIGINT"), column("sku", "TEXT"))
	shrunk := mustRender(c, oneTable(column("id", "BIGINT")),
		withRetiredFields(mustRender(c, full, baseOptions()).Data))

	opts := withRetiredFields(shrunk.Data)
	opts.OnNameReuse = protobufrender.NameReuseRelease
	res := mustRender(c, full, opts)

	c.Assert(section(string(res.Data), "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string sku = 3;\n\n  reserved 2;\n}")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`field "sku" reuses a reserved name; the name reservation was released and `+
			"JSON-name compatibility for it is abandoned, which buf breaking WIRE_JSON reports once")
}

func TestNameReuseReleaseForEnumValues(t *testing.T) {
	c := qt.New(t)

	enumFull := inlineEnumTable("new", "done")
	enumShrunk := mustRender(c, inlineEnumTable("new"),
		withPrevious(mustRender(c, enumFull, baseOptions()).Data))

	opts := withPrevious(enumShrunk.Data)
	opts.OnNameReuse = protobufrender.NameReuseRelease
	res := mustRender(c, enumFull, opts)

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

func changeBaseline(c *qt.C) []byte {
	c.Helper()
	return mustRender(c, oneTable(column("id", "BIGINT"), column("sku", "INTEGER")), baseOptions()).Data
}

func assertChangeRefused(c *qt.C, cc changeCase) {
	c.Helper()
	changed := oneTable(column("id", "BIGINT"), column("sku", cc.columnType))
	c.Assert(mustFail(c, changed, withPrevious(changeBaseline(c))), qt.Equals, cc.wantRefuse)
}

func assertChangeRenumbered(c *qt.C, cc changeCase) {
	c.Helper()
	changed := oneTable(column("id", "BIGINT"), column("sku", cc.columnType))
	opts := withPrevious(changeBaseline(c))
	opts.OnIncompatibleChange = protobufrender.ChangeRenumber
	res := mustRender(c, changed, opts)

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
			assertChangeRefused(qt.New(t), cc)
		})
	}
}

func TestIncompatibleChangeRenumberReservesTheOldNumber(t *testing.T) {
	for _, cc := range changeCases() {
		t.Run(cc.name, func(t *testing.T) {
			assertChangeRenumbered(qt.New(t), cc)
		})
	}
}

func TestCompatibleTypeSpellingChangeKeepsTheNumber(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(column("id", "BIGINT"), column("sku", "INTEGER")), baseOptions())

	// A different SQL spelling that translates to the same Protobuf type is not
	// a change at all, so nothing moves and no policy is consulted.
	res := mustRender(c, oneTable(column("id", "INT8"), column("sku", "MEDIUMINT")), withPrevious(baseline.Data))
	c.Assert(string(res.Data), qt.Equals, string(baseline.Data))
}

func TestScalarToEnumChangeIsRefused(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(column("state", "TEXT")), baseOptions())

	message := mustFail(c, inlineEnumTable("new", "done"), withPrevious(baseline.Data))
	c.Assert(message, qt.Equals,
		`field "state" on message "Thing" changed from string to ThingState, which is not wire compatible; `+
			"pass --proto-on-incompatible-change=renumber to reserve the old number and allocate a new one")
}

func TestFieldRemovalZeroValueRefuses(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(
		column("id", "BIGINT"),
		column("sku", "TEXT"),
	), baseOptions())

	// The CLI never reaches this: it parses "" into the error policy before
	// calling. A library caller who leaves the field alone is the one who does,
	// and every other policy here refuses on its zero value for the same
	// reason — a caller who said nothing did not ask for a contract change.
	opts := withPrevious(baseline.Data)
	c.Assert(opts.OnFieldRemoval, qt.Equals, protobufrender.FieldRemovalPolicy(""))

	_, err := protobufrender.Render(context.Background(), oneTable(column("id", "BIGINT")), opts)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "fields removed from Thing: sku")
}
