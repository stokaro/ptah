package protobufrender_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/protobufrender"
)

// aliasedColumn is a column published under a name of its own.
func aliasedColumn(name, apiName, columnType string) goschema.Field {
	return goschema.Field{Name: name, APIName: apiName, Type: columnType}
}

// The declared API name is what the protobuf field is called.
func TestFieldUsesTheDeclaredAPIName(t *testing.T) {
	c := qt.New(t)

	out := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		aliasedColumn("billing_amount_minor", "amount", "INTEGER"),
	), baseOptions())

	c.Assert(section(out, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  int32 amount = 2;\n}")
}

// This is the compatibility property the API name exists for, and the reason it
// is worth having a second identity at all: renaming the column while the
// declared name stays put must NOT renumber the field.
//
// The reconciler keys existing numbers by the protobuf field name. Deriving
// that name from the column would make every storage rename a wire-breaking
// change, which is exactly what the API name is there to prevent
// (stokaro/ptah#905).
func TestRenamingTheColumnKeepsTheFieldNumber(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(
		column("id", "BIGINT"),
		aliasedColumn("billing_amount_minor", "amount", "INTEGER"),
	), baseOptions())

	// Same published schema, different storage name underneath.
	renamed := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		aliasedColumn("invoice_total_cents", "amount", "INTEGER"),
	), withPrevious(baseline.Data))

	c.Assert(section(renamed, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  int32 amount = 2;\n}")
	c.Assert(renamed, qt.Not(qt.Contains), "reserved",
		qt.Commentf("a rename that keeps the published name reserves nothing"))
}

// The other half of the same contract, and the two features compose: changing
// the PUBLISHED name retires an identity consumers hold, so the field-removal
// policy from stokaro/ptah#905's sibling work refuses it by default — and the
// refusal names the remedy the API name makes possible.
func TestRenamingTheAPINameIsRefusedAsARemoval(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(
		column("id", "BIGINT"),
		aliasedColumn("billing_amount_minor", "amount", "INTEGER"),
	), baseOptions())

	_, err := protobufrender.Render(context.Background(), oneTable(
		column("id", "BIGINT"),
		aliasedColumn("billing_amount_minor", "total", "INTEGER"),
	), withPrevious(baseline.Data))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "fields removed from Thing: amount")
	c.Assert(err.Error(), qt.Contains, "restore the name if the column was renamed")
}

// With the retirement chosen explicitly, the old identity is reserved by number
// and by name, and the new one takes a fresh number. Nothing reuses 2.
func TestRenamingTheAPINameReservesTheOldIdentity(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(
		column("id", "BIGINT"),
		aliasedColumn("billing_amount_minor", "amount", "INTEGER"),
	), baseOptions())

	opts := withPrevious(baseline.Data)
	opts.OnFieldRemoval = protobufrender.FieldRemovalReserve
	republished := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		aliasedColumn("billing_amount_minor", "total", "INTEGER"),
	), opts)

	c.Assert(republished, qt.Contains, "total = 3")
	c.Assert(republished, qt.Contains, "reserved 2;")
	// Edition 2023 spells a reserved name as a bare identifier; the quoted
	// proto2/proto3 form would not appear here.
	c.Assert(republished, qt.Contains, "reserved amount;")
}

// A table's message name follows its declared API name, so an established
// message survives the table underneath being renamed.
func TestMessageUsesTheDeclaredTableAPIName(t *testing.T) {
	c := qt.New(t)

	out := mustRenderText(c, &goschema.Database{
		Tables: []goschema.Table{{StructName: "Thing", Name: "billing_invoices", APIName: "invoices"}},
		Fields: columns("Thing", column("id", "BIGINT")),
	}, baseOptions())

	c.Assert(out, qt.Contains, "message Invoice {")
	c.Assert(out, qt.Not(qt.Contains), "BillingInvoice")
}

// typedColumn is a column exported as a type other than its own.
func typedColumn(name, columnType, apiType string) goschema.Field {
	return goschema.Field{Name: name, Type: columnType, APIType: apiType}
}

// The override decides the wire type, and this is the case it exists for: the
// mapping deliberately refuses google.protobuf.Timestamp for a timezone-
// ambiguous TIMESTAMP, because it cannot know the column is UTC. The author
// can. Declaring the type they know it carries buys the well-typed field, and
// the import that goes with it.
func TestFieldUsesTheDeclaredAPIType(t *testing.T) {
	c := qt.New(t)

	out := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		column("ambiguous_at", "TIMESTAMP"),
		typedColumn("stored_utc_at", "TIMESTAMP", "TIMESTAMPTZ"),
	), baseOptions())

	c.Assert(section(out, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string ambiguous_at = 2;\n"+
			"  google.protobuf.Timestamp stored_utc_at = 3;\n}")
	c.Assert(out, qt.Contains, `import "google/protobuf/timestamp.proto";`,
		qt.Commentf("the override has to pull in the import its type needs"))
}

// An override the mapping cannot honor is refused. Here the stake is higher
// than in the other two exporters: a silently defaulted wire type would be
// pinned by the next reconcile against the generated file.
func TestUnknownAPITypeIsRefused(t *testing.T) {
	c := qt.New(t)

	msg := mustFail(c, oneTable(
		column("id", "BIGINT"),
		typedColumn("amount", "DECIMAL(12,2)", "money_ish"),
	), baseOptions())

	c.Assert(msg, qt.Contains, `declares api_type "money_ish"`)
	c.Assert(msg, qt.Contains, "Protobuf projection does not recognize")
}

// The control that keeps the refusal narrow: an unrecognized COLUMN type is
// still exported, as string, exactly as before.
func TestUnknownColumnTypeIsStillExported(t *testing.T) {
	c := qt.New(t)

	res := mustRender(c, oneTable(
		column("id", "BIGINT"),
		column("quirk", "money_ish"),
	), baseOptions())

	c.Assert(string(res.Data), qt.Contains, "string quirk = 2;")
}

// The override buys no exemption from the wire-compatibility policy. Changing
// it on a field that already exists in the generated file is the same
// incompatible change as editing the column's own type, and is refused with the
// same instruction.
func TestChangingTheAPITypeIsAWireIncompatibleChange(t *testing.T) {
	c := qt.New(t)

	published := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		typedColumn("amount", "DECIMAL(12,2)", "TEXT"),
	), baseOptions())

	msg := mustFail(c, oneTable(
		column("id", "BIGINT"),
		typedColumn("amount", "DECIMAL(12,2)", "BIGINT"),
	), withPrevious([]byte(published)))

	c.Assert(msg, qt.Contains, "changed from string to int64")
	c.Assert(msg, qt.Contains, "not wire compatible")
}

// The mirror of the case above, and the second reason the override is worth
// having: under a pinned api_type, re-typing the COLUMN changes nothing on the
// wire. Without it this same edit is int64 -> string, which is refused.
func TestChangingTheColumnUnderAPITypeKeepsTheWireType(t *testing.T) {
	c := qt.New(t)

	published := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		typedColumn("external_ref", "BIGINT", "BIGINT"),
	), baseOptions())

	out := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		typedColumn("external_ref", "TEXT", "BIGINT"),
	), withPrevious([]byte(published)))

	c.Assert(section(out, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  int64 external_ref = 2;\n}")
}
