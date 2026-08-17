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

	// The export compiles what it prints, so the assertion above is not only
	// about a line of text: dropping the import turns this render into
	// "unknown type google.protobuf.Timestamp". Re-reading the file adds the
	// other half -- an override survives its own output unchanged, keeping the
	// field number, rather than being re-derived from the column on every run.
	again := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		column("ambiguous_at", "TIMESTAMP"),
		typedColumn("stored_utc_at", "TIMESTAMP", "TIMESTAMPTZ"),
	), withPrevious([]byte(out)))
	c.Assert(again, qt.Equals, out)
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

// enumFixture is a schema with one declared enum and the four ways a column can
// meet it.
func enumFixture(fields ...goschema.Field) *goschema.Database {
	db := oneTable(fields...)
	db.Enums = []goschema.Enum{{Name: "invoice_state", Values: []string{"draft", "sent"}}}
	return db
}

// The override reaches enum resolution too, in both directions. Asking the type
// mapping alone would have refused the "promoted" case, and that one is the
// point: on a dialect with no native enum the column IS text, and publishing it
// as the enum is exactly the representation the stored type cannot express.
func TestAPITypeProjectsEnumsBothWays(t *testing.T) {
	c := qt.New(t)

	out := mustRenderText(c, enumFixture(
		column("id", "BIGINT"),
		column("state", "invoice_state"),
		typedColumn("flattened", "invoice_state", "TEXT"),
		typedColumn("promoted", "VARCHAR(32)", "invoice_state"),
	), baseOptions())

	c.Assert(section(out, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  InvoiceState state = 2;\n"+
			"  string flattened = 3;\n  InvoiceState promoted = 4;\n}")
	c.Assert(section(out, "enum InvoiceState {"), qt.Contains, "INVOICE_STATE_DRAFT = 1;",
		qt.Commentf("the projected column has to reach the same enum, declared once"))
}

// Inline enum values describe the stored column, and enum resolution consults
// them BEFORE the type. Left in place they answer first and the override does
// nothing at all -- silently, which is the one outcome this annotation exists to
// rule out.
func TestAPITypeOverridesInlineEnumValues(t *testing.T) {
	c := qt.New(t)

	out := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		goschema.Field{Name: "state", Type: "VARCHAR(16)", Enum: []string{"draft", "sent"}, APIType: "TEXT"},
	), baseOptions())

	c.Assert(section(out, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string state = 2;\n}")
	c.Assert(out, qt.Not(qt.Contains), "enum ")
}

// protoNamedColumn is a column published under a name that applies to the wire
// only.
func protoNamedColumn(name, apiName, protoName, columnType string) goschema.Field {
	return goschema.Field{
		Name:     name,
		APIName:  apiName,
		APINames: goschema.TargetNames{Protobuf: protoName},
		Type:     columnType,
	}
}

// The Protobuf name wins here and is not read by the other two exporters. It
// exists for the same reason as the GraphQL one -- a format's naming rules --
// and Protobuf's are strict: `buf lint` wants lower_snake_case, where a GraphQL
// field is conventionally camelCase.
func TestFieldPrefersTheProtoName(t *testing.T) {
	c := qt.New(t)

	out := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		protoNamedColumn("billing_amount_minor", "amountMinor", "amount_minor", "INTEGER"),
	), baseOptions())

	c.Assert(section(out, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  int32 amount_minor = 2;\n}")
}

// The compatibility property that makes the Protobuf name the loaded one: the
// field number is keyed by the published name, so a pinned proto_name absorbs
// both a column rename and a change to the shared API name.
func TestRenamingAroundTheProtoNameKeepsTheFieldNumber(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(
		column("id", "BIGINT"),
		protoNamedColumn("billing_amount_minor", "amountMinor", "amount_minor", "INTEGER"),
	), baseOptions())

	republished := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		protoNamedColumn("net_amount_minor", "netAmount", "amount_minor", "INTEGER"),
	), withPrevious(baseline.Data))

	c.Assert(section(republished, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  int32 amount_minor = 2;\n}")
}

// And the other side of it: the Protobuf name IS the identity, so changing it
// retires one consumers hold, and goes through the same policy as changing the
// shared name.
func TestRenamingTheProtoNameIsRefusedAsARemoval(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(
		column("id", "BIGINT"),
		protoNamedColumn("billing_amount_minor", "amountMinor", "amount_minor", "INTEGER"),
	), baseOptions())

	_, err := protobufrender.Render(context.Background(), oneTable(
		column("id", "BIGINT"),
		protoNamedColumn("billing_amount_minor", "amountMinor", "total_minor", "INTEGER"),
	), withPrevious(baseline.Data))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "fields removed from Thing: amount_minor")
}

// protoNamedTable is a table whose message name applies to the wire only.
func protoNamedTable(name, apiName, protoName string, fields ...goschema.Field) *goschema.Database {
	return &goschema.Database{
		Tables: []goschema.Table{{
			StructName: "Thing", Name: name,
			APIName:  apiName,
			APINames: goschema.TargetNames{Protobuf: protoName},
		}},
		Fields: columns("Thing", fields...),
	}
}

// The table-level scoped name carries the message identity, the same way the
// field-level one carries the field identity.
func TestMessageUsesTheProtoName(t *testing.T) {
	c := qt.New(t)

	out := mustRenderText(c,
		protoNamedTable("billing_invoices", "invoices", "invoice_records", column("id", "BIGINT")),
		baseOptions())

	c.Assert(out, qt.Contains, "message InvoiceRecord {")
	c.Assert(out, qt.Not(qt.Contains), "message Invoice {")
}

// And it carries the weight that goes with an identity: changing it retires a
// message consumers hold, through the policy that already exists for the shared
// name. Protobuf cannot reserve a top-level type name, so the refusal names the
// two ways out rather than choosing one.
func TestRenamingTheTableProtoNameIsRefusedAsARemoval(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c,
		protoNamedTable("billing_invoices", "invoices", "invoice_records", column("id", "BIGINT")),
		baseOptions())

	_, err := protobufrender.Render(context.Background(),
		protoNamedTable("billing_invoices", "invoices", "invoice_archive", column("id", "BIGINT")),
		withPrevious(baseline.Data))

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "types removed from the source schema: InvoiceRecord")
}

// A scoped name is an arbitrary annotation string like any other, so the
// format's naming rules still run on it -- and the diagnostic still names the
// TABLE, because that is the line the reader has to edit.
func TestSanitizationRunsOnTheProtoName(t *testing.T) {
	c := qt.New(t)

	res := mustRender(c,
		protoNamedTable("things", "", "2fa records", column("id", "BIGINT")),
		baseOptions())

	c.Assert(string(res.Data), qt.Contains, "message _2faRecord {")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`table "things" was sanitized to protobuf message "_2faRecord"`)
}
