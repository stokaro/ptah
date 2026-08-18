package protobufrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/internal/schemaexport"
)

// exposed returns a column carrying an api_expose declaration.
func exposed(name, columnType, expose string) goschema.Field {
	field := column(name, columnType)
	field.APIExpose = expose
	return field
}

// TestExposureHidingAnEmittedFieldRetiresItsNumber is the compatibility claim
// stokaro/ptah#904 makes about this target, measured.
//
// Withholding a column that a previous export published is, to Protobuf,
// indistinguishable from removing it from the schema — and removal already
// retires the number and reserves the name. That is why #904 needs no second
// mechanism here: the projection reaches the renderer as a shorter column list,
// and the existing numbering machinery does the rest.
func TestExposureHidingAnEmittedFieldRetiresItsNumber(t *testing.T) {
	c := qt.New(t)

	baseline := mustRender(c, oneTable(
		column("id", "BIGINT"),
		column("sku", "TEXT"),
		column("name", "TEXT"),
	), baseOptions())

	options := withRetiredFields(baseline.Data)
	options.FieldPolicy = schemaexport.FieldPolicyAll
	hidden := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		exposed("sku", "TEXT", "none"),
		column("name", "TEXT"),
	), options)

	// Byte-identical to what removing the column produces, which is the point:
	// one machinery, one wire outcome.
	c.Assert(section(hidden, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string name = 3;\n\n  reserved 2;\n  reserved sku;\n}")
}

// TestExposureExcludedBeforeFirstExportConsumesNoNumber pins the other half of
// #904's compatibility section: a column hidden before anything was ever
// published must not burn a field number.
//
// It cannot, because the renderer never sees it — but that is the kind of claim
// worth a test, since it would silently stop being true if the exposure were
// applied after numbering instead of before it.
func TestExposureExcludedBeforeFirstExportConsumesNoNumber(t *testing.T) {
	c := qt.New(t)

	rendered := mustRenderText(c, oneTable(
		column("id", "BIGINT"),
		exposed("secret", "TEXT", "none"),
		column("name", "TEXT"),
	), baseOptions())

	// name takes 2, not 3: the hidden column consumed nothing, and no
	// reservation was invented for a number that never existed.
	c.Assert(section(rendered, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string name = 2;\n}")
}

// TestExposureAllowlistWithholdsUndeclaredColumns pins that the allowlist policy
// reaches this target too, so the three exporters agree about what is published.
func TestExposureAllowlistWithholdsUndeclaredColumns(t *testing.T) {
	c := qt.New(t)
	options := baseOptions()
	options.FieldPolicy = schemaexport.FieldPolicyAllowlist

	rendered := mustRenderText(c, oneTable(
		exposed("id", "BIGINT", "read"),
		column("undeclared", "TEXT"),
	), options)

	c.Assert(section(rendered, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n}")
}

// TestExposureWriteOnlyColumnStillReachesTheMessage pins that a column exposed
// for one direction is published here.
//
// A Protobuf message carries no direction of its own: the same message is used
// for requests and responses, so a column reachable by either contract has to
// be in it. Dropping it would make the wire type unable to express what the
// schema says a caller may send.
func TestExposureWriteOnlyColumnStillReachesTheMessage(t *testing.T) {
	c := qt.New(t)
	options := baseOptions()
	options.FieldPolicy = schemaexport.FieldPolicyAllowlist

	rendered := mustRenderText(c, oneTable(
		exposed("id", "BIGINT", "read"),
		exposed("password_hash", "TEXT", "write"),
	), options)

	c.Assert(section(rendered, "message Thing {"), qt.Equals,
		"message Thing {\n  int64 id = 1;\n  string password_hash = 2;\n}")
}
