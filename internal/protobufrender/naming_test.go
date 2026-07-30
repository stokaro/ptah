package protobufrender_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/core/goschema"
)

func TestEnumSynthesizesTheZeroValue(t *testing.T) {
	c := qt.New(t)

	text := mustRenderText(c, inlineEnumTable("draft", "live"), baseOptions())

	// Editions default features.enum_type to OPEN and protoc rejects an open
	// enum whose first value is not zero, so the zero value is a language
	// requirement and not only a style rule.
	c.Assert(section(text, "enum ThingState {"), qt.Equals,
		"enum ThingState {\n"+
			"  THING_STATE_UNSPECIFIED = 0;\n"+
			"  THING_STATE_DRAFT = 1;\n"+
			"  THING_STATE_LIVE = 2;\n"+
			"}")
	c.Assert(section(text, "message Thing {"), qt.Equals, "message Thing {\n  ThingState state = 1;\n}")
}

func TestEnumValuePrefixFollowsBufsDigitRule(t *testing.T) {
	c := qt.New(t)

	// buf lint's ENUM_VALUE_PREFIX derives the required prefix by running its
	// own snake-casing over the *generated* type name, and snake -> PascalCase
	// -> snake is not the identity across digits. The naive
	// ENUM_USER_2FA_STATUS_ prefix is what buf rejects.
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Enum", Name: "enums"}},
		Fields: columns("Enum", goschema.Field{
			Name: "user_2fa_status",
			Type: enumCarrierType,
			Enum: []string{"pending", "done"},
		}),
	}

	text := mustRenderText(c, db, baseOptions())

	c.Assert(section(text, "enum EnumUser2faStatus {"), qt.Equals,
		"enum EnumUser2faStatus {\n"+
			"  ENUM_USER2FA_STATUS_UNSPECIFIED = 0;\n"+
			"  ENUM_USER2FA_STATUS_PENDING = 1;\n"+
			"  ENUM_USER2FA_STATUS_DONE = 2;\n"+
			"}")
	c.Assert(text, qt.Not(qt.Contains), "ENUM_USER_2FA_STATUS_")
}

func TestEnumNamedTypeDropsThePtahPrefixAndKeepsTheDigitRule(t *testing.T) {
	c := qt.New(t)

	// A column that references a named Ptah enum takes the type-derived branch:
	// "enum_user_2fa_status" loses its "enum_" prefix and becomes
	// User2faStatus, whose buf prefix is USER2FA_STATUS_ rather than
	// USER_2FA_STATUS_.
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Thing", Name: "things"}},
		Fields: columns("Thing", column("status", "enum_user_2fa_status")),
		Enums:  []goschema.Enum{{Name: "enum_user_2fa_status", Values: []string{"pending", "done"}}},
	}

	text := mustRenderText(c, db, baseOptions())

	c.Assert(section(text, "enum User2faStatus {"), qt.Equals,
		"enum User2faStatus {\n"+
			"  USER2FA_STATUS_UNSPECIFIED = 0;\n"+
			"  USER2FA_STATUS_PENDING = 1;\n"+
			"  USER2FA_STATUS_DONE = 2;\n"+
			"}")
	c.Assert(text, qt.Not(qt.Contains), "USER_2FA_STATUS_")
}

func TestEnumNamedTypeSharedByTwoColumnsProducesOneEnum(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Name: "users"},
			{StructName: "Admin", Name: "admins"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "status", Type: "enum_account_status"},
			{StructName: "Admin", Name: "status", Type: "enum_account_status"},
		},
		Enums: []goschema.Enum{{Name: "enum_account_status", Values: []string{"active", "retired"}}},
	}

	text := mustRenderText(c, db, baseOptions())

	c.Assert(text, qt.Contains, "message Admin {\n  AccountStatus status = 1;\n}")
	c.Assert(text, qt.Contains, "message User {\n  AccountStatus status = 1;\n}")
	// Deduplication is keyed on the enum type, so exactly one enum is emitted.
	c.Assert(enumDeclarations(text), qt.HasLen, 1)
}

func TestEnumLabelsAreSanitizedWithAWarningRatherThanRejected(t *testing.T) {
	c := qt.New(t)

	res := mustRender(c, inlineEnumTable("in-progress", "2fa"), baseOptions())
	text := string(res.Data)

	c.Assert(section(text, "enum ThingState {"), qt.Equals,
		"enum ThingState {\n"+
			"  THING_STATE_UNSPECIFIED = 0;\n"+
			"  THING_STATE_IN_PROGRESS = 1;\n"+
			"  THING_STATE_2FA = 2;\n"+
			"}")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`enum label "in-progress" was sanitized to protobuf value "THING_STATE_IN_PROGRESS"`)
	// The leading underscore SanitizeGraphQLName adds to "2fa" is a delimiter
	// buf's snake-caser trims, so the prefix supplies the separator instead.
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`enum label "2fa" was sanitized to protobuf value "THING_STATE_2FA"`)
}

func TestDigitLeadingColumnsSanitizeAndWarn(t *testing.T) {
	c := qt.New(t)

	res := mustRender(c, oneTable(
		column("2fa_enabled", "BOOLEAN"),
		column("3d_model_url", "TEXT"),
		column("Mixed-Case", "TEXT"),
	), baseOptions())

	c.Assert(section(string(res.Data), "message Thing {"), qt.Equals,
		"message Thing {\n"+
			"  bool _2fa_enabled = 1;\n"+
			"  string _3d_model_url = 2;\n"+
			"  string mixed_case = 3;\n"+
			"}")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`column "2fa_enabled" was sanitized to protobuf field "_2fa_enabled"; `+
			`buf lint STANDARD will report FIELD_LOWER_SNAKE_CASE for it`)
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`column "3d_model_url" was sanitized to protobuf field "_3d_model_url"`)
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`column "Mixed-Case" was sanitized to protobuf field "mixed_case"`)
}

func TestSanitizedTableNameWarns(t *testing.T) {
	c := qt.New(t)

	res := mustRender(c, &goschema.Database{
		Tables: []goschema.Table{{StructName: "S", Name: "2fa_tokens"}},
		Fields: columns("S", column("id", "BIGINT")),
	}, baseOptions())

	c.Assert(string(res.Data), qt.Contains, "message _2faToken {")
	c.Assert(diagnosticMessages(res), qt.Any(qt.Contains),
		`table "2fa_tokens" was sanitized to protobuf message "_2faToken"; `+
			`buf lint STANDARD will report MESSAGE_PASCAL_CASE for it`)
}

func TestEnumLabelUnspecifiedCollidesWithTheSynthesizedZeroValue(t *testing.T) {
	c := qt.New(t)

	// Protobuf enum values are siblings of their type at package scope, and the
	// synthesized zero value shares that namespace with real labels.
	message := mustFail(c, inlineEnumTable("unspecified", "live"), baseOptions())

	c.Assert(message, qt.Equals,
		"enum value names collide (protobuf enum values share their package's namespace): "+
			`THING_STATE_UNSPECIFIED: label "unspecified" of enum ThingState, `+
			"synthesized zero value of enum ThingState")
}

func TestEnumValuesCollideAcrossDifferentEnums(t *testing.T) {
	c := qt.New(t)

	// Alpha + label "x_y" and AlphaX + label "y" both spell ALPHA_X_Y, and
	// C++ scoping makes that a package-level clash rather than a per-enum one.
	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Thing", Name: "things"}},
		Fields: columns("Thing",
			column("a", "enum_alpha"),
			column("b", "enum_alpha_x"),
		),
		Enums: []goschema.Enum{
			{Name: "enum_alpha", Values: []string{"x_y"}},
			{Name: "enum_alpha_x", Values: []string{"y"}},
		},
	}

	message := mustFail(c, db, baseOptions())
	c.Assert(message, qt.Equals,
		"enum value names collide (protobuf enum values share their package's namespace): "+
			`ALPHA_X_Y: label "x_y" of enum Alpha, label "y" of enum AlphaX`)
}

func TestEnumTypeNamesCollide(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{{StructName: "Thing", Name: "things"}},
		Fields: columns("Thing",
			column("a", "enum_shared"),
			column("b", "enum_shared_"),
		),
		Enums: []goschema.Enum{
			{Name: "enum_shared", Values: []string{"one"}},
			{Name: "enum_shared_", Values: []string{"two"}},
		},
	}

	// Two distinct Ptah enums that PascalCase to the same identifier are an
	// error rather than an alias.
	message := mustFail(c, db, baseOptions())
	c.Assert(message, qt.Equals,
		`enum type name "Shared" is produced by more than one source; `+
			`rename the enum or the column that produces it`)
}

func TestTwoColumnsSanitizingToOneFieldNameAreRejected(t *testing.T) {
	c := qt.New(t)

	message := mustFail(c, oneTable(
		column("a-b", "TEXT"),
		column("a_b", "TEXT"),
	), baseOptions())

	c.Assert(message, qt.Equals,
		`columns "a-b" and "a_b" on table "things" both map to protobuf field "a_b"; rename one column`)
}

func TestTablesCollapsingToOneMessageNameAreRejected(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Order", Name: "order"},
			{StructName: "OrderRow", Name: "orders"},
		},
		Fields: []goschema.Field{
			{StructName: "Order", Name: "id", Type: "BIGINT"},
			{StructName: "OrderRow", Name: "id", Type: "BIGINT"},
		},
	}

	message := mustFail(c, db, baseOptions())

	// Both sources are named, and never disambiguated by ordinal: a numeric
	// suffix would make the result depend on table order.
	c.Assert(message, qt.Equals,
		"tables map to the same protobuf message name: "+
			"Order: order (struct Order), orders (struct OrderRow); "+
			"set a distinct schema= on one of them or exclude it with --exclude-tables")
}

func TestSchemaQualifiedDisambiguationIsOrderIndependent(t *testing.T) {
	c := qt.New(t)

	sales := goschema.Table{StructName: "Order", Name: "order", Schema: "sales"}
	rows := goschema.Table{StructName: "OrderRow", Name: "orders"}
	fields := []goschema.Field{
		{StructName: "Order", Name: "id", Type: "BIGINT"},
		{StructName: "OrderRow", Name: "id", Type: "BIGINT"},
	}

	forward := mustRenderText(c, &goschema.Database{
		Tables: []goschema.Table{sales, rows},
		Fields: fields,
	}, baseOptions())
	reversed := mustRenderText(c, &goschema.Database{
		Tables: []goschema.Table{rows, sales},
		Fields: fields,
	}, baseOptions())

	c.Assert(reversed, qt.Equals, forward)
	// The table without an explicit schema keeps the bare name, so adding a
	// schema-qualified twin never silently renames an existing message.
	c.Assert(forward, qt.Contains, "message Order {")
	c.Assert(forward, qt.Contains, "message SalesOrder {")
}

func TestSchemaQualificationThatStillCollidesIsRejected(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Order", Name: "order", Schema: "sales"},
			{StructName: "OrderRow", Name: "orders", Schema: "sales"},
		},
		Fields: []goschema.Field{
			{StructName: "Order", Name: "id", Type: "BIGINT"},
			{StructName: "OrderRow", Name: "id", Type: "BIGINT"},
		},
	}

	message := mustFail(c, db, baseOptions())
	c.Assert(message, qt.Equals,
		"tables map to the same protobuf message name: "+
			"SalesOrder: sales.order (struct Order), sales.orders (struct OrderRow); "+
			"set a distinct schema= on one of them or exclude it with --exclude-tables")
}

func TestMessagesAndEnumsAreOrderedByName(t *testing.T) {
	c := qt.New(t)

	db := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Zebra", Name: "zebras"},
			{StructName: "Apple", Name: "apples"},
		},
		Fields: []goschema.Field{
			{StructName: "Zebra", Name: "state", Type: enumCarrierType, Enum: []string{"a"}},
			{StructName: "Apple", Name: "state", Type: enumCarrierType, Enum: []string{"b"}},
		},
	}

	text := mustRenderText(c, db, baseOptions())

	c.Assert(indexOf(text, "message Apple {") < indexOf(text, "message Zebra {"), qt.IsTrue)
	c.Assert(indexOf(text, "message Zebra {") < indexOf(text, "enum AppleState {"), qt.IsTrue)
	c.Assert(indexOf(text, "enum AppleState {") < indexOf(text, "enum ZebraState {"), qt.IsTrue)
}
