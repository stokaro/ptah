package schemavalidate_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/schemavalidate"
)

// tableWithMySQLOptions is a schema written for MySQL and pointed at a second
// target, which is the case stokaro/ptah#2976 is about.
func tableWithMySQLOptions() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName:    "User",
			Name:          "users",
			Engine:        "InnoDB",
			AutoIncrement: "100",
			Charset:       "utf8mb4",
			Collate:       "utf8mb4_bin",
		}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INT", Primary: true},
		},
	}
}

// newDatabaseWithSerialColumn is the schema whose render the target refuses.
func newDatabaseWithSerialColumn() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Thing", Name: "things"}},
		Fields: []schemamodel.Field{
			{StructName: "Thing", Name: "id", Type: "SERIAL", Primary: true},
		},
	}
}

// newDatabaseWithUnsupportedIndexInclude carries a declaration the whole-schema
// gate refuses, so validation and rendering both meet the same fault.
func newDatabaseWithUnsupportedIndexInclude() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INT", Primary: true},
			{StructName: "User", Name: "email", Type: "VARCHAR(255)"},
			{StructName: "User", Name: "name", Type: "VARCHAR(255)"},
		},
		Indexes: []schemamodel.Index{{
			StructName:     "User",
			Name:           "idx_users_email",
			Fields:         []string{"email"},
			IncludeColumns: []string{"name"},
		}},
	}
}

// TestCollectWithOptions_NoSkippedNamesEveryLostTableOption is the check the
// flag turns on.
//
// The problems carry the object identity so the reader can find the table, and
// one line per option so a partial fix shows up as fewer lines rather than the
// same one.
func TestCollectWithOptions_NoSkippedNamesEveryLostTableOption(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.CollectWithOptions(
		tableWithMySQLOptions(),
		platform.Postgres,
		schemavalidate.Options{Capabilities: capability.Postgres17(), NoSkipped: true},
	)

	lines := make([]string, 0, len(problems))
	for _, problem := range problems {
		lines = append(lines, problem.String())
	}
	c.Assert(lines, qt.DeepEquals, []string{
		`postgres: table "users": table option AUTO_INCREMENT=100 would be skipped; ` +
			`declare the start on the key column with identity_start`,
		`postgres: table "users": table option CHARSET=utf8mb4 would be skipped`,
		`postgres: table "users": table option COLLATE=utf8mb4_bin would be skipped`,
		`postgres: table "users": table option ENGINE=InnoDB would be skipped`,
	})
}

// TestCollectWithOptions_TheDefaultIsUnchanged keeps the flag opt-in.
//
// A schema written for several engines is expected to lose engine-specific
// declarations on the others. Failing that by default would refuse the
// authoring style Ptah supports, so the same schema has to stay clean here.
func TestCollectWithOptions_TheDefaultIsUnchanged(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.CollectWithOptions(
		tableWithMySQLOptions(),
		platform.Postgres,
		schemavalidate.Options{Capabilities: capability.Postgres17()},
	)

	c.Assert(problems, qt.HasLen, 0)
}

// TestCollectWithOptions_ARenderRefusalIsAProblem covers the gap the
// documentation named before this existed.
//
// A SERIAL column validates against ClickHouse and renders as an error, so
// structural validation alone reported a schema this target cannot use. The
// caller asked what is wrong for this target, and "it does not render" answers
// that question rather than failing to answer it.
func TestCollectWithOptions_ARenderRefusalIsAProblem(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.CollectWithOptions(
		newDatabaseWithSerialColumn(),
		platform.ClickHouse,
		schemavalidate.Options{Capabilities: capability.ClickHouse24(), NoSkipped: true},
	)

	c.Assert(problems, qt.HasLen, 1)
	c.Assert(problems[0].Kind, qt.Equals, "schema")
	c.Assert(problems[0].Message, qt.Contains, "SERIAL has no auto-increment equivalent")
}

// TestCollectWithOptions_ARenderRefusalPassesWithoutTheFlag is that test's
// control.
//
// Without it, a check that always reported the refusal would satisfy the case
// above while changing what the default run does.
func TestCollectWithOptions_ARenderRefusalPassesWithoutTheFlag(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.CollectWithOptions(
		newDatabaseWithSerialColumn(),
		platform.ClickHouse,
		schemavalidate.Options{Capabilities: capability.ClickHouse24()},
	)

	c.Assert(problems, qt.HasLen, 0)
}

// TestCollectWithOptions_AValidationFaultIsReportedOnce is the dedup the issue
// asks for.
//
// The render begins with the validation that already ran, so a fault both find
// would arrive twice and read as two problems in a schema that has one.
func TestCollectWithOptions_AValidationFaultIsReportedOnce(t *testing.T) {
	c := qt.New(t)

	problems := schemavalidate.CollectWithOptions(
		newDatabaseWithUnsupportedIndexInclude(),
		platform.MySQL,
		schemavalidate.Options{Capabilities: capability.MySQL84(), NoSkipped: true},
	)

	c.Assert(problems, qt.HasLen, 1)
	c.Assert(problems[0].Kind, qt.Equals, "schema")
}
