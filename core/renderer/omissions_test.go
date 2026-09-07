package renderer_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// tableWithMySQLOptions is the schema every target below is asked to render.
//
// The four options are the ones the MySQL family owns, and they are what
// stokaro/ptah#2969 measured a PostgreSQL server rejecting outright. A schema
// carrying them is the ordinary case for an author who wrote for MySQL first
// and added a second target later.
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
			{StructName: "User", Name: "name", Type: "VARCHAR(255)"},
		},
	}
}

// omissionProperties lists the properties reported for one target, so a case
// row can carry the answer as data.
func omissionProperties(c *qt.C, omissions []renderer.Omission) []string {
	c.Helper()

	properties := make([]string, 0, len(omissions))
	for _, omission := range omissions {
		properties = append(properties, omission.Property)
	}
	return properties
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesEveryTableOptionATargetDrops
// is the check stokaro/ptah#2976 exists for.
//
// Before it, a render that dropped all four options exited 0 on every one of
// these targets. PostgreSQL wrote a comment and the other four wrote nothing at
// all, so a check built on the comment would have called SQLite, SQL Server and
// Oracle stricter than PostgreSQL for saying less.
func TestGetOrderedCreateStatementsReportingOmissions_NamesEveryTableOptionATargetDrops(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{
			name:    "postgres carries none of them",
			dialect: platform.Postgres,
			want: []string{
				"table option AUTO_INCREMENT",
				"table option CHARSET",
				"table option COLLATE",
				"table option ENGINE",
			},
		},
		{
			name:    "sqlite carries none of them and used to say nothing",
			dialect: platform.SQLite,
			want: []string{
				"table option AUTO_INCREMENT",
				"table option CHARSET",
				"table option COLLATE",
				"table option ENGINE",
			},
		},
		{
			name:    "sql server carries none of them and used to say nothing",
			dialect: platform.SQLServer,
			want: []string{
				"table option AUTO_INCREMENT",
				"table option CHARSET",
				"table option COLLATE",
				"table option ENGINE",
			},
		},
		{
			name:    "oracle carries none of them and used to say nothing",
			dialect: platform.Oracle,
			want: []string{
				"table option AUTO_INCREMENT",
				"table option CHARSET",
				"table option COLLATE",
				"table option ENGINE",
			},
		},
		{
			// ClickHouse renders the engine clause from ENGINE, so that option
			// is preserved rather than lost. Reporting it here would be a
			// finding about a declaration that did reach the output.
			name:    "clickhouse keeps the engine and drops the rest",
			dialect: platform.ClickHouse,
			want: []string{
				"table option AUTO_INCREMENT",
				"table option CHARSET",
				"table option COLLATE",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				tableWithMySQLOptions(),
				test.dialect,
				capability.ForDialect(test.dialect),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(omissionProperties(c, omissions), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_TheMySQLFamilyLosesNothing is
// the control.
//
// Every assertion above is satisfied by a report that fires for any table
// carrying options at all. MySQL and MariaDB render all four, so a finding here
// would mean the check reports what a target actually did.
func TestGetOrderedCreateStatementsReportingOmissions_TheMySQLFamilyLosesNothing(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				tableWithMySQLOptions(),
				test.dialect,
				capability.ForDialect(test.dialect),
			)

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(statements[0], qt.Contains, "ENGINE=InnoDB")
			c.Assert(statements[0], qt.Contains, "AUTO_INCREMENT=100")
			c.Assert(statements[0], qt.Contains, "CHARSET=utf8mb4")
			c.Assert(statements[0], qt.Contains, "COLLATE=utf8mb4_bin")
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_CarriesTheDeclaredValue
// pins the fields a reader needs to act on a report.
//
// A property name alone does not say what was lost: an author who declared
// AUTO_INCREMENT=100 needs the 100 back to put it anywhere else. The remedy is
// asserted on the one option that has one, because a remedy printed where it
// does not work is worse than none.
func TestGetOrderedCreateStatementsReportingOmissions_CarriesTheDeclaredValue(t *testing.T) {
	c := qt.New(t)

	_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		tableWithMySQLOptions(),
		platform.Postgres,
		capability.Postgres17(),
	)

	c.Assert(err, qt.IsNil)
	c.Assert(omissions[0], qt.DeepEquals, renderer.Omission{
		Dialect:  platform.Postgres,
		Reason:   "unsupported",
		Kind:     "table",
		Name:     "users",
		Property: "table option AUTO_INCREMENT",
		Detail:   "100",
		Remedy:   "declare the start on the key column with identity_start",
	})
	c.Assert(omissions[0].Message(), qt.Equals, "table option AUTO_INCREMENT=100 would be skipped")
	c.Assert(omissions[3].Remedy, qt.Equals, "",
		qt.Commentf("ENGINE has no PostgreSQL equivalent to point the author at"))
}

// TestGetOrderedCreateStatementsReportingOmissions_RendersTheSameStatements
// keeps the two entry points one render.
//
// A reporting path that produced different SQL would make the report describe
// a render nobody runs, which is the failure mode of checking a copy of the
// pipeline instead of the pipeline.
func TestGetOrderedCreateStatementsReportingOmissions_RendersTheSameStatements(t *testing.T) {
	c := qt.New(t)

	plain, err := renderer.GetOrderedCreateStatementsWithCapabilities(
		tableWithMySQLOptions(), platform.Postgres, capability.Postgres17())
	c.Assert(err, qt.IsNil)

	reported, _, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		tableWithMySQLOptions(), platform.Postgres, capability.Postgres17())
	c.Assert(err, qt.IsNil)

	c.Assert(reported, qt.DeepEquals, plain)
}

// TestGetOrderedCreateStatementsReportingOmissions_ReportsTheSameOrderTwice
// pins determinism.
//
// The options reach the renderer in a map, and a walk in map order produced a
// different render on every run once already (stokaro/ptah#2968). A report a
// pipeline compares against itself has to be stable for the same reason the
// SQL is.
func TestGetOrderedCreateStatementsReportingOmissions_ReportsTheSameOrderTwice(t *testing.T) {
	c := qt.New(t)

	_, first, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		tableWithMySQLOptions(), platform.SQLite, capability.SQLite3())
	c.Assert(err, qt.IsNil)
	_, second, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		tableWithMySQLOptions(), platform.SQLite, capability.SQLite3())
	c.Assert(err, qt.IsNil)

	c.Assert(second, qt.DeepEquals, first)
}

// TestGetOrderedCreateStatementsReportingOmissions_LeavesTheInputAlone keeps
// the report from costing the caller their schema.
//
// The value is checked after the render rather than the pointer, because a
// renderer that mutated the declaration in place would leave the same pointer
// holding different options.
func TestGetOrderedCreateStatementsReportingOmissions_LeavesTheInputAlone(t *testing.T) {
	c := qt.New(t)
	database := tableWithMySQLOptions()

	_, _, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, platform.Oracle, capability.Oracle23())

	c.Assert(err, qt.IsNil)
	c.Assert(database, qt.DeepEquals, tableWithMySQLOptions())
}

// TestGetOrderedCreateStatementsReportingOmissions_AScopedObjectIsNotAnOmission
// is the false-positive control the issue asks for by name.
//
// An object declared for another target is not part of this target's desired
// state, so it is absent rather than skipped. Reporting it would fail every
// multi-dialect schema, which is the authoring style the scope attribute exists
// to support.
func TestGetOrderedCreateStatementsReportingOmissions_AScopedObjectIsNotAnOmission(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "INT", Primary: true}},
		Sequences: []schemamodel.Sequence{{
			Name:     "user_ids",
			Dialects: []string{platform.Postgres},
		}},
	}

	_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, platform.SQLite, capability.SQLite3())

	c.Assert(err, qt.IsNil)
	c.Assert(omissions, qt.HasLen, 0)
}

// TestGetOrderedCreateStatementsReportingOmissions_AnUnscopedObjectIsAnOmission
// is that control's other half.
//
// Without it, a report that never fires satisfies the test above. The same
// sequence with no scope is part of SQLite's desired state and SQLite has no
// sequences, so the author does lose it.
func TestGetOrderedCreateStatementsReportingOmissions_AnUnscopedObjectIsAnOmission(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables:    []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields:    []schemamodel.Field{{StructName: "User", Name: "id", Type: "INT", Primary: true}},
		Sequences: []schemamodel.Sequence{{Name: "user_ids"}},
	}

	_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, platform.SQLite, capability.SQLite3())

	c.Assert(err, qt.IsNil)
	c.Assert(omissions, qt.HasLen, 1)
	c.Assert(omissions[0].Name, qt.Contains, "user_ids")
}

// TestGetOrderedCreateStatementsReportingOmissions_AnOverrideIsNotALoss covers
// the second false positive the issue names.
//
// A platform override is the author choosing what this target gets. Deciding
// what must survive before applying it would report the value they replaced.
func TestGetOrderedCreateStatementsReportingOmissions_AnOverrideIsNotALoss(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{
			StructName: "Event",
			Name:       "events",
			Engine:     "InnoDB",
			Overrides: map[string]map[string]string{
				platform.ClickHouse: {"engine": "MergeTree", "order_by": "id"},
			},
		}},
		Fields: []schemamodel.Field{{StructName: "Event", Name: "id", Type: "INT", Primary: true}},
	}

	statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, platform.ClickHouse, capability.ClickHouse24())

	c.Assert(err, qt.IsNil)
	c.Assert(omissions, qt.HasLen, 0)
	c.Assert(statements[0], qt.Contains, "MergeTree")
}

// TestGetOrderedCreateStatementsReportingOmissions_FailurePath keeps a refusal
// a refusal.
//
// A target that cannot render the schema at all has not skipped anything: there
// is no output for a declaration to be missing from. Returning an empty report
// beside a nil error would read as a clean render.
func TestGetOrderedCreateStatementsReportingOmissions_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Thing", Name: "things"}},
		Fields: []schemamodel.Field{{StructName: "Thing", Name: "id", Type: "SERIAL", Primary: true}},
	}

	statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, platform.ClickHouse, capability.ClickHouse24())

	c.Assert(err, qt.ErrorMatches, "(?s).*SERIAL has no auto-increment equivalent.*")
	c.Assert(statements, qt.IsNil)
	c.Assert(omissions, qt.IsNil)
}

// enumBackedTable declares an enum and a column typed by it.
//
// Five dialects have no CREATE TYPE and model the values on the column instead.
// That is a supported alternate representation, not a loss, and telling the two
// apart is what keeps this check from failing every schema with an enum.
func enumBackedTable() *schemamodel.Database {
	return &schemamodel.Database{
		Enums: []schemamodel.Enum{{Name: "user_status", Values: []string{"active", "banned"}}},
		Tables: []schemamodel.Table{{
			StructName: "User",
			Name:       "users",
		}},
		Fields: []schemamodel.Field{
			{StructName: "User", Name: "id", Type: "INT", Primary: true},
			{StructName: "User", Name: "status", Type: "user_status", Nullable: false},
		},
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_AnInlineEnumIsNotALoss is the
// alternate-representation control stokaro/ptah#2976 asks for by name.
//
// The values reach the output on every one of these targets, spelled the way
// the target spells them. Reporting the absent CREATE TYPE would call a
// successful translation a loss, and every schema with an enum would fail.
func TestGetOrderedCreateStatementsReportingOmissions_AnInlineEnumIsNotALoss(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sql server", dialect: platform.SQLServer},
		{name: "oracle", dialect: platform.Oracle},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				enumBackedTable(), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "active")
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "banned")
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_AnEnumWithNoInlineFormIsALoss
// is that control's other half.
//
// Every assertion above is satisfied by a check that never reports an enum.
// ClickHouse has no inline lowering for one, so the type is skipped and the
// author does lose it.
func TestGetOrderedCreateStatementsReportingOmissions_AnEnumWithNoInlineFormIsALoss(t *testing.T) {
	c := qt.New(t)

	_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		enumBackedTable(), platform.ClickHouse, capability.ClickHouse24())

	c.Assert(err, qt.IsNil)
	c.Assert(omissions, qt.HasLen, 1)
	c.Assert(omissions[0].Name, qt.Equals, "user_status")
}

// generatedKeyTable declares a key the target generates values for and a column
// with a default the target writes its own way.
func generatedKeyTable() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Order", Name: "id", Type: "SERIAL", Primary: true},
			{
				StructName:  "Order",
				Name:        "created_at",
				Type:        "TIMESTAMP",
				DefaultExpr: "CURRENT_TIMESTAMP",
			},
		},
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_AGeneratedDefaultIsNotALoss
// is the other false-positive control the issue names.
//
// A portable type mapped to the target's spelling preserves the declaration:
// SERIAL becomes AUTO_INCREMENT on the MySQL family and an identity column on
// Oracle and SQL Server. A check that read those as losses would fail every
// schema with a generated key, which is most of them.
func TestGetOrderedCreateStatementsReportingOmissions_AGeneratedDefaultIsNotALoss(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "postgres", dialect: platform.Postgres},
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sql server", dialect: platform.SQLServer},
		{name: "oracle", dialect: platform.Oracle},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				generatedKeyTable(), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "CURRENT_TIMESTAMP")
		})
	}
}
