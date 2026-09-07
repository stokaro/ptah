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

// columnSchema declares one table whose second column carries the property
// under test. The first column is a key, because ClickHouse needs a sorting key
// before it will render a MergeTree table at all.
func columnSchema(field schemamodel.Field) *schemamodel.Database {
	field.StructName = "Doc"
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Doc", Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "Doc", Name: "id", Type: "INT", Primary: true},
			field,
		},
	}
}

// lostProperties renders the schema for one target and returns what it reported
// losing, so a test reads a list of property names rather than a struct.
func lostProperties(c *qt.C, database *schemamodel.Database, dialect string) []string {
	c.Helper()
	_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
		database, dialect, capability.ForDialect(dialect))
	c.Assert(err, qt.IsNil)
	var properties []string
	for _, omission := range omissions {
		properties = append(properties, omission.Property)
	}
	return properties
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesALostCharacterSetAndCollation
// pins which targets carry a column's character set and collation.
//
// Only the MySQL family has a per-column CHARACTER SET. The collation is the
// half with a consequence: it decides which values a comparison and a unique
// index treat as equal, and four of the five targets that drop it here do have
// a column COLLATE clause, so the declaration is lost passing through Ptah
// rather than by the server's incapacity (stokaro/ptah#2983).
func TestGetOrderedCreateStatementsReportingOmissions_NamesALostCharacterSetAndCollation(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "mysql writes both", dialect: platform.MySQL, want: nil},
		{name: "mariadb writes both", dialect: platform.MariaDB, want: nil},
		{name: "postgres writes neither", dialect: platform.Postgres, want: []string{"character set", "collation"}},
		{name: "sqlite writes neither", dialect: platform.SQLite, want: []string{"character set", "collation"}},
		{name: "sql server writes neither", dialect: platform.SQLServer, want: []string{"character set", "collation"}},
		{name: "oracle writes neither", dialect: platform.Oracle, want: []string{"character set", "collation"}},
		{name: "clickhouse writes neither", dialect: platform.ClickHouse, want: []string{"character set", "collation"}},
	}

	database := columnSchema(schemamodel.Field{
		Name: "title", Type: "VARCHAR(80)", Charset: "utf8mb4", Collate: "utf8mb4_bin",
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostProperties(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesALostOnUpdateExpression
// pins the ON UPDATE expression, which only the MySQL family spells.
//
// A target without it leaves the column at whatever the insert wrote, on every
// later update, which is the opposite of what the author declared.
func TestGetOrderedCreateStatementsReportingOmissions_NamesALostOnUpdateExpression(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    []string
	}{
		{name: "mysql writes it", dialect: platform.MySQL, want: nil},
		{name: "mariadb writes it", dialect: platform.MariaDB, want: nil},
		{name: "postgres drops it", dialect: platform.Postgres, want: []string{"on update expression"}},
		{name: "sqlite drops it", dialect: platform.SQLite, want: []string{"on update expression"}},
		{name: "sql server drops it", dialect: platform.SQLServer, want: []string{"on update expression"}},
		{name: "oracle drops it", dialect: platform.Oracle, want: []string{"on update expression"}},
		{name: "clickhouse drops it", dialect: platform.ClickHouse, want: []string{"on update expression"}},
	}

	database := columnSchema(schemamodel.Field{
		Name: "seen", Type: "TIMESTAMP", UpdateExpression: "CURRENT_TIMESTAMP",
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostProperties(c, database, test.dialect), qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesALostNotNullConstraintName
// pins the name an author put on a column's NOT NULL.
//
// PostgreSQL is absent from the table on purpose: it answers a name it cannot
// persist with a refusal rather than by dropping it, which is the stricter
// answer and is asserted separately below.
func TestGetOrderedCreateStatementsReportingOmissions_NamesALostNotNullConstraintName(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "mysql", dialect: platform.MySQL},
		{name: "mariadb", dialect: platform.MariaDB},
		{name: "sqlite", dialect: platform.SQLite},
		{name: "sql server", dialect: platform.SQLServer},
		{name: "oracle", dialect: platform.Oracle},
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	database := columnSchema(schemamodel.Field{
		Name: "title", Type: "VARCHAR(80)", NotNullConstraintName: "docs_title_nn",
	})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostProperties(c, database, test.dialect),
				qt.DeepEquals, []string{"not null constraint name"})
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesTheConstraintsClickHouseDrops
// pins the two column properties that decide what the database accepts.
//
// A dropped UNIQUE admits rows the author meant to exclude, and a column
// declared to generate its own values generates none. ClickHouse refuses a
// table-level UNIQUE and reads past the column-level one, so before this the
// two answers to the same declaration were an error and silence.
func TestGetOrderedCreateStatementsReportingOmissions_NamesTheConstraintsClickHouseDrops(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
		want  []string
	}{
		{
			name:  "a unique column",
			field: schemamodel.Field{Name: "title", Type: "VARCHAR(80)", Unique: true},
			want:  []string{"unique constraint"},
		},
		{
			name:  "a generated key",
			field: schemamodel.Field{Name: "n", Type: "INT", AutoInc: true},
			want:  []string{"auto-increment"},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostProperties(c, columnSchema(test.field), platform.ClickHouse),
				qt.DeepEquals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_NamesAGeneratedKeyPostgresDoesNotSpell
// covers the row the issue's inventory had wrong.
//
// #2983 recorded PostgreSQL as emitting AutoInc. Measured on this tree it reads
// the flag nowhere: generation reaches the output through a sequence-backed
// type or an identity clause, and a column carrying neither renders as a plain
// integer whose values the caller has to supply.
func TestGetOrderedCreateStatementsReportingOmissions_NamesAGeneratedKeyPostgresDoesNotSpell(t *testing.T) {
	c := qt.New(t)

	database := columnSchema(schemamodel.Field{Name: "n", Type: "INT", AutoInc: true})

	c.Assert(lostProperties(c, database, platform.Postgres), qt.DeepEquals, []string{"auto-increment"})
}

// TestGetOrderedCreateStatementsReportingOmissions_ARemedyIsPrintedOnlyWhereItWorks
// pins the one remedy in this group, and its absence.
//
// PostgreSQL spells generation two other ways, so an author who reads the line
// can act on it. ClickHouse has no generated column, and a suggestion that does
// not work on the target it is printed for costs the reader more than silence.
func TestGetOrderedCreateStatementsReportingOmissions_ARemedyIsPrintedOnlyWhereItWorks(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		want    string
	}{
		{
			name:    "postgres has two other spellings",
			dialect: platform.Postgres,
			want:    "declare the column type as SERIAL or BIGSERIAL, or give it an identity clause with identity_generation",
		},
		{
			name:    "clickhouse has none",
			dialect: platform.ClickHouse,
			want:    "",
		},
	}

	database := columnSchema(schemamodel.Field{Name: "n", Type: "INT", AutoInc: true})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				database, test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 1)
			c.Assert(omissions[0].Property, qt.Equals, "auto-increment")
			c.Assert(omissions[0].Remedy, qt.Equals, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_PostgresKeepsAKeyItCanSpell is
// the control for the test above.
//
// Both spellings PostgreSQL does have must stay quiet, or every schema with a
// key would report a loss on the one target that renders it best.
func TestGetOrderedCreateStatementsReportingOmissions_PostgresKeepsAKeyItCanSpell(t *testing.T) {
	tests := []struct {
		name  string
		field schemamodel.Field
		want  string
	}{
		{
			name:  "a sequence-backed type",
			field: schemamodel.Field{Name: "n", Type: "SERIAL", AutoInc: true},
			want:  "SERIAL",
		},
		{
			name:  "the portable spelling of one",
			field: schemamodel.Field{Name: "n", Type: "AUTO_INCREMENT", AutoInc: true},
			want:  "SERIAL",
		},
		{
			name:  "an identity clause",
			field: schemamodel.Field{Name: "n", Type: "INT", AutoInc: true, IdentityGeneration: "ALWAYS"},
			want:  "GENERATED ALWAYS AS IDENTITY",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				columnSchema(test.field), platform.Postgres, capability.Postgres17())

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_PostgresRefusesANameItCannotKeep
// is the other half of the NOT NULL name.
//
// The table above leaves PostgreSQL out because it does not drop the name. That
// claim is only worth having if the refusal is asked for, and the capability
// that decides it is asked for on both sides.
func TestGetOrderedCreateStatementsReportingOmissions_PostgresRefusesANameItCannotKeep(t *testing.T) {
	database := columnSchema(schemamodel.Field{
		Name: "title", Type: "VARCHAR(80)", NotNullConstraintName: "docs_title_nn",
	})

	t.Run("a server that stores no name refuses", func(t *testing.T) {
		c := qt.New(t)

		_, _, err := renderer.GetOrderedCreateStatementsReportingOmissions(
			database, platform.Postgres, capability.Postgres17())

		c.Assert(err, qt.ErrorMatches, `(?s).*does not keep a NOT NULL constraint name.*`)
	})

	t.Run("a server that stores one writes it", func(t *testing.T) {
		c := qt.New(t)

		statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
			database, platform.Postgres, capability.Postgres18())

		c.Assert(err, qt.IsNil)
		c.Assert(omissions, qt.HasLen, 0)
		c.Assert(strings.Join(statements, "\n"), qt.Contains, "CONSTRAINT")
		c.Assert(strings.Join(statements, "\n"), qt.Contains, "docs_title_nn")
	})
}

// TestGetOrderedCreateStatementsReportingOmissions_ATargetWritesWhatItKeeps is
// the control every table above needs.
//
// Each row there is satisfied by a report that fires for any column with the
// property set. These assert the property is in the output on the targets the
// rows call keepers, so a report that started firing everywhere would be caught
// here rather than read as thoroughness.
func TestGetOrderedCreateStatementsReportingOmissions_ATargetWritesWhatItKeeps(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		field   schemamodel.Field
		want    string
	}{
		{
			name:    "mysql writes the character set and collation",
			dialect: platform.MySQL,
			field:   schemamodel.Field{Name: "title", Type: "VARCHAR(80)", Charset: "utf8mb4", Collate: "utf8mb4_bin"},
			want:    "CHARACTER SET utf8mb4 COLLATE utf8mb4_bin",
		},
		{
			name:    "mysql writes the on update expression",
			dialect: platform.MySQL,
			field:   schemamodel.Field{Name: "seen", Type: "TIMESTAMP", UpdateExpression: "CURRENT_TIMESTAMP"},
			want:    "ON UPDATE CURRENT_TIMESTAMP",
		},
		{
			name:    "postgres writes a unique column",
			dialect: platform.Postgres,
			field:   schemamodel.Field{Name: "title", Type: "VARCHAR(80)", Unique: true},
			want:    "UNIQUE",
		},
		{
			name:    "sqlite writes a unique column",
			dialect: platform.SQLite,
			field:   schemamodel.Field{Name: "title", Type: "VARCHAR(80)", Unique: true},
			want:    "UNIQUE",
		},
		{
			name:    "sql server writes a unique column",
			dialect: platform.SQLServer,
			field:   schemamodel.Field{Name: "title", Type: "VARCHAR(80)", Unique: true},
			want:    "UNIQUE",
		},
		{
			name:    "oracle writes a unique column",
			dialect: platform.Oracle,
			field:   schemamodel.Field{Name: "title", Type: "VARCHAR(80)", Unique: true},
			want:    "UNIQUE",
		},
		{
			name:    "mysql writes a generated key",
			dialect: platform.MySQL,
			field:   schemamodel.Field{Name: "n", Type: "INT", AutoInc: true},
			want:    "AUTO_INCREMENT",
		},
		{
			name:    "sql server writes a generated key",
			dialect: platform.SQLServer,
			field:   schemamodel.Field{Name: "n", Type: "INT", AutoInc: true},
			want:    "IDENTITY",
		},
		{
			name:    "oracle writes a generated key",
			dialect: platform.Oracle,
			field:   schemamodel.Field{Name: "n", Type: "INT", AutoInc: true},
			want:    "GENERATED BY DEFAULT AS IDENTITY",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, omissions, err := renderer.GetOrderedCreateStatementsReportingOmissions(
				columnSchema(test.field), test.dialect, capability.ForDialect(test.dialect))

			c.Assert(err, qt.IsNil)
			c.Assert(omissions, qt.HasLen, 0)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, test.want)
		})
	}
}

// TestGetOrderedCreateStatementsReportingOmissions_AnOrdinaryColumnReportsNothing
// keeps the report off a schema that declared none of this.
//
// Without it, every table on six targets would carry findings, and a check that
// fires on every schema is one nobody reads.
func TestGetOrderedCreateStatementsReportingOmissions_AnOrdinaryColumnReportsNothing(t *testing.T) {
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
		{name: "clickhouse", dialect: platform.ClickHouse},
	}

	database := columnSchema(schemamodel.Field{Name: "title", Type: "VARCHAR(80)", Nullable: true})

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(lostProperties(c, database, test.dialect), qt.HasLen, 0)
		})
	}
}
