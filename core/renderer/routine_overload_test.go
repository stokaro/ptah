package renderer_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
	"ptah.run/core/schemamodel"
)

// overloadedRoutines declares two routines of one kind and one name, each with
// its own argument list.
func overloadedRoutines(kind string) *schemamodel.Database {
	return &schemamodel.Database{Functions: []schemamodel.Function{
		{Name: "f", Kind: kind, Parameters: "a INT", Returns: "INT", Language: "sql", Body: "RETURN 1"},
		{Name: "f", Kind: kind, Parameters: "a INT, b INT", Returns: "INT", Language: "sql", Body: "RETURN 2"},
	}}
}

// The model keeps two overloads, because PostgreSQL tells them apart
// (stokaro/ptah#3672). A target without routine overloading cannot hold both.
// Applied as Ptah writes them, the second CREATE is Error 1304 on MySQL 8.4.11
// and MariaDB 11.8.9, and on SQL Server 2022 the second CREATE OR ALTER
// replaces the first without an error. Oracle is not measured for this case.
// So the document is refused before a statement is planned, and the message
// says what the target would do.
func TestValidateSchema_ATargetWithoutOverloadsRefusesTwoArgumentLists(t *testing.T) {
	tests := []struct {
		name        string
		dialect     string
		caps        capability.Capabilities
		kind        string
		consequence string
	}{
		{name: "MySQL functions", dialect: platform.MySQL, caps: capability.MySQL84(), kind: "", consequence: "the second CREATE fails with Error 1304"},
		{name: "MySQL procedures", dialect: platform.MySQL, caps: capability.MySQL84(), kind: "procedure", consequence: "the second CREATE fails with Error 1304"},
		{name: "MariaDB functions", dialect: platform.MariaDB, caps: capability.MariaDB1011(), kind: "", consequence: "the second CREATE fails with Error 1304"},
		{
			name: "SQL Server functions", dialect: platform.SQLServer, caps: capability.ForDialect(platform.SQLServer), kind: "",
			consequence: "Ptah writes CREATE OR ALTER, so the second replaces the first without an error",
		},
		{
			name: "SQL Server procedures", dialect: platform.SQLServer, caps: capability.ForDialect(platform.SQLServer), kind: "procedure",
			consequence: "Ptah writes CREATE OR ALTER, so the second replaces the first without an error",
		},
		{name: "Oracle functions", dialect: platform.Oracle, caps: capability.Oracle23(), kind: "", consequence: "the second takes the place of the first"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := renderer.ValidateSchemaWithCapabilities(overloadedRoutines(test.kind), test.dialect, test.caps)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `(?s).*is declared twice, as f\(a INT\) and f\(a INT, b INT\), and `+
				test.dialect+` has no routine overloading.*`+test.consequence+`.*`)
		})
	}
}

// The controls. PostgreSQL hosts both overloads, a repeated declaration of one
// overload is one routine, on MySQL a function and a procedure of one name
// live in two namespaces, and on SQL Server two names that differ in case are
// left to the database's collation.
func TestValidateSchema_OverloadsATargetCanHold(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		database *schemamodel.Database
	}{
		{name: "PostgreSQL overloads", dialect: platform.Postgres, caps: capability.ForDialect(platform.Postgres), database: overloadedRoutines("")},
		{
			name: "one overload declared twice", dialect: platform.MySQL, caps: capability.MySQL84(),
			database: &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "f", Parameters: "a INT", Returns: "INT", Language: "sql", Body: "RETURN 1"},
				{Name: "f", Parameters: "b INTEGER", Returns: "INT", Language: "sql", Body: "RETURN 1"},
			}},
		},
		{
			name: "a function and a procedure", dialect: platform.MySQL, caps: capability.MySQL84(),
			database: &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "f", Parameters: "a INT", Returns: "INT", Language: "sql", Body: "RETURN 1"},
				{Name: "f", Kind: "procedure", Parameters: "a INT, b INT", Language: "sql", Body: "SELECT 1"},
			}},
		},
		{
			name: "two spellings on SQL Server", dialect: platform.SQLServer, caps: capability.ForDialect(platform.SQLServer),
			database: &schemamodel.Database{Functions: []schemamodel.Function{
				{Name: "f", Parameters: "@a INT", Returns: "INT", Language: "sql", Body: "BEGIN RETURN @a END"},
				{Name: "F", Parameters: "@a INT, @b INT", Returns: "INT", Language: "sql", Body: "BEGIN RETURN @a + @b END"},
			}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(renderer.ValidateSchemaWithCapabilities(test.database, test.dialect, test.caps), qt.IsNil)
		})
	}
}
