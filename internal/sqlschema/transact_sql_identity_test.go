package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/internal/sqlschema"
)

// sqlServerIdentity is the part of a field these tests compare.
type sqlServerIdentity struct {
	Name      string
	AutoInc   bool
	Start     string
	Increment string
	Nullable  bool
}

// TestRead_TransactSQLIdentity_HappyPath reads the identity columns `ptah db
// read` prints for a SQL Server database into the model the catalog reader
// gives them: an identity column with its seed and increment, NOT NULL whether
// or not the definition says so, beside an ordinary nullable column.
func TestRead_TransactSQLIdentity_HappyPath(t *testing.T) {
	c := qt.New(t)
	const document = "CREATE TABLE [t] (\n  [id] BIGINT IDENTITY(100,5),\n  [a] INT\n);\n" +
		"CREATE TABLE [u] (\n  [id] INT IDENTITY PRIMARY KEY\n);\n"

	database, _, err := sqlschema.Read([]byte(document), platform.SQLServer)

	c.Assert(err, qt.IsNil)
	got := make([]sqlServerIdentity, 0, len(database.Fields))
	for _, field := range database.Fields {
		got = append(got, sqlServerIdentity{
			Name: field.StructName + "." + field.Name, AutoInc: field.AutoInc,
			Start: field.IdentityStart, Increment: field.IdentityIncrement, Nullable: field.Nullable,
		})
	}
	c.Assert(got, qt.DeepEquals, []sqlServerIdentity{
		{Name: "T.id", AutoInc: true, Start: "100", Increment: "5"},
		{Name: "T.a", Nullable: true},
		{Name: "U.id", AutoInc: true},
	})
}
