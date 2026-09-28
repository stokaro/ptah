package atlasschema

// White-box testing required: which databases a diff between two database
// reads compares is decided on the resolved states, and the exported entry
// point reaches them only by connecting to two whole MySQL-family servers
// that differ. CI runs one server per engine, so a server can only be
// compared with itself there, which is synced whether or not its databases
// are compared.

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/atlassource"
	"ptah.run/internal/convert/dbschematogo"
)

// serverState is a read of a whole MySQL server holding databases, each with
// one table named after it.
func serverState(databases ...string) atlassource.State {
	database := &catalog.Database{}
	for _, name := range databases {
		database.Schemas = append(database.Schemas, catalog.Schema{Name: name, Charset: "utf8mb4", Collate: "utf8mb4_0900_ai_ci"})
		database.Tables = append(database.Tables, catalog.Table{
			Name: "t_" + name, Schema: name,
			Columns: []catalog.Column{{Name: "id", DataType: "int", ColumnType: "int", IsNullable: "NO"}},
		})
	}
	return atlassource.State{
		Kind:        atlassource.KindDatabase,
		Schema:      dbschematogo.ConvertDBSchemaToGoSchema(database, platform.MySQL),
		DB:          database,
		RealmScoped: true,
		WholeServer: true,
	}
}

// TestDiffResolvedStates_ComparesTheDatabasesOfTwoServers plans the databases
// one whole server holds and the other does not, as the pinned community
// binary v1.3.0 plans them between two servers, measured on MySQL 8.4.11:
// `DROP DATABASE r3` and a creation of r9 (stokaro/ptah#3789).
func TestDiffResolvedStates_ComparesTheDatabasesOfTwoServers(t *testing.T) {
	c := qt.New(t)

	report, _, err := diffResolvedStates(t.Context(), nil, serverState("r1", "r3"), serverState("r1", "r9"),
		platform.MySQL, nil, devServerSides{}, DiffOptions{})

	c.Assert(err, qt.IsNil)
	statements := make([]string, 0, len(report.Changes))
	for _, change := range report.Changes {
		statements = append(statements, change.Cmd)
	}
	plan := strings.Join(statements, "\n")
	c.Assert(plan, qt.Contains, "CREATE SCHEMA IF NOT EXISTS `r9`")
	c.Assert(plan, qt.Contains, "DROP DATABASE `r3`")
}

// TestDiffResolvedStates_RefusesAServerBesideOneDatabase refuses a whole
// server compared with one database, in either order, with the message the
// pinned community binary v1.3.0 prints, measured on MySQL 8.4.11.
func TestDiffResolvedStates_RefusesAServerBesideOneDatabase(t *testing.T) {
	database := serverState("r1")
	database.WholeServer = false
	database.RealmScoped = false
	database.DefaultSchema = "r1"
	tests := []struct {
		name     string
		from, to atlassource.State
		wantErr  string
	}{
		{name: "from the server", from: serverState("r1"), to: database, wantErr: `cannot diff a schema "r1" with a database connection`},
		{name: "to the server", from: database, to: serverState("r1"), wantErr: `cannot diff a database connection with a schema "r1"`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			report, diff, err := diffResolvedStates(t.Context(), nil, test.from, test.to, platform.MySQL, nil, devServerSides{}, DiffOptions{})

			var mismatch *ServerScopeMismatchError
			c.Assert(err, qt.ErrorAs, &mismatch)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(report.Changes, qt.IsNil)
			c.Assert(diff, qt.IsNil)
		})
	}
}
