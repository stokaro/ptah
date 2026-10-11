package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgtype"
)

// These tests build the type owner's models themselves, rather than read them
// from a source, and drive them through the bundled runtime the commands use.

// usersWithEmail declares the domain app.email beside one table.
func usersWithEmail() *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "User", Name: "users"}},
		Fields: []schemamodel.Field{{StructName: "User", Name: "id", Type: "INTEGER", Primary: true}},
		FeatureObjects: must.Must(schemaext.NewObjects(must.Must(pgtype.DesiredObject(pgtype.Ref(pgtype.DomainKind, "app", "email"),
			&pgtype.DesiredDomain{BaseType: "text", Check: "VALUE <> ''", Comment: "mail"})))),
		FeatureCoverage: must.Must(pgtype.CompleteCoverage(schemaext.Desired)),
	}
	schemamodel.Finalize(database)
	return database
}

// TestPostgresRendersADeclaredType pins that the bundled runtime renders the
// type owner's declaration on the targets that host it: the domain, its
// comment and the table.
func TestPostgresRendersADeclaredType(t *testing.T) {
	for _, target := range []string{platform.Postgres, platform.YugabyteDB} {
		t.Run(target, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(usersWithEmail(), target)

			c.Assert(err, qt.IsNil)
			rendered := strings.Join(statements, "\n")
			c.Assert(rendered, qt.Contains, `CREATE DOMAIN "app"."email" AS text CHECK (VALUE <> '');`)
			c.Assert(rendered, qt.Contains, `COMMENT ON DOMAIN "app"."email" IS 'mail';`)
			c.Assert(rendered, qt.Contains, "users")
		})
	}
}

// TestDeclaredTypeRefusals pins where a declared type cannot go: a target of
// the family that does not create the kind refuses it with the scope advice of
// stokaro/ptah#1717, and a target outside the family refuses it by the owner's
// off-target policy, naming the targets that host the model.
func TestDeclaredTypeRefusals(t *testing.T) {
	scope := `PostgreSQL domain app.email cannot be planned on %s; scope its declaration to the targets that host it, ` +
		`as dialects="cockroachdb,postgres,spanner,yugabytedb" does in a Go annotation`
	tests := []struct {
		name   string
		target string
		want   string
	}{
		{name: "Spanner", target: platform.Spanner, want: `(?s).*spanner: CREATE DOMAIN email: this target does not create it, and a column ` +
			`declared with the type would be left naming something the server has no definition of; scope the declaration to the dialects that host it.*`},
		{name: "MySQL", target: platform.MySQL, want: strings.Replace(scope, "%s", "mysql", 1)},
		{name: "SQLite", target: platform.SQLite, want: strings.Replace(scope, "%s", "sqlite", 1)},
		{name: "SQL Server", target: platform.SQLServer, want: strings.Replace(scope, "%s", "sqlserver", 1)},
		{name: "YDB", target: platform.YDB, want: strings.Replace(scope, "%s", "ydb", 1)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(usersWithEmail(), test.target)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}
