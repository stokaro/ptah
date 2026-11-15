package atlasschema

// White-box testing required: the rehearsal scope check and the refusal of an
// unqualified table move decide on a connection's description and a
// comparison result, and the only exported path to either opens two
// databases. The engine tests under integration/ drive that path; these name
// the decisions without a server.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/difftypes"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/planrefusal"
)

// realmInfo describes a PostgreSQL connection whose URL adds query and whose
// session resolved to schema.
func realmInfo(query, schema string) catalog.ServerInfo {
	return catalog.ServerInfo{
		Dialect: "postgres",
		URL:     "postgres://rebase@db.example/rebase" + query,
		Schema:  schema,
	}
}

// movedNotes is a comparison that creates notes from a declaration written
// with schema and drops notes from public.
func movedNotes(schema string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{Name: "notes", Table: schemamodel.Table{Name: "notes", Schema: schema}}},
		TablesRemoved: difftypes.TableRemovals{{
			Name:    "public.notes",
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "notes", Schema: "public"}},
		}},
	}
}

func TestCheckSimulationSchemaScope_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		dev    catalog.ServerInfo
		target catalog.ServerInfo
	}{
		{
			name:   "realm URLs whose current schemas differ",
			dev:    realmInfo("", "public"),
			target: realmInfo("", "rebase"),
		},
		{
			name:   "bound URLs naming one schema",
			dev:    realmInfo("?search_path=app", "app"),
			target: realmInfo("?search_path=app", "app"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkSimulationSchemaScope(test.dev, test.target), qt.IsNil)
		})
	}
}

func TestCheckSimulationSchemaScope_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		dev    catalog.ServerInfo
		target catalog.ServerInfo
		want   string
	}{
		{
			name:   "bound URLs naming different schemas",
			dev:    realmInfo("?search_path=public", "public"),
			target: realmInfo("?search_path=rebase", "rebase"),
			want:   `--dev-url schema scope "public" does not match --url schema scope "rebase"; point --dev-url at a dev database using the same schema`,
		},
		{
			name:   "a realm dev URL beside a bound target",
			dev:    realmInfo("", "public"),
			target: realmInfo("?search_path=rebase", "rebase"),
			want:   `--dev-url schema scope "public" does not match --url schema scope "rebase"; point --dev-url at a dev database using the same schema`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkSimulationSchemaScope(test.dev, test.target), qt.ErrorMatches, test.want)
		})
	}
}

func TestCheckNativeSimulationSchemaScope_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		dev    catalog.ServerInfo
		target catalog.ServerInfo
	}{
		{
			name:   "realm URLs whose current schemas agree",
			dev:    realmInfo("", "public"),
			target: realmInfo("", "public"),
		},
		{
			name:   "bound URLs naming one schema",
			dev:    realmInfo("?search_path=app", "app"),
			target: realmInfo("?search_path=app", "app"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkNativeSimulationSchemaScope(test.dev, test.target), qt.IsNil)
		})
	}
}

func TestCheckNativeSimulationSchemaScope_FailurePath(t *testing.T) {
	c := qt.New(t)

	err := checkNativeSimulationSchemaScope(realmInfo("", "public"), realmInfo("", "rebase"))

	c.Assert(err, qt.ErrorMatches, `--dev-url schema scope "public" does not match --url schema scope "rebase"; `+
		`a plan that holds native operations or approved inputs is rehearsed only where both resolve names alike, `+
		`so point --dev-url at a dev database using the same schema or set search_path on both URLs`)
}

func TestRefuseUnqualifiedTableMoves_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name string
		info catalog.ServerInfo
		diff *difftypes.SchemaDiff
	}{
		{
			name: "the declaration names the schema",
			info: realmInfo("", "rebase"),
			diff: movedNotes("public"),
		},
		{
			name: "the declaration names the current schema",
			info: realmInfo("", "rebase"),
			diff: movedNotes("rebase"),
		},
		{
			name: "the current schema is the default",
			info: realmInfo("", "public"),
			diff: movedNotes(""),
		},
		{
			name: "the URL pins the schema",
			info: realmInfo("?search_path=rebase", "rebase"),
			diff: movedNotes(""),
		},
		{
			name: "nothing of the name is dropped",
			info: realmInfo("", "rebase"),
			diff: &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableChanges{{Name: "notes", Table: schemamodel.Table{Name: "notes"}}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(refuseUnqualifiedTableMoves(test.info, test.diff), qt.IsNil)
		})
	}
}

func TestRefuseUnqualifiedTableMoves_FailurePath(t *testing.T) {
	c := qt.New(t)
	err := refuseUnqualifiedTableMoves(realmInfo("", "rebase"), movedNotes(""))
	c.Assert(err, qt.ErrorAs, new(*planrefusal.DeclarationError))
	c.Assert(err, qt.ErrorMatches, `the desired schema names table "notes" without a schema, which this connection `+
		`resolves to its current schema "rebase", and the plan would create it there and drop "public"."notes" `+
		`with its rows; write the table as "public"."notes", or pin search_path in the URL`)
}
