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

// movedNotes is a comparison that creates notes in created and drops the
// notes the target read in dropped, empty for the connection's current schema.
func movedNotes(created, dropped string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		TablesAdded: difftypes.TableChanges{{Name: "notes", Table: schemamodel.Table{Name: "notes", Schema: created}}},
		TablesRemoved: difftypes.TableRemovals{{
			Name:    "notes",
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "notes", Schema: dropped}},
		}},
	}
}

// unqualifiedNotes is a desired state that wrote the table notes and the
// sequence s1 without a schema.
var unqualifiedNotes = unqualifiedNames{tables: map[string]bool{"notes": true}, sequences: map[string]bool{"s1": true}}

// movedSequence is a diff that creates sequence s1 in created and drops the
// one in dropped.
func movedSequence(created, dropped string) *difftypes.SchemaDiff {
	return &difftypes.SchemaDiff{
		SequencesAdded:   difftypes.SequenceChanges{{Name: "s1", Schema: created}},
		SequencesRemoved: difftypes.SequenceChanges{{Name: "s1", Schema: dropped}},
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

func TestNamingFor_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name   string
		target catalog.ServerInfo
		dev    *catalog.ServerInfo
		want   desiredNaming
	}{
		{
			name:   "no dev database, the current schema is the default",
			target: realmInfo("", "public"),
			want:   desiredNaming{resolved: true, schema: "public"},
		},
		{
			name:   "no dev database, another current schema",
			target: realmInfo("", "rebase"),
			want:   desiredNaming{resolved: true, schema: "rebase", other: "public"},
		},
		{
			name:   "a dev database that resolves alike",
			target: realmInfo("", "public"),
			dev:    new(realmInfo("", "public")),
			want:   desiredNaming{resolved: true, schema: "public", fromDev: true},
		},
		{
			name:   "a dev database that resolves apart",
			target: realmInfo("", "rebase"),
			dev:    new(realmInfo("", "public")),
			want:   desiredNaming{resolved: true, schema: "public", other: "rebase", fromDev: true},
		},
		{
			name:   "a dev database that pins its schema",
			target: realmInfo("", "rebase"),
			dev:    new(realmInfo("?search_path=rebase", "rebase")),
			want:   desiredNaming{resolved: true, schema: "rebase"},
		},
		{
			name:   "a target that pins its schema",
			target: realmInfo("?search_path=rebase", "rebase"),
			dev:    new(realmInfo("", "public")),
			want:   desiredNaming{resolved: true},
		},
		{
			name:   "an engine that resolves no search path",
			target: catalog.ServerInfo{Dialect: "mysql", URL: "mysql://root@db.example/app", Schema: "app"},
			want:   desiredNaming{resolved: true},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(namingFor(test.target, test.dev), qt.Equals, test.want)
		})
	}
}

func TestCheckDevNaming_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		planned desiredNaming
		dev     catalog.ServerInfo
	}{
		{
			name:    "the dev database resolves where the plan placed",
			planned: namingFor(realmInfo("", "rebase"), new(realmInfo("", "public"))),
			dev:     realmInfo("", "public"),
		},
		{
			name:    "the plan read no dev database",
			planned: namingFor(realmInfo("", "rebase"), nil),
			dev:     realmInfo("", "public"),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(checkDevNaming(test.planned, realmInfo("", "rebase"), test.dev), qt.IsNil)
		})
	}
}

func TestCheckDevNaming_FailurePath(t *testing.T) {
	c := qt.New(t)
	planned := namingFor(realmInfo("", "rebase"), new(realmInfo("", "public")))

	err := checkDevNaming(planned, realmInfo("", "rebase"), realmInfo("", "app"))

	c.Assert(err, qt.ErrorMatches, `the plan places a name the desired schema writes without a schema in "public", `+
		`where the dev database resolved it, and the dev database now resolves it to "app"; plan again`)
}

func TestRefuseUnqualifiedMoves_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name        string
		target      catalog.ServerInfo
		dev         *catalog.ServerInfo
		unqualified unqualifiedNames
		diff        *difftypes.SchemaDiff
	}{
		{
			name:   "the declaration names the schema",
			target: realmInfo("", "rebase"),
			diff:   movedNotes("rebase", "public"),
		},
		{
			name:   "the declaration names the sequence's schema",
			target: realmInfo("", "rebase"),
			diff:   movedSequence("rebase", "public"),
		},
		{
			name:        "a sequence of the name is dropped in neither reading",
			target:      realmInfo("", "rebase"),
			dev:         new(realmInfo("", "public")),
			unqualified: unqualifiedNotes,
			diff:        movedSequence("public", "app"),
		},
		{
			name:        "the current schema is the default",
			target:      realmInfo("", "public"),
			unqualified: unqualifiedNotes,
			diff:        movedNotes("public", "app"),
		},
		{
			name:        "the URL pins the schema",
			target:      realmInfo("?search_path=rebase", "rebase"),
			unqualified: unqualifiedNotes,
			diff:        movedNotes("", "public"),
		},
		{
			name:        "a dev database that resolves alike",
			target:      realmInfo("", "public"),
			dev:         new(realmInfo("", "public")),
			unqualified: unqualifiedNotes,
			diff:        movedNotes("public", "app"),
		},
		{
			name:        "the dropped table is in neither reading",
			target:      realmInfo("", "rebase"),
			dev:         new(realmInfo("", "public")),
			unqualified: unqualifiedNotes,
			diff:        movedNotes("public", "app"),
		},
		{
			name:        "nothing of the name is dropped",
			target:      realmInfo("", "rebase"),
			unqualified: unqualifiedNotes,
			diff: &difftypes.SchemaDiff{
				TablesAdded: difftypes.TableChanges{{Name: "notes", Table: schemamodel.Table{Name: "notes", Schema: "rebase"}}},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			naming := namingFor(test.target, test.dev)
			c.Assert(refuseUnqualifiedMoves(test.target, naming, test.unqualified, test.diff), qt.IsNil)
		})
	}
}

func TestRefuseUnqualifiedMoves_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name string
		dev  *catalog.ServerInfo
		diff *difftypes.SchemaDiff
		want string
	}{
		{
			name: "no dev database, the default schema holds the table",
			diff: movedNotes("rebase", "public"),
			want: `the desired schema names table "notes" without a schema, which this connection ` +
				`resolves to its current schema "rebase", and the plan would create it there and drop "public"."notes" ` +
				`with its rows; write the table as "public"."notes", or pin search_path in the URL`,
		},
		{
			name: "a dev database that resolves apart, the current schema holds the table",
			dev:  new(realmInfo("", "public")),
			diff: movedNotes("public", ""),
			want: `the desired schema names table "notes" without a schema, which the dev database resolves to "public" ` +
				`and this connection to its current schema "rebase", and the plan would create it in "public" and drop ` +
				`"rebase"."notes" with its rows; write the table as "rebase"."notes" to keep it, or as "public"."notes" to move it`,
		},
		{
			name: "no dev database, the default schema holds the sequence",
			diff: movedSequence("rebase", "public"),
			want: `the desired schema names sequence "s1" without a schema, which this connection ` +
				`resolves to its current schema "rebase", and the plan would create it there and drop "public"."s1" ` +
				`with its current value; write the sequence as "public"."s1", or pin search_path in the URL`,
		},
		{
			name: "a dev database that resolves apart, the current schema holds the sequence",
			dev:  new(realmInfo("", "public")),
			diff: movedSequence("public", ""),
			want: `the desired schema names sequence "s1" without a schema, which the dev database resolves to "public" ` +
				`and this connection to its current schema "rebase", and the plan would create it in "public" and drop ` +
				`"rebase"."s1" with its current value; write the sequence as "rebase"."s1" to keep it, or as "public"."s1" to move it`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			target := realmInfo("", "rebase")
			err := refuseUnqualifiedMoves(target, namingFor(target, test.dev), unqualifiedNotes, test.diff)
			c.Assert(err, qt.ErrorAs, new(*planrefusal.DeclarationError))
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
