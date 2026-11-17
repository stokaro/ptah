package schemaprep_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/definedtypetest"
	"ptah.run/internal/schemaprep"
)

// unplacedDatabase writes every kind PlaceUnqualified places once without a
// schema and once in schema "other", which it leaves as written.
func unplacedDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:            []schemamodel.Table{{Name: "notes"}, {Name: "kept", Schema: "other"}},
		Sequences:         []schemamodel.Sequence{{Name: "s"}, {Name: "s", Schema: "other"}},
		Enums:             []schemamodel.Enum{{Name: "mood"}, {Name: "shade", Schema: "other"}},
		Functions:         []schemamodel.Function{{Name: "f"}, {Name: "other.g"}},
		Views:             []schemamodel.View{{Name: "v"}, {Name: "other.w"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "mv"}, {Name: "other.mw"}},
		Fields: []schemamodel.Field{
			{StructName: "notes", Name: "m", Type: "mood"},
			{StructName: "notes", Name: "ms", Type: "mood[]"},
			{StructName: "notes", Name: "parent", Type: "bigint", Foreign: "notes(id)"},
			{StructName: "notes", Name: "away", Type: "bigint", Foreign: "other.kept(id)"},
		},
		Indexes: []schemamodel.Index{
			{Name: "notes_m", StructName: "notes", TableName: "notes"},
			{Name: "kept_x", StructName: "kept", TableName: "other.kept"},
		},
		Constraints: []schemamodel.Constraint{
			{Name: "fk", Table: "notes", ForeignTable: "notes"},
			{Name: "away", Table: "notes", ForeignTable: "other.kept"},
		},
		Triggers:         []schemamodel.Trigger{{Name: "t", Table: "notes", ExecuteFunction: "f"}},
		RLSPolicies:      []schemamodel.RLSPolicy{{Name: "p", Table: "notes"}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "notes"}},
		Grants:           []schemamodel.Grant{{OnTable: "notes"}, {OnSequence: "s"}, {OnRoutine: "f"}},
		RevokedGrants:    []schemamodel.Grant{{OnTable: "other.kept"}},
		ManagedData:      []schemamodel.ManagedData{{Table: "notes"}, {Table: "kept", Schema: "other"}},
		Dependencies:     map[string][]string{"notes": {"notes", "other.kept"}},
		FunctionDependencies: map[string][]string{
			"f": {"notes"},
		},
		SelfReferencingForeignKeys: map[string][]schemamodel.SelfReferencingFK{
			"notes":      {{FieldName: "parent", Foreign: "notes(id)"}},
			"other.kept": {{FieldName: "up", Foreign: "other.kept(id)"}},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(
			definedtypetest.Object(objectidentity.KindDomain, "", "amount", "numeric"),
			definedtypetest.Object(objectidentity.KindDomain, "other", "code", "text"),
		)),
	}
}

// placedDatabase is unplacedDatabase with every name it writes without a
// schema placed in app.
func placedDatabase() *schemamodel.Database {
	return &schemamodel.Database{
		Tables:            []schemamodel.Table{{Name: "notes", Schema: "app"}, {Name: "kept", Schema: "other"}},
		Sequences:         []schemamodel.Sequence{{Name: "s", Schema: "app"}, {Name: "s", Schema: "other"}},
		Enums:             []schemamodel.Enum{{Name: "app.mood"}, {Name: "shade", Schema: "other"}},
		Functions:         []schemamodel.Function{{Name: "app.f"}, {Name: "other.g"}},
		Views:             []schemamodel.View{{Name: "app.v"}, {Name: "other.w"}},
		MaterializedViews: []schemamodel.MaterializedView{{Name: "app.mv"}, {Name: "other.mw"}},
		Fields: []schemamodel.Field{
			{StructName: "notes", Name: "m", Type: "app.mood"},
			{StructName: "notes", Name: "ms", Type: "app.mood[]"},
			{StructName: "notes", Name: "parent", Type: "bigint", Foreign: "app.notes(id)"},
			{StructName: "notes", Name: "away", Type: "bigint", Foreign: "other.kept(id)"},
		},
		Indexes: []schemamodel.Index{
			{Name: "notes_m", StructName: "notes", TableName: "app.notes"},
			{Name: "kept_x", StructName: "kept", TableName: "other.kept"},
		},
		Constraints: []schemamodel.Constraint{
			{Name: "fk", Table: "app.notes", ForeignTable: "app.notes"},
			{Name: "away", Table: "app.notes", ForeignTable: "other.kept"},
		},
		Triggers:         []schemamodel.Trigger{{Name: "t", Table: "app.notes", ExecuteFunction: "app.f"}},
		RLSPolicies:      []schemamodel.RLSPolicy{{Name: "p", Table: "app.notes"}},
		RLSEnabledTables: []schemamodel.RLSEnabledTable{{Table: "app.notes"}},
		Grants:           []schemamodel.Grant{{OnTable: "app.notes"}, {OnSequence: "app.s"}, {OnRoutine: "app.f"}},
		RevokedGrants:    []schemamodel.Grant{{OnTable: "other.kept"}},
		ManagedData:      []schemamodel.ManagedData{{Table: "notes", Schema: "app"}, {Table: "kept", Schema: "other"}},
		Dependencies:     map[string][]string{"app.notes": {"app.notes", "other.kept"}},
		FunctionDependencies: map[string][]string{
			"app.f": {"app.notes"},
		},
		SelfReferencingForeignKeys: map[string][]schemamodel.SelfReferencingFK{
			"app.notes":  {{FieldName: "parent", Foreign: "app.notes(id)"}},
			"other.kept": {{FieldName: "up", Foreign: "other.kept(id)"}},
		},
	}
}

// TestPlaceUnqualified_HappyPath pins every name a source writes without a
// schema and every one it writes with one: the first takes the schema, the
// second stays as written.
func TestPlaceUnqualified_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := unplacedDatabase()

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	want := placedDatabase()
	got := *placed
	got.FeatureObjects = schemaext.Objects{}
	c.Assert(&got, qt.DeepEquals, want)
	c.Assert(database, qt.DeepEquals, unplacedDatabase())
}

// TestPlaceUnqualified_FeatureObjects pins the identity of a feature object
// whose schema the source left to the default: it names the schema, no longer
// defaulted, so the comparison binds it nowhere else, and a column of its type
// names the schema too.
func TestPlaceUnqualified_FeatureObjects(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		FeatureObjects: must.Must(schemaext.NewObjects(
			definedtypetest.Object(objectidentity.KindDomain, "", "amount", "numeric"),
			definedtypetest.Object(objectidentity.KindDomain, "other", "code", "text"),
		)),
		Fields: []schemamodel.Field{{Name: "a", Type: "amount"}, {Name: "c", Type: "code"}},
	}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(schemaext.DefinedTypes(placed.FeatureObjects), qt.HasLen, 2)
	amount := definedNamed(c, placed.FeatureObjects, "amount")
	c.Assert(amount.Schema.Source, qt.Equals, "app")
	c.Assert(amount.Schema.Defaulted, qt.IsFalse)
	c.Assert(definedNamed(c, placed.FeatureObjects, "code").Schema.Source, qt.Equals, "other")
	c.Assert(placed.Fields[0].Type, qt.Equals, "app.amount")
	c.Assert(placed.Fields[1].Type, qt.Equals, "other.code")
	c.Assert(definedNamed(c, database.FeatureObjects, "amount").Schema.Defaulted, qt.IsTrue)
}

// definedNamed is the identity of the type objects defines under name.
func definedNamed(c *qt.C, objects schemaext.Objects, name string) objectidentity.ID {
	c.Helper()
	defined := schemaext.DefinedTypes(objects)
	at := slices.IndexFunc(defined, func(d schemaext.DefinedType) bool { return d.ID.Name.Source == name })
	c.Assert(at, qt.Not(qt.Equals), -1, qt.Commentf("no type named %q", name))
	return defined[at].ID
}

// TestPlaceUnqualified_NothingToPlace pins the inputs PlaceUnqualified returns
// as they are.
func TestPlaceUnqualified_NothingToPlace(t *testing.T) {
	for _, test := range []struct {
		name     string
		database *schemamodel.Database
		schema   string
	}{
		{name: "no schema", database: unplacedDatabase(), schema: " "},
		{name: "no database", schema: "app"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			placed, err := schemaprep.PlaceUnqualified(test.database, test.schema, platform.Postgres)
			c.Assert(err, qt.IsNil)
			c.Assert(placed, qt.Equals, test.database)
		})
	}
}

// TestPlaceUnqualified_AmbiguousEnumType pins a column type two enums of one
// name could mean: it stays as written, as QualifyDeclaredUserTypes leaves it.
func TestPlaceUnqualified_AmbiguousEnumType(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Enums:  []schemamodel.Enum{{Name: "mood"}, {Name: "mood"}},
		Fields: []schemamodel.Field{{Name: "m", Type: "mood"}},
	}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(placed.Fields[0].Type, qt.Equals, "mood")
}

func TestPlaceUnqualified_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{FeatureObjects: must.Must(schemaext.NewObjects(
		definedtypetest.Object(objectidentity.KindDomain, "", "amount", "numeric"),
		definedtypetest.Object(objectidentity.KindDomain, "app", "amount", "numeric"),
	))}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.ErrorMatches, `place feature objects written without a schema in "app": .*`)
	c.Assert(placed, qt.IsNil)
}

// A reference to an object the database does not declare is resolved where
// the session finds it, so placement leaves it as written: a built-in trigger
// function lives in pg_catalog, and a table another tool manages lives where
// that tool put it. Placed, the trigger would name a function the placed
// schema does not hold and the rehearsal would fail with SQLSTATE 42883.
func TestPlaceUnqualified_LeavesAnUndeclaredReferenceAsWritten(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "docs"}},
		Fields: []schemamodel.Field{
			{StructName: "docs", Name: "owner", Type: "bigint", Foreign: "accounts(id)"},
		},
		Indexes:     []schemamodel.Index{{Name: "accounts_x", StructName: "accounts", TableName: "accounts"}},
		Constraints: []schemamodel.Constraint{{Name: "fk", Table: "docs", ForeignTable: "accounts"}},
		Triggers: []schemamodel.Trigger{
			{Name: "nored", Table: "docs", ExecuteFunction: "suppress_redundant_updates_trigger"},
		},
		RLSPolicies: []schemamodel.RLSPolicy{{Name: "p", Table: "accounts"}},
		Grants: []schemamodel.Grant{
			{OnTable: "accounts"}, {OnSequence: "accounts_id_seq"}, {OnRoutine: "lower(text)"},
		},
		ManagedData:          []schemamodel.ManagedData{{Table: "accounts"}},
		Dependencies:         map[string][]string{"docs": {"accounts"}},
		FunctionDependencies: map[string][]string{"f": {"lower"}},
	}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(placed.Tables[0].Schema, qt.Equals, "app")
	c.Assert(placed.Fields[0].Foreign, qt.Equals, "accounts(id)")
	c.Assert(placed.Indexes[0].TableName, qt.Equals, "accounts")
	c.Assert(placed.Constraints[0].Table, qt.Equals, "app.docs")
	c.Assert(placed.Constraints[0].ForeignTable, qt.Equals, "accounts")
	c.Assert(placed.Triggers[0].Table, qt.Equals, "app.docs")
	c.Assert(placed.Triggers[0].ExecuteFunction, qt.Equals, "suppress_redundant_updates_trigger")
	c.Assert(placed.RLSPolicies[0].Table, qt.Equals, "accounts")
	c.Assert(placed.Grants, qt.DeepEquals, database.Grants)
	c.Assert(placed.ManagedData[0].Schema, qt.Equals, "")
	c.Assert(placed.Dependencies, qt.DeepEquals, map[string][]string{"app.docs": {"accounts"}})
	c.Assert(placed.FunctionDependencies, qt.DeepEquals, map[string][]string{"f": {"lower"}})
}

// A serial or identity column creates its sequence beside its table, so a
// grant on that sequence is placed with the table. A grant on a sequence no
// declared column creates keeps its name.
func TestPlaceUnqualified_PlacesTheSequenceOfADeclaredColumn(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts"}, {StructName: "Item", Name: "items"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "SERIAL"},
			{StructName: "Account", Name: "code", Type: "bigint"},
			{StructName: "Item", Name: "id", Type: "bigint", IdentityGeneration: "ALWAYS"},
		},
		Grants: []schemamodel.Grant{
			{OnSequence: "accounts_id_seq"}, {OnSequence: "items_id_seq"},
			{OnSequence: "accounts_code_seq"}, {OnSequence: "other_id_seq"},
		},
	}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(placed.Grants, qt.DeepEquals, []schemamodel.Grant{
		{OnSequence: "app.accounts_id_seq"}, {OnSequence: "app.items_id_seq"},
		{OnSequence: "accounts_code_seq"}, {OnSequence: "other_id_seq"},
	})
}

// A grant keeps its name on a sequence no placed column is known to create:
// the one a column of another table would create under this table's name, the
// one of a table declared in a schema of its own, and the one an identity
// column names in its options.
func TestPlaceUnqualified_LeavesASequenceNoPlacedColumnIsKnownToCreate(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Account", Name: "accounts"},
			{StructName: "Bin", Name: "bins"},
			{StructName: "Ledger", Name: "ledgers", Schema: "billing"},
			{StructName: "Note", Name: "notes"},
		},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "id", Type: "SERIAL"},
			{StructName: "Bin", Name: "label", Type: "text"},
			{StructName: "Ledger", Name: "id", Type: "SERIAL"},
			{StructName: "Note", Name: "id", Type: "bigint", IdentityGeneration: "BY DEFAULT", IdentityOptions: "START WITH 10 sequence  name note_ids"},
		},
		Grants: []schemamodel.Grant{
			{OnSequence: "bins_id_seq"}, {OnSequence: "ledgers_id_seq"}, {OnSequence: "notes_id_seq"},
		},
	}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(placed.Grants, qt.DeepEquals, []schemamodel.Grant{
		{OnSequence: "bins_id_seq"}, {OnSequence: "ledgers_id_seq"}, {OnSequence: "notes_id_seq"},
	})
}

// A declared routine a grant names with its argument types is placed with
// them, and the trigger running it names it in the placed schema.
func TestPlaceUnqualified_PlacesADeclaredRoutineWithItsArguments(t *testing.T) {
	c := qt.New(t)
	database := &schemamodel.Database{
		Functions: []schemamodel.Function{{Name: "touch"}},
		Tables:    []schemamodel.Table{{Name: "docs"}},
		Triggers:  []schemamodel.Trigger{{Name: "t", Table: "docs", ExecuteFunction: "touch"}},
		Grants:    []schemamodel.Grant{{OnRoutine: "touch(integer)"}},
	}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(placed.Triggers[0].Table, qt.Equals, "app.docs")
	c.Assert(placed.Triggers[0].ExecuteFunction, qt.Equals, "app.touch")
	c.Assert(placed.Grants[0].OnRoutine, qt.Equals, "app.touch(integer)")
}

// A partition of a declared table names its parent in the placed schema, so
// its CREATE TABLE ... PARTITION OF names the schema the parent is created in.
// A parent the database does not declare, and one written with a schema, keep
// the name they were written with, and the database placed from is left as it
// was.
func TestPlaceUnqualified_PlacesTheParentOfAPartition(t *testing.T) {
	c := qt.New(t)
	member := func(parentSchema, parent string) schemaext.Facets {
		return must.Must(schemaext.NewFacets(&pgtable.DesiredPartitionMember{PartitionMember: pgtable.PartitionMember{
			ParentSchema: parentSchema, Parent: parent, Bound: "FOR VALUES FROM (1) TO (2)",
		}}))
	}
	database := &schemamodel.Database{Tables: []schemamodel.Table{
		{Name: "events"},
		{Name: "events_1", Facets: member("", "events")},
		{Name: "managed_1", Facets: member("", "managed")},
		{Name: "away_1", Facets: member("other", "events")},
	}}

	placed, err := schemaprep.PlaceUnqualified(database, "app", platform.Postgres)

	c.Assert(err, qt.IsNil)
	c.Assert(partitionParents(c, placed.Tables[1:]), qt.DeepEquals, []string{"app.events", ".managed", "other.events"})
	c.Assert(partitionParents(c, database.Tables[1:]), qt.DeepEquals, []string{".events", ".managed", "other.events"})
}

// partitionParents is schema.name of the parent each of tables names as a
// partition.
func partitionParents(c *qt.C, tables []schemamodel.Table) []string {
	c.Helper()
	parents := make([]string, 0, len(tables))
	for _, table := range tables {
		value, ok, err := table.Facets.Get(pgtable.PartitionMemberKind)
		c.Assert(err, qt.IsNil)
		c.Assert(ok, qt.IsTrue, qt.Commentf("table %s", table.Name))
		schema, name := value.(schemaext.TableParent).ParentTable()
		parents = append(parents, schema+"."+name)
	}
	return parents
}
