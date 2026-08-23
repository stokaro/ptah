package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/internal/schemachange"
	"go.5x5.cz/ptah/internal/schemastate"
)

// PostgreSQL, MySQL and MariaDB enforce a UNIQUE constraint with an index of the
// constraint's own name on the constraint's own table, and introspection reports
// that ONE object twice -- once in the index catalog, once in the constraint
// catalog. On MySQL and MariaDB there is not even a separate notion to report:
//
//	ALTER TABLE widget ADD CONSTRAINT uq_widget_code UNIQUE (code)
//	CREATE UNIQUE INDEX uq_widget_code ON widget (code)
//
// produce the same row (stokaro/ptah#1286).
//
// Reading both as objects made a desired state that declares the CONSTRAINT look
// like one that dropped the INDEX, and the plan came out as
// `DROP INDEX IF EXISTS "public"."uq_widget_code"` -- which drops the constraint
// with it.

func TestAConstraintsBackingIndexIsNotDropped(t *testing.T) {
	c := qt.New(t)

	changes := changesFor(c, widgetDeclaringUniqueConstraint(), widgetReportingBoth())

	c.Assert(changes, qt.HasLen, 0)
}

// TestAStandaloneUniqueIndexIsStillCompared is the control, and it is the case
// the suppression must not swallow.
//
// A schema that really does declare a unique INDEX has one, and the server
// creates no constraint beside it -- so nothing suppresses it, and a database
// missing it still gets one.
func TestAStandaloneUniqueIndexIsStillCompared(t *testing.T) {
	c := qt.New(t)
	description := describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	description.Indexes = append(description.Indexes, goschema.Index{
		StructName: "Widget", Name: "uq_widget_code", Fields: []string{"code"}, Unique: true,
	})

	changes := changesFor(c, description, widgetWithoutTheIndex())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Add)
}

// TestAnIndexThatBacksNothingIsStillDropped is the second control: the
// suppression must apply to a constraint's index and to nothing else, or an
// index the desired state really did remove would survive forever.
func TestAnIndexThatBacksNothingIsStillDropped(t *testing.T) {
	c := qt.New(t)
	catalog := widgetWithoutTheIndex()
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "idx_widget_code", TableName: "widget", Schema: "public",
		Columns: []string{"code"},
	}}

	changes := changesFor(c, widgetDeclaringNothing(), catalog)

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
}

// widgetDeclaringUniqueConstraint declares the guarantee as a CONSTRAINT and no
// index, which is what a schema author writes.
func widgetDeclaringUniqueConstraint() *goschema.Database {
	description := widgetDeclaringNothing()
	description.Constraints = append(description.Constraints, goschema.Constraint{
		StructName: "Widget", Name: "uq_widget_code", Type: "UNIQUE", Columns: []string{"code"},
	})
	return description
}

func widgetDeclaringNothing() *goschema.Database {
	return describedTable(
		goschema.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
}

// widgetReportingBoth is the catalog as every one of those engines reports it:
// the constraint AND the index it is enforced with, one object twice.
func widgetReportingBoth() *dbschematypes.DBSchema {
	catalog := widgetWithoutTheIndex()
	catalog.Constraints = []dbschematypes.DBConstraint{{
		Name: "uq_widget_code", TableName: "widget", Schema: "public",
		Type: "UNIQUE", ColumnNames: []string{"code"},
	}}
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "uq_widget_code", TableName: "widget", Schema: "public",
		Columns: []string{"code"}, IsUnique: true,
	}}
	return catalog
}

func widgetWithoutTheIndex() *dbschematypes.DBSchema {
	return catalogTable(
		dbschematypes.DBColumn{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"},
	)
}

// TestAPrimaryKeysBackingIndexIsNotDropped is the same fact for the index no
// statement addresses at all.
//
// A PRIMARY KEY's index is the constraint: PostgreSQL reports `widget_pkey` in
// pg_index beside the constraint of that name, and there is no DROP INDEX the
// server will run for it. It is answered a step earlier than the handover below,
// on the catalog ROW rather than between the two states, because unlike a UNIQUE
// constraint's index there is no spelling of the description that could claim
// it.
func TestAPrimaryKeysBackingIndexIsNotDropped(t *testing.T) {
	c := qt.New(t)
	description := describedTableWithKey([]string{"id"},
		goschema.Field{StructName: "Widget", Name: "id", Type: "int"},
		goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	catalog := widgetWithoutTheIndex()
	catalog.Constraints = []dbschematypes.DBConstraint{{
		Name: "widget_pkey", TableName: "widget", Schema: "public",
		Type: "PRIMARY KEY", ColumnNames: []string{"id"},
	}}
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "widget_pkey", TableName: "widget", Schema: "public",
		Columns: []string{"id"}, IsUnique: true, IsPrimary: true,
	}}

	changes := changesFor(c, description, catalog)

	c.Assert(changes, qt.HasLen, 0)
}

// TestSuppressionMatchesTheWholeIdentity pins what makes two rows one object.
//
// All three halves are load-bearing and none is visible from the rows above,
// where the constraint and the index agree on every one of them. An index that
// merely shares a table with a constraint, one that merely shares a NAME with a
// constraint on another table, and one that shares both with a constraint in
// another SCHEMA are each an index of their own, and are each still dropped.
//
// The last two are shapes the engines allow: an index name is unique per schema
// and a constraint name only per table, so one name can belong to gadget's
// constraint and widget's index at once; and two schemas can each hold a table
// called widget.
func TestSuppressionMatchesTheWholeIdentity(t *testing.T) {
	tests := []struct {
		name        string
		indexName   string
		indexTable  string
		indexSchema string
	}{
		{
			name:        "another name",
			indexName:   "idx_widget_code",
			indexTable:  "widget",
			indexSchema: "public",
		},
		{
			name:        "another table",
			indexName:   "uq_widget_code",
			indexTable:  "gadget",
			indexSchema: "public",
		},
		{
			name:        "another schema",
			indexName:   "uq_widget_code",
			indexTable:  "widget",
			indexSchema: "other",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, catalog := backingIdentityFixture(
				test.indexName, test.indexTable, test.indexSchema)

			changes := changesFor(c, description, catalog)

			c.Assert(changes, qt.HasLen, 1)
			c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
			c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
			c.Assert(changes[0].ID.Name.Source, qt.Equals, test.indexName)
		})
	}
}

// backingIdentityFixture builds one row of [TestSuppressionMatchesTheWholeIdentity]:
// a read reporting the UNIQUE constraint on public.widget and ONE index named and
// placed by the row, against a description that declares the constraint and every
// table the read reports -- and no index at all.
//
// The first row's index sits on the table that is already there; the other two
// name a table of their own, which both sides then have to carry.
func backingIdentityFixture(
	indexName, indexTable, indexSchema string,
) (*goschema.Database, *dbschematypes.DBSchema) {
	catalog := widgetWithoutTheIndex()
	catalog.Constraints = []dbschematypes.DBConstraint{{
		Name: "uq_widget_code", TableName: "widget", Schema: "public",
		Type: "UNIQUE", ColumnNames: []string{"code"},
	}}
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: indexName, TableName: indexTable, Schema: indexSchema,
		Columns: []string{"code"},
	}}
	description := widgetDeclaringUniqueConstraint()

	if indexTable == "widget" && indexSchema == "public" {
		return description, catalog
	}

	catalog.Tables = append(catalog.Tables, dbschematypes.DBTable{
		Name: indexTable, Schema: indexSchema,
		Columns: []dbschematypes.DBColumn{
			{Name: "code", DataType: "text", IsNullable: "YES"},
		},
	})
	description.Schemas = append(description.Schemas, goschema.Schema{Name: indexSchema})
	description.Tables = append(description.Tables, goschema.Table{
		StructName: "Other", Name: indexTable, Schema: indexSchema,
	})
	description.Fields = append(description.Fields, goschema.Field{
		StructName: "Other", Name: "code", Type: "text", Nullable: true,
	})
	return description, catalog
}

// TestAForeignKeysBackingIndexIsTheServersOnMySQL is the same fact for the
// index nobody asked for.
//
// MySQL and MariaDB create an index for every FOREIGN KEY, named after the
// constraint, so a schema written the ordinary way -- `CONSTRAINT ... FOREIGN
// KEY` and no `KEY` clause -- reads back with an index it never declared. The
// other targets create none, so an index that shares a foreign key's name there
// is an index somebody wrote, and dropping it is the whole point.
func TestAForeignKeysBackingIndexIsTheServersOnMySQL(t *testing.T) {
	tests := []struct {
		name    string
		profile schemastate.Profile
		schema  string
		dropped bool
	}{
		{
			name:    "MySQL creates it",
			profile: mysqlProfile(),
			dropped: false,
		},
		{
			name:    "MariaDB creates it",
			profile: mariadbProfile(),
			dropped: false,
		},
		{
			name:    "PostgreSQL creates none",
			profile: postgresProfile(),
			schema:  "public",
			dropped: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, catalog := foreignKeyBackingFixture(test.schema)

			changes := changesForProfile(c, description, catalog, test.profile)

			c.Assert(changes, qt.HasLen, droppedCount(test.dropped))
		})
	}
}

// TestSQLiteNamesTheBackingIndexItself pins the row no statement addresses.
//
// `CREATE TABLE t (a TEXT, CONSTRAINT uq UNIQUE(a))` leaves
// `sqlite_autoindex_t_1` in sqlite_master, under a name the schema never chose
// and DROP INDEX will not take. The ordinary index beside it in the same read is
// the control: it is dropped, so the row is not passing because the whole read
// was ignored.
func TestSQLiteNamesTheBackingIndexItself(t *testing.T) {
	c := qt.New(t)
	catalog := catalogTableInSchema("main",
		dbschematypes.DBColumn{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"},
	)
	catalog.Indexes = []dbschematypes.DBIndex{
		{
			Name: "sqlite_autoindex_widget_1", TableName: "widget", Schema: "main",
			Columns: []string{"code"}, IsUnique: true,
		},
		{
			Name: "idx_widget_code", TableName: "widget", Schema: "main",
			Columns: []string{"code"},
		},
	}

	changes := changesForProfile(c, widgetDeclaringNothing(), catalog, sqliteProfile())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
	c.Assert(changes[0].ID.Name.Source, qt.Equals, "idx_widget_code")
}

// TestSQLServerKeepsTheConstraintAndTheIndexApart is the exclusion's control.
//
// A UNIQUE constraint and a unique index are two objects there rather than one
// reported twice, so there is nothing to hand over: an index the description
// stopped declaring is dropped, and the constraint beside it is not what keeps
// it alive.
func TestSQLServerKeepsTheConstraintAndTheIndexApart(t *testing.T) {
	c := qt.New(t)

	// The read has to agree with the PROFILE about the default schema, which is
	// "dbo" here rather than PostgreSQL's "public" (stokaro/ptah#1662).
	catalog := catalogTableInSchema("dbo",
		dbschematypes.DBColumn{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		dbschematypes.DBColumn{Name: "code", DataType: "text", IsNullable: "YES"},
	)
	catalog.Constraints = []dbschematypes.DBConstraint{{
		Name: "uq_widget_code", TableName: "widget", Schema: "dbo",
		Type: "UNIQUE", ColumnNames: []string{"code"},
	}}
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "uq_widget_code", TableName: "widget", Schema: "dbo",
		Columns: []string{"code"}, IsUnique: true,
	}}

	changes := changesForProfile(c,
		widgetDeclaringUniqueConstraint(), catalog, sqlserverProfile())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
}

// foreignKeyBackingFixture is a child whose column references a parent through a
// named FOREIGN KEY, read back with an index of the constraint's name that the
// description never mentions.
func foreignKeyBackingFixture(schema string) (*goschema.Database, *dbschematypes.DBSchema) {
	description := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "Parent", Name: "parent", Schema: schema},
			{StructName: "Widget", Name: "widget", Schema: schema},
		},
		Fields: []goschema.Field{
			{StructName: "Parent", Name: "id", Type: "int", Primary: true},
			{StructName: "Widget", Name: "id", Type: "int", Primary: true},
			{
				StructName: "Widget", Name: "parent_id", Type: "int", Nullable: true,
				Foreign: "parent(id)", ForeignKeyName: "fk_widget_parent",
			},
		},
	}
	catalog := &dbschematypes.DBSchema{
		Tables: []dbschematypes.DBTable{
			{Name: "parent", Schema: schema, Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "widget", Schema: schema, Columns: []dbschematypes.DBColumn{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "int", IsNullable: "YES"},
			}},
		},
		Constraints: []dbschematypes.DBConstraint{{
			Name: "fk_widget_parent", TableName: "widget", Schema: schema,
			Type: "FOREIGN KEY", ColumnName: "parent_id",
			ForeignTable: pointerTo("parent"), ForeignColumn: pointerTo("id"),
		}},
		Indexes: []dbschematypes.DBIndex{{
			Name: "fk_widget_parent", TableName: "widget", Schema: schema,
			Columns: []string{"parent_id"},
		}},
	}
	return description, catalog
}

func droppedCount(dropped bool) int {
	if dropped {
		return 1
	}
	return 0
}

func pointerTo(value string) *string {
	return &value
}

// TestAPrimaryKeyIndexUnderAnotherNameIsStillTheServersPins the half of the
// primary key answer the name comparison cannot reach.
//
// Oracle takes `CONSTRAINT pk_widget PRIMARY KEY (id) USING INDEX (CREATE INDEX
// widget_key ON widget (id))`, so the row reported for the enforcing index does
// NOT carry the constraint's name -- and it is still the index the constraint
// is enforced with, still not droppable on its own. What marks it is the flag
// the read sets, not the name it shares.
func TestAPrimaryKeyIndexUnderAnotherNameIsStillTheServers(t *testing.T) {
	c := qt.New(t)
	description := describedTableWithKey([]string{"id"},
		goschema.Field{StructName: "Widget", Name: "id", Type: "int"},
		goschema.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	catalog := widgetWithoutTheIndex()
	catalog.Constraints = []dbschematypes.DBConstraint{{
		Name: "pk_widget", TableName: "widget", Schema: "public",
		Type: "PRIMARY KEY", ColumnNames: []string{"id"},
	}}
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "widget_key", TableName: "widget", Schema: "public",
		Columns: []string{"id"}, IsUnique: true, IsPrimary: true,
	}}

	changes := changesFor(c, description, catalog)

	c.Assert(changes, qt.HasLen, 0)
}

// TestTheAutoindexPrefixIsSQLitesAlone is the prefix rule's control: it is what
// SQLite calls the index it made, not a name reserved everywhere. PostgreSQL
// takes the identifier like any other, and an index the description stopped
// declaring is dropped whatever it is called.
func TestTheAutoindexPrefixIsSQLitesAlone(t *testing.T) {
	c := qt.New(t)
	catalog := widgetWithoutTheIndex()
	catalog.Indexes = []dbschematypes.DBIndex{{
		Name: "sqlite_autoindex_widget_1", TableName: "widget", Schema: "public",
		Columns: []string{"code"}, IsUnique: true,
	}}

	changes := changesFor(c, widgetDeclaringNothing(), catalog)

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
}
