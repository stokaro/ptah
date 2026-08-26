package schemachange_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/schemamodel"
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
		schemamodel.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		schemamodel.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	description.Indexes = append(description.Indexes, schemamodel.Index{
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
	current := widgetWithoutTheIndex()
	current.Indexes = []catalog.Index{{
		Name: "idx_widget_code", TableName: "widget", Schema: "public",
		Columns: []string{"code"},
	}}

	changes := changesFor(c, widgetDeclaringNothing(), current)

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
}

// widgetDeclaringUniqueConstraint declares the guarantee as a CONSTRAINT and no
// index, which is what a schema author writes.
func widgetDeclaringUniqueConstraint() *schemamodel.Database {
	description := widgetDeclaringNothing()
	description.Constraints = append(description.Constraints, schemamodel.Constraint{
		StructName: "Widget", Name: "uq_widget_code", Type: "UNIQUE", Columns: []string{"code"},
	})
	return description
}

func widgetDeclaringNothing() *schemamodel.Database {
	return describedTable(
		schemamodel.Field{StructName: "Widget", Name: "id", Type: "int", Primary: true},
		schemamodel.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
}

// widgetReportingBoth is the catalog as every one of those engines reports it:
// the constraint AND the index it is enforced with, one object twice.
func widgetReportingBoth() *catalog.Database {
	current := widgetWithoutTheIndex()
	current.Constraints = []catalog.Constraint{{
		Name: "uq_widget_code", TableName: "widget", Schema: "public",
		Type: "UNIQUE", ColumnNames: []string{"code"},
	}}
	current.Indexes = []catalog.Index{{
		Name: "uq_widget_code", TableName: "widget", Schema: "public",
		Columns: []string{"code"}, IsUnique: true,
	}}
	return current
}

func widgetWithoutTheIndex() *catalog.Database {
	return catalogTable(
		catalog.Column{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		catalog.Column{Name: "code", DataType: "text", IsNullable: "YES"},
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
		schemamodel.Field{StructName: "Widget", Name: "id", Type: "int"},
		schemamodel.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	current := widgetWithoutTheIndex()
	current.Constraints = []catalog.Constraint{{
		Name: "widget_pkey", TableName: "widget", Schema: "public",
		Type: "PRIMARY KEY", ColumnNames: []string{"id"},
	}}
	current.Indexes = []catalog.Index{{
		Name: "widget_pkey", TableName: "widget", Schema: "public",
		Columns: []string{"id"}, IsUnique: true, IsPrimary: true,
	}}

	changes := changesFor(c, description, current)

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
			description, current := backingIdentityFixture(
				test.indexName, test.indexTable, test.indexSchema)

			changes := changesFor(c, description, current)

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
) (*schemamodel.Database, *catalog.Database) {
	current := widgetWithoutTheIndex()
	current.Constraints = []catalog.Constraint{{
		Name: "uq_widget_code", TableName: "widget", Schema: "public",
		Type: "UNIQUE", ColumnNames: []string{"code"},
	}}
	current.Indexes = []catalog.Index{{
		Name: indexName, TableName: indexTable, Schema: indexSchema,
		Columns: []string{"code"},
	}}
	description := widgetDeclaringUniqueConstraint()

	if indexTable == "widget" && indexSchema == "public" {
		return description, current
	}

	current.Tables = append(current.Tables, catalog.Table{
		Name: indexTable, Schema: indexSchema,
		Columns: []catalog.Column{
			{Name: "code", DataType: "text", IsNullable: "YES"},
		},
	})
	description.Schemas = append(description.Schemas, schemamodel.Schema{Name: indexSchema})
	description.Tables = append(description.Tables, schemamodel.Table{
		StructName: "Other", Name: indexTable, Schema: indexSchema,
	})
	description.Fields = append(description.Fields, schemamodel.Field{
		StructName: "Other", Name: "code", Type: "text", Nullable: true,
	})
	return description, current
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
		drops   int
	}{
		{
			name:    "MySQL creates it",
			profile: mysqlProfile(),
			drops:   0,
		},
		{
			name:    "MariaDB creates it",
			profile: mariadbProfile(),
			drops:   0,
		},
		{
			name:    "PostgreSQL creates none",
			profile: postgresProfile(),
			schema:  "public",
			drops:   1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description, current := foreignKeyBackingFixture(test.schema)

			changes := changesForProfile(c, description, current, test.profile)

			c.Assert(changes, qt.HasLen, test.drops)
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
	current := catalogTableInSchema("main",
		catalog.Column{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		catalog.Column{Name: "code", DataType: "text", IsNullable: "YES"},
	)
	current.Indexes = []catalog.Index{
		{
			Name: "sqlite_autoindex_widget_1", TableName: "widget", Schema: "main",
			Columns: []string{"code"}, IsUnique: true,
		},
		{
			Name: "idx_widget_code", TableName: "widget", Schema: "main",
			Columns: []string{"code"},
		},
	}

	changes := changesForProfile(c, widgetDeclaringNothing(), current, sqliteProfile())

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
	current := catalogTableInSchema("dbo",
		catalog.Column{Name: "id", DataType: "integer", IsNullable: "NO", IsPrimaryKey: true},
		catalog.Column{Name: "code", DataType: "text", IsNullable: "YES"},
	)
	current.Constraints = []catalog.Constraint{{
		Name: "uq_widget_code", TableName: "widget", Schema: "dbo",
		Type: "UNIQUE", ColumnNames: []string{"code"},
	}}
	current.Indexes = []catalog.Index{{
		Name: "uq_widget_code", TableName: "widget", Schema: "dbo",
		Columns: []string{"code"}, IsUnique: true,
	}}

	changes := changesForProfile(c,
		widgetDeclaringUniqueConstraint(), current, sqlserverProfile())

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
}

// foreignKeyBackingFixture is a child whose column references a parent through a
// named FOREIGN KEY, read back with an index of the constraint's name that the
// description never mentions.
func foreignKeyBackingFixture(schema string) (*schemamodel.Database, *catalog.Database) {
	description := &schemamodel.Database{
		Tables: []schemamodel.Table{
			{StructName: "Parent", Name: "parent", Schema: schema},
			{StructName: "Widget", Name: "widget", Schema: schema},
		},
		Fields: []schemamodel.Field{
			{StructName: "Parent", Name: "id", Type: "int", Primary: true},
			{StructName: "Widget", Name: "id", Type: "int", Primary: true},
			{
				StructName: "Widget", Name: "parent_id", Type: "int", Nullable: true,
				Foreign: "parent(id)", ForeignKeyName: "fk_widget_parent",
			},
		},
	}
	current := &catalog.Database{
		Tables: []catalog.Table{
			{Name: "parent", Schema: schema, Columns: []catalog.Column{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
			}},
			{Name: "widget", Schema: schema, Columns: []catalog.Column{
				{Name: "id", DataType: "int", IsNullable: "NO", IsPrimaryKey: true},
				{Name: "parent_id", DataType: "int", IsNullable: "YES"},
			}},
		},
		Constraints: []catalog.Constraint{{
			Name: "fk_widget_parent", TableName: "widget", Schema: schema,
			Type: "FOREIGN KEY", ColumnName: "parent_id",
			ForeignTable: new("parent"), ForeignColumn: new("id"),
		}},
		Indexes: []catalog.Index{{
			Name: "fk_widget_parent", TableName: "widget", Schema: schema,
			Columns: []string{"parent_id"},
		}},
	}
	return description, current
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
		schemamodel.Field{StructName: "Widget", Name: "id", Type: "int"},
		schemamodel.Field{StructName: "Widget", Name: "code", Type: "text", Nullable: true},
	)
	current := widgetWithoutTheIndex()
	current.Constraints = []catalog.Constraint{{
		Name: "pk_widget", TableName: "widget", Schema: "public",
		Type: "PRIMARY KEY", ColumnNames: []string{"id"},
	}}
	current.Indexes = []catalog.Index{{
		Name: "widget_key", TableName: "widget", Schema: "public",
		Columns: []string{"id"}, IsUnique: true, IsPrimary: true,
	}}

	changes := changesFor(c, description, current)

	c.Assert(changes, qt.HasLen, 0)
}

// TestTheAutoindexPrefixIsSQLitesAlone is the prefix rule's control: it is what
// SQLite calls the index it made, not a name reserved everywhere. PostgreSQL
// takes the identifier like any other, and an index the description stopped
// declaring is dropped whatever it is called.
func TestTheAutoindexPrefixIsSQLitesAlone(t *testing.T) {
	c := qt.New(t)
	current := widgetWithoutTheIndex()
	current.Indexes = []catalog.Index{{
		Name: "sqlite_autoindex_widget_1", TableName: "widget", Schema: "public",
		Columns: []string{"code"}, IsUnique: true,
	}}

	changes := changesFor(c, widgetDeclaringNothing(), current)

	c.Assert(changes, qt.HasLen, 1)
	c.Assert(string(changes[0].ID.Kind), qt.Equals, "index")
	c.Assert(changes[0].Operation, qt.Equals, schemachange.Remove)
}

// TestAnExcludeConstraintOwnsItsIndexAndACheckOwnsNone pins the pair a read
// cannot tell apart on its own.
//
// Measured on PostgreSQL 17.6, the pg_index row for
// `EXCLUDE USING gist (room WITH =)` reports indisprimary false and indisunique
// false -- exactly like an ordinary index -- and only pg_constraint.conindid
// ties it to the constraint. Dropping it is refused:
// `cannot drop index ex_widget_room because constraint ex_widget_room on table
// widget requires it`.
//
// A CHECK is the other clause constraint and is enforced with no index at all,
// so an index that shares a CHECK's name is an index somebody wrote.
//
// The shipping comparator plans the DROP INDEX for the EXCLUDE pair; that is
// filed as stokaro/ptah#2013 rather than reproduced here.
func TestAnExcludeConstraintOwnsItsIndexAndACheckOwnsNone(t *testing.T) {
	tests := []struct {
		name       string
		constraint schemamodel.Constraint
		reported   catalog.Constraint
		drops      int
	}{
		{
			name: "an EXCLUDE is enforced with one",
			constraint: schemamodel.Constraint{
				StructName: "Widget", Name: "guard_widget_code", Type: "EXCLUDE",
				UsingMethod: "gist", ExcludeElements: "code WITH =",
			},
			reported: catalog.Constraint{
				Name: "guard_widget_code", TableName: "widget", Schema: "public",
				Type: "EXCLUDE", UsingMethod: new("gist"),
				ExcludeElements: new("code WITH ="),
			},
			drops: 0,
		},
		{
			name: "a CHECK is enforced with none",
			constraint: schemamodel.Constraint{
				StructName: "Widget", Name: "guard_widget_code", Type: "CHECK",
				CheckExpression: "code <> ''",
			},
			reported: catalog.Constraint{
				Name: "guard_widget_code", TableName: "widget", Schema: "public",
				Type: "CHECK", CheckClause: new("code <> ''"),
			},
			drops: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			description := widgetDeclaringNothing()
			description.Constraints = append(description.Constraints, test.constraint)
			current := widgetWithoutTheIndex()
			current.Constraints = []catalog.Constraint{test.reported}
			current.Indexes = []catalog.Index{{
				Name: "guard_widget_code", TableName: "widget", Schema: "public",
				Columns: []string{"code"},
			}}

			changes := changesFor(c, description, current)

			c.Assert(changes, qt.HasLen, test.drops)
		})
	}
}
