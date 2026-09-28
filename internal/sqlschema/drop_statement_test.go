package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// tableNamesOf lists the tables of the database, in order.
func tableNamesOf(database schemamodel.Database) []string {
	var names []string
	for _, table := range database.Tables {
		names = append(names, table.QualifiedName())
	}
	return names
}

// indexNamesOf lists the indexes of the database, in order.
func indexNamesOf(database schemamodel.Database) []string {
	var names []string
	for _, index := range database.Indexes {
		names = append(names, index.Name)
	}
	return names
}

// constraintNamesOf lists the constraints of the database, in order.
func constraintNamesOf(database schemamodel.Database) []string {
	var names []string
	for _, constraint := range database.Constraints {
		names = append(names, constraint.Name)
	}
	return names
}

// A schema file is a script the server runs in order, so a table it creates
// and then drops is not in the schema. Measured on MySQL 8.4.11 and
// PostgreSQL 18.6, each file applies and Atlas CE v1.3.0 reports it synced
// with the database it builds; read as though the drop were not there, the
// comparison plans the table back (stokaro/ptah#3876).
func TestRead_DropTable_HappyPath(t *testing.T) {
	tests := []struct {
		name       string
		dialect    string
		sql        string
		wantTables []string
		wantFields int
	}{
		{name: "PostgreSQL", dialect: "postgres", sql: "CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY);\nDROP TABLE a;", wantTables: []string{"c"}, wantFields: 1},
		{name: "MySQL", dialect: "mysql", sql: "CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY);\nDROP TABLE a;", wantTables: []string{"c"}, wantFields: 1},
		{
			name: "two tables in one statement", dialect: "postgres",
			sql:        "CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE b (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY);\nDROP TABLE a, b;",
			wantTables: []string{"c"}, wantFields: 1,
		},
		{
			name: "IF EXISTS of a table nothing declares", dialect: "postgres",
			sql:        "CREATE TABLE c (id int PRIMARY KEY);\nDROP TABLE IF EXISTS nope;",
			wantTables: []string{"c"}, wantFields: 1,
		},
		{
			// The foreign key goes with the table that holds it.
			name: "a referenced table beside the table referring to it", dialect: "postgres",
			sql: "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p_id int REFERENCES p (id));\n" +
				"CREATE TABLE other (id int PRIMARY KEY);\nDROP TABLE c, p;",
			wantTables: []string{"other"}, wantFields: 1,
		},
		{
			name: "a table created again after the drop", dialect: "postgres",
			sql:        "CREATE TABLE a (id int PRIMARY KEY, x int);\nDROP TABLE a;\nCREATE TABLE a (id int PRIMARY KEY, y int);",
			wantTables: []string{"a"}, wantFields: 2,
		},
		{
			// MySQL keeps a view over a dropped table, and it errors when read.
			name: "MySQL, a table a view reads", dialect: "mysql",
			sql:        "CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY);\nCREATE VIEW v AS SELECT id FROM a;\nDROP TABLE a;",
			wantTables: []string{"c"}, wantFields: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(tableNamesOf(database), qt.DeepEquals, test.wantTables)
			c.Assert(database.Fields, qt.HasLen, test.wantFields)
		})
	}
}

// What the server drops with a table goes with it: its indexes, constraints,
// row security, grants and the sequences its columns own. Measured on
// PostgreSQL 18.6.
func TestRead_DropTable_TakesWhatBelongsToTheTable(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE a (id int PRIMARY KEY, x int, CONSTRAINT a_x_ck CHECK (x > 0));\n"+
			"CREATE INDEX a_x ON a (x);\n"+
			"ALTER TABLE a ENABLE ROW LEVEL SECURITY;\n"+
			"CREATE POLICY pol ON a USING (x > 0);\n"+
			"GRANT SELECT ON a TO PUBLIC;\n"+
			"CREATE SEQUENCE a_seq OWNED BY a.id;\n"+
			"CREATE TABLE c (id int PRIMARY KEY);\n"+
			"DROP TABLE a;"), "postgres")

	c.Assert(err, qt.IsNil)
	c.Assert(tableNamesOf(database), qt.DeepEquals, []string{"c"})
	c.Assert(indexNamesOf(database), qt.IsNil)
	c.Assert(constraintNamesOf(database), qt.IsNil)
	c.Assert(database.RLSPolicies, qt.HasLen, 0)
	c.Assert(database.RLSEnabledTables, qt.HasLen, 0)
	c.Assert(database.Grants, qt.HasLen, 0)
	c.Assert(database.Sequences, qt.HasLen, 0)
}

// A later file of a schema directory drops a table an earlier file declared.
func TestReadOnto_DropsAnEarlierTable(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte("CREATE TABLE a (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY);"), "postgres")
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte("DROP TABLE a;"), "postgres", sqlschema.NewDocument(&earlier))

	c.Assert(err, qt.IsNil)
	c.Assert(later.Tables, qt.HasLen, 0)
	c.Assert(tableNamesOf(earlier), qt.DeepEquals, []string{"c"})
}

// A drop the server refuses is refused, before anything is dropped. Measured
// on MySQL 8.4.11 and PostgreSQL 18.6.
func TestRead_DropTable_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			// Unknown table 'k_src.nope'.
			name: "MySQL, a table nothing declares", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY);\nDROP TABLE nope;",
			wantErr: `DROP TABLE nope names a table this schema does not declare`,
		},
		{
			// table "nope" does not exist.
			name: "PostgreSQL, a table nothing declares", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY);\nDROP TABLE nope;",
			wantErr: `DROP TABLE nope names a table this schema does not declare`,
		},
		{
			// ERROR 3730: Cannot drop table 'p' referenced by a foreign key.
			name: "MySQL, a table a foreign key refers to", dialect: "mysql",
			sql:     "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p_id int, CONSTRAINT fk FOREIGN KEY (p_id) REFERENCES p (id));\nDROP TABLE p;",
			wantErr: `DROP TABLE p: the foreign key fk of c still refers to the table`,
		},
		{
			// cannot drop table p because other objects depend on it.
			name: "PostgreSQL, a table a column's foreign key refers to", dialect: "postgres",
			sql:     "CREATE TABLE p (id int PRIMARY KEY);\nCREATE TABLE c (id int PRIMARY KEY, p_id int REFERENCES p (id));\nDROP TABLE p;",
			wantErr: `DROP TABLE p: the foreign key .* still refers to the table`,
		},
		{
			// view v depends on table a.
			name: "PostgreSQL, a table a view reads", dialect: "postgres",
			sql:     "CREATE TABLE a (id int PRIMARY KEY);\nCREATE VIEW v AS SELECT id FROM a;\nDROP TABLE a;",
			wantErr: `DROP TABLE a: view v still refers to the table`,
		},
		{
			name: "CASCADE", dialect: "postgres",
			sql:     "CREATE TABLE a (id int PRIMARY KEY);\nDROP TABLE a CASCADE;",
			wantErr: `DROP TABLE a CASCADE also drops the objects that depend on the table, which a schema file does not list; drop them by name`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A dropped index is not in the schema. Measured on MySQL 8.4.11 and
// PostgreSQL 18.6, each file applies and Atlas CE v1.3.0 reports it synced;
// read as though the drop were not there, the comparison plans the index back
// (stokaro/ptah#3876).
func TestRead_DropIndex_HappyPath(t *testing.T) {
	tests := []struct {
		name            string
		dialect         string
		sql             string
		wantIndexes     []string
		wantConstraints []string
	}{
		{name: "PostgreSQL", dialect: "postgres", sql: "CREATE TABLE c (id int PRIMARY KEY, x int);\nCREATE INDEX ix ON c (x);\nCREATE INDEX iy ON c (id, x);\nDROP INDEX ix;", wantIndexes: []string{"iy"}},
		{name: "PostgreSQL CONCURRENTLY", dialect: "postgres", sql: "CREATE TABLE c (id int PRIMARY KEY, x int);\nCREATE INDEX ix ON c (x);\nDROP INDEX CONCURRENTLY ix;"},
		{name: "PostgreSQL IF EXISTS", dialect: "postgres", sql: "CREATE TABLE c (id int PRIMARY KEY, x int);\nCREATE INDEX ix ON c (x);\nDROP INDEX IF EXISTS ix;\nDROP INDEX IF EXISTS ix;"},
		{name: "PostgreSQL, a unique index", dialect: "postgres", sql: "CREATE TABLE c (id int PRIMARY KEY, x int);\nCREATE UNIQUE INDEX ux ON c (x);\nDROP INDEX ux;"},
		{
			name: "PostgreSQL, an index in a schema", dialect: "postgres",
			sql: "CREATE SCHEMA app;\nCREATE TABLE app.c (id int PRIMARY KEY, x int);\nCREATE INDEX ix ON app.c (x);\nDROP INDEX app.ix;",
		},
		{name: "SQLite", dialect: "sqlite", sql: "CREATE TABLE c (id integer PRIMARY KEY, x integer);\nCREATE INDEX ix ON c (x);\nDROP INDEX ix;"},
		{name: "MySQL", dialect: "mysql", sql: "CREATE TABLE c (id int PRIMARY KEY, x int);\nCREATE INDEX ix ON c (x);\nDROP INDEX ix ON c;"},
		{name: "SQL Server", dialect: "sqlserver", sql: "CREATE TABLE c (id int PRIMARY KEY, x int);\nCREATE INDEX ix ON c (x);\nDROP INDEX ix ON c;"},
		{
			// A UNIQUE is an index on MySQL, and DROP INDEX drops it.
			name: "MySQL, a UNIQUE constraint", dialect: "mysql",
			sql: "CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT uq UNIQUE (x));\nDROP INDEX uq ON c;",
		},
		{name: "MariaDB IF EXISTS", dialect: "mariadb", sql: "CREATE TABLE c (id int PRIMARY KEY, x int, KEY ix (x));\nDROP INDEX IF EXISTS nope ON c;", wantIndexes: []string{"ix"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(indexNamesOf(database), qt.DeepEquals, test.wantIndexes)
			c.Assert(constraintNamesOf(database), qt.DeepEquals, test.wantConstraints)
		})
	}
}

// A drop the server refuses is refused. Measured on MySQL 8.4.11 and
// PostgreSQL 18.6.
func TestRead_DropIndex_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		sql     string
		wantErr string
	}{
		{
			// index "nope" does not exist.
			name: "PostgreSQL, an index nothing declares", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int);\nDROP INDEX nope;",
			wantErr: `DROP INDEX nope names an index this schema does not declare`,
		},
		{
			// index "ix" does not exist: the bare name looks in public.
			name: "PostgreSQL, an index of another schema by its bare name", dialect: "postgres",
			sql:     "CREATE SCHEMA app;\nCREATE TABLE app.c (id int PRIMARY KEY, x int);\nCREATE INDEX ix ON app.c (x);\nDROP INDEX ix;",
			wantErr: `DROP INDEX ix names an index this schema does not declare`,
		},
		{
			// cannot drop index uq because constraint uq on table c requires it.
			name: "PostgreSQL, the index behind a UNIQUE", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT uq UNIQUE (x));\nDROP INDEX uq;",
			wantErr: `DROP INDEX uq: the index enforces unique constraint uq; drop the constraint instead`,
		},
		{
			name: "PostgreSQL, the index behind a column's own UNIQUE", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int UNIQUE);\nDROP INDEX c_x_key;",
			wantErr: `DROP INDEX c_x_key: the index enforces unique constraint c_x_key; drop the constraint instead`,
		},
		{
			name: "PostgreSQL, the index behind the primary key", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int);\nDROP INDEX c_pkey;",
			wantErr: `DROP INDEX c_pkey: the index enforces primary key constraint c_pkey; drop the constraint instead`,
		},
		{
			name: "CASCADE", dialect: "postgres",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int);\nCREATE INDEX ix ON c (x);\nDROP INDEX ix CASCADE;",
			wantErr: `DROP INDEX ix CASCADE also drops the objects that depend on the index, which a schema file does not list; drop them by name`,
		},
		{
			// SQL Server drops the index of a UNIQUE only with the constraint.
			name: "SQL Server, the index behind a UNIQUE", dialect: "sqlserver",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int, CONSTRAINT uq UNIQUE (x));\nDROP INDEX uq ON c;",
			wantErr: `DROP INDEX uq ON c: the index enforces unique constraint uq; drop the constraint instead`,
		},
		{
			name: "SQL Server, an index nothing declares", dialect: "sqlserver",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int);\nDROP INDEX nope ON c;",
			wantErr: `DROP INDEX nope ON c names an index this schema does not declare`,
		},
		{
			// ERROR 1091: Can't DROP 'nope'.
			name: "MySQL, an index nothing declares", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int);\nDROP INDEX nope ON c;",
			wantErr: `DROP INDEX nope ON c: ALTER TABLE c DROP CONSTRAINT nope names a constraint this schema does not declare by that name`,
		},
		{
			name: "MySQL, a table nothing declares", dialect: "mysql",
			sql:     "CREATE TABLE c (id int PRIMARY KEY, x int);\nDROP INDEX ix ON nope;",
			wantErr: `DROP INDEX ix ON nope names a table this schema does not declare`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// MySQL and MariaDB read `DROP INDEX b ON c` as `ALTER TABLE c DROP INDEX b`,
// so it drops a column's own UNIQUE by the name the server gives it, as the
// ALTER TABLE spelling does. Measured on MySQL 8.4.11, the file applies and
// Atlas CE v1.3.0 reports it synced.
func TestRead_DropIndexOnDropsAColumnsOwnKey(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb"} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte("CREATE TABLE c (id int PRIMARY KEY, b int UNIQUE);\nDROP INDEX b ON c;"), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(columnOfC(c, database, "b").Unique, qt.IsFalse)
		})
	}
}

// The indexes a MySQL server built for a dropped table's foreign keys go with
// the table. Kept, one over (x, y) stands in for the index a key over (x) on a
// table created again under the name, which the model then never records, and
// the index the server keeps once that key is dropped is planned away.
// Measured on MySQL 8.4.11 and MariaDB 11.8.9, the file leaves c with the
// index fk2 (x).
func TestRead_DropTable_ForgetsTheIndexesItsKeysBuilt(t *testing.T) {
	c := qt.New(t)

	database, _, err := sqlschema.Read([]byte(
		"CREATE TABLE p (a int PRIMARY KEY, b int, UNIQUE KEY ab (a, b));\n"+
			"CREATE TABLE c (id int PRIMARY KEY, x int, y int, CONSTRAINT fk FOREIGN KEY (x, y) REFERENCES p (a, b));\n"+
			"DROP TABLE c;\n"+
			"CREATE TABLE c (id int PRIMARY KEY, x int);\n"+
			"ALTER TABLE c ADD CONSTRAINT fk2 FOREIGN KEY (x) REFERENCES p (a);\n"+
			"ALTER TABLE c DROP FOREIGN KEY fk2;"), "mysql")

	c.Assert(err, qt.IsNil)
	c.Assert(indexNamesOf(database), qt.DeepEquals, []string{"fk2"})
}
