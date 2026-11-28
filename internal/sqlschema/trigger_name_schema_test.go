package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// A schema on a trigger's name is the trigger's schema, and the schema model
// puts a trigger in its table's. MySQL, MariaDB and SQLite put `CREATE TRIGGER
// q1.t ... ON u` in q1 and read u there, and refuse a table in another schema:
// MySQL with ERROR 1435, SQLite with `trigger t cannot reference objects in
// database q2`. PostgreSQL refuses a schema on a trigger's name with a syntax
// error. Read as one name, the qualified spelling became a trigger whose name
// holds a dot (stokaro/ptah#6850).

// TestRead_TriggerNameSchema_HappyPath reads the schema on the name as the
// table's.
func TestRead_TriggerNameSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name, dialect, sql string
		wantTable          string
	}{
		{
			name: "mysql, qualified table", dialect: "mysql",
			sql:       "CREATE TRIGGER `nm_db`.`trg_q` BEFORE INSERT ON `nm_db`.`u` FOR EACH ROW SET NEW.c = 1;",
			wantTable: "nm_db.u",
		},
		{
			name: "mysql, unqualified table", dialect: "mysql",
			sql:       "CREATE TRIGGER `nm_db`.`trg_q` BEFORE INSERT ON u FOR EACH ROW SET NEW.c = 1;",
			wantTable: "nm_db.u",
		},
		{
			name: "mariadb, unqualified table", dialect: "mariadb",
			sql:       "CREATE TRIGGER nm_db.trg_q BEFORE INSERT ON u FOR EACH ROW SET NEW.c = 1;",
			wantTable: "nm_db.u",
		},
		{
			name: "sqlite, the default schema", dialect: "sqlite",
			sql:       "CREATE TABLE u (id INTEGER PRIMARY KEY, c INTEGER);\nCREATE TRIGGER main.trg_q AFTER INSERT ON u BEGIN SELECT 1; END;",
			wantTable: "u",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Triggers, qt.HasLen, 1)
			c.Assert(database.Triggers[0].Name, qt.Equals, "trg_q")
			c.Assert(database.Triggers[0].Table, qt.Equals, test.wantTable)
		})
	}
}

// TestRead_TriggerNameSchema_FailurePath refuses what the server refuses: a
// table in another schema than the trigger's, and on PostgreSQL any schema on
// the name.
func TestRead_TriggerNameSchema_FailurePath(t *testing.T) {
	tests := []struct {
		name, dialect, sql, wantErr string
	}{
		{
			name: "mysql, table in another schema", dialect: "mysql",
			sql:     "CREATE TRIGGER q1.t BEFORE INSERT ON q2.u FOR EACH ROW SET NEW.c = 1;",
			wantErr: `(?s).*trigger q1\.t is in schema q1 and its table q2\.u in schema q2; a trigger has to be in its table's schema.*`,
		},
		{
			name: "mysql, schema in another letter case", dialect: "mysql",
			sql:     "CREATE TRIGGER Q1.t BEFORE INSERT ON q1.u FOR EACH ROW SET NEW.c = 1;",
			wantErr: `(?s).*trigger Q1\.t is in schema Q1 and its table q1\.u in schema q1.*`,
		},
		{
			name: "sqlite, table in another schema", dialect: "sqlite",
			sql:     "CREATE TRIGGER main.t AFTER INSERT ON aux.u BEGIN SELECT 1; END;",
			wantErr: `(?s).*trigger main\.t is in schema main and its table aux\.u in schema aux.*`,
		},
		{
			name: "postgres", dialect: "postgres",
			sql: "CREATE TABLE tq.u (id int PRIMARY KEY);\n" +
				"CREATE TRIGGER tq.trg BEFORE INSERT ON tq.u FOR EACH ROW EXECUTE FUNCTION tq.f();",
			wantErr: `(?s).*trigger name tq\.trg names a schema, and a trigger here is in its table's schema; ` +
				`write the trigger's name alone and qualify its table.*`,
		},
		{
			name: "mysql, a database prefix too", dialect: "mysql",
			sql:     "CREATE TRIGGER a.b.t BEFORE INSERT ON u FOR EACH ROW SET NEW.c = 1;",
			wantErr: `(?s).*trigger name a\.b\.t names a schema.*`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.sql), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}
