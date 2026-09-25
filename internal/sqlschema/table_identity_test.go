package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
)

// orphanedColumns is the key columnsByTable lists a column under when no table
// carries its struct name.
const orphanedColumns = "(no table)"

// columnsByTable lists each table's columns, joined the way every consumer of
// the model joins them: through the table's struct name. A column no table
// joins is listed under orphanedColumns, so a declaration that left one behind
// shows.
func columnsByTable(database schemamodel.Database) map[string][]string {
	columns := make(map[string][]string, len(database.Tables))
	joined := make(map[int]bool, len(database.Fields))
	for _, table := range database.Tables {
		columns[table.QualifiedName()] = make([]string, 0)
		for i, field := range database.Fields {
			if field.StructName == table.StructName {
				columns[table.QualifiedName()] = append(columns[table.QualifiedName()], field.Name)
				joined[i] = true
			}
		}
	}
	for i, field := range database.Fields {
		if !joined[i] {
			columns[orphanedColumns] = append(columns[orphanedColumns], field.StructName+"."+field.Name)
		}
	}
	return columns
}

// Two tables whose names derive one struct name stay two tables, each with its
// own columns, and a statement naming one of them changes that one. Sharing
// the struct name, they are one table to everything that joins on it: the
// reader keeps a single `id` for the pair, and `ALTER TABLE docs` changes
// "Docs" (stokaro/ptah#3642). Each column set differs, so a column joined to
// the wrong table shows.
//
// A statement reaches a table or column the way its dialect's server resolves
// the name, measured on each engine: SQLite ignores ASCII case, SQL Server
// under its default collation and the MySQL family's column names ignore case
// past ASCII too, and a MySQL table name matches exactly before it matches
// without case.
func TestRead_TableIdentity_HappyPath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		file    string
		want    map[string][]string
	}{
		{
			name:    "PostgreSQL/a quoted mixed-case name beside its folded spelling",
			dialect: "postgres",
			file:    `CREATE TABLE "Docs" (id int, a int); CREATE TABLE docs (id int, b int);`,
			want:    map[string][]string{"Docs": {"id", "a"}, "docs": {"id", "b"}},
		},
		{
			name:    "PostgreSQL/a camel-case name beside its snake-case spelling",
			dialect: "postgres",
			file:    `CREATE TABLE "orderItems" (id int, a int); CREATE TABLE order_items (id int, b int);`,
			want:    map[string][]string{"orderItems": {"id", "a"}, "order_items": {"id", "b"}},
		},
		{
			name:    "PostgreSQL/a singular name beside its plural",
			dialect: "postgres",
			file:    `CREATE TABLE doc (id int, a int); CREATE TABLE docs (id int, b int);`,
			want:    map[string][]string{"doc": {"id", "a"}, "docs": {"id", "b"}},
		},
		{
			name:    "no dialect/a schema-qualified name beside its underscored spelling",
			dialect: "",
			file:    `CREATE TABLE app.t (id int, a int); CREATE TABLE app_t (id int, b int);`,
			want:    map[string][]string{"app.t": {"id", "a"}, "app_t": {"id", "b"}},
		},
		{
			name:    "PostgreSQL/a third table whose name derives the same struct name",
			dialect: "postgres",
			file:    `CREATE TABLE "Docs" (id int, a int); CREATE TABLE docs (id int, b int); CREATE TABLE doc (id int, c int);`,
			want:    map[string][]string{"Docs": {"id", "a"}, "docs": {"id", "b"}, "doc": {"id", "c"}},
		},
		{
			name:    "PostgreSQL/an ALTER TABLE naming the second table",
			dialect: "postgres",
			file:    `CREATE TABLE "Docs" (id int); CREATE TABLE docs (id int); ALTER TABLE docs ADD COLUMN x int;`,
			want:    map[string][]string{"Docs": {"id"}, "docs": {"id", "x"}},
		},
		{
			name:    "PostgreSQL/an ALTER TABLE naming the first table",
			dialect: "postgres",
			file:    `CREATE TABLE "Docs" (id int); CREATE TABLE docs (id int); ALTER TABLE "Docs" ADD COLUMN x int;`,
			want:    map[string][]string{"Docs": {"id", "x"}, "docs": {"id"}},
		},
		{
			name:    "PostgreSQL/an ALTER TABLE spelling an unquoted name in another case",
			dialect: "postgres",
			file:    `CREATE TABLE Docs (id int); ALTER TABLE DOCS ADD COLUMN x int;`,
			want:    map[string][]string{"docs": {"id", "x"}},
		},
		{
			name:    "PostgreSQL/a table declared again under the same name",
			dialect: "postgres",
			file:    `CREATE TABLE docs (id int); CREATE TABLE IF NOT EXISTS docs (id int);`,
			want:    map[string][]string{"docs": {"id"}},
		},
		{
			name:    "SQLite/an ALTER TABLE spelling the table in another ASCII case",
			dialect: "sqlite",
			file:    `CREATE TABLE Docs (id int); ALTER TABLE docs ADD COLUMN x int;`,
			want:    map[string][]string{"Docs": {"id", "x"}},
		},
		{
			name:    "SQLite/a DROP COLUMN spelling the column in another ASCII case",
			dialect: "sqlite",
			file:    `CREATE TABLE t (id int, Note text); ALTER TABLE t DROP COLUMN note;`,
			want:    map[string][]string{"t": {"id"}},
		},
		{
			name:    "SQL Server/an ALTER TABLE spelling the table in another case past ASCII",
			dialect: "sqlserver",
			file:    `CREATE TABLE Ärger (id int); ALTER TABLE ärger ADD x int;`,
			want:    map[string][]string{"Ärger": {"id", "x"}},
		},
		{
			name:    "SQL Server/a DROP COLUMN spelling the column in another case",
			dialect: "sqlserver",
			file:    `CREATE TABLE t (id int, Note int); ALTER TABLE t DROP COLUMN note;`,
			want:    map[string][]string{"t": {"id"}},
		},
		{
			name:    "MySQL/an ALTER TABLE reaching the one table spelled in another case",
			dialect: "mysql",
			file:    `CREATE TABLE Docs (id int); ALTER TABLE docs ADD COLUMN x int;`,
			want:    map[string][]string{"Docs": {"id", "x"}},
		},
		{
			name:    "MySQL/an ALTER TABLE naming one of two tables that differ only in case",
			dialect: "mysql",
			file:    `CREATE TABLE Docs (id int, a int); CREATE TABLE docs (id int, b int); ALTER TABLE docs ADD COLUMN x int;`,
			want:    map[string][]string{"Docs": {"id", "a"}, "docs": {"id", "b", "x"}},
		},
		{
			name:    "MySQL/a DROP COLUMN spelling the column in another case past ASCII",
			dialect: "mysql",
			file:    `CREATE TABLE t (id int, Äpfel int); ALTER TABLE t DROP COLUMN äpfel;`,
			want:    map[string][]string{"t": {"id"}},
		},
		{
			name:    "MariaDB/an ADD COLUMN IF NOT EXISTS restating a column in another case",
			dialect: "mariadb",
			file:    `CREATE TABLE t (id int, Note int); ALTER TABLE t ADD COLUMN IF NOT EXISTS note int;`,
			want:    map[string][]string{"t": {"id", "Note"}},
		},
		{
			name:    "PostgreSQL/a DROP COLUMN beside a CHECK naming another column that differs only in case",
			dialect: "postgres",
			file:    `CREATE TABLE t (id int, "Note" text, note text, CHECK (length(note) > 0)); ALTER TABLE t DROP COLUMN "Note";`,
			want:    map[string][]string{"t": {"id", "note"}},
		},
		{
			name:    "PostgreSQL/a DROP COLUMN beside an index on another column that differs only in case",
			dialect: "postgres",
			file:    `CREATE TABLE t (id int, "Note" text, note text); CREATE INDEX t_note ON t (note); ALTER TABLE t DROP COLUMN "Note";`,
			want:    map[string][]string{"t": {"id", "note"}},
		},
		{
			name:    "SQLite/a DROP COLUMN whose own CHECK spells it in another case",
			dialect: "sqlite",
			file:    `CREATE TABLE t (id int, Note text CHECK (length(note) > 0)); ALTER TABLE t DROP COLUMN Note;`,
			want:    map[string][]string{"t": {"id"}},
		},
		{
			name:    "Oracle/an ALTER TABLE spelling the table in another ASCII case",
			dialect: "oracle",
			file:    `CREATE TABLE Docs (id int); ALTER TABLE docs ADD x int;`,
			want:    map[string][]string{"Docs": {"id", "x"}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(test.file), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(columnsByTable(database), qt.DeepEquals, test.want)
		})
	}
}

// A later file of a schema directory that declares a table whose name derives
// the struct name an earlier file's table holds gets a struct name of its own,
// so the two do not merge when the caller joins the files.
func TestReadOnto_TableIdentity_LaterTableKeepsItsOwnStructName(t *testing.T) {
	c := qt.New(t)
	earlier, _, err := sqlschema.Read([]byte(`CREATE TABLE "Docs" (id int, a int);`), "postgres")
	c.Assert(err, qt.IsNil)

	later, _, err := sqlschema.ReadOnto([]byte(`CREATE TABLE docs (id int, b int);`), "postgres", &earlier)

	c.Assert(err, qt.IsNil)
	c.Assert(later.Tables, qt.HasLen, 1)
	c.Assert(later.Tables[0].StructName, qt.Not(qt.Equals), earlier.Tables[0].StructName)
	c.Assert(columnsByTable(later), qt.DeepEquals, map[string][]string{"docs": {"id", "b"}})
}

// An ALTER TABLE naming a table the document does not declare is refused, even
// where the name derives the struct name of a table the document does declare.
// The dialect's server resolves each statement below to a table that does not
// exist.
func TestRead_TableIdentity_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		file    string
		wantErr string
	}{
		{
			name:    "PostgreSQL/the folded spelling of a quoted mixed-case table",
			dialect: "postgres",
			file:    `CREATE TABLE "Docs" (id int); ALTER TABLE docs ADD COLUMN x int;`,
			wantErr: `the schema model has no place for this statement: ALTER TABLE docs ADD COLUMN x names a table this schema does not declare`,
		},
		{
			name:    "PostgreSQL/the singular of a plural table",
			dialect: "postgres",
			file:    `CREATE TABLE docs (id int); ALTER TABLE doc ADD COLUMN x int;`,
			wantErr: `the schema model has no place for this statement: ALTER TABLE doc ADD COLUMN x names a table this schema does not declare`,
		},
		{
			name:    "no dialect/the underscored spelling of a schema-qualified table",
			dialect: "",
			file:    `CREATE TABLE app.t (id int); ALTER TABLE app_t ADD COLUMN x int;`,
			wantErr: `the schema model has no place for this statement: ALTER TABLE app_t ADD COLUMN x names a table this schema does not declare`,
		},
		{
			name:    "no dialect/another case",
			dialect: "",
			file:    `CREATE TABLE Docs (id int); ALTER TABLE docs ADD COLUMN x int;`,
			wantErr: `the schema model has no place for this statement: ALTER TABLE docs ADD COLUMN x names a table this schema does not declare`,
		},
		{
			name:    "SQLite/another case past ASCII",
			dialect: "sqlite",
			file:    `CREATE TABLE Ärger (id int); ALTER TABLE ärger ADD COLUMN x int;`,
			wantErr: `the schema model has no place for this statement: ALTER TABLE ärger ADD COLUMN x names a table this schema does not declare`,
		},
		{
			name:    "ClickHouse/another case",
			dialect: "clickhouse",
			file:    `CREATE TABLE Docs (id Int32) ENGINE = MergeTree ORDER BY id; ALTER TABLE docs ADD COLUMN x Int32;`,
			wantErr: `the schema model has no place for this statement: ALTER TABLE docs ADD COLUMN x names a table this schema does not declare`,
		},
		{
			name:    "MySQL/a spelling two declared tables differ from only in case",
			dialect: "mysql",
			file:    `CREATE TABLE Docs (id int); CREATE TABLE DOCS (id int); ALTER TABLE docs ADD COLUMN x int;`,
			wantErr: `the schema model has no place for this statement: ALTER TABLE docs ADD COLUMN x names a table this schema does not declare`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(test.file), test.dialect)

			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// An ALTER TABLE operation naming a column reaches it by the dialect's rule, so
// a spelling the server resolves to no column is refused, and one it resolves
// to a declared column cannot add that column again. A CHECK or an index that
// names the column in a spelling the server resolves to it holds on to the
// column the same way: SQLite refuses each DROP COLUMN below.
func TestRead_ColumnIdentity_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		file    string
		wantErr string
	}{
		{
			name:    "PostgreSQL/the folded spelling of a quoted mixed-case column",
			dialect: "postgres",
			file:    `CREATE TABLE t (id int, "Note" text); ALTER TABLE t DROP COLUMN note;`,
			wantErr: `ALTER TABLE t DROP COLUMN note names a column the table does not declare`,
		},
		{
			name:    "ClickHouse/another case",
			dialect: "clickhouse",
			file:    `CREATE TABLE t (id Int32, Note String) ENGINE = MergeTree ORDER BY id; ALTER TABLE t DROP COLUMN note;`,
			wantErr: `ALTER TABLE t DROP COLUMN note names a column the table does not declare`,
		},
		{
			name:    "MySQL/another accent",
			dialect: "mysql",
			file:    `CREATE TABLE t (id int, cafe int); ALTER TABLE t DROP COLUMN café;`,
			wantErr: `ALTER TABLE t DROP COLUMN café names a column the table does not declare`,
		},
		{
			name:    "SQLite/dropping a column a table CHECK spells in another case",
			dialect: "sqlite",
			file:    `CREATE TABLE t (id int, Note text, CHECK (length(note) > 0)); ALTER TABLE t DROP COLUMN Note;`,
			wantErr: `ALTER TABLE t DROP COLUMN Note: check constraint \(unnamed\) still refers to the column`,
		},
		{
			name:    "SQLite/renaming a column a table CHECK spells in another case",
			dialect: "sqlite",
			file:    `CREATE TABLE t (id int, Note text, CHECK (length(note) > 0)); ALTER TABLE t RENAME COLUMN Note TO Body;`,
			wantErr: `ALTER TABLE t RENAME COLUMN Note: check constraint \(unnamed\) names the column, and a schema file keeps its text; declare the column under its new name`,
		},
		{
			name:    "SQLite/dropping a column an index on another spelling of the table names",
			dialect: "sqlite",
			file:    `CREATE TABLE docs (id int, Note text); CREATE INDEX docs_note ON DOCS (note); ALTER TABLE docs DROP COLUMN Note;`,
			wantErr: `ALTER TABLE docs DROP COLUMN Note: index docs_note still refers to the column`,
		},
		{
			name:    "SQLite/adding a column declared in another ASCII case",
			dialect: "sqlite",
			file:    `CREATE TABLE t (id int, Note text); ALTER TABLE t ADD COLUMN note int;`,
			wantErr: `ALTER TABLE t ADD COLUMN note names a column the table already declares`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, statements, err := sqlschema.Read([]byte(test.file), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(database.Tables, qt.HasLen, 0)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// A COMMENT ON reaches its table and column by the rule an ALTER TABLE uses, so
// on a dialect whose server ignores case the comment lands on the object
// declared in another case. The PostgreSQL control, a lower-case name for a
// quoted mixed-case table, is refused in TestRead_CommentOn_FailurePath.
func TestRead_CommentOn_ReachesItsObjectByTheAlterTableRule(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
	}{
		{name: "SQLite", dialect: "sqlite"},
		{name: "SQL Server", dialect: "sqlserver"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(`CREATE TABLE Docs (id int, Note text);
COMMENT ON TABLE docs IS 'the table';
COMMENT ON COLUMN docs.note IS 'the column';`), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			structName := database.Tables[0].StructName
			c.Assert(commentsOf(database), qt.DeepEquals, map[string]string{
				"table Docs":                     "the table",
				"column " + structName + ".id":   "",
				"column " + structName + ".Note": "the column",
			})
		})
	}
}
