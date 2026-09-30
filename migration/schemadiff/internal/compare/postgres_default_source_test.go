package compare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff/internal/compare"
)

func TestColumns_PostgresSQLNullSourceMatchesAnImplicitDefault(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read(
		[]byte("CREATE TABLE defaults (value varchar(64) DEFAULT NULL);"), platform.Postgres,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Fields, qt.HasLen, 1)

	diff := compare.ColumnsWithDialect(database.Fields[0], catalog.Column{
		Name: "value", DataType: "varchar(64)", IsNullable: "YES",
	}, platform.Postgres)
	c.Assert(diff.Changes, qt.HasLen, 0)
}

func TestColumns_PostgresTextNullSourceDiffersFromAnImplicitDefault(t *testing.T) {
	c := qt.New(t)
	database, _, err := sqlschema.Read(
		[]byte("CREATE TABLE defaults (value varchar(64) DEFAULT 'NULL');"), platform.Postgres,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(database.Fields, qt.HasLen, 1)

	diff := compare.ColumnsWithDialect(database.Fields[0], catalog.Column{
		Name: "value", DataType: "varchar(64)", IsNullable: "YES",
	}, platform.Postgres)
	c.Assert(diff.Changes, qt.DeepEquals, map[string]string{"default_expr": " -> 'NULL'"})
}

func TestColumns_PostgresLiteralNullValueIsNotSQLNull(t *testing.T) {
	c := qt.New(t)
	diff := compare.ColumnsWithDialect(schemamodel.Field{
		Name: "value", Type: "varchar(64)", Nullable: true, Default: "NULL", DefaultSet: true,
	}, catalog.Column{
		Name: "value", DataType: "varchar(64)", IsNullable: "YES",
	}, platform.Postgres)
	c.Assert(diff.Changes, qt.DeepEquals, map[string]string{"default_expr": " -> 'NULL'"})
}
