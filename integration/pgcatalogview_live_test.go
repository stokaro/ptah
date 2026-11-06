//go:build integration

package integration_test

import (
	"fmt"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/dbschema"
	"ptah.run/internal/pgcatalogview"
)

// TestPGCatalogViewAgreesWithInformationSchemaLive runs each pg_catalog statement
// beside the view it replaces, on the connection whose role the views filter
// nothing for, and asks for every row either side holds and the other does not. The
// corpus is the server's own: every table and column of its catalogs, with the
// fixture for the type families those do not use. A server line that defines a view
// differently fails here rather than as a plan.
func TestPGCatalogViewAgreesWithInformationSchemaLive(t *testing.T) {
	const columnList = `table_schema::text, table_name::text, column_name::text, ordinal_position::int,
		column_default::text, is_nullable::text, data_type::text,
		character_maximum_length::int, numeric_precision::int, numeric_scale::int, datetime_precision::int,
		collation_name::text, domain_schema::text, domain_name::text, udt_schema::text, udt_name::text`
	tests := []struct {
		name    string
		catalog string
		view    string
		floor   int
	}{
		{
			name:    "tables",
			catalog: `SELECT table_schema::text, table_name::text, table_type::text FROM (` + pgcatalogview.Tables + `) v`,
			view: `SELECT table_schema::text, table_name::text, table_type::text
				FROM information_schema.tables WHERE table_type = 'BASE TABLE'`,
			floor: 60,
		},
		{
			name:    "columns",
			catalog: `SELECT ` + columnList + ` FROM (` + pgcatalogview.Columns + `) v`,
			view: `SELECT ` + columnList + ` FROM information_schema.columns v
				WHERE (table_schema, table_name) IN (
					SELECT table_schema, table_name FROM information_schema.tables WHERE table_type = 'BASE TABLE')`,
			floor: 600,
		},
		{
			// Below PostgreSQL 18 the view also lists a row per NOT NULL under a name
			// it makes up, and from 18 under the name pg_constraint holds. Neither
			// reaches a read, which takes contype from pg_constraint.
			name: "table constraints",
			catalog: `SELECT table_schema::text, table_name::text, constraint_name::text, constraint_type::text
				FROM (` + pgcatalogview.TableConstraints + `) v WHERE constraint_name NOT LIKE '%\_not\_null'`,
			view: `SELECT table_schema::text, table_name::text, constraint_name::text, constraint_type::text
				FROM information_schema.table_constraints WHERE constraint_name NOT LIKE '%\_not\_null'`,
			floor: 14,
		},
		{
			name: "table grants",
			catalog: `SELECT grantee::text, privilege_type::text, table_schema::text, table_name::text,
					is_grantable::text, grantor::text
				FROM (` + pgcatalogview.TableGrants + `) v`,
			view: `SELECT grantee::text, privilege_type::text, table_schema::text, table_name::text,
					is_grantable::text, grantor::text
				FROM information_schema.role_table_grants`,
			floor: 100,
		},
		{
			name: "view check options",
			catalog: `SELECT n.nspname::text, c.relname::text, ` + pgcatalogview.ViewCheckOption("c") + `
				FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace WHERE c.relkind = 'v'`,
			view:  `SELECT table_schema::text, table_name::text, check_option::text FROM information_schema.views`,
			floor: 60,
		},
		{
			name: "owned sequences",
			catalog: `SELECT n.nspname::text, c.relname::text, a.attname::text,
					` + pgcatalogview.OwnedSequence("c.oid", "a.attnum") + `
				FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped AND n.nspname = 'public'`,
			view: `SELECT n.nspname::text, c.relname::text, a.attname::text,
					pg_get_serial_sequence(format('%I.%I', n.nspname, c.relname), a.attname)
				FROM pg_attribute a JOIN pg_class c ON c.oid = a.attrelid JOIN pg_namespace n ON n.oid = c.relnamespace
				WHERE c.relkind IN ('r', 'p') AND a.attnum > 0 AND NOT a.attisdropped AND n.nspname = 'public'`,
			floor: 30,
		},
	}
	c := qt.New(t)
	roles := newConnectingRoles(c)
	database := scratchOwnedDatabase(c, roles, everyObjectKindSQL)
	conn, err := dbschema.ConnectToDatabase(c.Context(), database)
	c.Assert(err, qt.IsNil)
	c.Cleanup(func() { dbschema.CloseAndWarn(conn) })

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			var catalogRows, onlyOneSide int

			err := conn.QueryRowContext(c.Context(), fmt.Sprintf(`
				WITH catalog_rows AS (%[1]s), view_rows AS (%[2]s)
				SELECT
					(SELECT count(*) FROM catalog_rows),
					(SELECT count(*) FROM (SELECT * FROM catalog_rows EXCEPT SELECT * FROM view_rows) only_catalog)
					+ (SELECT count(*) FROM (SELECT * FROM view_rows EXCEPT SELECT * FROM catalog_rows) only_view)`,
				test.catalog, test.view)).Scan(&catalogRows, &onlyOneSide)

			c.Assert(err, qt.IsNil)
			c.Assert(onlyOneSide, qt.Equals, 0, qt.Commentf("%d rows from the catalog", catalogRows))
			c.Assert(catalogRows >= test.floor, qt.IsTrue, qt.Commentf("%d rows from the catalog", catalogRows))
		})
	}
}
