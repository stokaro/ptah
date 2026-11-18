package pgpublicationsource_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/platform"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpublication"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/schemafile"
	"ptah.run/internal/sqlschema"
)

// readSQL reads one PostgreSQL SQL document through the bundled runtime's
// owners.
func readSQL(c *qt.C, text string) (schemamodel.Database, error) {
	c.Helper()
	db, _, err := sqlschema.ReadWith(c.Context(), []byte(text), platform.Postgres, nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	return db, err
}

// publications returns the publications a document declares, by name.
func publications(c *qt.C, db schemamodel.Database) map[string]pgpublication.DesiredPublication {
	c.Helper()
	declared := make(map[string]pgpublication.DesiredPublication)
	for _, object := range must.Must(db.FeatureObjects.All()) {
		if value, ok := object.Value.(*pgpublication.DesiredPublication); ok {
			declared[pgpublication.Name(object.Ref)] = *value
		}
	}
	return declared
}

const tables = "CREATE SCHEMA audit;\nCREATE TABLE orders (id int PRIMARY KEY);\nCREATE TABLE audit.events (id int PRIMARY KEY);\n"

// TestSQL_HappyPath pins what each statement declares: the four membership
// forms kept apart, names folded as PostgreSQL folds them or taken as quoted,
// the options each WITH names and nothing else, and the ALTER statements
// pg_dump writes after a CREATE amending it.
func TestSQL_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want map[string]pgpublication.DesiredPublication
	}{
		{name: "empty", sql: "CREATE PUBLICATION Nothing_Yet;",
			want: map[string]pgpublication.DesiredPublication{"nothing_yet": {}}},
		{name: "all tables", sql: "CREATE PUBLICATION everything FOR ALL TABLES;",
			want: map[string]pgpublication.DesiredPublication{"everything": {AllTables: true}}},
		{name: "explicit tables", sql: `CREATE PUBLICATION "Orders Feed" FOR TABLE ONLY orders, audit.events *;`,
			want: map[string]pgpublication.DesiredPublication{"Orders Feed": {Tables: []pgpublication.Member{{Table: "orders"}, {Schema: "audit", Table: "events"}}}}},
		{name: "schema wide", sql: "CREATE PUBLICATION audit_feed FOR TABLES IN SCHEMA audit, public;",
			want: map[string]pgpublication.DesiredPublication{"audit_feed": {Schemas: []string{"audit", "public"}}}},
		{name: "tables and a schema", sql: "CREATE PUBLICATION mixed FOR TABLE orders, TABLES IN SCHEMA audit;",
			want: map[string]pgpublication.DesiredPublication{"mixed": {Tables: []pgpublication.Member{{Table: "orders"}}, Schemas: []string{"audit"}}}},
		{name: "options", sql: "CREATE PUBLICATION inserts FOR TABLE orders WITH (publish = 'insert, UPDATE', publish_via_partition_root = on, publish_generated_columns = stored);",
			want: map[string]pgpublication.DesiredPublication{"inserts": {Tables: []pgpublication.Member{{Table: "orders"}},
				Publish: []string{"insert", "update"}, ViaPartitionRoot: true, GeneratedColumns: pgpublication.GeneratedColumnsStored}}},
		{name: "an empty publish set", sql: "CREATE PUBLICATION quiet WITH (publish = '');",
			want: map[string]pgpublication.DesiredPublication{"quiet": {Publish: make([]string, 0)}}},
		{name: "pg_dump amendments", sql: "CREATE PUBLICATION feed WITH (publish = 'insert');\n" +
			"ALTER PUBLICATION feed ADD TABLE ONLY public.orders;\nALTER PUBLICATION feed ADD TABLES IN SCHEMA audit;\n" +
			"ALTER PUBLICATION feed OWNER TO \"Replicator\";",
			want: map[string]pgpublication.DesiredPublication{"feed": {Tables: []pgpublication.Member{{Schema: "public", Table: "orders"}},
				Schemas: []string{"audit"}, Publish: []string{"insert"}, Owner: "Replicator"}}},
		{name: "drop and set members", sql: "CREATE PUBLICATION feed FOR TABLE orders, audit.events;\n" +
			"ALTER PUBLICATION feed DROP TABLE audit.events;\nALTER PUBLICATION feed SET TABLES IN SCHEMA audit;",
			want: map[string]pgpublication.DesiredPublication{"feed": {Schemas: []string{"audit"}}}},
		{name: "set options", sql: "CREATE PUBLICATION feed;\nALTER PUBLICATION feed SET (publish = 'delete');",
			want: map[string]pgpublication.DesiredPublication{"feed": {Publish: []string{"delete"}}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := readSQL(c, tables+test.sql)
			c.Assert(err, qt.IsNil)
			c.Assert(publications(c, db), qt.DeepEquals, test.want)
		})
	}
}

// TestSQL_FailurePath pins the statements the owner refuses by name rather
// than widen: a row filter, a column list, CURRENT_SCHEMA, an option it does
// not know, and an ALTER no declaration explains.
func TestSQL_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		sql     string
		wantErr string
	}{
		{name: "row filter", sql: "CREATE PUBLICATION p FOR TABLE orders WHERE (id > 0);",
			wantErr: `(?s).*publication p publishes table orders with a row filter; Ptah does not write a publication's row filters.*`},
		{name: "column list", sql: "CREATE PUBLICATION p FOR TABLE orders (id);",
			wantErr: `(?s).*publication p publishes a column list of table orders; Ptah does not write a publication's column lists.*`},
		{name: "current schema", sql: "CREATE PUBLICATION p FOR TABLES IN SCHEMA CURRENT_SCHEMA;",
			wantErr: `(?s).*publication p names TABLES IN SCHEMA CURRENT_SCHEMA.*`},
		{name: "unknown option", sql: "CREATE PUBLICATION p WITH (streaming = on);",
			wantErr: `(?s).*publication p: unsupported publication option "streaming".*`},
		{name: "option twice", sql: "CREATE PUBLICATION p WITH (publish = 'insert', publish = 'update');",
			wantErr: `(?s).*publication p names option publish twice.*`},
		{name: "unknown operation", sql: "CREATE PUBLICATION p WITH (publish = 'merge');",
			wantErr: `(?s).*merge.*`},
		{name: "all tables with members", sql: "CREATE PUBLICATION p FOR ALL TABLES;\nALTER PUBLICATION p ADD TABLE orders;",
			wantErr: `(?s).*publication p is FOR ALL TABLES, and ALTER PUBLICATION ADD cannot change its members.*`},
		{name: "dropping a member it lacks", sql: "CREATE PUBLICATION p FOR TABLE orders;\nALTER PUBLICATION p DROP TABLE audit.events;",
			wantErr: `(?s).*publication p does not publish table audit.events.*`},
		{name: "rename", sql: "CREATE PUBLICATION p;\nALTER PUBLICATION p RENAME TO q;",
			wantErr: `(?s).*unsupported ALTER PUBLICATION p action "RENAME".*`},
		{name: "several drops", sql: "DROP PUBLICATION p, q;",
			wantErr: `(?s).*the publication owner reads one publication per DROP PUBLICATION.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			db, err := readSQL(c, tables+test.sql)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}

// TestDocumentCoverage_HappyPath pins the claim a load makes: it describes
// every publication, whether or not a document declares one, and a schema
// split across files claims the same as one file. A claim that held only
// where a file declares a publication would merge, across files, into no
// claim at all.
func TestDocumentCoverage_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{name: "one SQL file declaring", files: map[string]string{"schema.sql": tables + "CREATE PUBLICATION feed FOR TABLE orders;\n"}},
		{name: "one SQL file declaring none", files: map[string]string{"schema.sql": tables}},
		{name: "split SQL", files: map[string]string{"a.sql": tables, "b.sql": "CREATE PUBLICATION feed FOR TABLE orders;\n"}},
		{name: "split HCL", files: map[string]string{"a.hcl": hclTables, "b.hcl": "publication \"feed\" {\n  tables = [table.public.orders]\n}\n"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			dir := c.TempDir()
			for name, text := range test.files {
				c.Assert(os.WriteFile(filepath.Join(dir, name), []byte(text), 0o600), qt.IsNil)
			}
			db, err := schemafile.LoadAll(c.Context(), []string{dir}, schemafile.Options{
				SQL: builtintest.Runtime().SQL(), HCL: builtintest.Runtime().HCL(), Dialect: platform.Postgres})
			c.Assert(err, qt.IsNil)
			c.Assert(db.FeatureCoverage.Lookup(pgpublication.PublicationKind, pgpublication.Ref("other")).State, qt.Equals, schemaext.Complete)
		})
	}
}
