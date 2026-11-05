//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/schemaext"
	"ptah.run/dbschema"
)

// databaseKindsRead starts the database-kinds fixture, creates the table
// records in a new database, and reads it filtered to schemas.
func databaseKindsRead(c *qt.C, environment []string, schemas ...string) (*catalog.Database, error) {
	c.Helper()
	host, _ := yamlProviderEnvironment(c, "database-kinds", environment)
	connection := observedConnection(c, host, "records")
	return dbschema.ReadSchemaWithSchemasContext(c.Context(), connection, schemas)
}

// quotaObjects are the retention objects of a read, by name.
func quotaObjects(read *catalog.Database) []string {
	var names []string
	for _, ref := range read.FeatureObjects.Refs() {
		if ref.Kind == "example.org/fixture/retention" {
			names = append(names, ref.Name.Source)
		}
	}
	return names
}

// A read filtered to a schema that holds no table reports the object the
// database holds beside its scope, with complete knowledge of it, as an
// unfiltered read does.
func TestProviderDatabaseKindsRead_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		schemas []string
		tables  int
	}{
		{"a read filtered to another schema", []string{"app"}, 0},
		{"an unfiltered read", nil, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			read, err := databaseKindsRead(c, nil, test.schemas...)
			c.Assert(err, qt.IsNil)
			c.Assert(read.Tables, qt.HasLen, test.tables)
			c.Assert(quotaObjects(read), qt.DeepEquals, []string{"quota"})
			ref := read.FeatureObjects.Refs()[0]
			c.Assert(ref.Schema.Empty(), qt.IsTrue)
			c.Assert(read.FeatureCoverage.Lookup(schemaext.Kind("example.org/fixture/retention"), ref).State, qt.Equals, schemaext.Complete)
		})
	}
}

// A provider that does not list the feature leaves the object out of a
// filtered read, as a provider of an earlier contract does, and still
// reports it on an unfiltered read.
func TestProviderDatabaseKindsRead_Unlisted(t *testing.T) {
	c := qt.New(t)
	filtered, err := databaseKindsRead(c, []string{"FIXTURE_DATABASE_KINDS=silent"}, "app")
	c.Assert(err, qt.IsNil)
	c.Assert(quotaObjects(filtered), qt.HasLen, 0)
	whole, err := databaseKindsRead(c, []string{"FIXTURE_DATABASE_KINDS=silent"})
	c.Assert(err, qt.IsNil)
	c.Assert(quotaObjects(whole), qt.DeepEquals, []string{"quota"})
}
