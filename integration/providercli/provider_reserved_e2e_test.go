//go:build integration

package providercli_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/migration/schemadiff"
)

func TestProviderReservedStorageSurvivesInspectionAndComparison(t *testing.T) {
	c := qt.New(t)
	host, runtime := yamlProviderCommand(c, "reserved-storage")
	connection := observedConnection(c, host, "records")
	c.Assert(connection.Writer().ExecuteSQL(c.Context(), `CREATE TABLE "service_history" ("ptah_fixture_history_v1" INTEGER NOT NULL PRIMARY KEY);`), qt.IsNil)
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.Tables, qt.HasLen, 1)
	c.Assert(observed.Tables[0].Name, qt.Equals, "records")
	c.Assert(observed.ReservedNames, qt.HasLen, 1)
	c.Assert(observed.ReservedNames[0].Identity.Name.Normalized, qt.Equals, "service_history")
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "records", StructName: "Record"}}, Fields: []schemamodel.Field{{Name: "Id", StructName: "Record", Type: "INTEGER", Primary: true}}}
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), desired, observed, connection.Info(), nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	desired.Tables[0].Name = "SERVICE_HISTORY"
	diff, err = schemadiff.CompareWithDatabaseInfo(c.Context(), desired, observed, connection.Info(), nil, runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*reserved for history storage.*`)
	c.Assert(diff, qt.IsNil)
}

func TestProviderDoesNotReserveForeignStorageByName(t *testing.T) {
	c := qt.New(t)
	host, _ := yamlProviderCommand(c, "reserved-storage")
	connection := observedConnection(c, host, "service_history")
	observed, err := connection.Reader().ReadSchema()
	c.Assert(err, qt.IsNil)
	c.Assert(observed.ReservedNames, qt.HasLen, 0)
	c.Assert(observed.Tables, qt.HasLen, 1)
	c.Assert(observed.Tables[0].Name, qt.Equals, "service_history")
}
