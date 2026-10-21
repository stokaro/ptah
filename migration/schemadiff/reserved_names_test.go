package schemadiff_test

import (
	"encoding/json"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mssql/mssqlschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

func reservedHistory(engine, schema, name string) catalog.ReservedName {
	return catalog.ReservedName{ProviderID: "example.org/provider", Engine: engine, Purpose: "history", Identity: objectidentity.NewBuilder(identifier.ForDialect(engine)).TablePartsVerbatim(schema, name)}
}

func TestReservedStorageDoesNotBecomeDesiredAbsence(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{ReservedNames: []catalog.ReservedName{reservedHistory("postgres", "public", "history")}}
	// Catalog snapshots retain reservations through serialization.
	data, err := json.Marshal(current)
	c.Assert(err, qt.IsNil)
	var restored catalog.Database
	c.Assert(json.Unmarshal(data, &restored), qt.IsNil)
	c.Assert(restored.ReservedNames, qt.DeepEquals, current.ReservedNames)
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDialect(c.Context(), &schemamodel.Database{}, &restored, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "history", StructName: "History"}}}
	diff, err = schemadiff.CompareWithDialect(c.Context(), desired, &restored, "postgres", runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*reserved for history storage.*`)
	c.Assert(diff, qt.IsNil)
	desired.Tables[0].Schema = "other"
	diff, err = schemadiff.CompareWithDialect(c.Context(), desired, &restored, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesAdded, qt.HasLen, 1)
}

func TestReservedStorageRefusesConflictingObservationsAndNormalization(t *testing.T) {
	for _, row := range []struct {
		name   string
		change func(*catalog.Database)
	}{
		{"managed observation", func(d *catalog.Database) { d.Tables = []catalog.Table{{Name: "history", Schema: "public"}} }},
		{"normalized collision", func(d *catalog.Database) {
			d.ReservedNames = append(d.ReservedNames, reservedHistory("postgres", "", "history"))
		}},
		{"changed normalization", func(d *catalog.Database) { d.ReservedNames[0].Identity.Name.Normalized = "different" }},
		{"wrong engine", func(d *catalog.Database) { d.ReservedNames[0].Engine = "mysql" }},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			current := &catalog.Database{ReservedNames: []catalog.ReservedName{reservedHistory("postgres", "public", "history")}}
			row.change(current)
			diff, err := schemadiff.CompareWithDialect(c.Context(), &schemamodel.Database{}, current, "postgres", must.Must(builtin.New()))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(diff, qt.IsNil)
		})
	}
}

func TestReservedStoragePreventsSchemaRemoval(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{Schemas: []catalog.Schema{{Name: "service"}}, ReservedNames: []catalog.ReservedName{reservedHistory("mysql", "service", "history")}}
	runtime := must.Must(builtin.New())
	diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &schemamodel.Database{}, current, catalog.ServerInfo{Dialect: "mysql", WholeServer: true}, nil, runtime)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(diff, qt.IsNil)
	desired := &schemamodel.Database{Schemas: []schemamodel.Schema{{Name: "service"}}}
	diff, err = schemadiff.CompareWithDatabaseInfo(c.Context(), desired, current, catalog.ServerInfo{Dialect: "mysql", WholeServer: true}, nil, runtime)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.SchemasRemoved, qt.HasLen, 0)
}

func TestReservedStorageRejectsForeignKeyReferences(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{ReservedNames: []catalog.ReservedName{reservedHistory("postgres", "service", "history")}}
	desired := &schemamodel.Database{Tables: []schemamodel.Table{{Name: "users", StructName: "User"}}, Fields: []schemamodel.Field{{StructName: "User", Name: "revision", Type: "int", Foreign: "service.history(id)"}}}
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "postgres", must.Must(builtin.New()))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*reserved for history storage.*`)
	c.Assert(diff, qt.IsNil)
}

func TestReservedStorageRejectsDiscoveredFeatureReferences(t *testing.T) {
	c := qt.New(t)
	current := boundLive()
	desired := boundDesired([]string{"orders", "invoices"}, []string{"tenant_id", "note"}, &mssqlschema.DesiredSecurityPolicy{Predicates: []mssqlschema.Predicate{boundFilter("orders"), boundFilter("invoices")}})
	reservation := reservedHistory("sqlserver", "rls", "fn")
	reservation.Identity = objectidentity.NewBuilder(identifier.ForDialect("sqlserver")).Function("rls.fn", "")
	current.ReservedNames = []catalog.ReservedName{reservation}
	diff, err := schemadiff.CompareWithDialect(c.Context(), desired, current, "sqlserver", must.Must(builtin.New()))
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*reserved for history storage.*`)
	c.Assert(diff, qt.IsNil)
	current.ReservedNames[0].Identity.Name = objectidentity.Part{Source: "unrelated", Normalized: "unrelated"}
	_, err = schemadiff.CompareWithDialect(c.Context(), desired, current, "sqlserver", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
}
