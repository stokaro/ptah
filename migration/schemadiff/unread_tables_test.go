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
	"ptah.run/engine/builtin"
	"ptah.run/migration/schemadiff"
)

const unreadReason = "rep is a replica table no replication of this database writes any more"

// unreadCurrent is a PostgreSQL read that found app.rep and does not describe
// it, beside the described table app.src.
func unreadCurrent() *catalog.Database {
	identity := objectidentity.NewBuilder(identifier.ForDialect("postgres")).TablePartsVerbatim("app", "rep")
	return &catalog.Database{
		Tables:       []catalog.Table{{Name: "src", Schema: "app", Columns: []catalog.Column{{Name: "id", DataType: "integer", IsPrimaryKey: true}}}},
		UnreadTables: []catalog.UnreadTable{{Identity: identity, Reason: unreadReason}},
	}
}

// srcDesired declares app.src as the read describes it.
func srcDesired() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{Name: "src", Schema: "app", StructName: "Src"}},
		Fields: []schemamodel.Field{{Name: "id", StructName: "Src", Type: "integer", Primary: true}},
	}
}

// A schema that leaves the unread table out plans nothing about it: the
// table is not one the plan drops.
func TestCompareUnreadTables_HappyPath(t *testing.T) {
	c := qt.New(t)
	diff, err := schemadiff.CompareWithDialect(c.Context(), srcDesired(), unreadCurrent(), "postgres", must.Must(builtin.New()))
	c.Assert(err, qt.IsNil)
	c.Assert(diff.TablesRemoved, qt.HasLen, 0)
	c.Assert(diff.TablesAdded, qt.HasLen, 0)
}

// A declared table at the unread table, and a foreign key that references
// it, are refused with the reader's reason as the built-in YDB engine refuses
// a table at its orphaned replica: a capability error that reads as the table
// and the reason.
func TestCompareUnreadTables_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		desired func() *schemamodel.Database
	}{
		{"a declared table", func() *schemamodel.Database {
			desired := srcDesired()
			desired.Tables = append(desired.Tables, schemamodel.Table{Name: "rep", Schema: "app", StructName: "Rep"})
			desired.Fields = append(desired.Fields, schemamodel.Field{Name: "id", StructName: "Rep", Type: "integer", Primary: true})
			return desired
		}},
		{"a field's foreign key", func() *schemamodel.Database {
			desired := srcDesired()
			desired.Fields = append(desired.Fields, schemamodel.Field{Name: "rep_id", StructName: "Src", Type: "integer", Foreign: "app.rep(id)"})
			return desired
		}},
		{"a foreign key constraint", func() *schemamodel.Database {
			desired := srcDesired()
			desired.Constraints = append(desired.Constraints, schemamodel.Constraint{Name: "src_rep", Type: "FOREIGN KEY", Table: "app.src",
				Columns: []string{"id"}, ForeignTable: "app.rep", ForeignColumns: []string{"id"}})
			return desired
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff, err := schemadiff.CompareWithDialect(c.Context(), test.desired(), unreadCurrent(), "postgres", must.Must(builtin.New()))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, `table app\.rep: `+unreadReason)
			var refusal *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &refusal)
			c.Assert(refusal.Dialect, qt.Equals, "postgres")
			c.Assert(diff, qt.IsNil)
		})
	}
}

// An unread table is a fact about the read and stays out of every document:
// a JSON copy of the read does not carry it.
func TestCompareUnreadTables_NotSerialized(t *testing.T) {
	c := qt.New(t)
	data, err := json.Marshal(unreadCurrent())
	c.Assert(err, qt.IsNil)
	var restored catalog.Database
	c.Assert(json.Unmarshal(data, &restored), qt.IsNil)
	c.Assert(restored.UnreadTables, qt.HasLen, 0)
}

func TestCompareUnreadTables_RefusesAnInvalidRecord(t *testing.T) {
	for _, test := range []struct {
		name   string
		change func(*catalog.Database)
	}{
		{"no reason", func(d *catalog.Database) { d.UnreadTables[0].Reason = "" }},
		{"another kind", func(d *catalog.Database) { d.UnreadTables[0].Identity.Kind = objectidentity.KindView }},
		{"changed normalization", func(d *catalog.Database) { d.UnreadTables[0].Identity.Name.Normalized = "different" }},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			current := unreadCurrent()
			test.change(current)
			diff, err := schemadiff.CompareWithDialect(c.Context(), srcDesired(), current, "postgres", must.Must(builtin.New()))
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, `.*invalid unread table`)
			c.Assert(diff, qt.IsNil)
		})
	}
}
