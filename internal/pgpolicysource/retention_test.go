package pgpolicysource_test

import (
	"context"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/annotation"
	"ptah.run/core/schemaext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/pgpolicysource"
)

func retentionRequest(c *qt.C, enabled, forced bool, comment string) annotation.RetentionRequest {
	c.Helper()
	return annotation.RetentionRequest{
		Declarations: []annotation.Declaration{
			{Directive: pgpolicysource.EnableDirective, Struct: "Item", Attributes: map[string]string{"force": "true", "comment": "restricted table"}},
			{Directive: pgpolicysource.PolicyDirective, Struct: "Item", Attributes: map[string]string{"name": "tenant", "for": "select", "to": "reader", "using": "tenant_id = 7", "as": "restrictive", "comment": "tenant policy"}},
		},
		Tables:         annotation.Tables{{Name: "items", Schema: "app", Struct: "Item"}},
		ExportedTables: []annotation.RetainedTable{{Table: annotation.Table{Name: "items", Schema: "app", Struct: "ExportedItem"}, Facets: must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: enabled, Forced: forced, Comment: comment}))}},
		Objects:        must.Must(schemaext.NewObjects(schemaext.Object{Ref: pgpolicy.PolicyRef("app", "items", "tenant"), Value: &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandSelect, Roles: []pgpolicy.RoleSelector{{Name: "reader"}}, Using: new("tenant_id = 7"), Composition: pgpolicy.Restrictive, Comment: "tenant policy"}})),
	}
}

func TestAnnotationsRetained_IgnoresSourcePlacement(t *testing.T) {
	c := qt.New(t)
	request := retentionRequest(c, true, true, "restricted table")
	owners := must.Must(annotation.NewSet(pgpolicysource.Annotations()))
	result, err := owners.Retained(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []bool{true, true})
	value, found, err := schemaext.FacetAs[*pgpolicy.DesiredTableState](request.ExportedTables[0].Facets, pgpolicy.TableStateKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	c.Assert(value.StructName, qt.Equals, "")
}

func TestAnnotationsRetained_RefusesChangedTableSecurity(t *testing.T) {
	rows := []struct {
		name            string
		enabled, forced bool
		comment         string
	}{
		{name: "disabled", forced: true, comment: "restricted table"},
		{name: "not forced", enabled: true, comment: "restricted table"},
		{name: "comment", enabled: true, forced: true, comment: "different"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			owners := must.Must(annotation.NewSet(pgpolicysource.Annotations()))
			result, err := owners.Retained(context.Background(), retentionRequest(c, row.enabled, row.forced, row.comment))
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []bool{false, true})
		})
	}
}

func TestAnnotationsRetained_RefusesChangedPolicy(t *testing.T) {
	rows := []struct{ name, attribute, value string }{
		{"command", "for", "all"}, {"roles", "to", "writer"}, {"predicate", "using", "tenant_id = 8"},
		{"composition", "as", "permissive"}, {"comment", "comment", "changed"},
		{"name", "name", "another"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			request := retentionRequest(c, true, true, "restricted table")
			request.Declarations[1].Attributes[row.attribute] = row.value
			owners := must.Must(annotation.NewSet(pgpolicysource.Annotations()))
			result, err := owners.Retained(context.Background(), request)
			c.Assert(err, qt.IsNil)
			c.Assert(result, qt.DeepEquals, []bool{true, false})
		})
	}
}

func TestAnnotationsRetained_RefusesLostScope(t *testing.T) {
	c := qt.New(t)
	request := retentionRequest(c, true, true, "restricted table")
	request.Declarations[0].Targets = []string{"postgresql"}
	request.Declarations[1].Targets = []string{"postgresql"}
	owners := must.Must(annotation.NewSet(pgpolicysource.Annotations()))
	result, err := owners.Retained(context.Background(), request)
	c.Assert(err, qt.IsNil)
	c.Assert(result, qt.DeepEquals, []bool{false, false})
}
