package goschematogo_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/convert/goschematogo"
	"ptah.run/internal/pgpolicysource"
)

// rowSecuritySchema declares orders with both switches on, scoped to
// PostgreSQL, and two policies: one with every value away from its default and
// a binding to two targets, one with every value left to PostgreSQL. Table
// items declares no policy and both switches off.
func rowSecuritySchema() *schemamodel.Database {
	tenant := must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "tenant"), pgpolicy.DesiredPolicy{
		Command: pgpolicy.CommandUpdate, Roles: []pgpolicy.RoleSelector{{Name: "Reader"}, {Keyword: pgpolicy.CurrentUser}},
		Using: new("tenant_id = 1"), WithCheck: new("tenant_id > 0"), Composition: pgpolicy.Restrictive, Comment: "tenant rows",
	}))
	tenant.Targets = []string{"postgres", "cockroachdb"}
	switches := must.Must(must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Enabled: true, Forced: true, Comment: "isolated"})).
		WithTargetScope(pgpolicy.TableStateKind, "postgres"))
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Order", Name: "orders", Facets: switches}, {StructName: "Item", Name: "items",
			Facets: must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{}))}},
		Fields: []schemamodel.Field{
			{StructName: "Order", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
			{StructName: "Item", FieldName: "ID", Name: "id", Type: "INTEGER", Primary: true},
		},
		FeatureObjects: must.Must(schemaext.NewObjects(tenant,
			must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "orders", "all_rows"), pgpolicy.DesiredPolicy{})))),
		FeatureCoverage: must.Must(pgpolicy.CompleteCoverage(schemaext.Desired)),
	}
}

func renderOneFile(c *qt.C, db *schemamodel.Database) (string, error) {
	c.Helper()
	files, err := goschematogo.Render(c.Context(), db, goschematogo.Options{SingleFile: true, Dialect: "postgres", Runtime: must.Must(builtin.New())})
	var source strings.Builder
	for _, file := range files {
		source.Write(file.Data)
	}
	return source.String(), err
}

// TestRender_WritesRowSecurityAsAnnotations pins the annotations a table's
// policies and switches are written as, beside the table: each value a
// declaration states, nothing it leaves out, the role list in its canonical
// spelling, and each binding as the dialects it names.
func TestRender_WritesRowSecurityAsAnnotations(t *testing.T) {
	c := qt.New(t)

	source, err := renderOneFile(c, rowSecuritySchema())

	c.Assert(err, qt.IsNil)
	lines := []string{
		`//ptah:schema:rls:policy name="all_rows" table="orders"`,
		`//ptah:schema:rls:policy name="tenant" table="orders" for="UPDATE" to="CURRENT_USER, \"Reader\"" using="tenant_id = 1" ` +
			`with_check="tenant_id > 0" as="RESTRICTIVE" comment="tenant rows" dialects="cockroachdb,postgres"`,
		`//ptah:schema:rls:enable table="orders" force="true" comment="isolated" dialects="postgres"`,
		`//ptah:schema:table name="orders"`,
	}
	positions := make([]int, len(lines))
	for i, line := range lines {
		positions[i] = strings.Index(source, line)
		c.Assert(positions[i] >= 0, qt.IsTrue, qt.Commentf("%s is missing from\n%s", line, source))
	}
	c.Assert(positions[0] < positions[1] && positions[1] < positions[2] && positions[2] < positions[3], qt.IsTrue, qt.Commentf("%s", source))
	c.Assert(strings.Count(source, "ptah:schema:rls:"), qt.Equals, 3, qt.Commentf("items declares no row-level security:\n%s", source))
}

// TestRender_RowSecurity_FailurePath pins what the export refuses rather than
// leaves out: switches an annotation cannot say, a policy on a table the
// export does not hold, and coverage the source could not describe.
func TestRender_RowSecurity_FailurePath(t *testing.T) {
	forcedOnly := func() *schemamodel.Database {
		db := rowSecuritySchema()
		db.Tables[0].Facets = must.Must(schemaext.NewFacets(&pgpolicy.DesiredTableState{Forced: true}))
		return db
	}
	strayPolicy := func() *schemamodel.Database {
		db := rowSecuritySchema()
		db.FeatureObjects = must.Must(db.FeatureObjects.With(must.Must(pgpolicy.DesiredPolicyObject(pgpolicy.PolicyRef("", "invoices", "p"), pgpolicy.DesiredPolicy{}))))
		return db
	}
	uninspected := func() *schemamodel.Database {
		db := rowSecuritySchema()
		db.FeatureCoverage = must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}, nil))
		return db
	}
	tests := []struct {
		name     string
		database func() *schemamodel.Database
		want     string
	}{
		{name: "force without enable", database: forcedOnly, want: `.*cannot force row-level security on table orders without enabling it`},
		{name: "a policy on a table the export does not hold", database: strayPolicy, want: `.*policy .* names a table the export does not declare`},
		{name: "a model the source could not describe", database: uninspected, want: `.*cannot be exported without losing its coverage record: uninspected not read`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			source, err := renderOneFile(c, test.database())

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(source, qt.Equals, "")
		})
	}
}

// TestRender_WritesTheSwitchesASourceLeftUnmanaged pins that a table whose
// declaration names policies and leaves its switches to the database is
// written as its policies alone, which a source reads back the same way.
func TestRender_WritesTheSwitchesASourceLeftUnmanaged(t *testing.T) {
	c := qt.New(t)
	db := rowSecuritySchema()
	db.Tables[0].Facets = schemaext.Facets{}
	policies := must.Must(pgpolicy.Coverage(pgpolicy.PolicyKind, schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))
	db.FeatureCoverage = must.Must(policies.Combine(must.Must(pgpolicy.Coverage(pgpolicy.TableStateKind, schemaext.Desired,
		schemaext.Knowledge{State: schemaext.Complete}, []schemaext.SubjectCoverage{{Kind: pgpolicy.TableStateKind,
			Subject: pgpolicy.Table(pgpolicy.PolicyRef("", "orders", "x")), Knowledge: pgpolicysource.UnmanagedSwitches()}}))))

	source, err := renderOneFile(c, db)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Count(source, "ptah:schema:rls:policy"), qt.Equals, 2)
	c.Assert(source, qt.Not(qt.Contains), "ptah:schema:rls:enable")
}
