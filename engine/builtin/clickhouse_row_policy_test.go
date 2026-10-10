package builtin_test

import (
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chdiff"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/engine/builtin"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
	"ptah.run/migration/schemadiff/difftypes"
)

// The row policy owner is registered dormant: nothing in this repository's
// sources or readers produces the model yet, so these tests build it
// themselves and drive it through the bundled runtime the commands use.

var heldPolicy = chschema.ObservedRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Permissive,
	Roles: chschema.RoleSelection{Names: []string{"alice"}}}

// ordersWithPolicy declares table orders and, when policy is set, the row
// policy tenant on it, with complete coverage of the model.
func ordersWithPolicy(policy *chschema.DesiredRowPolicy) *schemamodel.Database {
	database := &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Orders", Name: "orders"}},
		Fields: []schemamodel.Field{
			{StructName: "Orders", Name: "id", Type: "UInt64", Primary: true},
			{StructName: "Orders", Name: "tenant", Type: "UInt8"},
		},
		FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil)),
	}
	if policy != nil {
		database.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(chschema.DesiredRowPolicyObject(chschema.RowPolicyRef("", "orders", "tenant"), *policy))))
	}
	schemamodel.Finalize(database)
	return database
}

// The owner's changes plan through the bundled runtime to one statement each.
func TestClickHouseRowPolicyChangesPlanThroughTheRuntime(t *testing.T) {
	restrictive := &chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{Names: []string{"alice"}}}
	for _, test := range []struct {
		name   string
		change *chdiff.RowPolicy
		want   string
	}{
		{"a creation", chdiff.NewRowPolicy(nil, restrictive), "CREATE ROW POLICY `tenant` ON `orders` USING (tenant = 1) AS RESTRICTIVE TO `alice`"},
		{"a change in place", chdiff.NewRowPolicy(&heldPolicy, restrictive), "ALTER ROW POLICY `tenant` ON `orders` USING (tenant = 1) AS RESTRICTIVE TO `alice`"},
		{"a drop", chdiff.NewRowPolicy(&heldPolicy, nil), "DROP ROW POLICY `tenant` ON `orders`"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			diff := &difftypes.SchemaDiff{FeatureChanges: []schemaext.ChangeRecord{{Subject: chschema.RowPolicyRef("", "orders", "tenant"), Value: test.change}}}

			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), must.Must(builtin.New()), diff, platform.ClickHouse, planner.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{test.want})
		})
	}
}

// A table created with a declared policy is followed by the policy, on the
// render surface and on the plan surface, because the comparison leaves the
// children of a created table to the table's own transition. That holds when
// the connection names a database the declaration leaves out, too.
func TestClickHouseCreatedTableCreatesItsRowPolicies(t *testing.T) {
	for _, test := range []struct {
		name   string
		server catalog.ServerInfo
	}{
		{"no database", catalog.ServerInfo{Dialect: platform.ClickHouse}},
		{"the connection's database", clickHouseServer()},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := ordersWithPolicy(&chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Roles: chschema.RoleSelection{All: true, Except: []string{"admin"}}})
			runtime := must.Must(builtin.New())
			const policy = "CREATE ROW POLICY `tenant` ON `orders` USING (tenant = 1) AS PERMISSIVE TO ALL EXCEPT `admin`"

			rendered, err := builtin.GetOrderedCreateStatements(declared, platform.ClickHouse)
			c.Assert(err, qt.IsNil)
			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, &catalog.Database{}, test.server, nil, runtime)
			c.Assert(err, qt.IsNil)
			planned, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.ClickHouse, planner.Options{})
			c.Assert(err, qt.IsNil)

			for _, statements := range [][]string{rendered, planned} {
				script := strings.Join(statements, "\n")
				c.Assert(script, qt.Contains, policy)
				c.Assert(strings.Index(script, "CREATE TABLE") < strings.Index(script, "CREATE ROW POLICY"), qt.IsTrue, qt.Commentf("%s", script))
			}
		})
	}
}

// ClickHouse keeps a row policy when its table is dropped, so a plan that
// drops a table drops the policies the read captured under it.
func TestClickHouseDroppedTableDropsItsRowPolicies(t *testing.T) {
	c := qt.New(t)
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "orders", Columns: []catalog.Column{{Name: "id", DataType: "UInt64"}},
			Facets: must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}))}},
		FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("", "orders", "tenant"), heldPolicy)))),
		FeatureCoverage: completeClickHouseObservation(),
	}
	desired := &schemamodel.Database{FeatureCoverage: must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, nil))}
	runtime := must.Must(builtin.New())

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), desired, current, catalog.ServerInfo{Dialect: platform.ClickHouse}, nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.ClickHouse, planner.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(diff.FeatureChanges, qt.HasLen, 0)
	c.Assert(statements, qt.Contains, "DROP ROW POLICY `tenant` ON `orders`")
}

// A policy that leaves the database to the connection finds its table in the
// connection's database, whichever side left it out: the held policy is
// changed in place rather than refused as an object without a table. A
// reference that took another default for the one left out takes the
// connection's instead.
func TestClickHouseRowPolicyFindsItsTableInTheConnectionsDatabase(t *testing.T) {
	elsewhere := identifier.ForDialect(platform.ClickHouse)
	elsewhere.DefaultSchema = "default"
	for _, test := range []struct {
		name              string
		declared, current objectidentity.ID
	}{
		{"a read that names the database", chschema.RowPolicyRef("", "orders", "tenant"), chschema.RowPolicyRef("app", "orders", "tenant")},
		{"a read that leaves it out", chschema.RowPolicyRef("", "orders", "tenant"), chschema.RowPolicyRef("", "orders", "tenant")},
		{"a read resolved under another default", chschema.RowPolicyRef("", "orders", "tenant"), chschema.RowPolicyRefWith(elsewhere, "", "orders", "tenant")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := ordersWithPolicy(nil)
			declared.FeatureObjects = must.Must(schemaext.NewObjects(must.Must(chschema.DesiredRowPolicyObject(test.declared,
				chschema.DesiredRowPolicy{Filter: new("tenant = 1"), Composition: chschema.Restrictive, Roles: chschema.RoleSelection{Names: []string{"alice"}}}))))
			current := &catalog.Database{
				Tables: []catalog.Table{{Name: "orders", Columns: []catalog.Column{{Name: "id", DataType: "UInt64", IsPrimaryKey: true}, {Name: "tenant", DataType: "UInt8"}},
					Facets: must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}))}},
				FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(chschema.ObservedRowPolicyObject(test.current, heldPolicy)))),
				FeatureCoverage: completeClickHouseObservation(),
			}
			runtime := must.Must(builtin.New())

			diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, clickHouseServer(), nil, runtime)
			c.Assert(err, qt.IsNil)
			statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.ClickHouse, planner.Options{})

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, []string{"ALTER ROW POLICY `tenant` ON `orders` USING (tenant = 1) AS RESTRICTIVE TO `alice`"})
		})
	}
}

// A source's limit on one policy follows the policy into the connection's
// database: the policy it could not describe is kept rather than dropped.
func TestClickHouseRowPolicyLimitFollowsThePolicy(t *testing.T) {
	c := qt.New(t)
	declared := ordersWithPolicy(nil)
	declared.FeatureCoverage = must.Must(chschema.RowPolicyCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete},
		[]schemaext.SubjectCoverage{{Kind: chschema.RowPolicyKind, Subject: chschema.RowPolicyRef("", "orders", "tenant"),
			Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "the source cannot state this policy"}}}))
	current := &catalog.Database{
		Tables: []catalog.Table{{Name: "orders", Columns: []catalog.Column{{Name: "id", DataType: "UInt64", IsPrimaryKey: true}, {Name: "tenant", DataType: "UInt8"}},
			Facets: must.Must(schemaext.NewFacets(&chschema.ObservedTable{Engine: "MergeTree", OrderBy: "id", PrimaryKey: "id"}))}},
		FeatureObjects:  must.Must(schemaext.NewObjects(must.Must(chschema.ObservedRowPolicyObject(chschema.RowPolicyRef("app", "orders", "tenant"), heldPolicy)))),
		FeatureCoverage: completeClickHouseObservation(),
	}
	runtime := must.Must(builtin.New())

	diff, err := schemadiff.CompareWithDatabaseInfo(t.Context(), declared, current, clickHouseServer(), nil, runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatementsWithOptions(t.Context(), runtime, diff, platform.ClickHouse, planner.Options{})

	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 0)
}

// The render surface refuses a declared policy it cannot write: one without a
// table, which is a database-wide policy, and one whose user list ClickHouse
// cannot express. The first is refused by schema validation, whose refusal
// carries the message but not the sentinel.
func TestClickHouseRowPolicyRender_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		ref    objectidentity.ID
		policy chschema.DesiredRowPolicy
		want   string
	}{
		{"a database-wide policy", chschema.RowPolicyRef("app", "", "tenant"), chschema.DesiredRowPolicy{}, `(?s).*database-wide policy \(ON db\.\*\) is not supported.*`},
		{"exceptions without ALL", chschema.RowPolicyRef("", "orders", "tenant"), chschema.DesiredRowPolicy{Roles: chschema.RoleSelection{Except: []string{"admin"}}}, `(?s).*a row policy names exceptions only beside TO ALL.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			declared := ordersWithPolicy(nil)
			declared.FeatureObjects = must.Must(schemaext.NewObjects(schemaext.Object{Ref: test.ref, Value: &test.policy}))

			statements, err := builtin.GetOrderedCreateStatements(declared, platform.ClickHouse)

			c.Assert(err, qt.ErrorMatches, test.want)
			c.Assert(statements, qt.IsNil)
		})
	}
}

// clickHouseServer is a connection to the database app, which a declaration
// that names no database is compared in.
func clickHouseServer() catalog.ServerInfo {
	semantics := identifier.ForDialect(platform.ClickHouse)
	semantics.DefaultSchema = "app"
	return catalog.ServerInfo{Dialect: platform.ClickHouse, Schema: "app", IdentifierSemantics: semantics}
}

// completeClickHouseObservation is the coverage a ClickHouse read claims for
// storage settings, skipping indexes and row policies.
func completeClickHouseObservation() schemaext.Coverage {
	var kinds []schemaext.KindCoverage
	for _, model := range must.Must(builtin.New()).Codecs().Definitions() {
		if slices.Contains([]schemaext.Kind{chschema.TableKind, chschema.IndexKind, chschema.RowPolicyKind}, model.Kind) && model.Representation == schemaext.Observed {
			kinds = append(kinds, schemaext.KindCoverage{Model: model, Knowledge: schemaext.Knowledge{State: schemaext.Complete}})
		}
	}
	return must.Must(schemaext.NewCoverage(schemaext.Observed, kinds, nil))
}
