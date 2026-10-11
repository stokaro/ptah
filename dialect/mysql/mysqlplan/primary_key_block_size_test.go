package mysqlplan_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/catalog"
	"ptah.run/core/ast"
	"ptah.run/core/featureplan"
	"ptah.run/core/objectidentity"
	"ptah.run/core/plangraph"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlast"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlplan"
	"ptah.run/dialect/mysql/mysqlschema"
)

var keySubject = objectidentity.PrimaryKeyOf(blockSizeTable)

// keyRequest plans the hint of the primary key of orders changing from 4 to
// 8, with both sides of the table captured: the key declared on the table and
// the PRIMARY KEY constraint MySQL reports, named PRIMARY.
func keyRequest(target string) featureplan.Request {
	return featureplan.Request{
		Target: target, Identifiers: identifier.ForDialect("mysql"),
		Changes: []schemaext.ChangeRecord{{Subject: keySubject, Value: &mysqldiff.IndexBlockSize{
			Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}, After: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8},
		}}},
		Tables: []featureplan.Table{{
			Action: featureplan.AlterTable, Subject: blockSizeTable,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "orders", PrimaryKey: []string{"id"}, PrimaryKeyComment: "lookup",
				PrimaryKeyFacets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "orders"}, Constraints: []catalog.Constraint{{
				Name: "PRIMARY", TableName: "orders", Type: "PRIMARY KEY", ColumnNames: []string{"id"}, Comment: "lookup",
				Facets: must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{}, mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true})),
			}}},
		}},
		ParentKinds: []schemaext.Kind{mysqlschema.IndexBlockSizeKind},
	}
}

// The change of a primary key's hint is one step that replaces the key with
// its declared definition: with the table copy on MySQL, which keeps the old
// hint through an in-place rebuild of an otherwise unchanged key, and in
// place on MariaDB, which stores the new one that way. It reads the table and
// alters the key.
func TestIndexBlockSizeService_PlansThePrimaryKeyReplacement(t *testing.T) {
	for _, test := range []struct {
		target    string
		tableCopy bool
		strategy  string
	}{
		{"mysql", true, "drop and add the primary key again in one statement with its declared definition and block size, with ALGORITHM=COPY, which rebuilds the table"},
		{"mariadb", false, "drop and add the primary key again in one statement with its declared definition and block size"},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)

			result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), keyRequest(test.target))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			replace := &mysqlast.ReplaceIndex{Index: mysqlast.Index{Name: mysqlast.PrimaryKeyName, Primary: true,
				Parts: []mysqlast.IndexPart{{Column: "id"}}, KeyBlockSize: 8, Comment: "lookup"}, TableCopy: test.tableCopy}
			id := plangraph.StepID{Owner: mysqlschema.Owner, Name: "index-block-size/0"}
			c.Assert(result.Contributions, qt.DeepEquals, []plangraph.Contribution[featureplan.Operation]{{
				Owner: mysqlschema.Owner,
				Steps: []plangraph.Step[featureplan.Operation]{{
					ID: id, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: blockSizeTable, Payload: replace},
					Effects:     []plangraph.Effect{{Subject: blockSizeTable, Action: plangraph.Read}, {Subject: keySubject, Action: plangraph.Alter}},
					Transaction: plangraph.TransactionAllowed, Impact: replace.Effect(),
				}},
			}})
			c.Assert(result.Changes, qt.DeepEquals, []featureplan.ChangePlan{{Subject: keySubject, Kind: mysqldiff.IndexBlockSizeKind,
				Strategy: test.strategy, Steps: []plangraph.StepID{id}}})
		})
	}
}

// A key the table declares as a PRIMARY KEY constraint is replaced with the
// constraint's definition and hint, whatever it is named.
func TestIndexBlockSizeService_PlansAConstraintKey(t *testing.T) {
	c := qt.New(t)
	request := keyRequest("mariadb")
	request.Tables[0].Desired.Table = schemamodel.Table{Name: "orders"}
	request.Tables[0].Desired.Constraints = []schemamodel.Constraint{{Name: "pk_orders", Type: "PRIMARY KEY", Columns: []string{"id", "region"},
		UsingMethod: "HASH", Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))}}

	result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Contributions[0].Steps[0].Payload.Payload, qt.DeepEquals, &mysqlast.ReplaceIndex{Index: mysqlast.Index{
		Name: mysqlast.PrimaryKeyName, Primary: true, Type: "HASH", Parts: []mysqlast.IndexPart{{Column: "id"}, {Column: "region"}}, KeyBlockSize: 8}})
}

// Where the common plan already replaces the key, which on MySQL and MariaDB
// is the one-statement DROP PRIMARY KEY, ADD PRIMARY KEY of a key that
// changed for another reason, that statement writes the declared hint and the
// owner adds nothing.
func TestIndexBlockSizeService_LeavesTheCommonKeyReplacement(t *testing.T) {
	c := qt.New(t)
	request := keyRequest("mysql")
	request.CommonSteps = []featureplan.CommonStep{{ID: plangraph.StepID{Owner: "ptah.run/sqlserver", Name: "replace"},
		Effects: []plangraph.Effect{{Subject: keySubject, Action: plangraph.Alter}}}}

	result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 0)
	c.Assert(result.Changes, qt.DeepEquals, []featureplan.ChangePlan{{Subject: keySubject, Kind: mysqldiff.IndexBlockSizeKind,
		Strategy: "the common plan replaces the primary key, and writes it with the declared block size"}})
}

// A change of a key's hint that disagrees with what is captured, or that has
// no key to replace, is refused as a completed refusal naming the change.
func TestIndexBlockSizeService_RefusesAKeyChangeItCannotApply(t *testing.T) {
	for _, test := range []struct {
		name    string
		edit    func(*featureplan.Request)
		message string
	}{
		{name: "a declaration that disagrees", edit: func(r *featureplan.Request) {
			r.Tables[0].Desired.Table.PrimaryKeyFacets = must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 16))
		}, message: `.*disagrees with its declaration.*`},
		{name: "a held hint that disagrees", edit: func(r *featureplan.Request) {
			r.Tables[0].Current.Constraints[0].Facets = must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{},
				mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 2, Retained: true}))
		}, message: `.*disagrees with the hint it holds.*`},
		{name: "no captured key", edit: func(r *featureplan.Request) { r.Tables[0].Current.Constraints = nil },
			message: `.*primary key orders.PRIMARY KEY is not captured in its current table.*`},
		{name: "no declared key", edit: func(r *featureplan.Request) { r.Tables[0].Desired.Table.PrimaryKey = nil },
			message: `.*table "orders" declares no primary key.*`},
		{name: "a common step that only drops the key", edit: func(r *featureplan.Request) {
			r.CommonSteps = []featureplan.CommonStep{{ID: plangraph.StepID{Owner: "ptah.run/sqlserver", Name: "drop"},
				Effects: []plangraph.Effect{{Subject: keySubject, Action: plangraph.Drop}}}}
		}, message: `.*the common plan removes or adds .*whose block size changes.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := keyRequest("mysql")
			test.edit(&request)

			result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Matches, test.message)
		})
	}
}

// A reversal replaces the key again with the hint it held.
func TestReversalService_ReversesAPrimaryKey(t *testing.T) {
	c := qt.New(t)

	reversed, err := mysqlplan.ReversalService{}.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "mysql", Changes: keyRequest("mysql").Changes})

	c.Assert(err, qt.IsNil)
	c.Assert(reversed, qt.HasLen, 1)
	c.Assert(reversed[0].Change.Subject, qt.Equals, keySubject)
	c.Assert(reversed[0].Strategy, qt.Equals, "replace the primary key with the block size it held")
}
