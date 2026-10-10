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
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlast"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlplan"
	"ptah.run/dialect/mysql/mysqlschema"
)

var (
	blockSizeBuilder = objectidentity.NewBuilder(identifier.ForDialect("mysql"))
	blockSizeTable   = blockSizeBuilder.TableParts("", "orders")
	blockSizeSubject = blockSizeBuilder.IndexParts("", "orders", "k")
)

// blockSizeRequest plans the hint of index k of orders changing from 4 to 8,
// with both sides of the table captured.
func blockSizeRequest(target string) featureplan.Request {
	return featureplan.Request{
		Target: target, Identifiers: identifier.ForDialect("mysql"),
		Changes: []schemaext.ChangeRecord{{Subject: blockSizeSubject, Value: &mysqldiff.IndexBlockSize{
			Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}, After: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8},
		}}},
		Tables: []featureplan.Table{{
			Action: featureplan.AlterTable, Subject: blockSizeTable,
			Desired: schemacapture.TableDeclaration{Table: schemamodel.Table{Name: "orders"}, Indexes: []schemamodel.Index{{
				Name: "k", TableName: "orders", Fields: []string{"total"}, Comment: "lookup",
				Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8)),
			}}},
			Current: schemacapture.TableObservation{Table: catalog.Table{Name: "orders"}, Indexes: []catalog.Index{{
				Name: "k", TableName: "orders", Columns: []string{"total"}, Comment: "lookup",
				Facets: must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{}, mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true})),
			}}},
		}},
		ParentKinds: []schemaext.Kind{mysqlschema.IndexBlockSizeKind},
	}
}

// The change is one step that replaces the index with its declared
// definition: with the table copy on MySQL, in place on MariaDB, which stores
// the new hint that way. It reads the table and alters the index, and its
// impact is the payload's.
func TestIndexBlockSizeService_PlansTheReplacement(t *testing.T) {
	for _, test := range []struct {
		target    string
		tableCopy bool
		strategy  string
	}{
		{"mysql", true, "drop and add the index again in one statement with its declared definition and block size, with ALGORITHM=COPY, which rebuilds the table"},
		{"mariadb", false, "drop and add the index again in one statement with its declared definition and block size"},
	} {
		t.Run(test.target, func(t *testing.T) {
			c := qt.New(t)

			result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), blockSizeRequest(test.target))

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			replace := &mysqlast.ReplaceIndex{Index: mysqlast.Index{Name: "k", Parts: []mysqlast.IndexPart{{Column: "total"}},
				KeyBlockSize: 8, Comment: "lookup"}, TableCopy: test.tableCopy}
			id := plangraph.StepID{Owner: mysqlschema.Owner, Name: "index-block-size/0"}
			c.Assert(result.Contributions, qt.DeepEquals, []plangraph.Contribution[featureplan.Operation]{{
				Owner: mysqlschema.Owner,
				Steps: []plangraph.Step[featureplan.Operation]{{
					ID: id, Payload: featureplan.Operation{Role: ast.AlterExtension, Parent: blockSizeTable, Payload: replace},
					Effects:     []plangraph.Effect{{Subject: blockSizeTable, Action: plangraph.Read}, {Subject: blockSizeSubject, Action: plangraph.Alter}},
					Transaction: plangraph.TransactionAllowed, Impact: replace.Effect(),
				}},
			}})
			c.Assert(result.Changes, qt.DeepEquals, []featureplan.ChangePlan{{Subject: blockSizeSubject, Kind: mysqldiff.IndexBlockSizeKind,
				Strategy: test.strategy, Steps: []plangraph.StepID{id}}})
			c.Assert(result.Parents, qt.DeepEquals, []featureplan.ParentPlan{{Subject: blockSizeTable, Kind: mysqlschema.IndexBlockSizeKind,
				Action: featureplan.AlterTable, Strategy: "an index keeps the block size it holds unless a planned change, or the common plan's replacement of the index, writes the declared one"}})
		})
	}
}

// A common step that replaces the index writes the declared hint with it, so
// the owner contributes no second replacement; a common step that only drops
// or only creates the index contradicts the comparison and is refused.
func TestIndexBlockSizeService_LeavesTheCommonReplacement(t *testing.T) {
	common := func(actions ...plangraph.Action) []featureplan.CommonStep {
		var steps []featureplan.CommonStep
		for i, action := range actions {
			steps = append(steps, featureplan.CommonStep{ID: plangraph.StepID{Owner: "ptah.run/sqlserver", Name: string(rune('a' + i))},
				Effects: []plangraph.Effect{{Subject: blockSizeTable, Action: plangraph.Alter}, {Subject: blockSizeSubject, Action: action}}})
		}
		return steps
	}
	for _, test := range []struct {
		name    string
		actions []plangraph.Action
	}{
		{"the one-statement replacement", []plangraph.Action{plangraph.Alter}},
		{"a drop and a creation", []plangraph.Action{plangraph.Drop, plangraph.Create}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := blockSizeRequest("mysql")
			request.CommonSteps = common(test.actions...)

			result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Diagnostics, qt.HasLen, 0)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Changes, qt.DeepEquals, []featureplan.ChangePlan{{Subject: blockSizeSubject, Kind: mysqldiff.IndexBlockSizeKind,
				Strategy: "the common plan replaces the index, and writes it with the declared block size"}})
		})
	}
}

// Without a capture of the current table, which a rollback beside a key
// change on MySQL has, the replacement is planned from the declaration.
func TestIndexBlockSizeService_PlansWithoutACurrentCapture(t *testing.T) {
	c := qt.New(t)
	request := blockSizeRequest("mysql")
	request.Tables[0].Current = schemacapture.TableObservation{}

	result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Diagnostics, qt.HasLen, 0)
	c.Assert(result.Contributions, qt.HasLen, 1)
}

// A change that disagrees with what is captured, or that cannot be applied to
// the captured table, is refused as a completed refusal naming the change.
func TestIndexBlockSizeService_RefusesAChangeItCannotApply(t *testing.T) {
	for _, test := range []struct {
		name    string
		edit    func(*featureplan.Request)
		message string
	}{
		{name: "a declaration that disagrees", edit: func(r *featureplan.Request) {
			r.Tables[0].Desired.Indexes[0].Facets = must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 16))
		}, message: `.*disagrees with its declaration.*`},
		{name: "a held hint that disagrees", edit: func(r *featureplan.Request) {
			r.Tables[0].Current.Indexes[0].Facets = must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{}, mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 2, Retained: true}))
		}, message: `.*disagrees with the hint it holds.*`},
		{name: "a rebuilt table", edit: func(r *featureplan.Request) { r.Tables[0].Action = featureplan.RebuildTable },
			message: `.*requires a surviving table outside a rebuild.*`},
		{name: "a common step that only drops the index", edit: func(r *featureplan.Request) {
			r.CommonSteps = []featureplan.CommonStep{{ID: plangraph.StepID{Owner: "ptah.run/sqlserver", Name: "drop"},
				Effects: []plangraph.Effect{{Subject: blockSizeSubject, Action: plangraph.Drop}}}}
		}, message: `.*the common plan removes or adds .*whose block size changes.*`},
		{name: "an index the table does not declare", edit: func(r *featureplan.Request) { r.Tables[0].Desired.Indexes = nil },
			message: `.*is not declared in its captured table.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := blockSizeRequest("mysql")
			test.edit(&request)

			result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Contributions, qt.HasLen, 0)
			c.Assert(result.Diagnostics, qt.HasLen, 1)
			c.Assert(result.Diagnostics[0].Change, qt.DeepEquals, new(0))
			c.Assert(result.Diagnostics[0].Problem.Kind, qt.Equals, string(mysqldiff.IndexBlockSizeKind))
			c.Assert(result.Diagnostics[0].Problem.Message, qt.Matches, test.message)
		})
	}
}

func TestIndexBlockSizeService_RefusesAnotherTarget(t *testing.T) {
	c := qt.New(t)

	result, err := mysqlplan.IndexBlockSizeService{}.PlanFeatures(t.Context(), blockSizeRequest("postgres"))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result.Complete, qt.IsFalse)
}

// A reversal replaces the index again with the hint it held, and predicts the
// hint the forward change leaves. Reversing it again is the change.
func TestReversalService_ReverseChanges(t *testing.T) {
	c := qt.New(t)
	request := blockSizeRequest("mysql")

	reversed, err := mysqlplan.ReversalService{}.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "mysql", Changes: request.Changes})

	c.Assert(err, qt.IsNil)
	inverse := &mysqldiff.IndexBlockSize{Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true}, After: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 4}}
	c.Assert(reversed, qt.DeepEquals, []schemaext.Reversal{{
		Change:       schemaext.ChangeRecord{Subject: blockSizeSubject, Value: inverse},
		ForwardState: []schemaext.ProjectedValue{{Placement: schemaext.FacetPlacement, Kind: mysqlschema.IndexBlockSizeKind, Value: inverse.Before}},
		Strategy:     "replace the index with the block size it held",
	}})
	again, err := mysqlplan.ReversalService{}.ReverseChanges(t.Context(), schemaext.ReversalRequest{Target: "mysql", Changes: []schemaext.ChangeRecord{reversed[0].Change}})
	c.Assert(err, qt.IsNil)
	c.Assert(again[0].Change, qt.DeepEquals, request.Changes[0])
}

func TestReversalService_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name   string
		edit   func(*schemaext.ReversalRequest)
		wantIs error
	}{
		{"another target", func(r *schemaext.ReversalRequest) { r.Target = "postgres" }, ptaherr.ErrUnsupportedDialect},
		{"a table subject", func(r *schemaext.ReversalRequest) { r.Changes[0].Subject = blockSizeTable }, schemaext.ErrInvalidValue},
		{"an invalid change", func(r *schemaext.ReversalRequest) {
			r.Changes[0].Value = &mysqldiff.IndexBlockSize{Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true}, After: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}}
		}, schemaext.ErrInvalidValue},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := schemaext.ReversalRequest{Target: "mysql", Changes: blockSizeRequest("mysql").Changes}
			test.edit(&request)

			reversed, err := mysqlplan.ReversalService{}.ReverseChanges(t.Context(), request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(reversed, qt.IsNil)
		})
	}
}
