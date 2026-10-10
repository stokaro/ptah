package mysqlcompare_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlcompare"
	"ptah.run/dialect/mysql/mysqldiff"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/dialect/mysql/mysqlsource"
)

var blockSizeIndex = objectidentity.NewBuilder(identifier.ForDialect("mysql")).IndexParts("", "orders", "k")

// blockSizeRequest compares one index. A nil observation leaves the current
// side without a record, as a converted declaration without a hint has.
func blockSizeRequest(target string, declared schemaext.Facets, declaredCoverage schemaext.Coverage,
	observed *mysqlschema.ObservedIndexBlockSize, observedCoverage schemaext.Coverage,
) schemaext.FacetComparisonRequest {
	request := schemaext.FacetComparisonRequest{
		Target: target, Kinds: []schemaext.Kind{mysqlschema.IndexBlockSizeKind},
		Owners:  []schemaext.ParentState{{Subject: blockSizeIndex, Desired: true, Current: true}},
		Desired: schemaext.FacetState{Records: []schemaext.FacetRecord{{Subject: blockSizeIndex, Values: declared}}, Coverage: declaredCoverage},
		Current: schemaext.FacetState{Coverage: observedCoverage},
	}
	if observed != nil {
		request.Current.Records = []schemaext.FacetRecord{{Subject: blockSizeIndex,
			Values: must.Must(mysqlschema.WithObservedIndexBlockSize(schemaext.Facets{}, *observed))}}
	}
	return request
}

func describedSource() schemaext.Coverage { return must.Must(mysqlsource.BlockSizeCoverage()) }

func describedRead() schemaext.Coverage {
	return must.Must(mysqlschema.IndexBlockSizeCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, nil))
}

// A change is reported where the index holds a retained hint other than the
// declared one, and the declarations come back as they were. A declaration
// without a hint asks for none when its source describes the hint, and keeps
// the one held when it does not. A current side without an observation holds
// what a declaration without a hint converts to on the target.
func TestIndexBlockSizeService_CompareFacets(t *testing.T) {
	declared8 := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))
	for _, test := range []struct {
		name             string
		target           string
		declared         schemaext.Facets
		declaredCoverage schemaext.Coverage
		observed         *mysqlschema.ObservedIndexBlockSize
		observedCoverage schemaext.Coverage
		want             []schemaext.FacetChange
	}{
		{name: "a different retained hint", target: "mysql", declared: declared8,
			observed: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true},
			want: []schemaext.FacetChange{{Kind: mysqlschema.IndexBlockSizeKind, Change: schemaext.ChangeRecord{Subject: blockSizeIndex,
				Value: &mysqldiff.IndexBlockSize{Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}, After: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}}}}}},
		{name: "a hint the server discards", target: "mysql", declared: declared8,
			observed: &mysqlschema.ObservedIndexBlockSize{}},
		{name: "the same hint", target: "mariadb", declared: declared8,
			observed: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 8, Retained: true}},
		{name: "none declared by a source that describes it", target: "mariadb", declaredCoverage: describedSource(),
			observed: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true},
			want: []schemaext.FacetChange{{Kind: mysqlschema.IndexBlockSizeKind, Change: schemaext.ChangeRecord{Subject: blockSizeIndex,
				Value: &mysqldiff.IndexBlockSize{Before: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}, After: &mysqlschema.DesiredIndexBlockSize{}}}}}},
		{name: "none declared by a source that does not describe it", target: "mariadb",
			observed: &mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}},
		{name: "a converted MariaDB index without a hint", target: "mariadb", declared: declared8, observedCoverage: describedRead(),
			want: []schemaext.FacetChange{{Kind: mysqlschema.IndexBlockSizeKind, Change: schemaext.ChangeRecord{Subject: blockSizeIndex,
				Value: &mysqldiff.IndexBlockSize{Before: &mysqlschema.ObservedIndexBlockSize{Retained: true}, After: &mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 8}}}}}},
		{name: "a converted MySQL index without a hint", target: "mysql", declared: declared8, observedCoverage: describedRead()},
		{name: "a current side that does not describe it", target: "mariadb", declared: declared8},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := blockSizeRequest(test.target, test.declared, test.declaredCoverage, test.observed, test.observedCoverage)

			result, err := mysqlcompare.IndexBlockSizeService{}.CompareFacets(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Changes, qt.DeepEquals, test.want)
			c.Assert(result.Undecided, qt.HasLen, 0)
			c.Assert(result.Desired.Records, qt.DeepEquals, request.Desired.Records)
		})
	}
}

// An explicit limit on either side leaves the hint undecided rather than
// planned or kept in silence.
func TestIndexBlockSizeService_ReportsLimitsAsUndecided(t *testing.T) {
	limit := []schemaext.SubjectCoverage{{Kind: mysqlschema.IndexBlockSizeKind, Subject: blockSizeIndex,
		Knowledge: schemaext.Knowledge{State: schemaext.Uninspected, Reason: "not read"}}}
	for _, test := range []struct {
		name     string
		desired  schemaext.Coverage
		observed schemaext.Coverage
		reason   string
	}{
		{name: "the declaration", desired: must.Must(mysqlschema.IndexBlockSizeCoverage(schemaext.Desired, schemaext.Knowledge{State: schemaext.Complete}, limit)),
			reason: "the desired source could not describe the index's MySQL block size"},
		{name: "the read", observed: must.Must(mysqlschema.IndexBlockSizeCoverage(schemaext.Observed, schemaext.Knowledge{State: schemaext.Complete}, limit)),
			reason: "the index's MySQL block size was not fully inspected"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := blockSizeRequest("mysql", must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8)), test.desired,
				&mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}, test.observed)

			result, err := mysqlcompare.IndexBlockSizeService{}.CompareFacets(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Changes, qt.HasLen, 0)
			c.Assert(result.Undecided, qt.DeepEquals, []schemaext.UndecidedChange{{Kind: mysqlschema.IndexBlockSizeKind, Subject: blockSizeIndex, Reason: test.reason}})
		})
	}
}

func TestIndexBlockSizeService_FailurePath(t *testing.T) {
	table := objectidentity.NewBuilder(identifier.ForDialect("mysql")).TableParts("", "orders")
	for _, test := range []struct {
		name    string
		edit    func(*schemaext.FacetComparisonRequest)
		wantIs  error
		wantErr string
	}{
		{name: "another target", edit: func(r *schemaext.FacetComparisonRequest) { r.Target = "postgres" },
			wantIs: ptaherr.ErrUnsupportedDialect, wantErr: `.*MySQL index block size comparison on "postgres"`},
		{name: "another kind", edit: func(r *schemaext.FacetComparisonRequest) { r.Kinds = []schemaext.Kind{mysqlschema.IndexKind} },
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*unsupported MySQL index block size comparison kinds`},
		{name: "a table owner", edit: func(r *schemaext.FacetComparisonRequest) { r.Owners[0].Subject = table },
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*a MySQL index block size attaches to a table's index.*`},
		{name: "a hint above the limit", edit: func(r *schemaext.FacetComparisonRequest) {
			r.Desired.Records[0].Values = must.Must(schemaext.NewFacets(&mysqlschema.DesiredIndexBlockSize{KeyBlockSize: 4294967296}))
		}, wantIs: schemaext.ErrInvalidValue, wantErr: `.*exceeds the mysql limit.*`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := blockSizeRequest("mysql", must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8)), schemaext.Coverage{},
				&mysqlschema.ObservedIndexBlockSize{KeyBlockSize: 4, Retained: true}, schemaext.Coverage{})
			test.edit(&request)

			result, err := mysqlcompare.IndexBlockSizeService{}.CompareFacets(t.Context(), request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Complete, qt.IsFalse)
		})
	}
}
