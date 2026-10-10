package mysqlconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
	"ptah.run/dialect/mysql/mysqlconvert"
	"ptah.run/dialect/mysql/mysqlschema"
)

// creationRequest declares table t with index hinted, hinted 8, and index
// plain, without a hint, under rowFormat.
func creationRequest(target, rowFormat string) schemaprojection.TableCreationRequest {
	semantics := identifier.ForDialect(target)
	return schemaprojection.TableCreationRequest{Target: target, Identifiers: semantics, Tables: []schemaprojection.TableCreationInput{{
		Subject: objectidentity.NewBuilder(semantics).TableParts("", "t"),
		Declaration: schemacapture.TableDeclaration{
			Table: schemamodel.Table{Name: "t", Overrides: map[string]map[string]string{target: {mysqlconvert.RowFormatProperty: rowFormat}}},
			Indexes: []schemamodel.Index{
				{Name: "hinted", Fields: []string{"a"}, Facets: must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, 8))},
				{Name: "plain", Fields: []string{"b"}},
			},
		},
	}}}
}

// The CREATE of a declaration leaves each index's hint retained where its
// table keeps hints: on MariaDB, and on MySQL with a compressed row format.
// An index gets the observation where it says more than its absence.
func TestCreationService_ObservesWhatTheTableKeeps(t *testing.T) {
	for _, test := range []struct {
		name, target, rowFormat string
		want                    map[string]mysqlschema.ObservedIndexBlockSize
	}{
		{"MySQL, compressed", "mysql", "COMPRESSED", map[string]mysqlschema.ObservedIndexBlockSize{
			"hinted": {KeyBlockSize: 8, Retained: true}, "plain": {Retained: true}}},
		{"MySQL, dynamic", "mysql", "DYNAMIC", map[string]mysqlschema.ObservedIndexBlockSize{
			"hinted": {KeyBlockSize: 8}}},
		{"MySQL, no row format", "mysql", "", map[string]mysqlschema.ObservedIndexBlockSize{
			"hinted": {KeyBlockSize: 8}}},
		{"MariaDB", "mariadb", "", map[string]mysqlschema.ObservedIndexBlockSize{
			"hinted": {KeyBlockSize: 8, Retained: true}}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := creationRequest(test.target, test.rowFormat)

			result, err := mysqlconvert.CreationService{}.ProjectTableCreations(t.Context(), request)

			c.Assert(err, qt.IsNil)
			c.Assert(result.Complete, qt.IsTrue)
			c.Assert(result.Tables, qt.HasLen, 1)
			c.Assert(result.Tables[0].Subject, qt.Equals, request.Tables[0].Subject)
			c.Assert(result.Tables[0].Facets, qt.HasLen, 0)
			got := make(map[string]mysqlschema.ObservedIndexBlockSize)
			for _, record := range result.Tables[0].Observed {
				observed, found, err := schemaext.FacetAs[*mysqlschema.ObservedIndexBlockSize](record.Values, mysqlschema.IndexBlockSizeKind)
				c.Assert(err, qt.IsNil)
				c.Assert(found, qt.IsTrue)
				got[record.Subject.Name.Source] = *observed
			}
			c.Assert(got, qt.DeepEquals, test.want)
			_, err = schemaprojection.AcceptTableCreations(request, result)
			c.Assert(err, qt.IsNil)
		})
	}
}

func TestCreationService_FailurePath(t *testing.T) {
	c := qt.New(t)

	result, err := mysqlconvert.CreationService{}.ProjectTableCreations(t.Context(), creationRequest("postgres", ""))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedDialect)
	c.Assert(result, qt.DeepEquals, schemaprojection.TableCreationResult{})
}
