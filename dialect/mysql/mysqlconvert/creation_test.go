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

// keyCreation declares table t with a primary key and, for a hinted key, the
// hint as the MySQL owner's facet: on the table, or on a PRIMARY KEY
// constraint, or on neither for a key its fields declare.
func keyCreation(target, rowFormat, spelling string, size uint64) schemaprojection.TableCreationRequest {
	request := creationRequest(target, rowFormat)
	declaration := &request.Tables[0].Declaration
	declaration.Indexes = nil
	declaration.Fields = []schemamodel.Field{{Name: "id", Type: "INT", Primary: spelling == "fields"}}
	hint := must.Must(mysqlschema.WithIndexBlockSize(schemaext.Facets{}, size))
	declaration.Table.PrimaryKey = map[string][]string{"table": {"id"}}[spelling]
	declaration.Table.PrimaryKeyFacets = map[string]schemaext.Facets{"table": hint}[spelling]
	declaration.Constraints = map[string][]schemamodel.Constraint{"constraint": {{Name: "pk_t", Type: "PRIMARY KEY", Columns: []string{"id"},
		Facets: hint}}}[spelling]
	return request
}

// The CREATE of a declaration leaves the primary key's hint as it leaves an
// index's, where the declaration converts the key to a PRIMARY KEY
// constraint: one declared on the table or as a constraint. A key its fields
// declare converts to no constraint, so it gets no observation.
func TestCreationService_ObservesThePrimaryKey(t *testing.T) {
	for _, test := range []struct {
		name, target, rowFormat, spelling string
		size                              uint64
		want                              []mysqlschema.ObservedIndexBlockSize
	}{
		{"MySQL, compressed, a hint", "mysql", "COMPRESSED", "table", 4, []mysqlschema.ObservedIndexBlockSize{{KeyBlockSize: 4, Retained: true}}},
		{"MySQL, compressed, no hint", "mysql", "COMPRESSED", "table", 0, []mysqlschema.ObservedIndexBlockSize{{Retained: true}}},
		{"MySQL, dynamic, no hint", "mysql", "DYNAMIC", "table", 0, nil},
		{"MariaDB, a constraint's hint", "mariadb", "", "constraint", 2, []mysqlschema.ObservedIndexBlockSize{{KeyBlockSize: 2, Retained: true}}},
		{"MySQL, compressed, a key of fields", "mysql", "COMPRESSED", "fields", 0, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			request := keyCreation(test.target, test.rowFormat, test.spelling, test.size)

			result, err := mysqlconvert.CreationService{}.ProjectTableCreations(t.Context(), request)

			c.Assert(err, qt.IsNil)
			var got []mysqlschema.ObservedIndexBlockSize
			for _, record := range result.Tables[0].Observed {
				c.Assert(record.Subject, qt.Equals, objectidentity.PrimaryKeyOf(request.Tables[0].Subject))
				got = append(got, observedHint(c, record.Values))
			}
			c.Assert(got, qt.DeepEquals, test.want)
			_, err = schemaprojection.AcceptTableCreations(request, result)
			c.Assert(err, qt.IsNil)
		})
	}
}

// observedHint is the observation facets hold.
func observedHint(c *qt.C, facets schemaext.Facets) mysqlschema.ObservedIndexBlockSize {
	c.Helper()
	observed, found, err := schemaext.FacetAs[*mysqlschema.ObservedIndexBlockSize](facets, mysqlschema.IndexBlockSizeKind)
	c.Assert(err, qt.IsNil)
	c.Assert(found, qt.IsTrue)
	return *observed
}
