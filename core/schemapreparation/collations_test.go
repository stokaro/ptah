package schemapreparation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
)

// collationsRequest is one table declaring the column name with no collation
// and the column code with its own, and its reply stating the collation name
// inherits and the table's default collation.
func collationsRequest() (schemapreparation.Request, schemapreparation.Result) {
	request := schemapreparation.Request{Tables: []schemapreparation.Table{{Desired: schemacapture.TableDeclaration{
		Table:  schemamodel.Table{Name: "events", Collate: "utf8mb4_unicode_ci"},
		Fields: []schemamodel.Field{{Name: "name", Type: "varchar(20)"}, {Name: "code", Type: "varchar(8)", Collate: "utf8mb4_bin"}},
	}}}}
	reply := request.Clone()
	reply.Tables[0].InheritedCollations = map[string]string{"name": "utf8mb4_unicode_ci"}
	reply.Tables[0].DefaultCollation = "utf8mb4_unicode_ci"
	return request, schemapreparation.Result{Complete: true, Tables: reply.Tables}
}

func TestAcceptCollations_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := collationsRequest()

	capture, err := schemapreparation.Accept(request, result)

	c.Assert(err, qt.IsNil)
	c.Assert(capture.Prepared[0].InheritedCollations, qt.DeepEquals, map[string]string{"name": "utf8mb4_unicode_ci"})
	c.Assert(capture.Prepared[0].DefaultCollation, qt.Equals, "utf8mb4_unicode_ci")
	c.Assert(capture.Source[0].InheritedCollations, qt.IsNil)
	c.Assert(capture.Source[0].DefaultCollation, qt.Equals, "")
	// The declaration keeps naming no collation, and the capture keeps its own
	// copy of the stated ones.
	c.Assert(capture.Prepared[0].Desired.Fields[0].Collate, qt.Equals, "")
	result.Tables[0].InheritedCollations["name"] = "changed"
	c.Assert(capture.Prepared[0].InheritedCollations["name"], qt.Equals, "utf8mb4_unicode_ci")
	clone := capture.Clone()
	clone.Prepared[0].InheritedCollations["name"] = "changed"
	c.Assert(capture.Prepared[0].InheritedCollations["name"], qt.Equals, "utf8mb4_unicode_ci")
}

func TestAcceptCollations_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name          string
		wantErr       string
		inherited     map[string]string
		statedColumns map[string]string
		statedTable   string
	}{
		{name: "a column the table does not declare", wantErr: `.*inherited collation for undeclared column "other"`,
			inherited: map[string]string{"other": "utf8mb4_bin"}},
		{name: "a column that declares its own", wantErr: `.*inherited collation for column "code", which declares its own`,
			inherited: map[string]string{"code": "utf8mb4_unicode_ci"}},
		{name: "an empty collation", wantErr: `.*empty inherited collation for column "name"`,
			inherited: map[string]string{"name": ""}},
		{name: "an input that already states column collations", wantErr: `.*preparation input already states collations`,
			inherited: map[string]string{"name": "utf8mb4_unicode_ci"}, statedColumns: map[string]string{"name": "utf8mb4_unicode_ci"}},
		{name: "an input that already states the table's", wantErr: `.*preparation input already states collations`,
			inherited: map[string]string{"name": "utf8mb4_unicode_ci"}, statedTable: "utf8mb4_unicode_ci"},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := collationsRequest()
			result.Tables[0].InheritedCollations = row.inherited
			request.Tables[0].InheritedCollations = row.statedColumns
			request.Tables[0].DefaultCollation = row.statedTable

			capture, err := schemapreparation.Accept(request, result)

			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
		})
	}
}

// TestRequestClone_CopiesTheCharacterSetFacts proves a clone owns the server
// facts it carries: changing the clone's maps leaves the request's alone.
func TestRequestClone_CopiesTheCharacterSetFacts(t *testing.T) {
	c := qt.New(t)
	request := schemapreparation.Request{
		CharsetDefaultCollations: map[string]string{"utf8mb4": "utf8mb4_0900_ai_ci"},
		CharsetAliases:           map[string]string{"utf8": "utf8mb3"},
	}

	clone := request.Clone()
	clone.CharsetDefaultCollations["utf8mb4"] = "changed"
	clone.CharsetAliases["utf8"] = "changed"

	c.Assert(request.CharsetDefaultCollations, qt.DeepEquals, map[string]string{"utf8mb4": "utf8mb4_0900_ai_ci"})
	c.Assert(request.CharsetAliases, qt.DeepEquals, map[string]string{"utf8": "utf8mb3"})
}
