package schemaprojection_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemacapture"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemaprojection"
)

// collationCreation is one table declaring the column name with no collation
// and the column code with its own, and a reply predicting the collation name
// takes and the table's default collation.
func collationCreation() (schemaprojection.TableCreationRequest, schemaprojection.TableCreationResult) {
	subject := objectidentity.NewBuilder(identifier.Semantics{}).Table("events")
	request := schemaprojection.TableCreationRequest{
		Tables: []schemaprojection.TableCreationInput{{Subject: subject, Declaration: schemacapture.TableDeclaration{
			Table:  schemamodel.Table{Name: "events", Collate: "utf8mb4_bin"},
			Fields: []schemamodel.Field{{Name: "name", Type: "varchar(20)"}, {Name: "code", Type: "varchar(8)", Collate: "latin1_bin"}},
		}}},
		CharsetDefaultCollations: map[string]string{"utf8mb4": "utf8mb4_0900_ai_ci"},
		CharsetAliases:           map[string]string{"utf8": "utf8mb3"},
	}
	result := schemaprojection.TableCreationResult{Complete: true, Tables: []schemaprojection.TableCreation{{
		Subject: subject, Collation: "utf8mb4_bin", ColumnCollations: map[string]string{"name": "utf8mb4_bin"},
	}}}
	return request, result
}

// TestAcceptTableCreations_KeepsPredictedCollations proves an accepted result
// carries the predicted collations in its own copy, and that a request clone
// owns the server facts it carries.
func TestAcceptTableCreations_KeepsPredictedCollations(t *testing.T) {
	c := qt.New(t)
	request, result := collationCreation()

	accepted, err := schemaprojection.AcceptTableCreations(request, result)

	c.Assert(err, qt.IsNil)
	c.Assert(accepted.Tables[0].Collation, qt.Equals, "utf8mb4_bin")
	c.Assert(accepted.Tables[0].ColumnCollations, qt.DeepEquals, map[string]string{"name": "utf8mb4_bin"})
	result.Tables[0].ColumnCollations["name"] = "changed"
	c.Assert(accepted.Tables[0].ColumnCollations["name"], qt.Equals, "utf8mb4_bin")
	clone := request.Clone()
	clone.CharsetDefaultCollations["utf8mb4"] = "changed"
	clone.CharsetAliases["utf8"] = "changed"
	c.Assert(request.CharsetDefaultCollations, qt.DeepEquals, map[string]string{"utf8mb4": "utf8mb4_0900_ai_ci"})
	c.Assert(request.CharsetAliases, qt.DeepEquals, map[string]string{"utf8": "utf8mb3"})
}

func TestAcceptTableCreations_RefusesPredictedCollations(t *testing.T) {
	for _, row := range []struct {
		name       string
		collations map[string]string
		wantErr    string
	}{
		{name: "an undeclared column", collations: map[string]string{"other": "utf8mb4_bin"},
			wantErr: `.*collation for undeclared column "other"`},
		{name: "a column that declares its own", collations: map[string]string{"code": "latin1_swedish_ci"},
			wantErr: `.*collation for column "code", which declares its own`},
		{name: "an empty collation", collations: map[string]string{"name": ""},
			wantErr: `.*empty collation for column "name"`},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := collationCreation()
			result.Tables[0].ColumnCollations = row.collations

			accepted, err := schemaprojection.AcceptTableCreations(request, result)

			c.Assert(err, qt.ErrorIs, schemaprojection.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(accepted, qt.DeepEquals, schemaprojection.TableCreationResult{})
		})
	}
}
