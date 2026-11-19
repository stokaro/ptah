package schemapreparation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemapreparation"
)

// TestAcceptStoredTypes_HappyPath accepts a stored type for a declared column
// and keeps its own copy of it, while the source keeps none.
func TestAcceptStoredTypes_HappyPath(t *testing.T) {
	c := qt.New(t)
	request, result := collationsRequest()
	result.Tables[0].StoredTypes = map[string]string{"code": "varbinary(8)"}

	capture, err := schemapreparation.Accept(request, result)

	c.Assert(err, qt.IsNil)
	c.Assert(capture.Prepared[0].StoredTypes, qt.DeepEquals, map[string]string{"code": "varbinary(8)"})
	c.Assert(capture.Source[0].StoredTypes, qt.IsNil)
	c.Assert(capture.Prepared[0].Desired.Fields[1].Type, qt.Equals, "varchar(8)")
	result.Tables[0].StoredTypes["code"] = "changed"
	c.Assert(capture.Prepared[0].StoredTypes["code"], qt.Equals, "varbinary(8)")
	clone := capture.Clone()
	clone.Prepared[0].StoredTypes["code"] = "changed"
	c.Assert(capture.Prepared[0].StoredTypes["code"], qt.Equals, "varbinary(8)")
}

func TestAcceptStoredTypes_FailurePath(t *testing.T) {
	for _, row := range []struct {
		name    string
		wantErr string
		stored  map[string]string
		stated  map[string]string
	}{
		{name: "a column the table does not declare", wantErr: `.*stored type for undeclared column "other"`,
			stored: map[string]string{"other": "varbinary(8)"}},
		{name: "an empty type", wantErr: `.*empty stored type for column "code"`,
			stored: map[string]string{"code": " "}},
		{name: "an input that already states stored types", wantErr: `.*preparation input already states stored types`,
			stored: map[string]string{"code": "varbinary(8)"}, stated: map[string]string{"code": "varbinary(8)"}},
	} {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			request, result := collationsRequest()
			result.Tables[0].StoredTypes = row.stored
			request.Tables[0].StoredTypes = row.stated

			capture, err := schemapreparation.Accept(request, result)

			c.Assert(err, qt.ErrorIs, schemapreparation.ErrInvalid)
			c.Assert(err, qt.ErrorMatches, row.wantErr)
			c.Assert(capture, qt.DeepEquals, schemapreparation.Capture{})
		})
	}
}
