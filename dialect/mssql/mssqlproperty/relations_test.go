package mssqlproperty_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/dialect/mssql/mssqlschema"
)

// relationRequest asks about values on SQL Server, each under the identity
// its property makes.
func relationRequest(values ...schemaext.Value) schemaext.RelationRequest {
	request := schemaext.RelationRequest{Target: platform.SQLServer, Representation: schemaext.Desired,
		Identifiers: identifier.ForDialect(platform.SQLServer)}
	for _, value := range values {
		var ref objectidentity.ID
		switch typed := value.(type) {
		case *mssqlproperty.DesiredProperty:
			ref = typed.Ref()
		case *mssqlproperty.ObservedProperty:
			ref = typed.Ref()
		}
		request.Values = append(request.Values, schemaext.RelationValue{Value: value,
			Subject: schemaext.RelationSubject{Kind: mssqlproperty.Kind, Placement: schemaext.ObjectPlacement, Subject: ref}})
	}
	return request
}

// A property on a table or on one of its columns is attached to the table,
// and one on a schema or on the database to nothing; every record is
// complete, in input order.
func TestRelationService_ReportsTheTableAPropertyHangsOff(t *testing.T) {
	c := qt.New(t)
	builder := objectidentity.NewBuilder(identifier.ForDialect(platform.SQLServer))
	request := relationRequest(
		&mssqlproperty.DesiredProperty{Property: mssqlproperty.Property{Schema: "dbo", Table: "Orders", Name: "MS_Description", Value: "t"}},
		&mssqlproperty.ObservedProperty{Property: mssqlproperty.Property{Schema: "sales", Table: "Lines", Column: "qty", Name: "unit", Value: "c"},
			ValueType: "nvarchar"},
		&mssqlproperty.DesiredProperty{Property: mssqlproperty.Property{Schema: "dbo", Name: "owner", Value: "s"}},
		&mssqlproperty.DesiredProperty{Property: mssqlproperty.Property{Name: "release", Value: "d"}},
	)

	result, err := mssqlproperty.RelationService{}.DescribeRelations(t.Context(), request)

	c.Assert(err, qt.IsNil)
	c.Assert(result.Complete, qt.IsTrue)
	c.Assert(result.Values, qt.DeepEquals, []schemaext.ValueRelations{
		{Subject: request.Values[0].Subject, Attachment: builder.TableParts("dbo", "Orders"), Complete: true},
		{Subject: request.Values[1].Subject, Attachment: builder.TableParts("sales", "Lines"), Complete: true},
		{Subject: request.Values[2].Subject, Complete: true},
		{Subject: request.Values[3].Subject, Complete: true},
	})
}

// Another target, and a value of another model, are refused before anything
// is described.
func TestRelationService_FailurePath(t *testing.T) {
	property := &mssqlproperty.DesiredProperty{Property: mssqlproperty.Property{Schema: "dbo", Table: "Orders", Name: "n", Value: "v"}}
	tests := []struct {
		name    string
		request schemaext.RelationRequest
		wantIs  error
		wantErr string
	}{
		{name: "another target", request: func() schemaext.RelationRequest {
			request := relationRequest(property)
			request.Target = platform.Postgres
			return request
		}(), wantIs: ptaherr.ErrUnsupportedDialect, wantErr: `.*SQL Server extended property relation discovery on "postgres"`},
		{name: "another model", request: func() schemaext.RelationRequest {
			request := relationRequest(property)
			request.Values[0].Value = &mssqlschema.DesiredSecurityPolicy{}
			return request
		}(), wantIs: schemaext.ErrInvalidValue, wantErr: `.*unexpected extended property relation value \*mssqlschema.DesiredSecurityPolicy`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			result, err := mssqlproperty.RelationService{}.DescribeRelations(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(result.Values, qt.HasLen, 0)
		})
	}
}
