package mysqlconvert_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mysql/mysqlconvert"
	"ptah.run/dialect/mysql/mysqlschema"
)

func TestColumnService_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		from, to schemaext.Representation
		value    schemaext.Value
		want     schemaext.Value
	}{
		{name: "an observation becomes the declaration that writes it", from: schemaext.Observed, to: schemaext.Desired,
			value: &mysqlschema.ObservedColumnSettings{Charset: "utf8mb4", OnUpdate: "CURRENT_TIMESTAMP"},
			want:  &mysqlschema.DesiredColumnSettings{Charset: "utf8mb4", OnUpdate: "CURRENT_TIMESTAMP"}},
		{name: "a declaration becomes the observation it creates", from: schemaext.Desired, to: schemaext.Observed,
			value: &mysqlschema.DesiredColumnSettings{Charset: "latin1"},
			want:  &mysqlschema.ObservedColumnSettings{Charset: "latin1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := mysqlconvert.ColumnService{}.ConvertFeatures(t.Context(), schemaext.ConversionRequest{
				Target: "mariadb", From: test.from, To: test.to, Values: []schemaext.Value{test.value}})

			c.Assert(err, qt.IsNil)
			c.Assert(converted, qt.DeepEquals, []schemaext.Value{test.want})
		})
	}
}

func TestColumnService_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		request schemaext.ConversionRequest
		wantIs  error
		wantErr string
	}{
		{name: "another target", request: schemaext.ConversionRequest{Target: "ydb", From: schemaext.Desired, To: schemaext.Observed},
			wantIs: ptaherr.ErrUnsupportedDialect, wantErr: `.*MySQL conversion on "ydb"`},
		{name: "one representation", request: schemaext.ConversionRequest{Target: "mysql", From: schemaext.Desired, To: schemaext.Desired},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*invalid MySQL conversion direction`},
		{name: "a value of the other representation", request: schemaext.ConversionRequest{Target: "mysql", From: schemaext.Observed, To: schemaext.Desired,
			Values: []schemaext.Value{&mysqlschema.DesiredColumnSettings{Charset: "utf8"}}},
			wantIs: schemaext.ErrInvalidValue, wantErr: `.*expected observed MySQL column settings.*`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			converted, err := mysqlconvert.ColumnService{}.ConvertFeatures(t.Context(), test.request)

			c.Assert(err, qt.ErrorIs, test.wantIs)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(converted, qt.IsNil)
		})
	}
}
