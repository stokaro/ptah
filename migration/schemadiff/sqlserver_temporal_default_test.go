package schemadiff_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/platform"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/schemadiff"
)

// A SQL schema's temporal default converges with the expression reported by
// SQL Server's default-constraint catalog.
func TestCompareSQLServerTemporalDefaultDeclaration(t *testing.T) {
	tests := []struct{ declared, reported string }{
		{"GETDATE()", "getdate()"},
		{"CURRENT_TIMESTAMP", "current_timestamp"},
		{"SYSDATETIME()", "sysdatetime()"},
	}
	for _, test := range tests {
		t.Run(test.declared, func(t *testing.T) {
			c := qt.New(t)
			runtime := builtintest.Runtime()
			sql := "CREATE TABLE t (stamp datetime2 DEFAULT " + test.declared + ");"
			declared, _, err := sqlschema.ReadWith(c.Context(), []byte(sql), platform.SQLServer, nil,
				sqlschema.Options{Owners: runtime.SQL()})
			c.Assert(err, qt.IsNil)
			c.Assert(declared.Fields, qt.HasLen, 1)
			c.Assert(strings.EqualFold(declared.Fields[0].DefaultExpr, test.declared), qt.IsTrue,
				qt.Commentf("field: %+v", declared.Fields[0]))
			current, err := goschematodb.ToDBSchema(c.Context(), &declared, platform.SQLServer, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(current.Tables, qt.HasLen, 1)
			c.Assert(current.Tables[0].Columns, qt.HasLen, 1)
			current.Tables[0].Columns[0].ColumnDefault = &test.reported
			diff, err := schemadiff.CompareWithDatabaseInfo(c.Context(), &declared, current,
				catalog.ServerInfo{Dialect: platform.SQLServer}, nil, runtime)
			c.Assert(err, qt.IsNil)
			c.Assert(diff.TablesModified, qt.HasLen, 0)
		})
	}
}
