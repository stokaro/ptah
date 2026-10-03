package ydbgap_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbgap"
)

// Each refusal names YDB, the plan and the phase that implements the layer,
// which is what a reader needs to know whether to wait or to work around it.
func TestLayer_Message_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		layer ydbgap.Layer
		want  string
	}{
		{name: "schema files", layer: ydbgap.SchemaFiles, want: "reading a YDB schema file is not implemented yet (stokaro/ptah#4015, phase 2)"},
		{name: "migrating", layer: ydbgap.Migrating, want: "running versioned migrations against YDB is not implemented yet (stokaro/ptah#4015, phase 6)"},
		{name: "query building", layer: ydbgap.QueryBuilding, want: "building a YQL query is not implemented yet (stokaro/ptah#4015, phase 7)"},
		{name: "data changes", layer: ydbgap.DataChanges, want: "writing YDB rows is not implemented yet (stokaro/ptah#4015, phase 7)"},
		{name: "linting", layer: ydbgap.Linting, want: "linting YQL for YDB is not implemented yet (stokaro/ptah#4015, phase 8)"},
		{name: "creating databases", layer: ydbgap.CreatingDatabases, want: "creating a YDB database is not implemented yet (stokaro/ptah#4015, phase 9)"},
		{name: "dev databases", layer: ydbgap.DevDatabases, want: "using a YDB database as a dev or shadow database is not implemented yet (stokaro/ptah#4015, phase 9)"},
		{name: "comments", layer: ydbgap.Comments, want: "storing a comment on a YDB object is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "views", layer: ydbgap.Views, want: "managing YDB views is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "access control", layer: ydbgap.AccessControl, want: "managing YDB users, groups and permissions is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "table settings", layer: ydbgap.TableSettings, want: "setting YDB table options (TTL, partitioning, column families, changefeeds) is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "index families", layer: ydbgap.IndexFamilies, want: "reading or creating a YDB vector, full-text, JSON or column-table index is not implemented yet (stokaro/ptah#4015, phase 10)"},
		{name: "compatibility", layer: ydbgap.Compatibility, want: "using a YDB database through ptah-compat is not implemented yet (stokaro/ptah#4015, phase 11)"},
		{name: "other surfaces", layer: ydbgap.OtherSurfaces, want: "running this command against YDB is not implemented yet (stokaro/ptah#4015, phase 12)"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(test.layer.Message(), qt.Equals, test.want)
		})
	}
}

// A value outside the declared layers names no phase rather than borrowing
// another layer's.
func TestLayer_Message_FailurePath(t *testing.T) {
	c := qt.New(t)

	c.Assert(ydbgap.Layer(0).Phase(), qt.Equals, 0)
	c.Assert(ydbgap.Layer(0).Message(), qt.Equals, "this YDB operation is not implemented yet (stokaro/ptah#4015, phase 0)")
}
