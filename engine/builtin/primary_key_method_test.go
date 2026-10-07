package builtin_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

// primaryKeyMethodSchema is table t whose primary key asks for HASH.
func primaryKeyMethodSchema() *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t", PrimaryKey: []string{"id"}, PrimaryKeyMethod: "HASH"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT", Primary: true}},
	}
}

// primaryKeyConstraintSchema is table t whose primary key is a PRIMARY KEY
// constraint asking for method, the spelling a Go annotation, a YAML document
// and an HCL constraint block produce.
func primaryKeyConstraintSchema(method string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "T", Name: "t"}},
		Fields: []schemamodel.Field{{StructName: "T", Name: "id", Type: "INT"}},
		Constraints: []schemamodel.Constraint{{
			StructName: "T", Table: "t", Name: "t_pk", Type: "PRIMARY KEY", Columns: []string{"id"},
			UsingMethod: method,
		}},
	}
}

// TestRender_PrimaryKeyMethod_HappyPath writes the method after the key's
// parts, where MariaDB 11.8.9 prints it back. The key is a table constraint,
// because the column spelling has no place for the clause (stokaro/ptah#3853).
func TestRender_PrimaryKeyMethod_HappyPath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		database *schemamodel.Database
	}{
		{name: "table key on MariaDB", dialect: platform.MariaDB, caps: capability.MariaDB1011(), database: primaryKeyMethodSchema()},
		{name: "table key on MySQL", dialect: platform.MySQL, caps: capability.MySQL84(), database: primaryKeyMethodSchema()},
		{
			name: "constraint on MariaDB", dialect: platform.MariaDB, caps: capability.MariaDB1011(),
			database: primaryKeyConstraintSchema("hash"),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.database, test.dialect, test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, "PRIMARY KEY (`id`) USING HASH")
		})
	}
}

// TestRender_PrimaryKeyMethod_BTREEWritesNothing renders a key asking for
// BTREE without the clause. The server reports BTREE for it and for a key that
// asked for nothing alike, so the two are one key to every reader. The method
// is read as written in a document, in any case and with surrounding spaces.
func TestRender_PrimaryKeyMethod_BTREEWritesNothing(t *testing.T) {
	c := qt.New(t)

	statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(primaryKeyConstraintSchema(" btree "),
		platform.MariaDB, capability.MariaDB1011())

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "PRIMARY KEY (`id`)")
	c.Assert(strings.Join(statements, "\n"), qt.Not(qt.Contains), "USING")
}

// TestRender_PrimaryKeyMethod_FailurePath refuses a method the target cannot
// build: any method outside the MySQL family, where there is no clause for it,
// and a method other than BTREE or HASH inside it. Either way the key would
// silently become the engine's default.
func TestRender_PrimaryKeyMethod_FailurePath(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		caps     capability.Capabilities
		database *schemamodel.Database
		wantErr  string
	}{
		{
			name: "table key on PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres18(),
			database: primaryKeyMethodSchema(),
			wantErr:  `.*the primary key of "t" asks for USING HASH, which only MySQL and MariaDB write`,
		},
		{
			name: "constraint on PostgreSQL", dialect: platform.Postgres, caps: capability.Postgres18(),
			database: primaryKeyConstraintSchema("HASH"),
			wantErr:  `.*the primary key of "t" asks for USING HASH, which only MySQL and MariaDB write`,
		},
		{
			name: "constraint asking for gist on MariaDB", dialect: platform.MariaDB, caps: capability.MariaDB1011(),
			database: primaryKeyConstraintSchema("gist"),
			wantErr:  `.*the primary key of "t" asks for USING gist; a primary key is built USING BTREE or USING HASH`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(test.database, test.dialect, test.caps)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(statements, qt.IsNil)
		})
	}
}
