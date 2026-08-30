package schemadiff_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/catalog"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/schemamodel"
	"go.5x5.cz/ptah/migration/schemadiff"
)

// duplicateIndexTarget declares one index name twice, on two different tables.
func duplicateIndexTarget(tableNames ...string) *schemamodel.Database {
	return &schemamodel.Database{
		Indexes: []schemamodel.Index{
			{Name: "idx_shared", TableName: tableNames[0], Fields: []string{"tenant_id"}, Type: "minmax"},
			{Name: "idx_shared", TableName: tableNames[1], Fields: []string{"tenant_id"}, Type: "minmax"},
		},
	}
}

// TestCompareWithDatabaseInfo_TargetIndexConflictRejected covers a desired
// schema that names one index twice.
//
// The refusal belongs to the comparison because it is a fact about the target
// alone: which indexes a schema declares does not depend on what the server
// holds, and the conflict is between two declarations neither of which any
// change set has to mention. The plan reads only the diff, so a validation
// that has to see the whole target runs where the whole target is supplied
// (stokaro/ptah#2315).
func TestCompareWithDatabaseInfo_TargetIndexConflictRejected(t *testing.T) {
	tests := []struct {
		name          string
		dialect       string
		addedTable    string
		existingTable string
	}{
		{name: "postgresql", dialect: platform.Postgres, addedTable: "app.orders", existingTable: "app.users"},
		{name: "spanner", dialect: platform.Spanner, addedTable: "app.orders", existingTable: "app.users"},
		{name: "sqlite", dialect: platform.SQLite, addedTable: "main.orders", existingTable: "main.users"},
		{name: "postgresql mixed qualification", dialect: platform.Postgres, addedTable: "orders", existingTable: "public.users"},
		{name: "yugabytedb mixed qualification", dialect: platform.YugabyteDB, addedTable: "orders", existingTable: "public.users"},
		{name: "spanner mixed qualification", dialect: platform.Spanner, addedTable: "orders", existingTable: "public.users"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			diff, err := schemadiff.CompareWithDatabaseInfo(
				duplicateIndexTarget(test.addedTable, test.existingTable),
				&catalog.Database{},
				catalog.ServerInfo{Dialect: test.dialect},
				nil,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(diff, qt.IsNil)
		})
	}
}

// TestCompareWithDatabaseInfo_TargetIndexConflictRejectedAgainstAPopulatedServer
// is the control that keeps the refusal from being read as a fact about
// additions.
//
// The tables the conflicting indexes name are already on the server, so no
// table is created and the comparison has nothing to plan. The declaration is
// still wrong, and it is still refused.
func TestCompareWithDatabaseInfo_TargetIndexConflictRejectedAgainstAPopulatedServer(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		duplicateIndexTarget("app.users", "app.orders"),
		&catalog.Database{
			Tables: []catalog.Table{
				{Name: "users", Schema: "app", Type: "TABLE"},
				{Name: "orders", Schema: "app", Type: "TABLE"},
			},
		},
		catalog.ServerInfo{Dialect: platform.Postgres},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(diff, qt.IsNil)
}

// TestCompareWithDatabaseInfo_AmbiguousStructOwnerRejected covers an index
// declared by struct name where two tables carry that struct name.
func TestCompareWithDatabaseInfo_AmbiguousStructOwnerRejected(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&schemamodel.Database{
			Tables: []schemamodel.Table{
				{StructName: "Shared", Schema: "app", Name: "users"},
				{StructName: "Shared", Schema: "archive", Name: "users"},
			},
			Indexes: []schemamodel.Index{
				{Name: "idx_shared", StructName: "Shared", Fields: []string{"email"}},
			},
		},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: platform.Postgres},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(diff, qt.IsNil)
}

// TestCompareWithDatabaseInfo_UnknownQualifiedTargetOwnerRejected covers an
// index whose qualified owner no table declares -- here a transposed schema
// name, which is what the mistake looks like in a file.
func TestCompareWithDatabaseInfo_UnknownQualifiedTargetOwnerRejected(t *testing.T) {
	c := qt.New(t)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&schemamodel.Database{
			Tables: []schemamodel.Table{
				{StructName: "User", Schema: "public", Name: "users"},
			},
			Indexes: []schemamodel.Index{
				{Name: "idx_users_email", TableName: "publci.users", Fields: []string{"email"}},
			},
		},
		&catalog.Database{},
		catalog.ServerInfo{Dialect: platform.Postgres},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(diff, qt.IsNil)
}
