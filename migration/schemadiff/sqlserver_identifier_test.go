package schemadiff_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"github.com/stokaro/ptah/config"
	"github.com/stokaro/ptah/core/goschema"
	"github.com/stokaro/ptah/core/platform/identifier"
	"github.com/stokaro/ptah/core/ptaherr"
	"github.com/stokaro/ptah/dbschema/types"
	"github.com/stokaro/ptah/migration/schemadiff"
	difftypes "github.com/stokaro/ptah/migration/schemadiff/types"
)

func TestCompareWithDatabaseInfo_SQLServerCaseInsensitiveIdentity(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users", "USERS"},
		[]string{"email"},
		[]string{"idx_email"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "idx_email", TableName: "users", Fields: []string{"email"}},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{Name: "idx_email", Schema: "dbo", TableName: "USERS", Columns: []string{"email"}},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
	c.Assert(diff.IdentifierSemantics, qt.DeepEquals, &semantics)
}

func TestCompareWithDatabaseInfo_SQLServerCaseOnlyRenamePreservesSpelling(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"email"},
		[]string{"idx_email", "IDX_Email"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "IDX_Email",
			TableName: "dbo.users",
			Fields:    []string{"email"},
		},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{
			Name:      "idx_email",
			Schema:    "dbo",
			TableName: "users",
			Columns:   []string{"email"},
		},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "IDX_Email", TableName: "dbo.users"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_email", TableName: "dbo.users"},
	})
}

func TestCompareWithDatabaseInfo_SQLServerDefinitionReplacement(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"email"},
		[]string{"status"},
		[]string{"idx_users_lookup"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "idx_users_lookup", TableName: "dbo.users", Fields: []string{"status"}},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{
			Name:      "idx_users_lookup",
			Schema:    "dbo",
			TableName: "users",
			Columns:   []string{"email"},
		},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_lookup", TableName: "dbo.users"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_lookup", TableName: "dbo.users"},
	})
}

func TestCompareWithDatabaseInfo_SQLServerUniqueReplacement(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"email"},
		[]string{"idx_users_email"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_users_email",
			TableName: "dbo.users",
			Fields:    []string{"email"},
			Unique:    true,
		},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{
			Name:      "idx_users_email",
			Schema:    "dbo",
			TableName: "users",
			Columns:   []string{"email"},
		},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_email", TableName: "dbo.users"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_email", TableName: "dbo.users"},
	})
}

func TestCompareWithDatabaseInfo_SQLServerIndexPartDirectionIdentity(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"email"},
		[]string{"status"},
		[]string{"idx_users_lookup"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_users_lookup",
			TableName: "dbo.users",
			Fields:    []string{"email", "status"},
			Parts: []goschema.IndexPart{
				{Name: "email", Desc: true},
				{Name: "status"},
			},
		},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{
			Name:      "idx_users_lookup",
			Schema:    "dbo",
			TableName: "users",
			Columns:   []string{"email", "status"},
			Parts: []types.DBIndexPart{
				{Name: "email", Desc: true},
				{Name: "status"},
			},
		},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

func TestCompareWithDatabaseInfo_SQLServerIndexPartDirectionReplacement(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"email"},
		[]string{"status"},
		[]string{"idx_users_lookup"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_users_lookup",
			TableName: "dbo.users",
			Fields:    []string{"email", "status"},
			Parts: []goschema.IndexPart{
				{Name: "email", Desc: true},
				{Name: "status"},
			},
		},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{
			Name:      "idx_users_lookup",
			Schema:    "dbo",
			TableName: "users",
			Columns:   []string{"email", "status"},
			Parts: []types.DBIndexPart{
				{Name: "email"},
				{Name: "status"},
			},
		},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	want := []difftypes.IndexRef{
		{Name: "idx_users_lookup", TableName: "dbo.users"},
	}
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, want)
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, want)
}

func TestCompareWithDatabaseInfo_SQLServerCaseSensitiveVariantsRemainDistinct(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CS_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"email"},
		[]string{"idx_email"},
		[]string{"IDX_Email"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "IDX_Email", TableName: "dbo.users", Fields: []string{"email"}},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{Name: "idx_email", Schema: "dbo", TableName: "users", Columns: []string{"email"}},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "IDX_Email", TableName: "dbo.users"},
	})
	c.Assert(diff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_email", TableName: "dbo.users"},
	})
}

func TestCompareWithDatabaseInfo_SQLServerCatalogTableIdentity(t *testing.T) {
	c := qt.New(t)
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{
			{Schema: "dbo", Name: "Users"},
		},
	}

	c.Run("catalog-equivalent table has no drift", func(c *qt.C) {
		semantics := resolvedSQLServerSemantics(
			"SQL_Latin1_General_CP1_CI_AS",
			[]string{"dbo"},
			[]string{"users", "Users"},
		)
		diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
			Dialect:             "sqlserver",
			IdentifierSemantics: semantics,
		}, nil)
		c.Assert(err, qt.IsNil)
		c.Assert(diff.HasChanges(), qt.IsFalse)
	})

	c.Run("catalog-distinct table remains independent", func(c *qt.C) {
		semantics := resolvedSQLServerSemantics(
			"SQL_Latin1_General_CP1_CS_AS",
			[]string{"dbo"},
			[]string{"users"},
			[]string{"Users"},
		)
		diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
			Dialect:             "sqlserver",
			IdentifierSemantics: semantics,
		}, nil)
		c.Assert(err, qt.IsNil)
		c.Assert(diff.TablesAdded, qt.DeepEquals, []string{"dbo.users"})
		c.Assert(diff.TablesRemoved, qt.DeepEquals, []string{"dbo.Users"})
	})
}

func TestCompareWithDatabaseInfo_SQLServerCatalogColumnIdentity(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"Email", "email"},
	)
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "Email", Type: "INT"},
		},
	}
	database := &types.DBSchema{
		Tables: []types.DBTable{
			{
				Schema: "dbo",
				Name:   "users",
				Columns: []types.DBColumn{
					{Name: "email", DataType: "int"},
				},
			},
		},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.HasChanges(), qt.IsFalse)
}

func TestCompareWithDatabaseInfo_SQLServerCaseInsensitiveTargetCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"email"},
		[]string{"status"},
		[]string{"idx_email", "IDX_Email"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{Name: "idx_email", TableName: "dbo.users", Fields: []string{"email"}},
		{Name: "IDX_Email", TableName: "dbo.users", Fields: []string{"status"}},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{
			Name:      "IDX_Email",
			Schema:    "dbo",
			TableName: "users",
			Columns:   []string{"status"},
		},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*target indexes dbo\.users\.idx_email and dbo\.users\.IDX_Email conflict.*`,
	)
	c.Assert(diff, qt.IsNil)
}

func TestCompareWithDatabaseInfo_SQLServerIncompleteSnapshot_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
	)
	generated := &goschema.Database{Tables: []goschema.Table{
		{StructName: "User", Schema: "dbo", Name: "Users"},
	}}
	database := &types.DBSchema{Tables: []types.DBTable{
		{Schema: "dbo", Name: "Orders"},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*snapshot does not resolve "Users".*`)
	c.Assert(diff, qt.IsNil)
}

func TestCompareWithDatabaseInfo_SQLServerInvalidSnapshot_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog(
		"SQL_Latin1_General_CP1_CI_AS",
	)

	diff, err := schemadiff.CompareWithDatabaseInfo(
		&goschema.Database{},
		&types.DBSchema{},
		types.DBInfo{
			Dialect:             "sqlserver",
			IdentifierSemantics: semantics,
		},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*invalid identifier semantics snapshot.*`)
	c.Assert(diff, qt.IsNil)
}

func TestCompareWithOptions_SQLServerIncompleteSnapshotFallsBackConservatively(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
	)
	generated := &goschema.Database{Tables: []goschema.Table{
		{StructName: "User", Schema: "dbo", Name: "Users"},
	}}
	database := &types.DBSchema{Tables: []types.DBTable{
		{Schema: "dbo", Name: "Orders"},
	}}
	opts := config.DefaultCompareOptions()
	opts.Dialect = "sqlserver"
	opts.IdentifierSemantics = &semantics

	diff := schemadiff.CompareWithOptions(generated, database, opts)

	c.Assert(diff.TablesAdded, qt.DeepEquals, []string{"dbo.Users"})
	c.Assert(diff.TablesRemoved, qt.DeepEquals, []string{"dbo.Orders"})
	c.Assert(diff.IdentifierSemantics, qt.IsNotNil)
	c.Assert(
		diff.IdentifierSemantics.TableNames,
		qt.Equals,
		identifier.ComparisonCatalogUnknown,
	)
}

func TestCompareWithDatabaseInfo_SQLServerTableCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"Users", "users"},
	)
	generated := &goschema.Database{Tables: []goschema.Table{
		{StructName: "UpperUser", Schema: "dbo", Name: "Users"},
		{StructName: "LowerUser", Schema: "dbo", Name: "users"},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(
		generated,
		&types.DBSchema{},
		types.DBInfo{
			Dialect:             "sqlserver",
			IdentifierSemantics: semantics,
		},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*target tables dbo\.Users and dbo\.users may have the same catalog identity.*`)
	c.Assert(diff, qt.IsNil)
}

func TestCompareWithDatabaseInfo_SQLServerColumnCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"Email", "email"},
	)
	generated := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "Email", Type: "INT"},
			{StructName: "User", Name: "email", Type: "INT"},
		},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(
		generated,
		&types.DBSchema{},
		types.DBInfo{
			Dialect:             "sqlserver",
			IdentifierSemantics: semantics,
		},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*target columns dbo\.users\.Email and dbo\.users\.email may have the same catalog identity.*`,
	)
	c.Assert(diff, qt.IsNil)
}

func TestCompareWithDatabaseInfo_SQLServerExplicitUniqueIndexNamedAfterColumn(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"users"},
		[]string{"email"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "email",
			TableName: "dbo.users",
			Fields:    []string{"email"},
			Unique:    true,
		},
	}}
	database := &types.DBSchema{Indexes: []types.DBIndex{
		{
			Name:      "email",
			Schema:    "dbo",
			TableName: "users",
			Columns:   []string{"email"},
			IsUnique:  true,
		},
	}}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

func TestCompareWithDatabaseInfo_SQLServerExplicitIndexNamedAfterForeignKey(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		"SQL_Latin1_General_CP1_CI_AS",
		[]string{"dbo"},
		[]string{"orders"},
		[]string{"customer_id"},
		[]string{"fk_orders_customer"},
	)
	generated := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "fk_orders_customer",
			TableName: "dbo.orders",
			Fields:    []string{"customer_id"},
		},
	}}
	database := &types.DBSchema{
		Indexes: []types.DBIndex{
			{
				Name:      "fk_orders_customer",
				Schema:    "dbo",
				TableName: "orders",
				Columns:   []string{"customer_id"},
			},
		},
		Constraints: []types.DBConstraint{
			{
				Name:      "fk_orders_customer",
				Schema:    "dbo",
				TableName: "orders",
				Type:      "FOREIGN KEY",
			},
		},
	}

	diff, err := schemadiff.CompareWithDatabaseInfo(generated, database, types.DBInfo{
		Dialect:             "sqlserver",
		IdentifierSemantics: semantics,
	}, nil)

	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.HasLen, 0)
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)
}

func resolvedSQLServerSemantics(
	collation string,
	groups ...[]string,
) identifier.Semantics {
	resolved := make([]identifier.ResolvedName, 0)
	for _, group := range groups {
		key := slices.Min(group)
		for _, name := range group {
			resolved = append(resolved, identifier.ResolvedName{
				Name: name,
				Key:  key,
			})
		}
	}
	return identifier.ForSQLServerCatalog(collation).WithResolvedNames(resolved)
}
