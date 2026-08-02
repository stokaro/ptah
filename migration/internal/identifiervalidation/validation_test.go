package identifiervalidation_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/migration/internal/identifiervalidation"
)

func TestValidateCoverage_HappyPath(t *testing.T) {
	c := qt.New(t)

	err := identifiervalidation.ValidateCoverage(
		identifier.ForDialect(platform.Postgres),
		[]string{"public", "users", "email"},
	)

	c.Assert(err, qt.IsNil)
}

func TestValidateCoverage_IncompleteCatalogSnapshot_FailurePath(t *testing.T) {
	c := qt.New(t)
	semantics := resolvedSQLServerSemantics(
		identifier.ResolvedName{Name: "dbo", Key: "dbo"},
	)

	err := identifiervalidation.ValidateCoverage(
		semantics,
		[]string{"", "dbo", "users"},
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*identifier semantics snapshot does not resolve "users".*`,
	)
}

func TestValidateTarget_HappyPath(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "public", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "email", Type: "VARCHAR(320)"},
		},
		Indexes: []goschema.Index{
			{
				Name:           "idx_users_email",
				TableName:      "public.users",
				Fields:         []string{"email"},
				IncludeColumns: []string{"status"},
				Parts: []goschema.IndexPart{
					{Name: "email"},
				},
			},
		},
	}

	err := identifiervalidation.ValidateTarget(
		database,
		platform.Postgres,
		identifier.ForDialect(platform.Postgres),
	)

	c.Assert(err, qt.IsNil)
}

func TestValidateTarget_NilDatabase(t *testing.T) {
	c := qt.New(t)

	err := identifiervalidation.ValidateTarget(
		nil,
		platform.SQLServer,
		identifier.ForDialect(platform.SQLServer),
	)

	c.Assert(err, qt.IsNil)
}

func TestValidateTarget_IncompleteCatalogSnapshot_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
	}
	semantics := resolvedSQLServerSemantics(
		identifier.ResolvedName{Name: "dbo", Key: "dbo"},
	)

	err := identifiervalidation.ValidateTarget(
		database,
		platform.SQLServer,
		semantics,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*snapshot does not resolve "users".*`)
}

func TestValidateTarget_TableCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "LowerUser", Schema: "dbo", Name: "users"},
			{StructName: "UpperUser", Schema: "dbo", Name: "Users"},
		},
	}
	semantics := resolvedSQLServerSemantics(
		identifier.ResolvedName{Name: "Users", Key: "Users"},
		identifier.ResolvedName{Name: "dbo", Key: "dbo"},
		identifier.ResolvedName{Name: "users", Key: "Users"},
	)

	err := identifiervalidation.ValidateTarget(
		database,
		platform.SQLServer,
		semantics,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*target tables dbo\.users and dbo\.Users may have the same catalog identity.*`,
	)
}

func TestValidateTarget_ColumnCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "email", Type: "NVARCHAR(320)"},
			{StructName: "User", Name: "Email", Type: "NVARCHAR(320)"},
		},
	}
	semantics := resolvedSQLServerSemantics(
		identifier.ResolvedName{Name: "Email", Key: "Email"},
		identifier.ResolvedName{Name: "dbo", Key: "dbo"},
		identifier.ResolvedName{Name: "email", Key: "Email"},
		identifier.ResolvedName{Name: "users", Key: "users"},
	)

	err := identifiervalidation.ValidateTarget(
		database,
		platform.SQLServer,
		semantics,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*target columns dbo\.users\.email and dbo\.users\.Email may have the same catalog identity.*`,
	)
}

func TestValidateTarget_IndexCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	database := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "email", Type: "NVARCHAR(320)"},
		},
		Indexes: []goschema.Index{
			{
				Name:      "idx_email",
				TableName: "dbo.users",
				Fields:    []string{"email"},
			},
			{
				Name:      "IDX_Email",
				TableName: "dbo.users",
				Fields:    []string{"email"},
			},
		},
	}
	semantics := resolvedSQLServerSemantics(
		identifier.ResolvedName{Name: "IDX_Email", Key: "IDX_Email"},
		identifier.ResolvedName{Name: "dbo", Key: "dbo"},
		identifier.ResolvedName{Name: "email", Key: "email"},
		identifier.ResolvedName{Name: "idx_email", Key: "IDX_Email"},
		identifier.ResolvedName{Name: "users", Key: "users"},
	)

	err := identifiervalidation.ValidateTarget(
		database,
		platform.SQLServer,
		semantics,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*index.*idx_email.*IDX_Email.*`)
}

func resolvedSQLServerSemantics(
	names ...identifier.ResolvedName,
) identifier.Semantics {
	return identifier.ForSQLServerCatalog(
		"SQL_Latin1_General_CP1_CI_AS",
	).WithResolvedNames(names)
}
