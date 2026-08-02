package dbschema_test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform/identifier"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/dbschema"
	dbschematypes "go.5x5.cz/ptah/dbschema/types"
	"go.5x5.cz/ptah/migration/planner"
	"go.5x5.cz/ptah/migration/schemadiff"
	difftypes "go.5x5.cz/ptah/migration/schemadiff/types"
)

const (
	sqlServerCICollation             = "SQL_Latin1_General_CP1_CI_AS"
	sqlServerCSCollation             = "SQL_Latin1_General_CP1_CS_AS"
	sqlServerTurkishCollation        = "Turkish_100_CI_AS"
	sqlServerAccentCollation         = "Latin1_General_100_CI_AI"
	sqlServerGreekCollation          = "Greek_100_CI_AS"
	sqlServerJapaneseCollation       = "Japanese_XJIS_140_CI_AS"
	sqlServerCollationAllowlistRegex = `^(SQL_Latin1_General_CP1_(CI|CS)_AS|Turkish_100_CI_AS|Latin1_General_100_CI_AI|Greek_100_CI_AS|Japanese_XJIS_140_CI_AS)$`
)

func TestSQLServerLiveIdentifierSemantics_CaseInsensitiveCaseOnlyRename(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(t, sqlServerCICollation)
	dbURL = withSQLServerDefaultSchema(t, dbURL, "audit")
	conn := connectSQLServerCollationDatabase(t, dbURL)
	ctx := t.Context()

	info := conn.Info()
	c.Assert(info.IdentifierSemantics.IndexNames, qt.Equals, identifier.ComparisonCatalogResolved)
	c.Assert(info.IdentifierSemantics.CatalogCollation, qt.Equals, sqlServerCICollation)
	c.Assert(info.IdentifierSemantics.ResolvedNames, qt.HasLen, 0)
	c.Assert(info.Schema, qt.Equals, "audit")
	c.Assert(info.IdentifierSemantics.DefaultSchema, qt.Equals, "audit")

	_, err := conn.ExecContext(ctx, `
CREATE TABLE [dbo].[users] (
    [id] int NOT NULL,
    [email] nvarchar(320) NOT NULL,
    [status] int NOT NULL
)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(
		ctx,
		`CREATE INDEX [idx_email] ON [dbo].[users] ([email])`,
	)
	c.Assert(err, qt.IsNil)

	target := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "IDX_Email",
			TableName: "dbo.users",
			Fields:    []string{"email"},
		},
	}}
	current, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	replacementDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(current),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(replacementDiff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "IDX_Email", TableName: "dbo.users"},
	})
	c.Assert(replacementDiff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_email", TableName: "dbo.users"},
	})

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		replacementDiff,
		target,
		info.Dialect,
		info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(strings.ToUpper(statements[0]), qt.Contains, "DROP INDEX")
	c.Assert(strings.ToUpper(statements[1]), qt.Contains, "CREATE INDEX")
	_, err = conn.ExecContext(ctx, statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[0]))
	_, err = conn.ExecContext(ctx, statements[1])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[1]))

	actual, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	finalDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(actual),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(finalDiff.HasChanges(), qt.IsFalse)
}

func TestSQLServerLiveIdentifierSemantics_CaseInsensitiveDefinitionReplacement(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(t, sqlServerCICollation)
	conn := connectSQLServerCollationDatabase(t, dbURL)
	ctx := t.Context()
	info := conn.Info()

	_, err := conn.ExecContext(ctx, `
CREATE TABLE [dbo].[users] (
    [id] int NOT NULL,
    [email] nvarchar(320) NOT NULL,
    [status] int NOT NULL
)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(
		ctx,
		`CREATE INDEX [idx_users_lookup] ON [dbo].[users] ([email])`,
	)
	c.Assert(err, qt.IsNil)

	target := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_users_lookup",
			TableName: "dbo.users",
			Fields:    []string{"status"},
		},
	}}
	current, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	replacementDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(current),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(replacementDiff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_lookup", TableName: "dbo.users"},
	})
	c.Assert(replacementDiff.IndexRemovals(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_users_lookup", TableName: "dbo.users"},
	})

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		replacementDiff,
		target,
		info.Dialect,
		info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(strings.ToUpper(statements[0]), qt.Contains, "DROP INDEX")
	c.Assert(strings.ToUpper(statements[1]), qt.Contains, "CREATE INDEX")
	_, err = conn.ExecContext(ctx, statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[0]))
	_, err = conn.ExecContext(ctx, statements[1])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[1]))

	actual, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	finalDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(actual),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(finalDiff.HasChanges(), qt.IsFalse)
}

func TestSQLServerLiveIdentifierSemantics_IndexPartDirectionReplacement(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(t, sqlServerCICollation)
	conn := connectSQLServerCollationDatabase(t, dbURL)
	ctx := t.Context()
	info := conn.Info()

	_, err := conn.ExecContext(ctx, `
CREATE TABLE [dbo].[users] (
    [id] int NOT NULL,
    [email] nvarchar(320) NOT NULL,
    [status] int NOT NULL
)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(
		ctx,
		`CREATE INDEX [idx_users_lookup] ON [dbo].[users] ([email] ASC, [status] ASC)`,
	)
	c.Assert(err, qt.IsNil)

	target := &goschema.Database{Indexes: []goschema.Index{
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
	current, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	c.Assert(current.Indexes, qt.HasLen, 1)
	c.Assert(current.Indexes[0].Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
		{Name: "email"},
		{Name: "status"},
	})
	replacementDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(current),
		nil,
	)
	c.Assert(err, qt.IsNil)
	want := []difftypes.IndexRef{
		{Name: "idx_users_lookup", TableName: "dbo.users"},
	}
	c.Assert(replacementDiff.IndexAdditions(), qt.DeepEquals, want)
	c.Assert(replacementDiff.IndexRemovals(), qt.DeepEquals, want)

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		replacementDiff,
		target,
		info.Dialect,
		info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(strings.ToUpper(statements[0]), qt.Contains, "DROP INDEX")
	c.Assert(
		statements[1],
		qt.Contains,
		"([email] DESC, [status])",
	)
	_, err = conn.ExecContext(ctx, statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[0]))
	_, err = conn.ExecContext(ctx, statements[1])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[1]))

	actual, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	c.Assert(actual.Indexes, qt.HasLen, 1)
	c.Assert(actual.Indexes[0].Parts, qt.DeepEquals, []dbschematypes.DBIndexPart{
		{Name: "email", Desc: true},
		{Name: "status"},
	})
	finalDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(actual),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(finalDiff.HasChanges(), qt.IsFalse)
}

func TestSQLServerLiveIdentifierSemantics_FilteredIndexReplacement(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(t, sqlServerCICollation)
	conn := connectSQLServerCollationDatabase(t, dbURL)
	ctx := t.Context()
	info := conn.Info()

	_, err := conn.ExecContext(ctx, `
CREATE TABLE [dbo].[users] (
    [id] int NOT NULL,
    [status] int NOT NULL
)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(
		ctx,
		`CREATE INDEX [idx_active_users] ON [dbo].[users] ([status]) WHERE ([status]=(1))`,
	)
	c.Assert(err, qt.IsNil)

	target := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_active_users",
			TableName: "dbo.users",
			Fields:    []string{"status"},
			Condition: "([status]=(2))",
		},
	}}
	current, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	c.Assert(current.Indexes, qt.HasLen, 1)
	c.Assert(current.Indexes[0].Condition, qt.Equals, "([status]=(1))")
	replacementDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(current),
		nil,
	)
	c.Assert(err, qt.IsNil)
	want := []difftypes.IndexRef{
		{Name: "idx_active_users", TableName: "dbo.users"},
	}
	c.Assert(replacementDiff.IndexAdditions(), qt.DeepEquals, want)
	c.Assert(replacementDiff.IndexRemovals(), qt.DeepEquals, want)

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		replacementDiff,
		target,
		info.Dialect,
		info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 2)
	c.Assert(strings.ToUpper(statements[0]), qt.Contains, "DROP INDEX")
	c.Assert(statements[1], qt.Contains, "WHERE ([status]=(2))")
	_, err = conn.ExecContext(ctx, statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[0]))
	_, err = conn.ExecContext(ctx, statements[1])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[1]))

	actual, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	c.Assert(actual.Indexes, qt.HasLen, 1)
	c.Assert(actual.Indexes[0].Condition, qt.Equals, "([status]=(2))")
	finalDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(actual),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(finalDiff.HasChanges(), qt.IsFalse)
}

// TestSQLServerLiveIdentifierSemantics_FilteredIndexCreateRoundTrip proves the
// #781 acceptance criteria for a fresh filtered index authored in natural
// spelling: comparison plans a single CREATE INDEX ... WHERE, the predicate
// survives execution, and re-introspection against the canonical
// sys.indexes.filter_definition spelling reports zero diff.
func TestSQLServerLiveIdentifierSemantics_FilteredIndexCreateRoundTrip(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(t, sqlServerCICollation)
	conn := connectSQLServerCollationDatabase(t, dbURL)
	ctx := t.Context()
	info := conn.Info()

	_, err := conn.ExecContext(ctx, `
CREATE TABLE [dbo].[users] (
    [id] int NOT NULL,
    [status] int NOT NULL
)`)
	c.Assert(err, qt.IsNil)

	target := &goschema.Database{Indexes: []goschema.Index{
		{
			Name:      "idx_active_users",
			TableName: "dbo.users",
			Fields:    []string{"status"},
			Condition: "status = 1",
		},
	}}
	current, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	c.Assert(current.Indexes, qt.HasLen, 0)
	createDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(current),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(createDiff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "idx_active_users", TableName: "dbo.users"},
	})
	c.Assert(createDiff.IndexRemovals(), qt.HasLen, 0)

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		createDiff,
		target,
		info.Dialect,
		info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	c.Assert(statements[0], qt.Contains, "WHERE status = 1")
	_, err = conn.ExecContext(ctx, statements[0])
	c.Assert(err, qt.IsNil, qt.Commentf("statement failed:\n%s", statements[0]))

	actual, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	c.Assert(actual.Indexes, qt.HasLen, 1)
	c.Assert(actual.Indexes[0].Condition, qt.Equals, "([status]=(1))")
	finalDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(actual),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(finalDiff.HasChanges(), qt.IsFalse)
}

func TestSQLServerLiveIdentifierSemantics_CaseSensitiveVariantsCoexist(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(t, sqlServerCSCollation)
	conn := connectSQLServerCollationDatabase(t, dbURL)
	ctx := t.Context()

	info := conn.Info()
	c.Assert(info.IdentifierSemantics.IndexNames, qt.Equals, identifier.ComparisonCatalogResolved)
	c.Assert(info.IdentifierSemantics.CatalogCollation, qt.Equals, sqlServerCSCollation)

	_, err := conn.ExecContext(ctx, `
CREATE TABLE [dbo].[users] (
    [id] int NOT NULL,
    [email] nvarchar(320) NOT NULL,
    [status] int NOT NULL
)`)
	c.Assert(err, qt.IsNil)
	_, err = conn.ExecContext(ctx, `CREATE INDEX [idx_email] ON [dbo].[users] ([email])`)
	c.Assert(err, qt.IsNil)

	target := &goschema.Database{Indexes: []goschema.Index{
		{Name: "idx_email", TableName: "dbo.users", Fields: []string{"email"}},
		{Name: "IDX_Email", TableName: "dbo.users", Fields: []string{"status"}},
	}}
	current, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	diff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(current),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(diff.IndexAdditions(), qt.DeepEquals, []difftypes.IndexRef{
		{Name: "IDX_Email", TableName: "dbo.users"},
	})
	c.Assert(diff.IndexRemovals(), qt.HasLen, 0)

	statements, err := planner.GenerateSchemaDiffSQLStatementsWithCapabilities(
		diff,
		target,
		info.Dialect,
		info.Capabilities,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(statements, qt.HasLen, 1)
	_, err = conn.ExecContext(ctx, statements[0])
	c.Assert(err, qt.IsNil)

	actual, err := dbschema.ReadSchemaWithSchemas(conn, []string{"dbo"})
	c.Assert(err, qt.IsNil)
	finalDiff, err := schemadiff.CompareWithDatabase(
		ctx,
		conn,
		target,
		indexOnlySchema(actual),
		nil,
	)
	c.Assert(err, qt.IsNil)
	c.Assert(finalDiff.HasChanges(), qt.IsFalse)
}

func TestSQLServerLiveIdentifierSemantics_CatalogEquivalenceMatrix(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name      string
		collation string
		left      string
		right     string
	}{
		{
			name:      "Turkish dotless I",
			collation: sqlServerTurkishCollation,
			left:      "I",
			right:     "\u0131",
		},
		{
			name:      "accent insensitive Latin",
			collation: sqlServerAccentCollation,
			left:      "resume",
			right:     "r\u00e9sum\u00e9",
		},
		{
			name:      "Greek sigma",
			collation: sqlServerGreekCollation,
			left:      "\u03a3",
			right:     "\u03c2",
		},
		{
			name:      "Japanese kana",
			collation: sqlServerJapaneseCollation,
			left:      "\u304b",
			right:     "\u30ab",
		},
		{
			name:      "Japanese width",
			collation: sqlServerJapaneseCollation,
			left:      "\u30ab",
			right:     "\uff76",
		},
		{
			name:      "bound hostile identifier text",
			collation: sqlServerCICollation,
			left:      "name]'; DROP DATABASE master;--",
			right:     "NAME]'; DROP DATABASE MASTER;--",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			dbURL := provisionSQLServerCollationDatabase(c, test.collation)
			conn := connectSQLServerCollationDatabase(c, dbURL)
			semantics, err := conn.ResolveIdentifierSemantics(
				c.Context(),
				[]string{test.left, test.right},
			)
			c.Assert(err, qt.IsNil)
			c.Assert(semantics.CatalogCollation, qt.Equals, test.collation)
			c.Assert(
				semantics.IndexIdentityKey(test.left),
				qt.Equals,
				semantics.IndexIdentityKey(test.right),
			)
		})
	}
}

func TestSQLServerLiveIdentifierSemantics_TurkishDistinctPair(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(c, sqlServerTurkishCollation)
	conn := connectSQLServerCollationDatabase(c, dbURL)

	semantics, err := conn.ResolveIdentifierSemantics(
		t.Context(),
		[]string{"I", "i"},
	)

	c.Assert(err, qt.IsNil)
	c.Assert(
		semantics.IndexIdentityKey("I"),
		qt.Not(qt.Equals),
		semantics.IndexIdentityKey("i"),
	)
}

func TestSQLServerLiveIdentifierSemantics_OversizedIdentifier_FailurePath(t *testing.T) {
	c := qt.New(t)
	conn := connectSQLServerCollationDatabase(c, sqlServerTestURL(t))

	_, err := conn.ResolveIdentifierSemantics(
		t.Context(),
		[]string{strings.Repeat("x", 129)},
	)

	c.Assert(
		err,
		qt.ErrorMatches,
		`resolve SQL Server identifier semantics: identifier "x+" exceeds 128 characters`,
	)
}

func TestSQLServerLiveIdentifierSemantics_TargetTableCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(c, sqlServerCICollation)
	conn := connectSQLServerCollationDatabase(c, dbURL)
	target := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "LowerUsers", Schema: "dbo", Name: "users"},
			{StructName: "UpperUsers", Schema: "dbo", Name: "Users"},
		},
	}

	_, err := schemadiff.CompareWithDatabase(
		t.Context(),
		conn,
		target,
		&dbschematypes.DBSchema{},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `.*target tables dbo\.users and dbo\.Users may have the same catalog identity.*`)
}

func TestSQLServerLiveIdentifierSemantics_EmbeddedColumnCollision_FailurePath(t *testing.T) {
	c := qt.New(t)
	dbURL := provisionSQLServerCollationDatabase(c, sqlServerCICollation)
	conn := connectSQLServerCollationDatabase(c, dbURL)
	target := &goschema.Database{
		Tables: []goschema.Table{
			{StructName: "User", Schema: "dbo", Name: "users"},
		},
		Fields: []goschema.Field{
			{StructName: "User", Name: "email", Type: "NVARCHAR(255)"},
			{StructName: "Contact", Name: "Email", Type: "NVARCHAR(255)"},
		},
		EmbeddedFields: []goschema.EmbeddedField{
			{
				StructName:       "User",
				Mode:             "inline",
				EmbeddedTypeName: "Contact",
			},
		},
	}

	_, err := schemadiff.CompareWithDatabase(
		t.Context(),
		conn,
		target,
		&dbschematypes.DBSchema{},
		nil,
	)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(
		err,
		qt.ErrorMatches,
		`.*target columns dbo\.users\.email and dbo\.users\.Email may have the same catalog identity.*`,
	)
}

func provisionSQLServerCollationDatabase(t testing.TB, collation string) string {
	t.Helper()
	c := qt.New(t)
	adminURL := sqlServerTestURL(t)
	c.Assert(
		collation,
		qt.Matches,
		sqlServerCollationAllowlistRegex,
	)

	databaseName := fmt.Sprintf("ptah_777_%d", time.Now().UnixNano())
	admin, err := dbschema.ConnectToDatabase(t.Context(), adminURL)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(admin)
	_, err = admin.ExecContext(
		t.Context(),
		"CREATE DATABASE "+quoteSQLServerIdentifier(databaseName)+" COLLATE "+collation,
	)
	c.Assert(err, qt.IsNil)

	t.Cleanup(func() {
		cleanupCtx, cancelCleanup := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancelCleanup()
		cleanup, cleanupErr := dbschema.ConnectToDatabase(cleanupCtx, adminURL)
		c.Assert(
			cleanupErr,
			qt.IsNil,
			qt.Commentf("connect for SQL Server database cleanup"),
		)
		defer dbschema.CloseAndWarn(cleanup)
		_, cleanupErr = cleanup.ExecContext(
			cleanupCtx,
			"ALTER DATABASE "+quoteSQLServerIdentifier(databaseName)+
				" SET SINGLE_USER WITH ROLLBACK IMMEDIATE; DROP DATABASE "+
				quoteSQLServerIdentifier(databaseName),
		)
		c.Assert(
			cleanupErr,
			qt.IsNil,
			qt.Commentf("drop SQL Server test database %s", databaseName),
		)
	})

	parsed, err := url.Parse(adminURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("database", databaseName)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}

func sqlServerTestURL(t testing.TB) string {
	t.Helper()
	adminURL := os.Getenv("PTAH_SQLSERVER_TEST_URL")
	if adminURL == "" {
		t.Skip("set PTAH_SQLSERVER_TEST_URL to run SQL Server live schema tests")
	}
	return adminURL
}

func connectSQLServerCollationDatabase(
	t testing.TB,
	dbURL string,
) *dbschema.DatabaseConnection {
	t.Helper()
	c := qt.New(t)
	conn, err := dbschema.ConnectToDatabase(t.Context(), dbURL)
	c.Assert(err, qt.IsNil)
	t.Cleanup(func() {
		dbschema.CloseAndWarn(conn)
	})
	return conn
}

func indexOnlySchema(schema *dbschematypes.DBSchema) *dbschematypes.DBSchema {
	return &dbschematypes.DBSchema{Indexes: schema.Indexes}
}

func withSQLServerDefaultSchema(t *testing.T, dbURL, schema string) string {
	t.Helper()
	c := qt.New(t)
	parsed, err := url.Parse(dbURL)
	c.Assert(err, qt.IsNil)
	query := parsed.Query()
	query.Set("schema", schema)
	parsed.RawQuery = query.Encode()
	return parsed.String()
}
