package identifier_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/identifier"
)

func TestForDialect(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name    string
		dialect string
		want    identifier.Semantics
	}{
		{
			name:    "postgres",
			dialect: "postgres",
			want: identifier.Semantics{
				DefaultSchema:  "public",
				IndexNamespace: identifier.IndexNamespaceSchema,
				IndexNames:     identifier.ComparisonExact,
				TableNames:     identifier.ComparisonExact,
				ColumnNames:    identifier.ComparisonExact,
			},
		},
		{
			name:    "sqlite",
			dialect: "sqlite",
			want: identifier.Semantics{
				DefaultSchema:  "main",
				IndexNamespace: identifier.IndexNamespaceSchema,
				IndexNames:     identifier.ComparisonASCIIInsensitive,
				TableNames:     identifier.ComparisonASCIIInsensitive,
				ColumnNames:    identifier.ComparisonASCIIInsensitive,
			},
		},
		{
			name:    "sqlserver is conservative offline",
			dialect: "sqlserver",
			want: identifier.Semantics{
				DefaultSchema:  "dbo",
				IndexNamespace: identifier.IndexNamespaceTable,
				IndexNames:     identifier.ComparisonCatalogUnknown,
				TableNames:     identifier.ComparisonCatalogUnknown,
				ColumnNames:    identifier.ComparisonCatalogUnknown,
			},
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			got := identifier.ForDialect(test.dialect)
			c.Assert(got, qt.DeepEquals, test.want)
		})
	}
}

func TestForSQLServerCatalog(t *testing.T) {
	c := qt.New(t)

	c.Run("catalog comparison requires resolved keys", func(c *qt.C) {
		got := identifier.ForSQLServerCatalog("Turkish_100_CI_AS")
		c.Assert(got.IndexNames, qt.Equals, identifier.ComparisonCatalogResolved)
		c.Assert(got.TableNames, qt.Equals, identifier.ComparisonCatalogResolved)
		c.Assert(got.ColumnNames, qt.Equals, identifier.ComparisonCatalogResolved)
		c.Assert(got.CatalogCollation, qt.Equals, "Turkish_100_CI_AS")
	})

	c.Run("server-resolved equivalence overrides Go casing", func(c *qt.C) {
		got := identifier.ForSQLServerCatalog("Turkish_100_CI_AS").
			WithResolvedNames([]identifier.ResolvedName{
				{Name: "I", Key: "I"},
				{Name: "i", Key: "i"},
				{Name: "ı", Key: "I"},
			})
		c.Assert(got.IndexIdentityKey("I"), qt.Equals, got.IndexIdentityKey("ı"))
		c.Assert(got.IndexIdentityKey("I"), qt.Not(qt.Equals), got.IndexIdentityKey("i"))
	})

	c.Run("resolved names are sorted deterministically", func(c *qt.C) {
		got := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
			WithResolvedNames([]identifier.ResolvedName{
				{Name: "users", Key: "users"},
				{Name: "dbo", Key: "dbo"},
			})
		c.Assert(got.ResolvedNames, qt.DeepEquals, []identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "users", Key: "users"},
		})
	})
}

func TestSemanticsNormalize(t *testing.T) {
	c := qt.New(t)

	c.Run("partial public value falls back conservatively", func(c *qt.C) {
		got := (identifier.Semantics{
			CatalogCollation: "SQL_Latin1_General_CP1_CI_AS",
		}).Normalize("sqlserver")
		want := identifier.ForDialect("sqlserver")
		c.Assert(got, qt.DeepEquals, want)
	})

	c.Run("invalid enum falls back conservatively", func(c *qt.C) {
		got := (identifier.Semantics{
			DefaultSchema:  "dbo",
			IndexNamespace: identifier.IndexNamespaceTable,
			IndexNames:     identifier.Comparison("invalid"),
			TableNames:     identifier.ComparisonExact,
			ColumnNames:    identifier.ComparisonExact,
		}).Normalize("sqlserver")
		want := identifier.ForDialect("sqlserver")
		c.Assert(got, qt.DeepEquals, want)
	})

	c.Run("complete catalog value is retained", func(c *qt.C) {
		got := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
			WithResolvedNames([]identifier.ResolvedName{
				{Name: "dbo", Key: "dbo"},
			})
		c.Assert(got.Normalize("sqlserver"), qt.DeepEquals, got)
		c.Assert(got.CatalogCollation, qt.Equals, "SQL_Latin1_General_CP1_CI_AS")
	})

	c.Run("unresolved catalog template falls back conservatively", func(c *qt.C) {
		got := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
			Normalize("sqlserver")
		c.Assert(got, qt.DeepEquals, identifier.ForDialect("sqlserver"))
	})

	c.Run("missing equivalence key falls back conservatively", func(c *qt.C) {
		got := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
			WithResolvedNames([]identifier.ResolvedName{
				{Name: "users", Key: "missing"},
			}).
			Normalize("sqlserver")
		c.Assert(got, qt.DeepEquals, identifier.ForDialect("sqlserver"))
	})

	c.Run("noncanonical equivalence key falls back conservatively", func(c *qt.C) {
		got := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
			WithResolvedNames([]identifier.ResolvedName{
				{Name: "Users", Key: "users"},
				{Name: "users", Key: "users"},
			}).
			Normalize("sqlserver")
		c.Assert(got, qt.DeepEquals, identifier.ForDialect("sqlserver"))
	})

	c.Run("resolved names on static semantics fall back conservatively", func(c *qt.C) {
		got := identifier.ForDialect("postgres").
			WithResolvedNames([]identifier.ResolvedName{
				{Name: "users", Key: "users"},
			}).
			Normalize("postgres")
		c.Assert(got, qt.DeepEquals, identifier.ForDialect("postgres"))
	})
}

func TestSemanticsCatalogCoverage(t *testing.T) {
	c := qt.New(t)
	semantics := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
			{Name: "users", Key: "users"},
		})

	c.Assert(semantics.Resolves("users"), qt.IsTrue)
	c.Assert(semantics.Resolves("orders"), qt.IsFalse)
	c.Assert(semantics.ResolvesQualifiedTable("dbo.users"), qt.IsTrue)
	c.Assert(semantics.ResolvesQualifiedTable("users"), qt.IsTrue)
	c.Assert(semantics.ResolvesQualifiedTable("dbo.orders"), qt.IsFalse)
	c.Assert(identifier.ForDialect("postgres").Resolves("anything"), qt.IsTrue)
}

func TestSemanticsEqual_IgnoresDiagnosticCatalogLabel(t *testing.T) {
	c := qt.New(t)
	left := identifier.ForSQLServerCatalog("Latin1_General_100_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
		})
	right := identifier.ForSQLServerCatalog("SQL_Latin1_General_CP1_CI_AS").
		WithResolvedNames([]identifier.ResolvedName{
			{Name: "dbo", Key: "dbo"},
		})

	c.Assert(left.Equal(right), qt.IsTrue)
}

func TestComparisonKeys(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name         string
		comparison   identifier.Comparison
		value        string
		wantIdentity string
		wantConflict string
	}{
		{
			name:         "exact",
			comparison:   identifier.ComparisonExact,
			value:        "IDX_Email",
			wantIdentity: "IDX_Email",
			wantConflict: "IDX_Email",
		},
		{
			name:         "ASCII insensitive",
			comparison:   identifier.ComparisonASCIIInsensitive,
			value:        "IDX_Émail",
			wantIdentity: "idx_Émail",
			wantConflict: "idx_Émail",
		},
		{
			name:         "Unicode insensitive",
			comparison:   identifier.ComparisonUnicodeInsensitive,
			value:        "IDX_Émail",
			wantIdentity: "idx_émail",
			wantConflict: "idx_émail",
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(test.comparison.IdentityKey(test.value), qt.Equals, test.wantIdentity)
			c.Assert(test.comparison.ConflictKey(test.value), qt.Equals, test.wantConflict)
		})
	}

	c.Run("unknown catalog preserves identity and conflicts conservatively", func(c *qt.C) {
		comparison := identifier.ComparisonCatalogUnknown
		c.Assert(comparison.IdentityKey("IDX_Email"), qt.Equals, "IDX_Email")
		c.Assert(
			comparison.ConflictKey("resume"),
			qt.Equals,
			comparison.ConflictKey("r\u00e9sum\u00e9"),
		)
		c.Assert(
			comparison.ConflictKey("\u304b"),
			qt.Equals,
			comparison.ConflictKey("\u30ab"),
		)
	})
}
