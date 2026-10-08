package builtin_test

import (
	"fmt"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/engine/builtin"
)

func TestIndexIncludeSupportedDialects(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		method   string
		usingSQL string
	}{
		{name: "postgres default", dialect: platform.Postgres},
		{name: "postgres btree", dialect: platform.Postgres, method: "BTREE"},
		{name: "postgres gist", dialect: platform.Postgres, method: "GIST", usingSQL: " USING GIST"},
		{name: "postgres spgist", dialect: platform.Postgres, method: "SPGIST", usingSQL: " USING SPGIST"},
		{name: "yugabytedb default", dialect: platform.YugabyteDB},
		{name: "yugabytedb lsm", dialect: platform.YugabyteDB, method: "LSM", usingSQL: " USING LSM"},
		{name: "yugabytedb btree", dialect: platform.YugabyteDB, method: "BTREE"},
		// CockroachDB spells the payload STORING and takes INCLUDE as a synonym.
		// Measured on v26.3.1: both rows answer CREATE INDEX, and SHOW CREATE
		// TABLE reports `INDEX i (email ASC) STORING (name)` for each.
		{name: "cockroachdb default", dialect: platform.CockroachDB},
		// BTREE renders with no USING clause, as it does for YugabyteDB: it names
		// the default rather than a second access method, and CockroachDB's
		// pg_am has no btree row to render back to.
		{name: "cockroachdb btree", dialect: platform.CockroachDB, method: "BTREE"},
		{name: "spanner default", dialect: platform.Spanner},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(indexIncludeSchema(test.method), test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(
				strings.Join(statements, "\n"),
				qt.Contains,
				test.usingSQL+` ("email") INCLUDE ("display_name");`,
			)

			sql, err := builtin.RenderSQL(test.dialect, indexIncludeNode(test.method))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, test.usingSQL+` ("email") INCLUDE ("display_name");`)
		})
	}
}

// TestIndexIncludeSQLServer renders the payload in SQL Server's own quoting.
// BTREE names what the server builds anyway, so it renders as the default.
func TestIndexIncludeSQLServer(t *testing.T) {
	for _, method := range []string{"", "BTREE"} {
		t.Run("method "+method, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(indexIncludeSchema(method), platform.SQLServer)

			c.Assert(err, qt.IsNil)
			c.Assert(strings.Join(statements, "\n"), qt.Contains, `([email]) INCLUDE ([display_name]);`)

			sql, err := builtin.RenderSQL(platform.SQLServer, indexIncludeNode(method))

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Contains, `([email]) INCLUDE ([display_name]);`)
		})
	}
}

func TestIndexIncludeUnsupportedDialectsFailClosed(t *testing.T) {
	dialects := []string{
		platform.MySQL,
		platform.MariaDB,
		platform.SQLite,
		platform.ClickHouse,
		platform.Oracle,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(indexIncludeSchema(""), dialect)

			c.Assert(statements, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(
				err,
				qt.ErrorMatches,
				fmt.Sprintf(
					`%s does not support INCLUDE columns on index "idx_accounts_email"; `+
						`target cockroachdb, postgres, spanner, sqlserver, ydb, or yugabytedb`,
					dialect,
				),
			)
			var capabilityErr *ptaherr.CapabilityError
			c.Assert(err, qt.ErrorAs, &capabilityErr)
			c.Assert(capabilityErr.Feature, qt.Equals, "index INCLUDE columns")

			sql, directErr := builtin.RenderSQL(dialect, indexIncludeNode(""))

			c.Assert(sql, qt.Equals, "")
			c.Assert(directErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(directErr.Error(), qt.Equals, err.Error())
		})
	}
}

func TestIndexIncludeVisitorPathFailsClosedAndResetsOutput(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRenderer(platform.MySQL)
	c.Assert(err, qt.IsNil)
	c.Assert(ast.NewIndex("idx_seed", "accounts", "email").Accept(r), qt.IsNil)
	c.Assert(r.Output(), qt.Not(qt.Equals), "")

	err = indexIncludeNode("").Accept(r)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(r.Output(), qt.Equals, "")
}

func TestIndexIncludeVisitorPathRendersSupportedDialect(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRenderer(platform.Postgres)
	c.Assert(err, qt.IsNil)

	err = indexIncludeNode("GIST").Accept(r)

	c.Assert(err, qt.IsNil)
	c.Assert(r.Output(), qt.Contains, `USING GIST ("email") INCLUDE ("display_name");`)
}

func TestIndexIncludeVisitorPathDelegateFailureResetsOutput(t *testing.T) {
	c := qt.New(t)
	r, err := builtin.NewRenderer(platform.Postgres)
	c.Assert(err, qt.IsNil)
	c.Assert(ast.NewIndex("idx_seed", "accounts", "email").Accept(r), qt.IsNil)
	c.Assert(r.Output(), qt.Not(qt.Equals), "")

	nullsDistinct := true
	invalid := indexIncludeNode("")
	invalid.NullsDistinct = &nullsDistinct
	err = invalid.Accept(r)

	c.Assert(err, qt.ErrorMatches, "postgresql NULLS DISTINCT is only valid for unique indexes")
	c.Assert(r.Output(), qt.Equals, "")
}

func TestIndexIncludeYugabyteBTREEAliasMatchesDefaultLSM(t *testing.T) {
	c := qt.New(t)

	defaultStatements, err := builtin.GetOrderedCreateStatements(indexIncludeSchema(""), platform.YugabyteDB)
	c.Assert(err, qt.IsNil)
	btreeStatements, err := builtin.GetOrderedCreateStatements(indexIncludeSchema("BTREE"), platform.YugabyteDB)
	c.Assert(err, qt.IsNil)

	c.Assert(btreeStatements, qt.DeepEquals, defaultStatements)
	c.Assert(strings.Join(btreeStatements, "\n"), qt.Not(qt.Contains), "USING BTREE")

	defaultSQL, err := builtin.RenderSQL(platform.YugabyteDB, indexIncludeNode(""))
	c.Assert(err, qt.IsNil)
	btreeSQL, err := builtin.RenderSQL(platform.YugabyteDB, indexIncludeNode("BTREE"))
	c.Assert(err, qt.IsNil)
	lsmSQL, err := builtin.RenderSQL(platform.YugabyteDB, indexIncludeNode("LSM"))
	c.Assert(err, qt.IsNil)

	c.Assert(btreeSQL, qt.Equals, defaultSQL)
	c.Assert(btreeSQL, qt.Not(qt.Contains), "USING BTREE")
	c.Assert(lsmSQL, qt.Contains, `USING LSM ("email") INCLUDE ("display_name")`)
}

func TestIndexIncludeUnsupportedAccessMethodsFailClosed(t *testing.T) {
	tests := []struct {
		name      string
		dialect   string
		method    string
		supported string
	}{
		{name: "postgres gin", dialect: platform.Postgres, method: "GIN", supported: "the default, BTREE, GIST, or SPGIST access method"},
		{name: "postgres hash", dialect: platform.Postgres, method: "HASH", supported: "the default, BTREE, GIST, or SPGIST access method"},
		{name: "postgres brin", dialect: platform.Postgres, method: "BRIN", supported: "the default, BTREE, GIST, or SPGIST access method"},
		{name: "yugabytedb gist", dialect: platform.YugabyteDB, method: "GIST", supported: "the default, LSM, or BTREE access method"},
		{name: "yugabytedb spgist", dialect: platform.YugabyteDB, method: "SPGIST", supported: "the default, LSM, or BTREE access method"},
		{name: "yugabytedb gin", dialect: platform.YugabyteDB, method: "GIN", supported: "the default, LSM, or BTREE access method"},
		// CockroachDB answers `inverted indexes don't support stored columns` for
		// GIN and GIST alike, because GIST is how it spells an inverted index.
		{name: "cockroachdb gin", dialect: platform.CockroachDB, method: "GIN", supported: "the default or BTREE access method"},
		{name: "cockroachdb gist", dialect: platform.CockroachDB, method: "GIST", supported: "the default or BTREE access method"},
		{name: "cockroachdb spgist", dialect: platform.CockroachDB, method: "SPGIST", supported: "the default or BTREE access method"},
		{name: "spanner btree", dialect: platform.Spanner, method: "BTREE", supported: "the default access method"},
		{name: "spanner lsm", dialect: platform.Spanner, method: "LSM", supported: "the default access method"},
		{name: "spanner gist", dialect: platform.Spanner, method: "GIST", supported: "the default access method"},
		// SQL Server has no clause for an access method; one that names
		// anything but the nonclustered rowstore index Ptah builds is reported
		// as lost, and the payload is not carried on it.
		{name: "sqlserver gin", dialect: platform.SQLServer, method: "GIN", supported: "the default or BTREE access method"},
		{name: "sqlserver hash", dialect: platform.SQLServer, method: "HASH", supported: "the default or BTREE access method"},
		// YDB's COVER belongs to a global index; any other access method is
		// not an index YDB has, covering or not.
		{name: "ydb hash", dialect: platform.YDB, method: "HASH", supported: "a global index, synchronous or asynchronous"},
		{name: "ydb gin", dialect: platform.YDB, method: "GIN", supported: "a global index, synchronous or asynchronous"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			normalizedMethod := strings.ToUpper(strings.TrimSpace(test.method))

			statements, err := builtin.GetOrderedCreateStatements(indexIncludeSchema(test.method), test.dialect)

			c.Assert(statements, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(
				err,
				qt.ErrorMatches,
				fmt.Sprintf(
					`%s INCLUDE columns on index "idx_accounts_email" require %s; access method %q is not supported`,
					test.dialect,
					test.supported,
					normalizedMethod,
				),
			)

			sql, directErr := builtin.RenderSQL(test.dialect, indexIncludeNode(test.method))

			c.Assert(sql, qt.Equals, "")
			c.Assert(directErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(directErr.Error(), qt.Equals, err.Error())
		})
	}
}

func TestIndexIncludeWhitespacePaddedAccessMethodsFailClosed(t *testing.T) {
	for _, method := range []string{"   ", " GIST", "GIST "} {
		t.Run(fmt.Sprintf("%q", method), func(t *testing.T) {
			c := qt.New(t)

			statements, err := builtin.GetOrderedCreateStatements(indexIncludeSchema(method), platform.Postgres)

			c.Assert(statements, qt.IsNil)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(
				err,
				qt.ErrorMatches,
				fmt.Sprintf(`index "idx_accounts_email" access method %q has leading or trailing whitespace`, method),
			)

			sql, directErr := builtin.RenderSQL(platform.Postgres, indexIncludeNode(method))

			c.Assert(sql, qt.Equals, "")
			c.Assert(directErr, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(directErr.Error(), qt.Equals, err.Error())
		})
	}
}

func TestIndexIncludeSPGiSTPostgres13FailsClosed(t *testing.T) {
	c := qt.New(t)
	caps := capability.Postgres13()

	statements, schemaErr := builtin.GetOrderedCreateStatementsWithCapabilities(
		indexIncludeSchema("SPGIST"),
		platform.Postgres,
		caps,
	)
	sql, directErr := builtin.RenderSQLWithCapabilities(
		platform.Postgres,
		caps,
		indexIncludeNode("SPGIST"),
	)

	c.Assert(statements, qt.IsNil)
	c.Assert(schemaErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(
		schemaErr,
		qt.ErrorMatches,
		`postgres INCLUDE columns on index "idx_accounts_email" require `+
			`the default, BTREE, or GIST access method; access method "SPGIST" is not supported`,
	)
	c.Assert(sql, qt.Equals, "")
	c.Assert(directErr, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(directErr.Error(), qt.Equals, schemaErr.Error())
}

func TestIndexIncludeSPGiSTPostgres14AndNewer(t *testing.T) {
	c := qt.New(t)
	caps := capability.Postgres16()

	statements, schemaErr := builtin.GetOrderedCreateStatementsWithCapabilities(
		indexIncludeSchema("SPGIST"),
		platform.Postgres,
		caps,
	)
	sql, directErr := builtin.RenderSQLWithCapabilities(
		platform.Postgres,
		caps,
		indexIncludeNode("SPGIST"),
	)

	c.Assert(schemaErr, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, `USING SPGIST ("email") INCLUDE ("display_name")`)
	c.Assert(directErr, qt.IsNil)
	c.Assert(sql, qt.Contains, `USING SPGIST ("email") INCLUDE ("display_name")`)
}

func TestIndexIncludeEmptyModelColumnFailsClosed(t *testing.T) {
	c := qt.New(t)
	database := indexIncludeSchema("")
	database.Indexes[0].IncludeColumns = []string{"display_name", " "}

	statements, err := builtin.GetOrderedCreateStatements(database, platform.Postgres)

	c.Assert(statements, qt.IsNil)
	c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
	c.Assert(err, qt.ErrorMatches, `index "idx_accounts_email" has an empty INCLUDE column at position 2`)
}

func indexIncludeSchema(method string) *schemamodel.Database {
	return &schemamodel.Database{
		Tables: []schemamodel.Table{{StructName: "Account", Name: "accounts"}},
		Fields: []schemamodel.Field{
			{StructName: "Account", Name: "email", Type: "TEXT"},
			{StructName: "Account", Name: "display_name", Type: "TEXT"},
		},
		Indexes: []schemamodel.Index{{
			StructName:     "Account",
			Name:           "idx_accounts_email",
			Fields:         []string{"email"},
			Type:           method,
			IncludeColumns: []string{"display_name"},
		}},
	}
}

func indexIncludeNode(method string) *ast.IndexNode {
	return &ast.IndexNode{
		Name:           "idx_accounts_email",
		Table:          "accounts",
		Columns:        []string{"email"},
		Type:           method,
		IncludeColumns: []string{"display_name"},
	}
}
