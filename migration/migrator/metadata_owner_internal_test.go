package migrator

// White-box testing required: metadataTableOwnerQuery and refuseForeignSchema
// are deliberately internal, and which dialects answer the ownership question
// at all, and what the refusal makes of each answer the catalog gives, cannot
// be read through the public API without a live server of each engine.

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/dataext"
	"ptah.run/core/platform"
	"ptah.run/internal/migratorext"
	"ptah.run/internal/migratorext/builtin"
)

// Which dialects answer is the scope of the refusal, so it is pinned rather
// than left to whichever engine a live test happens to run against.
//
// The engines that answer no are not an oversight. The MySQL family has no
// table owner, and a trigger there runs as its definer rather than as the
// connected account, so the same squat gains the squatter a hook on every
// migration and not the migration role's privileges -- measured on MySQL
// 8.4.11 and MariaDB 12.3.3.
//
// SQL Server is out for a different reason: `sys.objects.principal_id` is NULL
// for an ordinary object, which the catalog reports as the schema's owner
// rather than the creator. Measured on SQL Server 2022, a `db_ddladmin`
// account created a table in `dbo` and the catalog named `dbo` as its owner,
// so the answer says nothing about who created it and a refusal built on it
// would reject the table Ptah itself had just created.
//
// SQLite has no roles, and ClickHouse no per-table owner to compare against
// (stokaro/ptah#3474).
func Test_metadataTableOwnerQuery_Coverage(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		wantOK  bool
	}{
		{name: "postgres answers", dialect: platform.Postgres, wantOK: true},
		{name: "cockroachdb answers", dialect: platform.CockroachDB, wantOK: true},
		{name: "yugabytedb answers", dialect: platform.YugabyteDB, wantOK: true},
		{name: "sqlserver reports the schema owner, not the creator", dialect: platform.SQLServer, wantOK: false},
		{name: "oracle answers", dialect: platform.Oracle, wantOK: true},
		{name: "mysql has no table owner", dialect: platform.MySQL, wantOK: false},
		{name: "mariadb has no table owner", dialect: platform.MariaDB, wantOK: false},
		{name: "sqlite has no roles", dialect: platform.SQLite, wantOK: false},
		{name: "clickhouse has no table owner", dialect: platform.ClickHouse, wantOK: false},
		{name: "an unknown dialect answers nothing", dialect: "nonesuch", wantOK: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			query, args, ok := metadataTableOwnerQuery(test.dialect, "public", "public", "schema_migrations")
			c.Assert(ok, qt.Equals, test.wantOK)
			c.Assert(query != "", qt.Equals, test.wantOK)
			c.Assert(len(args) == 2, qt.Equals, test.wantOK)
		})
	}
}

// Every query that answers binds the schema and the table rather than pasting
// them, so a name carrying a quote cannot reshape the statement.
func Test_metadataTableOwnerQuery_BindsItsNames(t *testing.T) {
	dialects := []string{
		platform.Postgres,
		platform.CockroachDB,
		platform.YugabyteDB,
		platform.Oracle,
	}

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			query, args, ok := metadataTableOwnerQuery(dialect, "app", "app", "schema_migrations")
			c.Assert(ok, qt.IsTrue)
			c.Assert(args, qt.DeepEquals, []any{"app", "schema_migrations"})
			c.Assert(query, qt.Not(qt.Contains), "app")
			c.Assert(query, qt.Not(qt.Contains), "schema_migrations")
		})
	}
}

// ownerAnswer is the row a fake catalog answers the schema owner query with.
type ownerAnswer struct {
	owner, role string
	mine        bool
	err         error
}

// Scan writes the answer whatever err says, as database/sql leaves the
// destinations alone on an error, and the refusal reads none of them then.
func (a ownerAnswer) Scan(dest ...any) error {
	*dest[0].(*string), *dest[1].(*string), *dest[2].(*bool) = a.owner, a.role, a.mine
	return a.err
}

func (a ownerAnswer) Err() error { return a.err }

// ownerCatalog answers every query with one row and records what it was asked.
type ownerCatalog struct {
	answer ownerAnswer
	asked  [][]any
}

func (o *ownerCatalog) row(_ context.Context, _ string, args ...any) dataext.Row {
	o.asked = append(o.asked, args)
	return o.answer
}

// oracleMetadataTable is a revision table configured into the schema OWNER.
var oracleMetadataTable = migratorext.Table{ConfiguredSchema: "OWNER", ConnectionSchema: "APP", Name: "PTAH_LOCKS"}

// On Oracle a table belongs to the owner of its schema, so an absent table in
// a schema the account owns, or in a schema that does not exist, is nothing
// to refuse, and the catalog is asked about the configured schema.
func Test_refuseForeignSchema_HappyPath(t *testing.T) {
	tests := []struct {
		name   string
		answer ownerAnswer
	}{
		{name: "the account owns the schema", answer: ownerAnswer{owner: "OWNER", role: "OWNER", mine: true}},
		{name: "no such schema", answer: ownerAnswer{err: sql.ErrNoRows}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			catalog := &ownerCatalog{answer: test.answer}

			err := refuseForeignSchema(c.Context(), catalog.row, builtin.Lookup(platform.Oracle).Owners, oracleMetadataTable, `"OWNER"."PTAH_LOCKS"`)

			c.Assert(err, qt.IsNil)
			c.Assert(catalog.asked, qt.DeepEquals, [][]any{{"OWNER"}})
		})
	}
}

// Where what Ptah creates it owns, the schema's owner is not asked about.
func Test_refuseForeignSchema_AnEngineWhoseCreatorOwnsTheTable(t *testing.T) {
	c := qt.New(t)
	catalog := &ownerCatalog{answer: ownerAnswer{owner: "postgres", role: "app"}}

	err := refuseForeignSchema(c.Context(), catalog.row, builtin.Lookup(platform.Postgres).Owners, oracleMetadataTable, `"OWNER"."PTAH_LOCKS"`)

	c.Assert(err, qt.IsNil)
	c.Assert(catalog.asked, qt.HasLen, 0)
}

// A schema another account owns is refused before the table is created, and a
// catalog that cannot answer stops the run with the variable that skips the
// check named.
func Test_refuseForeignSchema_FailurePath(t *testing.T) {
	t.Run("another account owns the schema", func(t *testing.T) {
		c := qt.New(t)
		catalog := &ownerCatalog{answer: ownerAnswer{owner: "OWNER", role: "APP"}}

		err := refuseForeignSchema(c.Context(), catalog.row, builtin.Lookup(platform.Oracle).Owners, oracleMetadataTable, `"OWNER"."PTAH_LOCKS"`)

		c.Assert(err, qt.ErrorIs, ErrForeignMetadataTable)
		c.Assert(err, qt.DeepEquals, error(&ForeignMetadataTableError{Table: `"OWNER"."PTAH_LOCKS"`, Owner: "OWNER", Role: "APP", Absent: true}))
	})
	t.Run("the catalog cannot answer", func(t *testing.T) {
		c := qt.New(t)
		refused := errors.New("ORA-01031: insufficient privileges")
		catalog := &ownerCatalog{answer: ownerAnswer{err: refused}}

		err := refuseForeignSchema(c.Context(), catalog.row, builtin.Lookup(platform.Oracle).Owners, oracleMetadataTable, `"OWNER"."PTAH_LOCKS"`)

		c.Assert(err, qt.ErrorIs, refused)
		c.Assert(err, qt.ErrorMatches, `failed to read the owner of the schema of metadata table PTAH_LOCKS `+
			`\(PTAH_ALLOW_FOREIGN_METADATA_TABLE=1 skips the ownership check\): ORA-01031: insufficient privileges`)
	})
}
