package migrator

// White-box testing required: postgresCreatedIndexNames,
// postgresDroppedIndexNames, unusableIndexRepairError and
// unusableIndexApplyError are the pure halves of a
// refusal whose other half is a live pg_index query. Reaching them through
// RepairMigration or MigrateUpWithOptions needs a PostgreSQL connection and a
// half-built concurrent index, so the boundary cases exercised here --
// substring near-misses, quoted and unquoted spellings, comments and string
// literals, the name-less CREATE INDEX form, and the plural rendering that no
// single-index fixture reaches -- are only observable directly. The end-to-end
// behavior is covered against a live database in integration/gonative.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestPostgresCreatedIndexNames(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name string
		sql  string
		want []postgresIndexRef
	}{
		{
			name: "generated concurrent unique index",
			sql: "-- +ptah no_transaction\n" +
				`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_members_email" ON "members" ("email");`,
			want: []postgresIndexRef{{Table: "members", Name: "idx_members_email", IfNotExists: true}},
		},
		{
			name: "unquoted name folds to lower case like the catalog",
			sql:  `CREATE INDEX CONCURRENTLY IdX_Members_Email ON members (email);`,
			want: []postgresIndexRef{{Table: "members", Name: "idx_members_email"}},
		},
		{
			name: "quoted name keeps its bytes",
			sql:  `CREATE INDEX "IdX_Members_Email" ON members (email);`,
			want: []postgresIndexRef{{Table: "members", Name: "IdX_Members_Email"}},
		},
		{
			name: "quoted name unescapes a doubled quote",
			sql:  `CREATE INDEX "odd""name" ON members (email);`,
			want: []postgresIndexRef{{Table: "members", Name: `odd"name`}},
		},
		{
			name: "schema qualified target table supplies the index schema",
			sql:  `CREATE UNIQUE INDEX CONCURRENTLY "idx_members_email" ON "app"."members" ("email");`,
			want: []postgresIndexRef{{Schema: "app", Table: "members", Name: "idx_members_email"}},
		},
		{
			name: "only target table supplies the index schema",
			sql:  `CREATE INDEX idx_members_email ON ONLY App.members (email);`,
			want: []postgresIndexRef{{Schema: "app", Table: "members", Name: "idx_members_email"}},
		},
		{
			name: "schema qualified index name is not valid PostgreSQL syntax",
			sql:  `CREATE INDEX app.idx_members_email ON app.members (email);`,
			want: nil,
		},
		{
			name: "a name that is another name's prefix stays distinct from it",
			sql: `CREATE INDEX "idx_members" ON members (id);` + "\n" +
				`CREATE INDEX "idx_members_email" ON members (email);`,
			want: []postgresIndexRef{
				{Table: "members", Name: "idx_members"},
				{Table: "members", Name: "idx_members_email"},
			},
		},
		{
			name: "several statements yield every name in order",
			sql: `CREATE INDEX "idx_a" ON members (a);` + "\n" +
				`CREATE UNIQUE INDEX CONCURRENTLY "idx_b" ON members (b);`,
			want: []postgresIndexRef{{Table: "members", Name: "idx_a"}, {Table: "members", Name: "idx_b"}},
		},
		{
			name: "a repeated name is reported once",
			sql: `CREATE INDEX IF NOT EXISTS "idx_a" ON members (a);` + "\n" +
				`CREATE INDEX IF NOT EXISTS "idx_a" ON members (a);`,
			want: []postgresIndexRef{{Table: "members", Name: "idx_a", IfNotExists: true}},
		},
		{
			name: "a name inside a comment is not a name",
			sql:  "-- CREATE INDEX idx_commented ON members (email);\nCREATE INDEX idx_real ON members (email);",
			want: []postgresIndexRef{{Table: "members", Name: "idx_real"}},
		},
		{
			name: "a name inside a string literal is not a name",
			sql:  `INSERT INTO audit (note) VALUES ('CREATE INDEX idx_quoted ON members (email)');`,
			want: nil,
		},
		{
			name: "CREATE TABLE names no index",
			sql:  `CREATE TABLE members (id INTEGER PRIMARY KEY, email TEXT NOT NULL);`,
			want: nil,
		},
		{
			name: "the name-less CREATE INDEX form yields nothing",
			sql:  `CREATE INDEX ON members (email);`,
			want: nil,
		},
		{
			name: "DROP INDEX names nothing for this probe",
			sql:  `DROP INDEX CONCURRENTLY IF EXISTS "idx_members_email";`,
			want: nil,
		},
		{
			name: "CREATE inside a larger construct is not a statement start",
			sql:  `SELECT 1; GRANT CREATE ON SCHEMA app TO ptah;`,
			want: nil,
		},
		{
			name: "empty SQL yields nothing",
			sql:  "",
			want: nil,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			c.Assert(postgresCreatedIndexNames(tt.sql), qt.DeepEquals, tt.want)
		})
	}
}

func TestPostgresConditionalCreatedIndexNames(t *testing.T) {
	c := qt.New(t)
	sqlText := "CREATE INDEX idx_transient ON members (email);\n" +
		"CREATE INDEX IF NOT EXISTS idx_guarded ON members (email);"

	c.Assert(postgresConditionalCreatedIndexNames(sqlText), qt.DeepEquals, []postgresIndexRef{{
		Table:       "members",
		Name:        "idx_guarded",
		IfNotExists: true,
	}})
}

func TestPostgresDroppedIndexNames(t *testing.T) {
	c := qt.New(t)
	tests := []struct {
		name string
		sql  string
		want []postgresIndexRef
	}{
		{
			name: "unqualified concurrent index",
			sql:  `DROP INDEX CONCURRENTLY IF EXISTS idx_members_email;`,
			want: []postgresIndexRef{{Name: "idx_members_email"}},
		},
		{
			name: "qualified quoted indexes",
			sql:  `DROP INDEX IF EXISTS "app"."idx_a", "audit"."idx_b" CASCADE;`,
			want: []postgresIndexRef{{Schema: "app", Name: "idx_a"}, {Schema: "audit", Name: "idx_b"}},
		},
		{
			name: "create is not a drop",
			sql:  `CREATE INDEX idx_members_email ON members (email);`,
			want: nil,
		},
		{
			name: "comment is ignored",
			sql:  `-- DROP INDEX idx_old;` + "\nSELECT 1;",
			want: nil,
		},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(postgresDroppedIndexNames(test.sql), qt.DeepEquals, test.want)
		})
	}
}

func TestUnusableIndexRepairError(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		unusable []postgresUnusableIndex
		want     string
	}{
		{
			name: "one index names it, its flags, and the rebuild command",
			unusable: []postgresUnusableIndex{
				{Schema: "public", Name: "idx_members_email"},
			},
			want: `migration 1785756328 cannot be repaired: PostgreSQL reports index ` +
				`"public"."idx_members_email" (indisvalid=false, indisready=false) unusable, ` +
				`so recording the migration applied would report a constraint that is not enforced; ` +
				`run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", ` +
				`or drop the index and rerun the migration, then repair again`,
		},
		{
			name: "a ready but invalid index reports both flags as measured",
			unusable: []postgresUnusableIndex{
				{Schema: "public", Name: "idx_members_email", Ready: true},
			},
			want: `migration 1785756328 cannot be repaired: PostgreSQL reports index ` +
				`"public"."idx_members_email" (indisvalid=false, indisready=true) unusable, ` +
				`so recording the migration applied would report a constraint that is not enforced; ` +
				`run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", ` +
				`or drop the index and rerun the migration, then repair again`,
		},
		{
			name: "two indexes are both named and both get a rebuild command",
			unusable: []postgresUnusableIndex{
				{Schema: "app", Name: "idx_a"},
				{Schema: "public", Name: "idx_b", Valid: true},
			},
			want: `migration 1785756328 cannot be repaired: PostgreSQL reports indexes ` +
				`"app"."idx_a" (indisvalid=false, indisready=false), ` +
				`"public"."idx_b" (indisvalid=true, indisready=false) unusable, ` +
				`so recording the migration applied would report a constraint that is not enforced; ` +
				`run REINDEX INDEX CONCURRENTLY "app"."idx_a"; REINDEX INDEX CONCURRENTLY "public"."idx_b", ` +
				`or drop the indexes and rerun the migration, then repair again`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			err := unusableIndexRepairError(1785756328, tt.unusable)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, tt.want)
		})
	}
}

// TestUnusableIndexApplyError pins the refusal the up path renders. It has to
// say something the repair refusal does not: why running the body is not itself
// the fix, because running it is the action an operator reaching for
// --allow-dirty has already chosen.
func TestUnusableIndexApplyError(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name     string
		unusable []postgresUnusableIndex
		want     string
	}{
		{
			name: "one index names it, its flags, and the rebuild command",
			unusable: []postgresUnusableIndex{
				{Schema: "public", Name: "idx_members_email"},
			},
			want: `migration 1785756328 cannot be applied: PostgreSQL reports index ` +
				`"public"."idx_members_email" (indisvalid=false, indisready=false) unusable, ` +
				`and CREATE INDEX ... IF NOT EXISTS finds the name taken and skips it rather than rebuilding it, ` +
				`so this run would record the migration applied over a constraint that is not enforced; ` +
				`run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", or drop the index, then run the migration again`,
		},
		{
			name: "a ready but invalid index reports both flags as measured",
			unusable: []postgresUnusableIndex{
				{Schema: "public", Name: "idx_members_email", Ready: true},
			},
			want: `migration 1785756328 cannot be applied: PostgreSQL reports index ` +
				`"public"."idx_members_email" (indisvalid=false, indisready=true) unusable, ` +
				`and CREATE INDEX ... IF NOT EXISTS finds the name taken and skips it rather than rebuilding it, ` +
				`so this run would record the migration applied over a constraint that is not enforced; ` +
				`run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", or drop the index, then run the migration again`,
		},
		{
			name: "two indexes are both named and both get a rebuild command",
			unusable: []postgresUnusableIndex{
				{Schema: "app", Name: "idx_a"},
				{Schema: "public", Name: "idx_b", Valid: true},
			},
			want: `migration 1785756328 cannot be applied: PostgreSQL reports indexes ` +
				`"app"."idx_a" (indisvalid=false, indisready=false), ` +
				`"public"."idx_b" (indisvalid=true, indisready=false) unusable, ` +
				`and CREATE INDEX ... IF NOT EXISTS finds the name taken and skips it rather than rebuilding it, ` +
				`so this run would record the migration applied over a constraint that is not enforced; ` +
				`run REINDEX INDEX CONCURRENTLY "app"."idx_a"; REINDEX INDEX CONCURRENTLY "public"."idx_b", ` +
				`or drop the indexes, then run the migration again`,
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func(c *qt.C) {
			err := unusableIndexApplyError(1785756328, tt.unusable)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, tt.want)
		})
	}
}

func TestUnusableIndexRollbackError(t *testing.T) {
	c := qt.New(t)

	err := unusableIndexRollbackError(1785756328, []postgresUnusableIndex{{
		Schema: "public",
		Name:   "idx_members_email",
	}})

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals,
		`migration 1785756328 cannot be repaired: PostgreSQL reports index `+
			`"public"."idx_members_email" (indisvalid=false, indisready=false) unusable, `+
			`so completing the rollback would hide an unusable index behind a deleted revision; `+
			`run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", `+
			`or drop the index and resume the rollback, then repair again`)
}
