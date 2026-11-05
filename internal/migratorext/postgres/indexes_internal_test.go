package postgres

// White-box testing required: the index names a migration's statements
// create and drop, the constraint attachments, the search_path a replay
// leaves, and the wording of a refusal are the pure halves of a guard whose
// other half is a live pg_index query. Reaching them through the migrator
// needs a PostgreSQL connection and a half-built concurrent index, so the
// boundary cases exercised here -- substring near-misses, quoted and unquoted
// spellings, comments and string literals, the name-less CREATE INDEX form,
// and the plural rendering that no single-index fixture reaches -- are only
// observable directly. The end-to-end behavior is covered against a live
// database in integration/gonative.

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/jackc/pgx/v5/pgconn"

	"ptah.run/internal/migratorext"
)

// The refusals the migrator words its repair and rollback with.
var (
	repairRefusal = migratorext.IndexRefusal{
		UnusableOutcome: "recording the migration applied would report a constraint that is not enforced",
		RetryAction:     "rerun the migration",
	}
	rollbackRefusal = migratorext.IndexRefusal{
		UnusableOutcome: "completing the rollback would hide an unusable index behind a deleted revision",
		RetryAction:     "resume the rollback",
	}
)

func TestPostgresCreatedIndexNames(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []migratorext.IndexRef
	}{
		{
			name: "generated concurrent unique index",
			sql: "-- +ptah no_transaction\n" +
				`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_members_email" ON "members" ("email");`,
			want: []migratorext.IndexRef{{Table: "members", Name: "idx_members_email", IfNotExists: true}},
		},
		{
			name: "unquoted name folds to lower case like the catalog",
			sql:  `CREATE INDEX CONCURRENTLY IdX_Members_Email ON members (email);`,
			want: []migratorext.IndexRef{{Table: "members", Name: "idx_members_email"}},
		},
		{
			name: "quoted name keeps its bytes",
			sql:  `CREATE INDEX "IdX_Members_Email" ON members (email);`,
			want: []migratorext.IndexRef{{Table: "members", Name: "IdX_Members_Email"}},
		},
		{
			name: "quoted name unescapes a doubled quote",
			sql:  `CREATE INDEX "odd""name" ON members (email);`,
			want: []migratorext.IndexRef{{Table: "members", Name: `odd"name`}},
		},
		{
			name: "schema qualified target table supplies the index schema",
			sql:  `CREATE UNIQUE INDEX CONCURRENTLY "idx_members_email" ON "app"."members" ("email");`,
			want: []migratorext.IndexRef{{Schema: "app", Table: "members", Name: "idx_members_email"}},
		},
		{
			name: "only target table supplies the index schema",
			sql:  `CREATE INDEX idx_members_email ON ONLY App.members (email);`,
			want: []migratorext.IndexRef{{Schema: "app", Table: "members", Name: "idx_members_email"}},
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
			want: []migratorext.IndexRef{
				{Table: "members", Name: "idx_members"},
				{Table: "members", Name: "idx_members_email"},
			},
		},
		{
			name: "several statements yield every name in order",
			sql: `CREATE INDEX "idx_a" ON members (a);` + "\n" +
				`CREATE UNIQUE INDEX CONCURRENTLY "idx_b" ON members (b);`,
			want: []migratorext.IndexRef{{Table: "members", Name: "idx_a"}, {Table: "members", Name: "idx_b"}},
		},
		{
			name: "a repeated name is reported once",
			sql: `CREATE INDEX IF NOT EXISTS "idx_a" ON members (a);` + "\n" +
				`CREATE INDEX IF NOT EXISTS "idx_a" ON members (a);`,
			want: []migratorext.IndexRef{{Table: "members", Name: "idx_a", IfNotExists: true}},
		},
		{
			name: "a name inside a comment is not a name",
			sql:  "-- CREATE INDEX idx_commented ON members (email);\nCREATE INDEX idx_real ON members (email);",
			want: []migratorext.IndexRef{{Table: "members", Name: "idx_real"}},
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(postgresCreatedIndexNames(tt.sql), qt.DeepEquals, tt.want)
		})
	}
}

func TestPostgresConditionalCreatedIndexNames(t *testing.T) {
	c := qt.New(t)
	sqlText := "CREATE INDEX idx_transient ON members (email);\n" +
		"CREATE INDEX IF NOT EXISTS idx_guarded ON members (email);"

	c.Assert(postgresConditionalCreatedIndexNames(sqlText), qt.DeepEquals, []migratorext.IndexRef{{
		Table:       "members",
		Name:        "idx_guarded",
		IfNotExists: true,
	}})
}

// TestPostgresUnconditionalConcurrentIndexNames pins what repair has to look
// for beyond the conditional creates: only a concurrent build leaves an index
// behind when it fails, and a guarded one is the conditional check's.
func TestPostgresUnconditionalConcurrentIndexNames(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []migratorext.IndexRef
	}{
		{
			name: "the build a generated migration writes",
			sql:  "-- +ptah no_transaction\n" + `CREATE UNIQUE INDEX CONCURRENTLY "idx_members_email" ON "members" ("email");`,
			want: []migratorext.IndexRef{{Table: "members", Name: "idx_members_email"}},
		},
		{
			name: "a guarded build is left to the conditional check",
			sql:  `CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "idx_members_email" ON "members" ("email");`,
		},
		{
			name: "a plain build leaves nothing behind",
			sql:  `CREATE UNIQUE INDEX "idx_members_email" ON "members" ("email");`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(postgresUnconditionalConcurrentIndexNames(tt.sql), qt.DeepEquals, tt.want)
		})
	}
}

func TestPostgresDroppedIndexNames(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []migratorext.IndexRef
	}{
		{
			name: "unqualified concurrent index",
			sql:  `DROP INDEX CONCURRENTLY IF EXISTS idx_members_email;`,
			want: []migratorext.IndexRef{{Name: "idx_members_email"}},
		},
		{
			name: "qualified quoted indexes",
			sql:  `DROP INDEX IF EXISTS "app"."idx_a", "audit"."idx_b" CASCADE;`,
			want: []migratorext.IndexRef{{Schema: "app", Name: "idx_a"}, {Schema: "audit", Name: "idx_b"}},
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
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(postgresDroppedIndexNames(test.sql), qt.DeepEquals, test.want)
		})
	}
}

// failingQueryer answers every read with an error and counts the reads.
type failingQueryer struct{ reads int }

func (q *failingQueryer) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	q.reads++
	return nil, errors.New("connection lost")
}

// concurrentBuild is a build whose failure leaves an unusable index, and the one
// statement the guard asks the catalog about.
const concurrentBuild = `CREATE UNIQUE INDEX CONCURRENTLY "i" ON "t" ("a")`

// TestExplainBuildFailure_LeavesOtherFailuresAlone pins every path that returns the
// failure as it arrived, and whether it read the catalog to decide.
func TestExplainBuildFailure_LeavesOtherFailuresAlone(t *testing.T) {
	duplicate := &pgconn.PgError{Code: duplicateRelationCode, Message: `relation "i" already exists`}
	tests := []struct {
		name      string
		statement string
		failure   error
		reads     int
	}{
		{name: "an error the server did not answer", statement: concurrentBuild, failure: errors.New("broken pipe")},
		{name: "another server error", statement: concurrentBuild, failure: &pgconn.PgError{Code: "23505"}},
		{name: "a build whose failure leaves no index", statement: `CREATE UNIQUE INDEX "i" ON "t" ("a")`, failure: duplicate},
		{name: "a guarded build", statement: `CREATE INDEX CONCURRENTLY IF NOT EXISTS "i" ON "t" ("a")`, failure: duplicate},
		{name: "a table the server already holds", statement: `CREATE TABLE "i" (id int)`, failure: duplicate},
		{name: "a read that fails", statement: concurrentBuild, failure: duplicate, reads: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			queryer := &failingQueryer{}

			got := indexGuard{}.ExplainBuildFailure(t.Context(), queryer, test.statement, test.failure)

			c.Assert(got, qt.Equals, test.failure)
			c.Assert(queryer.reads, qt.Equals, test.reads)
		})
	}
}

// TestUnusableHolderError pins the sentence the operator reads, with the server's
// error first and the index it names, and that nothing is added when no unusable
// index holds the name.
func TestUnusableHolderError(t *testing.T) {
	failure := errors.New(`relation "i" already exists`)
	tests := []struct {
		name     string
		unusable []unusableIndex
		want     string
	}{
		{name: "no unusable index", want: `relation "i" already exists`},
		{
			name:     "an index",
			unusable: []unusableIndex{{Schema: "app", Name: "i"}},
			want: "relation \"i\" already exists\n" +
				`PostgreSQL reports index "app"."i" (indisvalid=false, indisready=false) unusable, as a failed concurrent build leaves it, ` +
				`and it holds the name; drop the index and run the migration again, or run REINDEX INDEX CONCURRENTLY "app"."i" and repair with --force once the rest of the migration has run`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			got := unusableHolderError(failure, test.unusable)

			c.Assert(got, qt.ErrorIs, failure)
			c.Assert(got.Error(), qt.Equals, test.want)
		})
	}
}

func TestUnusableIndexRepairError(t *testing.T) {
	tests := []struct {
		name     string
		unusable []unusableIndex
		want     string
	}{
		{
			name: "one index names it, its flags, and the rebuild command",
			unusable: []unusableIndex{
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
			unusable: []unusableIndex{
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
			unusable: []unusableIndex{
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			err := unusableIndexStateError(1785756328, tt.unusable, repairRefusal.UnusableOutcome, repairRefusal.RetryAction)
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
	tests := []struct {
		name     string
		unusable []unusableIndex
		want     string
	}{
		{
			name: "one index names it, its flags, and the rebuild command",
			unusable: []unusableIndex{
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
			unusable: []unusableIndex{
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
			unusable: []unusableIndex{
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
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			err := unusableIndexApplyError(1785756328, tt.unusable)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Equals, tt.want)
		})
	}
}

func TestUnusableIndexRollbackError(t *testing.T) {
	c := qt.New(t)

	err := unusableIndexStateError(1785756328, []unusableIndex{{
		Schema: "public",
		Name:   "idx_members_email",
	}}, rollbackRefusal.UnusableOutcome, rollbackRefusal.RetryAction)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Equals,
		`migration 1785756328 cannot be repaired: PostgreSQL reports index `+
			`"public"."idx_members_email" (indisvalid=false, indisready=false) unusable, `+
			`so completing the rollback would hide an unusable index behind a deleted revision; `+
			`run REINDEX INDEX CONCURRENTLY "public"."idx_members_email", `+
			`or drop the index and resume the rollback, then repair again`)
}

func TestPostgresIndexConstraintAttachments(t *testing.T) {
	tests := []struct {
		name string
		sql  string
		want []migratorext.IndexAttachment
	}{
		{
			name: "unique constraint",
			sql:  `ALTER TABLE members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key;`,
			want: []migratorext.IndexAttachment{{Index: migratorext.IndexRef{Table: "members", Name: "temporary_key"}, Name: "members_key"}},
		},
		{
			name: "qualified primary key",
			sql:  `ALTER TABLE IF EXISTS ONLY App.Members ADD CONSTRAINT Members_Key PRIMARY KEY USING INDEX Temporary_Key DEFERRABLE;`,
			want: []migratorext.IndexAttachment{{Index: migratorext.IndexRef{Schema: "app", Table: "members", Name: "temporary_key"}, Name: "members_key"}},
		},
		{
			name: "quoted identifiers",
			sql:  `ALTER TABLE "App"."Members" ADD CONSTRAINT "Members""Key" UNIQUE USING INDEX "TemporaryKey";`,
			want: []migratorext.IndexAttachment{{Index: migratorext.IndexRef{Schema: "App", Table: "Members", Name: "TemporaryKey"}, Name: `Members"Key`}},
		},
		{
			name: "multiple alter actions",
			sql:  `ALTER TABLE members ADD COLUMN note text CHECK (note IN ('a', 'b')), ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key, ADD CONSTRAINT other_key PRIMARY KEY USING INDEX other_index;`,
			want: []migratorext.IndexAttachment{
				{Index: migratorext.IndexRef{Table: "members", Name: "temporary_key"}, Name: "members_key"},
				{Index: migratorext.IndexRef{Table: "members", Name: "other_index"}, Name: "other_key"},
			},
		},
		{
			name: "comments and inheritance marker",
			sql:  `/* attach */ ALTER TABLE members * ADD CONSTRAINT members_key UNIQUE /* catalog name */ USING INDEX temporary_key;`,
			want: []migratorext.IndexAttachment{{Index: migratorext.IndexRef{Table: "members", Name: "temporary_key"}, Name: "members_key"}},
		},
		{name: "unnamed attachment keeps its name", sql: `ALTER TABLE members ADD UNIQUE USING INDEX temporary_key;`},
		{name: "column constraint creates its own index", sql: `ALTER TABLE members ADD CONSTRAINT members_key UNIQUE (id);`},
		{name: "check constraint", sql: `ALTER TABLE members ADD CONSTRAINT positive CHECK (id > 0);`},
		{name: "attachment text in a literal", sql: `ALTER TABLE members ADD COLUMN note text DEFAULT 'ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key';`},
		{name: "attachment text in a comment", sql: `-- ALTER TABLE members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key;`},
		{name: "procedural block", sql: `DO $$ BEGIN ALTER TABLE members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key; END $$;`},
		{name: "different statement", sql: `SELECT 'ALTER TABLE members ADD CONSTRAINT members_key UNIQUE USING INDEX temporary_key';`},
		{name: "incomplete primary key", sql: `ALTER TABLE members ADD CONSTRAINT members_key PRIMARY KEY USING INDEX;`},
		{name: "empty statement"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(postgresIndexConstraintAttachments(test.sql), qt.DeepEquals, test.want)
		})
	}
}

func TestPostgresSearchPathReplayState(t *testing.T) {
	tests := []struct {
		name        string
		statement   string
		wantChanged bool
		wantKnown   bool
	}{
		{name: "explicit assignment", statement: "SET search_path = app, public", wantChanged: true, wantKnown: true},
		{name: "explicit session assignment", statement: "SET SESSION search_path TO app", wantChanged: true, wantKnown: true},
		{name: "local assignment", statement: "SET LOCAL search_path TO app", wantChanged: true, wantKnown: false},
		{name: "default", statement: "SET search_path TO DEFAULT", wantChanged: true, wantKnown: false},
		{name: "current", statement: "SET search_path FROM CURRENT", wantChanged: true, wantKnown: false},
		{name: "role placeholder", statement: `SET search_path = "$user", public`, wantChanged: true, wantKnown: false},
		{name: "reset search path", statement: "RESET search_path", wantChanged: true, wantKnown: false},
		{name: "reset all", statement: "RESET ALL", wantChanged: true, wantKnown: false},
		{name: "unrelated setting", statement: "SET statement_timeout = '1s'", wantChanged: false, wantKnown: false},
		{name: "durable statement", statement: "CREATE TABLE users (id BIGINT)", wantChanged: false, wantKnown: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			changed, known := searchPathReplayState(test.statement)
			c.Assert(changed, qt.Equals, test.wantChanged)
			c.Assert(known, qt.Equals, test.wantKnown)
		})
	}
}
