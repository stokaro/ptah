package postgres

// White-box testing required: declaredIndex is the text the definition probe
// executes, and the refusal for a role without TEMPORARY is worded before any
// probe runs. The comparison itself is covered against a live database in
// integration/gonative.

import (
	"context"
	"database/sql"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/migratorext"
)

func TestDeclaredIndex(t *testing.T) {
	tests := []struct {
		name       string
		sql        string
		wantUnique bool
		wantTail   string
		wantOK     bool
	}{
		{
			name:     "plain index keeps method, keys and predicate as written",
			sql:      "CREATE INDEX IF NOT EXISTS i ON t USING btree (a) WHERE a > 0",
			wantTail: " USING btree (a) WHERE a > 0",
			wantOK:   true,
		},
		{
			name:       "unique index on a schema-qualified ONLY target drops trailing semicolons",
			sql:        "CREATE UNIQUE INDEX CONCURRENTLY i ON ONLY app.t (a);;",
			wantUnique: true,
			wantTail:   " (a)",
			wantOK:     true,
		},
		{
			name: "a semicolon before the end of the tail is refused",
			sql:  "CREATE INDEX i ON t (a); DROP TABLE x",
		},
		{
			name: "a statement that is not CREATE INDEX is refused",
			sql:  "CREATE TABLE t (a int)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			unique, tail, ok := declaredIndex(tt.sql)
			c.Assert(ok, qt.Equals, tt.wantOK)
			c.Assert(unique, qt.Equals, tt.wantUnique)
			c.Assert(tail, qt.Equals, tt.wantTail)
		})
	}
}

func TestSameDefinitionNamesTheTemporaryPrivilegeItNeeds(t *testing.T) {
	c := qt.New(t)
	state := migratorext.IndexState{TargetSchema: "public", TargetTable: "members", Name: "members_email_key"}

	_, err := indexGuard{}.SameDefinition(context.Background(), refusingQueryer{sqlState: insufficientPrivilege},
		"CREATE UNIQUE INDEX IF NOT EXISTS members_email_key ON members (email)", state)

	c.Assert(err, qt.ErrorMatches, `(?s).*"public"."members_email_key".*needs the TEMPORARY privilege on the database.*`)
}

type refusingQueryer struct{ sqlState string }

func (q refusingQueryer) QueryContext(context.Context, string, ...any) (*sql.Rows, error) {
	return nil, sqlStateError(q.sqlState)
}

type sqlStateError string

func (e sqlStateError) Error() string    { return "SQLSTATE " + string(e) }
func (e sqlStateError) SQLState() string { return string(e) }
