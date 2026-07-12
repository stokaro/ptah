package sqlutil_test

import (
	"testing"

	"github.com/stokaro/ptah/core/sqlutil"
)

func TestRebind(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		query   string
		want    string
	}{
		{
			name:    "postgres simple",
			dialect: "postgres",
			query:   "SELECT * FROM t WHERE a = ? AND b = ?",
			want:    "SELECT * FROM t WHERE a = $1 AND b = $2",
		},
		{
			name:    "postgresql alias",
			dialect: "postgresql",
			query:   "INSERT INTO t (a, b) VALUES (?, ?)",
			want:    "INSERT INTO t (a, b) VALUES ($1, $2)",
		},
		{
			name:    "pgx alias",
			dialect: "pgx",
			query:   "DELETE FROM t WHERE id = ?",
			want:    "DELETE FROM t WHERE id = $1",
		},
		{
			name:    "cockroachdb postgres-family placeholders",
			dialect: "cockroachdb",
			query:   "INSERT INTO t (a, b) VALUES (?, ?)",
			want:    "INSERT INTO t (a, b) VALUES ($1, $2)",
		},
		{
			name:    "yugabytedb postgres-family placeholders",
			dialect: "yugabytedb",
			query:   "SELECT ?",
			want:    "SELECT $1",
		},
		{
			name:    "spanner postgres-family placeholders",
			dialect: "spanner",
			query:   "DELETE FROM t WHERE id = ?",
			want:    "DELETE FROM t WHERE id = $1",
		},
		{
			name:    "case-insensitive dialect",
			dialect: "PostgreSQL",
			query:   "SELECT ?",
			want:    "SELECT $1",
		},
		{
			name:    "mysql untouched",
			dialect: "mysql",
			query:   "SELECT * FROM t WHERE a = ? AND b = ?",
			want:    "SELECT * FROM t WHERE a = ? AND b = ?",
		},
		{
			name:    "mariadb untouched",
			dialect: "mariadb",
			query:   "SELECT ?",
			want:    "SELECT ?",
		},
		{
			name:    "unknown dialect untouched",
			dialect: "sqlite",
			query:   "SELECT ?",
			want:    "SELECT ?",
		},
		{
			// clickhouse-go/v2 binds `?` placeholders natively, so Rebind
			// must pass them through unchanged. Locks in the regression
			// class where someone might later add a wrong `$N` mapping
			// for clickhouse.
			name:    "clickhouse untouched",
			dialect: "clickhouse",
			query:   "SELECT ?,?,?",
			want:    "SELECT ?,?,?",
		},
		{
			name:    "postgres preserves single-quoted ?",
			dialect: "postgres",
			query:   "SELECT 'a?b' WHERE x = ?",
			want:    "SELECT 'a?b' WHERE x = $1",
		},
		{
			name:    "postgres preserves escaped quotes inside literal",
			dialect: "postgres",
			query:   "SELECT 'it''s ? ok', ? FROM t WHERE x = ?",
			want:    "SELECT 'it''s ? ok', $1 FROM t WHERE x = $2",
		},
		{
			name:    "postgres preserves double-quoted identifier",
			dialect: "postgres",
			query:   `SELECT "col?name" FROM t WHERE x = ?`,
			want:    `SELECT "col?name" FROM t WHERE x = $1`,
		},
		{
			name:    "postgres no placeholders",
			dialect: "postgres",
			query:   "SELECT 1",
			want:    "SELECT 1",
		},
		{
			name:    "postgres empty",
			dialect: "postgres",
			query:   "",
			want:    "",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sqlutil.Rebind(tc.dialect, tc.query)
			if got != tc.want {
				t.Errorf("Rebind(%q, %q) = %q, want %q", tc.dialect, tc.query, got, tc.want)
			}
		})
	}
}
