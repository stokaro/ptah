package sqlschema_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/sqlschema"
)

// modifyFile changes a column with ALTER TABLE ... MODIFY COLUMN.
const modifyFile = "CREATE TABLE c (id bigint PRIMARY KEY, x int);\nALTER TABLE c MODIFY COLUMN x bigint;"

// TestRead_ModifyColumn_HappyPath reads MODIFY where it is MySQL's: on MySQL
// and MariaDB, and in a document read with no dialect.
func TestRead_ModifyColumn_HappyPath(t *testing.T) {
	for _, dialect := range []string{"mysql", "mariadb", ""} {
		t.Run("dialect "+dialect, func(t *testing.T) {
			c := qt.New(t)

			database, _, err := sqlschema.Read([]byte(modifyFile), dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(database.Fields[1].Type, qt.Equals, "bigint")
		})
	}
}

// TestRead_ModifyColumn_FailurePath refuses MODIFY where the dialect has none,
// or one that is not MySQL's. Measured on PostgreSQL 18.6, the server answers
// `syntax error at or near "MODIFY"` and Atlas CE v1.3.0 refuses the file,
// where the reader read MySQL's MODIFY and the plan changed the column's type
// (stokaro/ptah#3920).
func TestRead_ModifyColumn_FailurePath(t *testing.T) {
	tests := []struct {
		dialect string
		wantErr string
	}{
		{dialect: "postgres", wantErr: `(?s).*MODIFY at position \d+ in ALTER TABLE: postgres has no MODIFY clause.*`},
		{dialect: "cockroachdb", wantErr: `(?s).*MODIFY at position \d+ in ALTER TABLE: cockroachdb has no MODIFY clause.*`},
		{dialect: "sqlite", wantErr: `(?s).*MODIFY at position \d+ in ALTER TABLE: sqlite has no MODIFY clause.*`},
		{dialect: "sqlserver", wantErr: `(?s).*MODIFY at position \d+ in ALTER TABLE: sqlserver has no MODIFY clause.*`},
		{dialect: "oracle", wantErr: `(?s).*MODIFY at position \d+ in ALTER TABLE: oracle's MODIFY is not MySQL's.*`},
		{dialect: "clickhouse", wantErr: `(?s).*MODIFY at position \d+ in ALTER TABLE: clickhouse's MODIFY is not MySQL's.*`},
	}
	for _, test := range tests {
		t.Run(test.dialect, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := sqlschema.Read([]byte(modifyFile), test.dialect)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}
