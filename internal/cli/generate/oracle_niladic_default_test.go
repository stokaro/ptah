package generate_test

import (
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"
)

// TestGenerateCommand_OracleNiladicDefault_HappyPath renders a SQL schema file
// whose defaults call Oracle functions that take no parentheses. Each is
// written bare. Read as a value, SYSDATE renders as the string 'SYSDATE', and
// a current-time keyword gains `()`, which Oracle refuses with ORA-03077. A
// quoted 'SYSDATE' stays a string.
func TestGenerateCommand_OracleNiladicDefault_HappyPath(t *testing.T) {
	rows := []struct {
		name, column, want string
	}{
		{name: "SYSDATE", column: "a DATE DEFAULT SYSDATE", want: "a DATE DEFAULT SYSDATE,"},
		{name: "SYSDATE in lower case", column: "a DATE DEFAULT sysdate", want: "a DATE DEFAULT sysdate,"},
		{name: "SYSTIMESTAMP", column: "a TIMESTAMP DEFAULT SYSTIMESTAMP", want: "a TIMESTAMP DEFAULT SYSTIMESTAMP,"},
		{name: "CURRENT_DATE", column: "a DATE DEFAULT CURRENT_DATE", want: "a DATE DEFAULT CURRENT_DATE,"},
		{
			name: "CURRENT_TIMESTAMP", column: "a TIMESTAMP DEFAULT CURRENT_TIMESTAMP",
			want: "a TIMESTAMP DEFAULT CURRENT_TIMESTAMP,",
		},
		{name: "LOCALTIMESTAMP", column: "a TIMESTAMP DEFAULT LOCALTIMESTAMP", want: "a TIMESTAMP DEFAULT LOCALTIMESTAMP,"},
		{name: "USER", column: "a VARCHAR2(128) DEFAULT USER", want: "a VARCHAR2(128) DEFAULT USER,"},
		{name: "UID", column: "a NUMBER DEFAULT UID", want: "a NUMBER DEFAULT UID,"},
		{name: "a quoted SYSDATE", column: "a VARCHAR2(16) DEFAULT 'SYSDATE'", want: "a VARCHAR2(16) DEFAULT 'SYSDATE',"},
	}
	for _, row := range rows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			schema := filepath.Join(c.TempDir(), "schema.sql")
			content := "CREATE TABLE ev (\n  id NUMBER PRIMARY KEY,\n  " + row.column + ",\n  z NUMBER\n);\n"
			c.Assert(os.WriteFile(schema, []byte(content), 0o600), qt.IsNil)

			stdout, stderr, err := renderFixture(c, "--schema-file", schema, "--dialect", "oracle")

			c.Assert(err, qt.IsNil, qt.Commentf("stderr:\n%s", stderr))
			c.Assert(stdout, qt.Contains, "\n  "+row.want+"\n")
		})
	}
}
