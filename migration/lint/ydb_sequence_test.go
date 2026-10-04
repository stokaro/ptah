package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// serialTables is an up migration creating tables whose keys are Serial
// columns of each width, under each spelling YQL takes, one of them in a
// directory.
const serialTables = "CREATE TABLE `shop/orders` (id BigSerial NOT NULL, PRIMARY KEY (id));\n" +
	"CREATE TABLE counters (id serial4 NOT NULL, n SmallSerial, PRIMARY KEY (id));\n" +
	"CREATE TABLE tags (id Serial2 NOT NULL, PRIMARY KEY (id));\n"

// TestYDBSequenceRules_Report pins the two sequence traps measured on
// 26.2.1.14 and 25.1.4.7: an ALTER SEQUENCE on a 16-bit or 32-bit Serial's
// sequence, which raises its maximum to the Int64 maximum (YD107), and an
// ALTER SEQUENCE without a RESTART of its own on a sequence an earlier
// statement restarted, which YDB replays (YD108).
func TestYDBSequenceRules_Report(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "a 32-bit Serial and a 16-bit one, altered",
			files: map[string]string{
				"0001_t.up.sql": serialTables,
				"0002_t.up.sql": "ALTER SEQUENCE `/local/counters/_serial_column_id` INCREMENT BY 1;\n" +
					"ALTER SEQUENCE `/Root/db/counters/_serial_column_n` START WITH 5;\n" +
					"ALTER SEQUENCE `/local/tags/_serial_column_id` RESTART WITH 2;\n",
			},
			want: []string{"0002_t.up.sql:1:YD107", "0002_t.up.sql:2:YD107", "0002_t.up.sql:3:YD107"},
		},
		{
			name: "a restart an earlier migration made, replayed",
			files: map[string]string{
				"0001_t.up.sql": serialTables +
					"ALTER SEQUENCE `/local/shop/orders/_serial_column_id` START WITH 100 INCREMENT BY 5 RESTART WITH 100;\n",
				"0002_t.up.sql": "ALTER SEQUENCE `/local/shop/orders/_serial_column_id` INCREMENT BY 10;\n",
			},
			want: []string{"0002_t.up.sql:1:YD108"},
		},
		{
			name: "a restart earlier in the same file, replayed by a down half",
			files: map[string]string{
				"0001_t.up.sql": serialTables,
				"0002_t.up.sql": "ALTER SEQUENCE `/local/shop/orders/_serial_column_id` RESTART;\n",
				"0002_t.down.sql": "ALTER SEQUENCE `/local/shop/orders/_serial_column_id` START WITH 1;\n",
			},
			want: []string{"0002_t.down.sql:1:YD108"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, "")), qt.DeepEquals, test.want)
		})
	}
}

// TestYDBSequenceRules_LeaveWhatTheServerKeeps holds the controls: a 64-bit
// Serial's sequence, which ends at the Int64 maximum already; a restart that
// sets a value of its own; a sequence restarted before its table was dropped
// and created again; and a sequence of a table the directory never created,
// or of another table whose path the altered one only resembles.
func TestYDBSequenceRules_LeaveWhatTheServerKeeps(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
	}{
		{name: "a 64-bit Serial, altered", files: map[string]string{
			"0001_t.up.sql": serialTables,
			"0002_t.up.sql": "ALTER SEQUENCE `/local/shop/orders/_serial_column_id` START WITH 100 INCREMENT BY 5;\n"}},
		{name: "a restarted sequence, restarted again", files: map[string]string{
			"0001_t.up.sql": serialTables + "ALTER SEQUENCE `/local/shop/orders/_serial_column_id` RESTART WITH 100;\n",
			"0002_t.up.sql": "ALTER SEQUENCE `/local/shop/orders/_serial_column_id` INCREMENT BY 10 RESTART WITH 1000;\n"}},
		{name: "a restarted sequence whose table was created again", files: map[string]string{
			"0001_t.up.sql": serialTables + "ALTER SEQUENCE `/local/shop/orders/_serial_column_id` RESTART WITH 100;\n",
			"0002_t.up.sql": "DROP TABLE `shop/orders`;\n" +
				"CREATE TABLE `shop/orders` (id BigSerial NOT NULL, PRIMARY KEY (id));\n" +
				"ALTER SEQUENCE `/local/shop/orders/_serial_column_id` INCREMENT BY 10;\n"}},
		{name: "a sequence of a table the directory never created", files: map[string]string{
			"0001_t.up.sql": "ALTER SEQUENCE `/local/elsewhere/_serial_column_id` INCREMENT BY 2;\n"}},
		{name: "a table whose name ends another one's path", files: map[string]string{
			"0001_t.up.sql": serialTables,
			"0002_t.up.sql": "ALTER SEQUENCE `/local/shop/xcounters/_serial_column_id` INCREMENT BY 2;\n"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, "")), qt.HasLen, 0)
		})
	}
}

// TestYDBSequenceRules_SayWhatToDo pins the two messages.
func TestYDBSequenceRules_SayWhatToDo(t *testing.T) {
	c := qt.New(t)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_t.up.sql": serialTables +
			"ALTER SEQUENCE `/local/counters/_serial_column_n` INCREMENT BY 2;\n" +
			"ALTER SEQUENCE `/local/shop/orders/_serial_column_id` RESTART WITH 100;\n" +
			"ALTER SEQUENCE `/local/shop/orders/_serial_column_id` INCREMENT BY 3;\n",
	}), lint.Options{Dialect: "ydb"})

	c.Assert(err, qt.IsNil)
	messages := make([]string, 0, len(findings))
	for _, finding := range findings {
		messages = append(messages, finding.Rule+": "+finding.Message)
	}
	c.Assert(messages, qt.Contains, "YD107: ALTER SEQUENCE /local/counters/_serial_column_n raises the maximum of the "+
		"sequence of SmallSerial column counters.n to the Int64 maximum, and the column then stores the value past its own "+
		"maximum as a negative number without an error; declare the column BigSerial to give its sequence a start or an increment")
	c.Assert(messages, qt.Contains, "YD108: ALTER SEQUENCE /local/shop/orders/_serial_column_id alters a sequence an "+
		"earlier statement restarted at 100, and YDB replays that restart on every later ALTER SEQUENCE, so the next row of "+
		"shop/orders takes 100 again and fails on a key a row already holds (Conflict with existing key); restart it in this "+
		"statement at a value past every row, or leave it as it is")
}
