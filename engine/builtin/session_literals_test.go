package builtin_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/engine/builtin"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

// sessionLiteralSchema is a MySQL schema file whose quoted defaults, column
// comment, enum value, index comment and table comment each hold a backslash
// in the default sql_mode's spelling, one of them before a quote and a
// semicolon.
const sessionLiteralSchema = "CREATE TABLE t (id int PRIMARY KEY, a varchar(20) DEFAULT 'a\\\\b' COMMENT 'c\\\\d', q varchar(20) DEFAULT 'it\\\\\\';x', " +
	"e enum('x\\\\y','z'), KEY k (a) COMMENT 'i\\\\j') COMMENT='t\\\\c';\n"

// Each literal a render writes from the model is written as the target
// session reads it: under the default sql_mode with each backslash doubled,
// and on a session whose sql_mode names NO_BACKSLASH_ESCAPES with each one
// single, so both store the characters the file declares.
func TestGetOrderedCreateStatementsWithCapabilities_LiteralsForTheSession(t *testing.T) {
	tests := []struct {
		name string
		caps capability.Capabilities
		want []string
	}{
		{
			name: "the default sql_mode",
			caps: capability.ForDialect(platform.MySQL),
			want: []string{
				"CREATE TABLE `t` (\n  `id` int PRIMARY KEY,\n  `a` varchar(20) COMMENT 'c\\\\d' DEFAULT 'a\\\\b',\n" +
					"  `q` varchar(20) DEFAULT 'it\\\\\\';x',\n  `e` enum('x\\\\y','z')\n) COMMENT='t\\\\c';\n\n",
				"CREATE INDEX `k` ON `t` (`a`) COMMENT 'i\\\\j';\n",
			},
		},
		{
			name: "NO_BACKSLASH_ESCAPES",
			caps: capability.ForDialect(platform.MySQL).With(capability.NoBackslashEscapes, true),
			want: []string{
				"CREATE TABLE `t` (\n  `id` int PRIMARY KEY,\n  `a` varchar(20) COMMENT 'c\\d' DEFAULT 'a\\b',\n" +
					"  `q` varchar(20) DEFAULT 'it\\'';x',\n  `e` enum('x\\y','z')\n) COMMENT='t\\c';\n\n",
				"CREATE INDEX `k` ON `t` (`a`) COMMENT 'i\\j';\n",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(t.Context(), []byte(sessionLiteralSchema), platform.MySQL, nil,
				sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.IsNil)

			statements, err := builtin.GetOrderedCreateStatementsWithCapabilities(&database, platform.MySQL, test.caps)

			c.Assert(err, qt.IsNil)
			c.Assert(statements, qt.DeepEquals, test.want)
		})
	}
}
