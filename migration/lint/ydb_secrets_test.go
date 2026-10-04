package lint_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/migration/lint"
)

// TestYD120_ReportsASecretValueInTheMigration reports every statement that
// writes a secret's value into the file, up and down alike, and none that
// refers to a variable Ptah defines when the statement runs.
func TestYD120_ReportsASecretValueInTheMigration(t *testing.T) {
	tests := []struct {
		name  string
		files map[string]string
		want  []string
	}{
		{
			name: "a literal in CREATE SECRET and in ALTER SECRET",
			files: map[string]string{
				"0001_s.up.sql": "CREATE SECRET `ext/pw` WITH (value = 's3cr3t');\nALTER SECRET `ext/pw` WITH (value = \"rotated\");\n",
			},
			want: []string{"0001_s.up.sql:1:YD120", "0001_s.up.sql:2:YD120"},
		},
		{
			name: "a named expression Ptah does not define",
			files: map[string]string{
				"0001_s.up.sql": "$v = 's3cr3t';\nCREATE SECRET pw WITH (value = $v);\n",
			},
			want: []string{"0001_s.up.sql:2:YD120"},
		},
		{
			name: "the deprecated secret object, in the down half too",
			files: map[string]string{
				"0001_s.up.sql":   "CREATE OBJECT pw (TYPE SECRET) WITH value = 's3cr3t';\n",
				"0001_s.down.sql": "UPSERT OBJECT pw (TYPE SECRET) WITH value = 'old';\n",
			},
			want: []string{"0001_s.down.sql:1:YD120", "0001_s.up.sql:1:YD120"},
		},
		{
			name: "the statements Ptah writes: a reference, a rotation and a drop",
			files: map[string]string{
				"0001_s.up.sql": "CREATE SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n" +
					"ALTER SECRET `ext/pw` WITH (value = $PTAH_SECRET_PW);\n",
				"0001_s.down.sql": "DROP SECRET `ext/pw`;\n",
			},
			want: make([]string, 0),
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbSites(ydbLint(c, test.files, "")), qt.DeepEquals, test.want)
		})
	}
}

// TestYD120_NamesTheSecretAndNeverTheValue says where the value should come
// from, and repeats nothing the statement wrote after the secret's path.
func TestYD120_NamesTheSecretAndNeverTheValue(t *testing.T) {
	c := qt.New(t)
	target, err := lint.ResolveTarget("ydb", "")
	c.Assert(err, qt.IsNil)

	findings, err := lint.LintFS(fixture(map[string]string{
		"0001_s.up.sql": "CREATE SECRET `ext/pw` WITH (value = 's3cr3t-SENTINEL');\n",
	}), lint.Options{Dialect: "ydb", Target: target})

	c.Assert(err, qt.IsNil)
	c.Assert(findings, qt.HasLen, 1)
	c.Assert(findings[0].Message, qt.Equals, "CREATE SECRET ext/pw writes the secret's value into the migration file, "+
		"where anyone who reads the file or a plan reads it; write `CREATE SECRET ext/pw WITH (value = "+
		"$PTAH_SECRET_NAME)` and set PTAH_SECRET_NAME where the migration runs, which Ptah defines when the "+
		"statement runs and never writes down")
	c.Assert(findings[0].Message, qt.Not(qt.Contains), "SENTINEL")
}
