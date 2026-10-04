package ydbsecret_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/ydbsecret"
)

// environment answers lookups from a fixed set of variables, the way
// os.LookupEnv answers them from the process.
func environment(values map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		value, set := values[name]
		return value, set
	}
}

func TestExpand_HappyPath(t *testing.T) {
	env := environment(map[string]string{
		"PTAH_SECRET_PW":    `a'b\c`,
		"PTAH_SECRET_KEY":   "k-SENTINEL",
		"PTAH_SECRET_EMPTY": "",
	})
	tests := []struct {
		name    string
		query   string
		want    string
		defines bool
	}{
		{
			name:  "a query that refers to no secret value is unchanged",
			query: "SELECT 1;",
			want:  "SELECT 1;",
		},
		{
			name:    "the value of CREATE SECRET is defined in front of it, escaped as a String literal",
			query:   "CREATE SECRET `app/pw` WITH (value = $PTAH_SECRET_PW);",
			want:    "$PTAH_SECRET_PW = 'a\\'b\\\\c';\nCREATE SECRET `app/pw` WITH (value = $PTAH_SECRET_PW);",
			defines: true,
		},
		{
			name:    "the value of ALTER SECRET is defined the same way",
			query:   "ALTER SECRET `app/pw` WITH (value = $PTAH_SECRET_KEY);",
			want:    "$PTAH_SECRET_KEY = 'k-SENTINEL';\nALTER SECRET `app/pw` WITH (value = $PTAH_SECRET_KEY);",
			defines: true,
		},
		{
			name:  "the definition follows the translation setting and the pragma ahead of the statement",
			query: "--!syntax_v1\nPRAGMA TablePathPrefix = '/local/r';\nCREATE SECRET s WITH (value = $PTAH_SECRET_KEY);",
			want: "--!syntax_v1\nPRAGMA TablePathPrefix = '/local/r';\n$PTAH_SECRET_KEY = 'k-SENTINEL';\n" +
				"CREATE SECRET s WITH (value = $PTAH_SECRET_KEY);",
			defines: true,
		},
		{
			name:  "a variable two statements name is defined once, ahead of the first",
			query: "CREATE SECRET a WITH (value = $PTAH_SECRET_KEY);\nALTER SECRET b WITH (value = $PTAH_SECRET_KEY);",
			want: "$PTAH_SECRET_KEY = 'k-SENTINEL';\nCREATE SECRET a WITH (value = $PTAH_SECRET_KEY);\n" +
				"ALTER SECRET b WITH (value = $PTAH_SECRET_KEY);",
			defines: true,
		},
		{
			name:    "keywords in any case, and the value after another option",
			query:   "create secret s with (inherit_permissions = TRUE, VALUE = $PTAH_SECRET_KEY)",
			want:    "$PTAH_SECRET_KEY = 'k-SENTINEL';\ncreate secret s with (inherit_permissions = TRUE, VALUE = $PTAH_SECRET_KEY)",
			defines: true,
		},
		{
			name:    "a variable that is set and empty is the empty value",
			query:   "CREATE SECRET s WITH (value = $PTAH_SECRET_EMPTY);",
			want:    "$PTAH_SECRET_EMPTY = '';\nCREATE SECRET s WITH (value = $PTAH_SECRET_EMPTY);",
			defines: true,
		},
		{
			name:  "a reference inside a string literal or a comment is text, not a reference",
			query: "SELECT '$PTAH_SECRET_KEY' /* $PTAH_SECRET_KEY */; -- $PTAH_SECRET_KEY",
			want:  "SELECT '$PTAH_SECRET_KEY' /* $PTAH_SECRET_KEY */; -- $PTAH_SECRET_KEY",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbsecret.Expand(tc.query, env)
			c.Assert(err, qt.IsNil)
			c.Assert(got.Text, qt.Equals, tc.want)
			c.Assert(got.Defines(), qt.Equals, tc.defines)
		})
	}
}

func TestExpand_FailurePath(t *testing.T) {
	env := environment(map[string]string{"PTAH_SECRET_KEY": "k-SENTINEL"})
	references := []struct {
		name  string
		query string
	}{
		{name: "a read", query: "SELECT $PTAH_SECRET_KEY;"},
		{name: "a write", query: "UPSERT INTO t (id, v) VALUES (1, $PTAH_SECRET_KEY);"},
		{name: "a definition the query writes itself",
			query: "$PTAH_SECRET_KEY = 'other'; CREATE SECRET s WITH (value = $PTAH_SECRET_KEY);"},
		{name: "a definition that copies the value",
			query: "$v = $PTAH_SECRET_KEY; CREATE SECRET s WITH (value = $PTAH_SECRET_KEY);"},
		{name: "the path of a secret", query: "CREATE SECRET $PTAH_SECRET_KEY WITH (value = 'x');"},
		{name: "an expression around the value",
			query: "CREATE SECRET s WITH (value = $PTAH_SECRET_KEY || 'x');"},
		{name: "another option", query: "CREATE SECRET s WITH (inherit_permissions = $PTAH_SECRET_KEY);"},
		{name: "an action body",
			query: "DEFINE ACTION $a() AS CREATE SECRET s WITH (value = $PTAH_SECRET_KEY); END DEFINE; DO $a();"},
		{name: "the deprecated secret object",
			query: "CREATE OBJECT s (TYPE SECRET) WITH value = $PTAH_SECRET_KEY;"},
		{name: "a spelling in another case outside a value", query: "SELECT $ptah_secret_key;"},
	}
	for _, tc := range references {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := ydbsecret.Expand(tc.query, env)
			c.Assert(err, qt.ErrorIs, ydbsecret.ErrReference)
			c.Assert(err, qt.Not(qt.ErrorMatches), ".*SENTINEL.*")
			c.Assert(got.Text, qt.Equals, "")
			c.Assert(got.Defines(), qt.IsFalse)
		})
	}

	t.Run("a variable that is not set", func(t *testing.T) {
		c := qt.New(t)
		got, err := ydbsecret.Expand("CREATE SECRET s WITH (value = $PTAH_SECRET_MISSING);", env)
		c.Assert(err, qt.ErrorMatches,
			"a secret's value comes from environment variable PTAH_SECRET_MISSING, which is not set")
		c.Assert(got.Text, qt.Equals, "")
		c.Assert(got.Defines(), qt.IsFalse)
	})

	t.Run("a value spelled in another case than the prefix", func(t *testing.T) {
		c := qt.New(t)
		got, err := ydbsecret.Expand("CREATE SECRET s WITH (value = $ptah_secret_key);", env)
		c.Assert(err, qt.ErrorMatches, `secret value \$ptah_secret_key: invalid value_env: "ptah_secret_key" does `+
			`not start with PTAH_SECRET_ .*`)
		c.Assert(got.Text, qt.Equals, "")
		c.Assert(got.Defines(), qt.IsFalse)
	})
}

func TestExpansion_Redact_HappyPath(t *testing.T) {
	c := qt.New(t)
	env := environment(map[string]string{"PTAH_SECRET_A": "abc", "PTAH_SECRET_B": "abcdef"})
	expanded, err := ydbsecret.Expand(
		"CREATE SECRET a WITH (value = $PTAH_SECRET_A); CREATE SECRET b WITH (value = $PTAH_SECRET_B);", env)
	c.Assert(err, qt.IsNil)

	// The longer value goes first, so a value holding another is replaced
	// whole rather than leaving its tail behind.
	c.Assert(expanded.Redact("failed near 'abcdef' and 'abc'"), qt.Equals, "failed near '[secret]' and '[secret]'")
	c.Assert(ydbsecret.Expansion{Text: "SELECT 1"}.Redact("abc"), qt.Equals, "abc")
}

func TestReferences_HappyPath(t *testing.T) {
	tests := []struct {
		name  string
		query string
		want  []string
	}{
		{name: "none", query: "SELECT 1"},
		{name: "each variable once, in order",
			query: "ALTER SECRET a WITH (value = $PTAH_SECRET_B); SELECT $PTAH_SECRET_A, $PTAH_SECRET_B;",
			want:  []string{"PTAH_SECRET_B", "PTAH_SECRET_A"}},
		{name: "a string literal holds none", query: "SELECT '$PTAH_SECRET_A'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			c.Assert(ydbsecret.References(tc.query), qt.DeepEquals, tc.want)
		})
	}
}

func TestClearValue_HappyPath(t *testing.T) {
	tests := []struct {
		name      string
		statement string
		form      string
		path      string
		writes    bool
	}{
		{name: "a literal value", statement: "CREATE SECRET `app/pw` WITH (value = 's3cr3t')",
			form: "CREATE SECRET", path: "app/pw", writes: true},
		{name: "a literal rotation", statement: "alter secret pw with (VALUE = \"x\")",
			form: "ALTER SECRET", path: "pw", writes: true},
		{name: "a named expression Ptah does not define",
			statement: "CREATE SECRET pw WITH (value = $v)", form: "CREATE SECRET", path: "pw", writes: true},
		{name: "the prefix in another case, which Ptah does not define",
			statement: "CREATE SECRET pw WITH (value = $ptah_secret_pw)", form: "CREATE SECRET", path: "pw", writes: true},
		{name: "a reference Ptah defines", statement: "CREATE SECRET pw WITH (value = $PTAH_SECRET_PW)",
			form: "CREATE SECRET", path: "pw"},
		{name: "the deprecated object", statement: "CREATE OBJECT pw (TYPE SECRET) WITH value = 's3cr3t'",
			form: "CREATE OBJECT ... (TYPE SECRET)", path: "pw", writes: true},
		{name: "the deprecated object, written twice", statement: "UPSERT OBJECT pw (TYPE SECRET) WITH (value = 'x')",
			form: "UPSERT OBJECT ... (TYPE SECRET)", path: "pw", writes: true},
		{name: "a drop carries no value", statement: "DROP SECRET pw"},
		{name: "a deprecated drop carries none either", statement: "DROP OBJECT pw (TYPE SECRET)"},
		{name: "another statement", statement: "SELECT 's3cr3t'"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			form, path, writes := ydbsecret.ClearValue(tc.statement)
			c.Assert(form, qt.Equals, tc.form)
			c.Assert(path, qt.Equals, tc.path)
			c.Assert(writes, qt.Equals, tc.writes)
		})
	}
}
