package pgdefaultprivilegesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/internal/builtintest"
	"ptah.run/internal/parser"
	"ptah.run/internal/sqlschema"
)

func TestSQL_InvalidOrUnsupportedSyntaxIsRefused(t *testing.T) {
	tests := []struct{ name, text, reason string }{
		{"session grantor", "ALTER DEFAULT PRIVILEGES GRANT SELECT ON TABLES TO reader;", "requires FOR ROLE"},
		{"routines", alterDefault + "GRANT EXECUTE ON ROUTINES TO reader;", "ROUTINES"},
		{"scoped schemas", alterDefault + "GRANT USAGE ON SCHEMAS TO reader;", "SCHEMAS"},
		{"scoped large objects", alterDefault + "REVOKE SELECT ON LARGE OBJECTS FROM reader;", "LARGE OBJECTS"},
		{"incomplete class", "ALTER DEFAULT PRIVILEGES FOR ROLE owner GRANT SELECT ON LARGE TO reader;", "expected OBJECTS"},
		{"grantor list", "ALTER DEFAULT PRIVILEGES FOR ROLE owner, other GRANT SELECT ON TABLES TO reader;", "one grantor role"},
		{"schema list", "ALTER DEFAULT PRIVILEGES FOR ROLE owner IN SCHEMA app, other GRANT SELECT ON TABLES TO reader;", "one schema"},
		{"grantee list", alterDefault + "GRANT SELECT ON TABLES TO reader, other;", "one grantee"},
		{"unknown action", alterDefault + "SELECT ON TABLES TO reader;", "expected GRANT or REVOKE"},
		{"group scope", "ALTER DEFAULT PRIVILEGES FOR GROUP owner GRANT SELECT ON TABLES TO reader;", "expected ROLE or USER"},
		{"misspelled privileges", "ALTER DEFAULT PRIVILEGE FOR ROLE owner GRANT SELECT ON TABLES TO reader;", "unsupported ALTER target: DEFAULT"},
		{"cascade", alterDefault + "REVOKE SELECT ON TABLES FROM reader CASCADE;", "CASCADE"},
		{"grantor clause", alterDefault + "GRANT SELECT ON TABLES TO reader GRANTED BY other;", "GRANTED"},
		{"column privilege", alterDefault + "GRANT SELECT (id) ON TABLES TO reader;", "expected ON"},
		{"unknown privilege", alterDefault + "GRANT EXECUTE ON TABLES TO reader;", "EXECUTE"},
		{"unclosed name", alterDefault + `GRANT SELECT ON TABLES TO "reader;`, "closed quoted grantee"},
		{"empty name", alterDefault + `GRANT SELECT ON TABLES TO "";`, "grantee"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.text, parser.WithDialect("postgres"), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.reason+".*")
			c.Assert(statements, qt.IsNil)
		})
	}
}

func TestSQL_UnmodeledIntentKeepsSyntaxButRefusesSchemaLoading(t *testing.T) {
	tests := []struct{ name, text, reason string }{
		{"option without grant", alterDefault + "REVOKE GRANT OPTION FOR SELECT ON TABLES FROM reader;", "this schema does not grant"},
		{"individual revoke under all", alterDefault + "GRANT ALL ON TABLES TO reader;\n" + alterDefault + "REVOKE SELECT ON TABLES FROM reader;", "server-specific privilege set"},
		{"individual option under all", alterDefault + "GRANT ALL ON TABLES TO reader WITH GRANT OPTION;\n" + alterDefault + "REVOKE GRANT OPTION FOR SELECT ON TABLES FROM reader;", "server-specific privilege set"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			statements, err := parser.NewParser(test.text, parser.WithDialect("postgres"), parser.WithSQL(builtintest.Runtime().SQL())).Parse()
			c.Assert(err, qt.IsNil)
			c.Assert(len(statements.Statements) > 0, qt.IsTrue)
			db, _, err := sqlschema.ReadWith(c.Context(), []byte(test.text), "postgres", nil, sqlschema.Options{Owners: builtintest.Runtime().SQL()})
			c.Assert(err, qt.ErrorIs, sqlschema.ErrUnmodeledStatement)
			c.Assert(err, qt.ErrorMatches, "(?s).*"+test.reason+".*")
			c.Assert(db.FeatureObjects.Len(), qt.Equals, 0)
		})
	}
}
