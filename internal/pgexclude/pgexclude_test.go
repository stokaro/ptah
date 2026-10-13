package pgexclude_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/internal/pgexclude"
)

func TestParse_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name, sql string
		want      pgexclude.Definition
	}{
		{"elements", "EXCLUDE USING gist (room_id WITH =, during WITH &&)", pgexclude.Definition{UsingMethod: "gist", Elements: "room_id WITH =, during WITH &&"}},
		{"predicate", "EXCLUDE USING btree (r WITH =) WHERE ((r > 0))", pgexclude.Definition{UsingMethod: "btree", Elements: "r WITH =", WhereCondition: "(r > 0)"}},
		{"quoted close in element", `EXCLUDE USING btree (replace(t, ')', '(') WITH =)`, pgexclude.Definition{UsingMethod: "btree", Elements: `replace(t, ')', '(') WITH =`}},
		{"quoted open in element", `EXCLUDE USING btree (replace(t, '(', '') WITH =)`, pgexclude.Definition{UsingMethod: "btree", Elements: `replace(t, '(', '') WITH =`}},
		{"quoted identifiers", `EXCLUDE USING btree ("r)" WITH =) WHERE ("r)" > 0)`, pgexclude.Definition{UsingMethod: "btree", Elements: `"r)" WITH =`, WhereCondition: `"r)" > 0`}},
		{"dollar quoted text", `EXCLUDE USING btree (replace(t, $q$)$q$, '') WITH =)`, pgexclude.Definition{UsingMethod: "btree", Elements: `replace(t, $q$)$q$, '') WITH =`}},
		{"comment delimiters", `EXCLUDE /* ) */ USING btree (r /* ( */ WITH =) WHERE (r > 0 /* ) */)`, pgexclude.Definition{UsingMethod: "btree", Elements: `r /* ( */ WITH =`, WhereCondition: `r > 0 /* ) */`}},
		{"deferrable", "EXCLUDE USING btree (r WITH =) DEFERRABLE", pgexclude.Definition{UsingMethod: "btree", Elements: "r WITH =", Deferrable: true}},
		{"deferred predicate", "EXCLUDE USING btree (r WITH =) WHERE ((r > 0)) DEFERRABLE INITIALLY DEFERRED", pgexclude.Definition{UsingMethod: "btree", Elements: "r WITH =", WhereCondition: "(r > 0)", Deferrable: true, Initially: "deferred"}},
		{"immediate", "EXCLUDE USING btree (r WITH =) NOT DEFERRABLE INITIALLY IMMEDIATE", pgexclude.Definition{UsingMethod: "btree", Elements: "r WITH =", Initially: "immediate"}},
		{"deferral in a string", `EXCLUDE USING btree (t WITH =) WHERE (t <> ' DEFERRABLE INITIALLY DEFERRED')`, pgexclude.Definition{UsingMethod: "btree", Elements: "t WITH =", WhereCondition: `t <> ' DEFERRABLE INITIALLY DEFERRED'`}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgexclude.Parse(test.sql)
			c.Assert(err, qt.IsNil)
			c.Assert(*got, qt.Equals, test.want)
		})
	}
}

func TestParse_FailurePath(t *testing.T) {
	for _, test := range []struct{ name, sql, reason string }{
		{"another constraint", "CHECK (r > 0)", "expected EXCLUDE USING"},
		{"method", "EXCLUDE USING", "expected an index method after USING"},
		{"opening group", "EXCLUDE USING btree r WITH =", `expected '\(' before elements`},
		{"unclosed group", "EXCLUDE USING btree (r WITH =", "unclosed elements"},
		{"empty elements", "EXCLUDE USING btree ()", "empty elements"},
		{"empty predicate", "EXCLUDE USING btree (r WITH =) WHERE ()", "empty WHERE predicate"},
		{"unclosed predicate", "EXCLUDE USING btree (r WITH =) WHERE (r > 0", "unclosed WHERE predicate"},
		{"unparenthesized predicate", "EXCLUDE USING btree (r WITH =) WHERE r > 0", `expected '\(' before WHERE predicate`},
		{"include", "EXCLUDE USING btree (r WITH =) INCLUDE (s)", "unrepresented or unexpected clause INCLUDE"},
		{"storage parameters", "EXCLUDE USING btree (r WITH =) WITH (fillfactor = 80)", "unrepresented or unexpected clause WITH"},
		{"tablespace", "EXCLUDE USING btree (r WITH =) USING INDEX TABLESPACE custom", "unrepresented or unexpected clause USING"},
		{"extra expression", "EXCLUDE USING btree (r WITH =) WHERE (r > 0) OR (r < 0)", "unrepresented or unexpected clause OR"},
		{"incomplete timing", "EXCLUDE USING btree (r WITH =) DEFERRABLE INITIALLY", "expected DEFERRED or IMMEDIATE after INITIALLY"},
		{"contradictory timing", "EXCLUDE USING btree (r WITH =) NOT DEFERRABLE INITIALLY DEFERRED", "INITIALLY DEFERRED requires DEFERRABLE"},
		{"statement separator", "EXCLUDE USING btree (r WITH =); DROP TABLE t", "unexpected token ;"},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := pgexclude.Parse(test.sql)
			c.Assert(err, qt.ErrorIs, schemaext.ErrInvalidValue)
			c.Assert(err, qt.ErrorMatches, `.*PostgreSQL EXCLUDE: `+test.reason)
			c.Assert(got, qt.IsNil)
		})
	}
}
