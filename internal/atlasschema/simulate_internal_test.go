package atlasschema

// White-box testing required: normalizeBaselineSerialColumns adapts
// introspected PostgreSQL SERIAL columns for dev-database baseline
// recreation, and its effect is only observable through SQL executed against
// a live PostgreSQL server. The pure rewrite rules are asserted here; the
// end-to-end behavior is covered by the live schema apply verification.

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
)

func TestNextvalSequenceName(t *testing.T) {
	c := qt.New(t)

	tests := []struct {
		name        string
		defaultExpr string
		want        string
	}{
		{name: "regclass cast", defaultExpr: `nextval('users_id_seq'::regclass)`, want: "users_id_seq"},
		{name: "plain", defaultExpr: `nextval('users_id_seq')`, want: "users_id_seq"},
		{name: "case insensitive prefix", defaultExpr: `NEXTVAL('users_id_seq')`, want: "users_id_seq"},
		{name: "schema qualified", defaultExpr: `nextval('public.users_id_seq'::regclass)`, want: "users_id_seq"},
		{name: "quoted identifier", defaultExpr: `nextval('"Users_id_seq"'::regclass)`, want: "Users_id_seq"},
		{name: "not nextval", defaultExpr: `now()`, want: ""},
		{name: "empty", defaultExpr: "", want: ""},
		{name: "missing quotes", defaultExpr: `nextval(users_id_seq)`, want: ""},
	}

	for _, test := range tests {
		c.Run(test.name, func(c *qt.C) {
			c.Assert(nextvalSequenceName(test.defaultExpr), qt.Equals, test.want)
		})
	}
}

func TestNormalizeBaselineSerialColumns(t *testing.T) {
	c := qt.New(t)

	c.Run("implicit serial sequence becomes SERIAL", func(c *qt.C) {
		baseline := &goschema.Database{Fields: []goschema.Field{
			{Name: "id", Type: "integer", DefaultExpr: `nextval('users_id_seq'::regclass)`},
			{Name: "big_id", Type: "bigint", DefaultExpr: `nextval('users_big_id_seq'::regclass)`},
			{Name: "small_id", Type: "smallint", DefaultExpr: `nextval('users_small_id_seq'::regclass)`},
		}}

		normalizeBaselineSerialColumns(baseline, "postgres")

		c.Assert(baseline.Fields[0].Type, qt.Equals, "SERIAL")
		c.Assert(baseline.Fields[0].DefaultExpr, qt.Equals, "")
		c.Assert(baseline.Fields[1].Type, qt.Equals, "BIGSERIAL")
		c.Assert(baseline.Fields[2].Type, qt.Equals, "SMALLSERIAL")
	})

	c.Run("explicitly introspected sequence keeps its default", func(c *qt.C) {
		baseline := &goschema.Database{
			Sequences: []goschema.Sequence{{Name: "order_number_seq"}},
			Fields: []goschema.Field{
				{Name: "order_number", Type: "integer", DefaultExpr: `nextval('order_number_seq'::regclass)`},
			},
		}

		normalizeBaselineSerialColumns(baseline, "postgres")

		c.Assert(baseline.Fields[0].Type, qt.Equals, "integer")
		c.Assert(baseline.Fields[0].DefaultExpr, qt.Equals, `nextval('order_number_seq'::regclass)`)
	})

	c.Run("non integer type keeps its default", func(c *qt.C) {
		baseline := &goschema.Database{Fields: []goschema.Field{
			{Name: "label", Type: "text", DefaultExpr: `nextval('labels_seq'::regclass)`},
		}}

		normalizeBaselineSerialColumns(baseline, "postgres")

		c.Assert(baseline.Fields[0].Type, qt.Equals, "text")
		c.Assert(baseline.Fields[0].DefaultExpr, qt.Equals, `nextval('labels_seq'::regclass)`)
	})

	c.Run("non postgres dialect is untouched", func(c *qt.C) {
		baseline := &goschema.Database{Fields: []goschema.Field{
			{Name: "id", Type: "integer", DefaultExpr: `nextval('users_id_seq'::regclass)`},
		}}

		normalizeBaselineSerialColumns(baseline, "mysql")

		c.Assert(baseline.Fields[0].Type, qt.Equals, "integer")
		c.Assert(baseline.Fields[0].DefaultExpr, qt.Equals, `nextval('users_id_seq'::regclass)`)
	})
}
