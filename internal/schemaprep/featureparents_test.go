package schemaprep_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/clickhouse/chschema"
	"ptah.run/internal/schemaprep"
)

// policyOn is a database that declares the table schema.table and the child
// ref names. Only a ClickHouse row policy is a feature object with a parent
// table, so the PostgreSQL rows build one under PostgreSQL's rules: the check
// reads the identity, not the kind.
func policyOn(c *qt.C, schema, table string, ref objectidentity.ID) *schemamodel.Database {
	c.Helper()
	policy := must.Must(chschema.DesiredRowPolicyObject(ref, chschema.DesiredRowPolicy{}))
	return &schemamodel.Database{Tables: []schemamodel.Table{{Schema: schema, Name: table}}, FeatureObjects: must.Must(schemaext.NewObjects(policy))}
}

// connectionTo is dialect's identifier semantics for a connection whose
// default schema is schema, under which an unqualified name is built.
func connectionTo(dialect, schema string) identifier.Semantics {
	semantics := identifier.ForDialect(dialect)
	semantics.DefaultSchema = schema
	return semantics
}

// TestValidateFeatureParents_HappyPath accepts a child on t beside its table.
// On ClickHouse, which has no default schema, that holds whether the child's
// identity was built under the dialect's rules or under a connection's
// database, as a rollback captures it from a read. On PostgreSQL a child
// built under the connection's app pairs with app.t.
func TestValidateFeatureParents_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect string
		schema  string
		ref     objectidentity.ID
	}{
		{name: "ClickHouse, built under the dialect's rules", dialect: "clickhouse",
			ref: chschema.RowPolicyRefWith(identifier.ForDialect("clickhouse"), "", "t", "p")},
		{name: "ClickHouse, built under a connection's database", dialect: "clickhouse",
			ref: chschema.RowPolicyRefWith(connectionTo("clickhouse", "d1"), "", "t", "p")},
		{name: "PostgreSQL, built under a connection's schema", dialect: "postgres", schema: "app",
			ref: chschema.RowPolicyRefWith(connectionTo("postgres", "app"), "", "t", "p")},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateFeatureParents(policyOn(c, test.schema, "t", test.ref), test.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestValidateFeatureParents_FailurePath refuses a child whose table the
// database does not declare: on ClickHouse a policy on t beside only u,
// however its identity was built, and one whose schema the author wrote as d2
// beside the table t, which names none. On PostgreSQL a child built under the
// connection's app is refused beside public.t, the table an unqualified name
// means under the dialect's own default.
func TestValidateFeatureParents_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		dialect string
		schema  string
		table   string
		ref     objectidentity.ID
		want    string
	}{
		{name: "ClickHouse, no table t under the dialect's rules", dialect: "clickhouse", table: "u",
			ref:  chschema.RowPolicyRefWith(identifier.ForDialect("clickhouse"), "", "t", "p"),
			want: `.*feature object ptah.run/clickhouse/row-policy t\.p has no declared parent table`},
		{name: "ClickHouse, no table t under a connection's database", dialect: "clickhouse", table: "u",
			ref:  chschema.RowPolicyRefWith(connectionTo("clickhouse", "d1"), "", "t", "p"),
			want: `.*feature object ptah.run/clickhouse/row-policy d1\.t\.p has no declared parent table`},
		{name: "ClickHouse, a schema the author wrote", dialect: "clickhouse", table: "t",
			ref:  chschema.RowPolicyRef("d2", "t", "p"),
			want: `.*feature object ptah.run/clickhouse/row-policy d2\.t\.p has no declared parent table`},
		{name: "PostgreSQL, the dialect's default schema beside a connection's", dialect: "postgres", schema: "public", table: "t",
			ref:  chschema.RowPolicyRefWith(connectionTo("postgres", "app"), "", "t", "p"),
			want: `.*feature object ptah.run/clickhouse/row-policy app\.t\.p has no declared parent table`},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := schemaprep.ValidateFeatureParents(policyOn(c, test.schema, test.table, test.ref), test.dialect)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrInvalidSchemaDiff)
			c.Assert(err, qt.ErrorMatches, test.want)
		})
	}
}
