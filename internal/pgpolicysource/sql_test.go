package pgpolicysource_test

import (
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/sqlext"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/sqlschema"
)

func readPolicySQL(c *qt.C, source string) (schemamodel.Database, error) {
	c.Helper()
	runtime := builtintest.Runtime()
	database, _, err := sqlschema.ReadWith(c.Context(), []byte(source), "postgres", nil,
		sqlschema.Options{Owners: runtime.SQL(), Relations: runtime})
	return database, err
}

// TestSQL_TableLifetime keeps switches within the table a statement names.
// A DROP followed by CREATE gives the name a new table with new defaults.
func TestSQL_TableLifetime(t *testing.T) {
	tests := []struct {
		name, source string
		want         pgpolicy.DesiredTableState
	}{
		{name: "enable", source: `CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, want: pgpolicy.DesiredTableState{Enabled: true}},
		{name: "force after enable", source: `CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY; ALTER TABLE orders FORCE ROW LEVEL SECURITY;`, want: pgpolicy.DesiredTableState{Enabled: true, Forced: true}},
		{name: "force before enable", source: `CREATE TABLE orders(id int); ALTER TABLE orders FORCE ROW LEVEL SECURITY; ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, want: pgpolicy.DesiredTableState{Enabled: true, Forced: true}},
		{name: "only qualified aliases", source: `CREATE TABLE orders(id int); ALTER TABLE ONLY public.orders FORCE ROW LEVEL SECURITY; ALTER TABLE "public"."orders" ENABLE ROW LEVEL SECURITY;`, want: pgpolicy.DesiredTableState{Enabled: true, Forced: true}},
		{name: "dropped force is not inherited", source: `CREATE TABLE orders(id int); ALTER TABLE orders FORCE ROW LEVEL SECURITY; DROP TABLE orders; CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, want: pgpolicy.DesiredTableState{Enabled: true}},
		{name: "dropped enable is not a duplicate", source: `CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY; DROP TABLE orders; CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, want: pgpolicy.DesiredTableState{Enabled: true}},
		{name: "a drop of another table leaves force", source: `CREATE TABLE orders(id int); CREATE TABLE other(id int); ALTER TABLE orders FORCE ROW LEVEL SECURITY; DROP TABLE other; ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, want: pgpolicy.DesiredTableState{Enabled: true, Forced: true}},
		{name: "a multi-table drop ends the lifetime", source: `CREATE TABLE orders(id int); CREATE TABLE other(id int); ALTER TABLE orders FORCE ROW LEVEL SECURITY; DROP TABLE other, orders; CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, want: pgpolicy.DesiredTableState{Enabled: true}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, err := readPolicySQL(c, test.source)
			c.Assert(err, qt.IsNil)
			c.Assert(database.Tables, qt.HasLen, 1)
			c.Assert(switchesOf(c, &database), qt.DeepEquals, &test.want)
			c.Assert(database.RLSEnabledTables, qt.HasLen, 0)
		})
	}
}

// TestSQL_PolicyPayload preserves role identity, expressions and comments in
// the owner model. Common row-security fields stay empty.
func TestSQL_PolicyPayload(t *testing.T) {
	c := qt.New(t)
	database, err := readPolicySQL(c, `CREATE TABLE "Orders"(id int); CREATE POLICY "Read" ON "Orders" AS RESTRICTIVE FOR UPDATE TO "CURRENT_USER", reader, CURRENT_USER, "a,b", "a""b" USING (id > 0 AND coalesce('x)', 'y') = 'x)') WITH CHECK (id < 100); COMMENT ON POLICY "Read" ON public."Orders" IS 'tenant''s policy';`)
	c.Assert(err, qt.IsNil)
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref.Name.Source, qt.Equals, "Read")
	c.Assert(objects[0].Ref.Parent.Source, qt.Equals, "Orders")
	c.Assert(objects[0].Value, qt.DeepEquals, &pgpolicy.DesiredPolicy{
		Command: pgpolicy.CommandUpdate, Composition: pgpolicy.Restrictive,
		Roles: []pgpolicy.RoleSelector{{Name: "CURRENT_USER"}, {Name: "reader"}, {Keyword: pgpolicy.CurrentUser}, {Name: "a,b"}, {Name: `a"b`}},
		Using: new(`id > 0 AND coalesce('x)', 'y') = 'x)'`), WithCheck: new("id < 100"), Comment: "tenant's policy",
	})
	c.Assert(database.RLSPolicies, qt.HasLen, 0)
}

// TestSQL_CommentAndDrop applies comments to the declaration in scope. A
// policy with a reused name starts with no comment from the dropped table.
func TestSQL_CommentAndDrop(t *testing.T) {
	tests := []struct{ name, tail, want string }{
		{name: "replace", tail: `COMMENT ON POLICY p ON orders IS 'second';`, want: "second"},
		{name: "clear", tail: `COMMENT ON POLICY p ON orders IS NULL;`},
		{name: "dollar string", tail: `COMMENT ON POLICY p ON orders IS $note$it's second$note$;`, want: "it's second"},
		{name: "escape string", tail: `COMMENT ON POLICY p ON orders IS E'it\'s second';`, want: "it's second"},
		{name: "recreate", tail: `DROP TABLE orders; CREATE TABLE orders(id int); CREATE POLICY p ON orders USING (true);`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, err := readPolicySQL(c, `CREATE TABLE orders(id int); CREATE POLICY p ON orders USING (true); COMMENT ON POLICY p ON orders IS 'first';`+test.tail)
			c.Assert(err, qt.IsNil)
			objects, err := database.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.HasLen, 1)
			policy, ok := objects[0].Value.(*pgpolicy.DesiredPolicy)
			c.Assert(ok, qt.IsTrue)
			c.Assert(policy.Comment, qt.Equals, test.want)
			c.Assert(policy.Using, qt.DeepEquals, new("true"))
		})
	}
}

// TestSQL_RefusesInvalidDeclarations checks the public reader so a parser
// rejection and a declaration-order rejection both reach the caller.
func TestSQL_RefusesInvalidDeclarations(t *testing.T) {
	tests := []struct{ name, source, want string }{
		{name: "force without enable", source: `CREATE TABLE orders(id int); ALTER TABLE orders FORCE ROW LEVEL SECURITY;`, want: "has no effect until the table enables"},
		{name: "force before table", source: `ALTER TABLE orders FORCE ROW LEVEL SECURITY; CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, want: `table "orders" is not declared`},
		{name: "dropped force without table", source: `ALTER TABLE orders FORCE ROW LEVEL SECURITY; DROP TABLE IF EXISTS orders;`, want: `table "orders" is not declared`},
		{name: "enable is not inherited", source: `CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY; DROP TABLE orders; CREATE TABLE orders(id int); ALTER TABLE orders FORCE ROW LEVEL SECURITY;`, want: "has no effect until the table enables"},
		{name: "duplicate enable", source: `CREATE TABLE orders(id int); ALTER TABLE orders ENABLE ROW LEVEL SECURITY; ALTER TABLE public.orders ENABLE ROW LEVEL SECURITY;`, want: "declares ENABLE ROW LEVEL SECURITY twice"},
		{name: "duplicate policy", source: `CREATE POLICY p ON orders USING (true); CREATE POLICY p ON public.orders USING (true);`, want: `policy "p" on table public.orders is declared twice`},
		{name: "comment before policy", source: `COMMENT ON POLICY p ON orders IS 'first'; CREATE POLICY p ON orders USING (true);`, want: "is not declared"},
		{name: "comment missing policy", source: `COMMENT ON POLICY p ON orders IS 'first';`, want: "is not declared"},
		{name: "comment missing table", source: `COMMENT ON POLICY p IS 'first';`, want: "expected ON"},
		{name: "duplicate clause", source: `CREATE POLICY p ON orders USING (true) USING (false);`, want: "duplicate USING clause"},
		{name: "quoted command", source: `CREATE POLICY p ON orders FOR "SELECT";`, want: "expected command after FOR"},
		{name: "insert using", source: `CREATE POLICY p ON orders FOR INSERT USING (true);`, want: "INSERT"},
		{name: "select check", source: `CREATE POLICY p ON orders FOR SELECT WITH CHECK (true);`, want: "SELECT"},
		{name: "unclosed policy quote", source: `CREATE POLICY "`, want: "expected a closed quoted policy name"},
		{name: "unclosed role quote", source: `CREATE POLICY p ON orders TO "reader`, want: "expected a closed quoted role after TO"},
		{name: "unknown command", source: `CREATE POLICY p ON orders FOR UPSERT;`, want: "unknown policy command"},
		{name: "unterminated expression", source: `CREATE POLICY p ON orders USING ((true);`, want: "unterminated USING expression"},
		{name: "empty expression", source: `CREATE POLICY p ON orders USING ();`, want: "expression"},
		{name: "role trailing comma", source: `CREATE POLICY p ON orders TO reader,;`, want: "expected role after TO"},
		{name: "public beside role", source: `CREATE POLICY p ON orders TO PUBLIC, reader;`, want: "PUBLIC"},
		{name: "disable", source: `ALTER TABLE orders DISABLE ROW LEVEL SECURITY;`, want: "DISABLE ROW LEVEL SECURITY"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, err := readPolicySQL(c, test.source)
			c.Assert(err, qt.IsNotNil)
			c.Assert(err.Error(), qt.Contains, test.want)
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestSQL_DroppedPolicyCoverage derives defaults from surviving policies.
func TestSQL_DroppedPolicyCoverage(t *testing.T) {
	c := qt.New(t)
	database, err := readPolicySQL(c, `CREATE TABLE orders(id int); CREATE POLICY p ON orders USING (true); DROP TABLE orders; CREATE TABLE orders(id int);`)
	c.Assert(err, qt.IsNil)
	c.Assert(database.FeatureObjects.Refs(), qt.HasLen, 0)
	c.Assert(database.FeatureCoverage.SubjectRecords(), qt.HasLen, 0)
	c.Assert(database.Tables[0].Facets.IsZero(), qt.IsTrue)
}

// TestSQL_UnselectedAndUnsupportedTargets prevents the shared grammar from
// silently taking policy syntax when the policy owner is absent.
func TestSQL_UnselectedAndUnsupportedTargets(t *testing.T) {
	tests := []struct {
		name, dialect, source string
		owners                sqlext.Set
	}{
		{name: "unselected create", dialect: "postgres", source: `CREATE POLICY p ON orders USING (true);`, owners: sqlext.None()},
		{name: "unselected enable", dialect: "postgres", source: `ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, owners: sqlext.None()},
		{name: "unselected force", dialect: "postgres", source: `ALTER TABLE orders FORCE ROW LEVEL SECURITY;`, owners: sqlext.None()},
		{name: "Spanner policy", dialect: "spanner", source: `CREATE POLICY p ON orders USING (true);`, owners: builtintest.Runtime().SQL()},
		{name: "Spanner enable", dialect: "spanner", source: `ALTER TABLE orders ENABLE ROW LEVEL SECURITY;`, owners: builtintest.Runtime().SQL()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(c.Context(), []byte(test.source), test.dialect, nil, sqlschema.Options{Owners: test.owners})
			c.Assert(err, qt.IsNotNil)
			c.Assert(strings.ToLower(err.Error()), qt.Contains, "unsupported")
			c.Assert(database, qt.DeepEquals, schemamodel.Database{})
		})
	}
}

// TestSQL_CommentOnEarlierFile retains the complete policy while changing
// its comment, including a default-schema alias in the later file.
func TestSQL_CommentOnEarlierFile(t *testing.T) {
	c := qt.New(t)
	database, err := readPolicySQL(c, `CREATE TABLE orders(id int); CREATE POLICY p ON orders FOR UPDATE USING (id > 0) WITH CHECK (true);`)
	c.Assert(err, qt.IsNil)
	later, _, err := sqlschema.ReadWith(c.Context(), []byte(`COMMENT ON POLICY p ON public.orders IS 'later';`), "postgres", sqlschema.NewDocument(&database), sqlschema.Options{Owners: builtintest.Runtime().SQL()})
	c.Assert(err, qt.IsNil)
	c.Assert(later.FeatureObjects.Refs(), qt.HasLen, 0)
	objects, err := database.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Value, qt.DeepEquals, &pgpolicy.DesiredPolicy{Command: pgpolicy.CommandUpdate, Using: new("id > 0"), WithCheck: new("true"), Comment: "later"})
	c.Assert(database.FeatureCoverage.SubjectRecords(), qt.DeepEquals, []schemaext.SubjectCoverage{{Kind: pgpolicy.TableStateKind, Subject: pgpolicy.Table(objects[0].Ref), Knowledge: pgpolicy.DefaultedSwitches()}})
}
