package pgtablesource_test

import (
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"

	"ptah.run/core/objectidentity"
	"ptah.run/core/schemaext"
	"ptah.run/core/sqlext"
	"ptah.run/dialect/postgres/pgtable"
	"ptah.run/internal/pgtablesource"
	"ptah.run/internal/sqlschema"
)

func exclusionSQLOwners() sqlext.Set { return must.Must(sqlext.NewSet(pgtablesource.SQL()...)) }

func TestExclusionSQLNamesAndFolds(t *testing.T) {
	tests := []struct {
		name, sql string
		names     []string
		common    int
	}{
		{name: "unnamed", sql: `CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =));`, names: []string{"t_a_excl"}},
		{name: "add unnamed", sql: `CREATE TABLE t (a int); ALTER TABLE t ADD EXCLUDE USING btree (a WITH =);`, names: []string{"t_a_excl"}},
		{name: "add named", sql: `CREATE TABLE t (a int); ALTER TABLE t ADD CONSTRAINT explicit EXCLUDE USING btree (a WITH =);`, names: []string{"explicit"}},
		{name: "alter keeps duplicates", sql: `CREATE TABLE t (a int); ALTER TABLE t ADD EXCLUDE USING btree (a WITH =), ADD EXCLUDE USING btree (a WITH =);`, names: []string{"t_a_excl", "t_a_excl1"}},
		{name: "create then alter keeps duplicates", sql: `CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =)); ALTER TABLE t ADD EXCLUDE USING btree (a WITH =);`, names: []string{"t_a_excl", "t_a_excl1"}},

		{name: "named", sql: `CREATE TABLE t (a int, CONSTRAINT explicit EXCLUDE USING btree (a WITH =));`, names: []string{"explicit"}},
		{name: "quoted", sql: `CREATE TABLE "T.dot" ("A" int, CONSTRAINT "E.dot" EXCLUDE USING btree ("A" WITH =));`, names: []string{"E.dot"}},
		{name: "expression", sql: `CREATE TABLE t (a int, EXCLUDE USING btree ((a + 1) WITH =));`, names: []string{"t_expr_excl"}},
		{name: "duplicate expression names", sql: `CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =, a WITH <>));`, names: []string{"t_a_a1_excl"}},
		{name: "equal declarations", sql: `CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =), EXCLUDE USING btree (a WITH =));`, names: []string{"t_a_excl"}},
		{name: "fold inherits written name", sql: `CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =), CONSTRAINT explicit EXCLUDE USING btree (a WITH =));`, names: []string{"explicit"}},
		{name: "distinct timing", sql: `CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =), EXCLUDE USING btree (a WITH =) DEFERRABLE);`, names: []string{"t_a_excl", "t_a_excl1"}},
		{name: "distinct predicate", sql: `CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =) WHERE (a > 0), EXCLUDE USING btree (a WITH =) WHERE (a < 0));`, names: []string{"t_a_excl", "t_a_excl1"}},
		{name: "check holds name", sql: `CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =), CONSTRAINT t_a_excl CHECK (a > 0));`, names: []string{"t_a_excl1"}, common: 1},
		{name: "unique first", sql: `CREATE TABLE t (a int, b int, CONSTRAINT t_a_excl UNIQUE (b), EXCLUDE USING btree (a WITH =));`, names: []string{"t_a_excl1"}, common: 1},
		{name: "column key comes first", sql: `CREATE TABLE t (a int UNIQUE, b int, EXCLUDE USING btree (a WITH =));`, names: []string{"t_a_excl"}},
		{name: "earlier relation", sql: `CREATE TABLE t_a_excl (id int); CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =));`, names: []string{"t_a_excl1"}},
		{name: "earlier owned relation", sql: `CREATE TABLE other (b int, CONSTRAINT t_a_excl EXCLUDE USING btree (b WITH =)); CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =));`, names: []string{"t_a_excl", "t_a_excl1"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(t.Context(), []byte(test.sql), "postgres", nil, sqlschema.Options{Owners: exclusionSQLOwners()})
			c.Assert(err, qt.IsNil)
			c.Assert(database.Constraints, qt.HasLen, test.common)
			var names []string
			for _, ref := range database.FeatureObjects.Refs() {
				c.Assert(ref.Kind, qt.Equals, objectidentity.Kind(pgtable.ExclusionKind))
				names = append(names, ref.Name.Source)
				object, found, getErr := database.FeatureObjects.Get(ref)
				c.Assert(getErr, qt.IsNil)
				c.Assert(found, qt.IsTrue)
				_, ok := object.Value.(*pgtable.DesiredExclusion)
				c.Assert(ok, qt.IsTrue)
			}
			c.Assert(names, qt.DeepEquals, test.names)
			c.Assert(schemaext.DefinedConstraints(database.FeatureObjects), qt.HasLen, len(test.names))
		})
	}
}

func TestExclusionSQLWrittenNameCollisions(t *testing.T) {
	tests := []struct{ name, sql string }{
		{name: "unique after owned", sql: `CREATE TABLE t (a int, b int, EXCLUDE USING btree (a WITH =), CONSTRAINT t_a_excl UNIQUE (b));`},
		{name: "owned after column key", sql: `CREATE TABLE t (a int UNIQUE, b int, CONSTRAINT t_a_key EXCLUDE USING btree (b WITH =));`},
		{name: "two distinct owned constraints", sql: `CREATE TABLE t (a int, b int, CONSTRAINT explicit EXCLUDE USING btree (a WITH =), CONSTRAINT explicit EXCLUDE USING btree (b WITH =));`},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, _, err := sqlschema.ReadWith(t.Context(), []byte(test.sql), "postgres", nil, sqlschema.Options{Owners: exclusionSQLOwners()})
			c.Assert(err, qt.ErrorMatches, ".*two constraints of one CREATE TABLE claim the same name.*")
		})
	}
}

func TestExclusionSQLConstraintLifecycle(t *testing.T) {
	const create = `CREATE TABLE t (a int, b int, EXCLUDE USING btree (a WITH =));`
	tests := []struct {
		name, sql, constraint, comment string
		count                          int
	}{
		{name: "comment derived name", sql: create + ` COMMENT ON CONSTRAINT t_a_excl ON t IS 'first';`, constraint: "t_a_excl", comment: "first", count: 1},
		{name: "last comment", sql: create + ` COMMENT ON CONSTRAINT t_a_excl ON t IS 'first'; COMMENT ON CONSTRAINT t_a_excl ON t IS 'second';`, constraint: "t_a_excl", comment: "second", count: 1},
		{name: "clear", sql: create + ` COMMENT ON CONSTRAINT t_a_excl ON t IS 'first'; COMMENT ON CONSTRAINT t_a_excl ON t IS NULL;`, constraint: "t_a_excl", count: 1},
		{name: "constraint rename", sql: create + ` COMMENT ON CONSTRAINT t_a_excl ON t IS 'first'; ALTER TABLE t RENAME CONSTRAINT t_a_excl TO renamed;`, constraint: "renamed", comment: "first", count: 1},
		{name: "index rename", sql: create + ` COMMENT ON CONSTRAINT t_a_excl ON t IS 'first'; ALTER INDEX t_a_excl RENAME TO renamed;`, constraint: "renamed", comment: "first", count: 1},
		{name: "comment after rename", sql: create + ` ALTER INDEX t_a_excl RENAME TO renamed; COMMENT ON CONSTRAINT renamed ON t IS 'second';`, constraint: "renamed", comment: "second", count: 1},
		{name: "drop constraint", sql: create + ` COMMENT ON CONSTRAINT t_a_excl ON t IS 'first'; ALTER TABLE t DROP CONSTRAINT t_a_excl;`},
		{name: "drop and add constraint", sql: create + ` COMMENT ON CONSTRAINT t_a_excl ON t IS 'first'; ALTER TABLE t DROP CONSTRAINT t_a_excl, ADD EXCLUDE USING btree (a WITH =);`, constraint: "t_a_excl", count: 1},
		{name: "comment on addition", sql: `CREATE TABLE t (a int); ALTER TABLE t ADD EXCLUDE USING btree (a WITH =); COMMENT ON CONSTRAINT t_a_excl ON t IS 'added';`, constraint: "t_a_excl", comment: "added", count: 1},

		{name: "new table lifetime", sql: create + ` COMMENT ON CONSTRAINT t_a_excl ON t IS 'first'; DROP TABLE t;` + create, constraint: "t_a_excl", count: 1},
		{name: "unrelated column rename", sql: create + ` ALTER TABLE t RENAME COLUMN b TO c;`, constraint: "t_a_excl", count: 1},
		{name: "unrelated column drop", sql: create + ` ALTER TABLE t DROP COLUMN b;`, constraint: "t_a_excl", count: 1},
		{name: "quoted parent", sql: `CREATE TABLE "T.dot" (a int, CONSTRAINT "E.dot" EXCLUDE USING btree (a WITH =)); COMMENT ON CONSTRAINT "E.dot" ON "T.dot" IS 'quoted';`, constraint: "E.dot", comment: "quoted", count: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(t.Context(), []byte(test.sql), "postgres", nil, sqlschema.Options{Owners: exclusionSQLOwners()})
			c.Assert(err, qt.IsNil)
			objects, err := database.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.HasLen, test.count)
			for _, object := range objects {
				c.Assert(object.Ref.Name.Source, qt.Equals, test.constraint)
				value, ok := object.Value.(*pgtable.DesiredExclusion)
				c.Assert(ok, qt.IsTrue)
				c.Assert(value.Comment, qt.Equals, test.comment)
			}
		})
	}
}

func TestExclusionSQLRefusesDanglingReferences(t *testing.T) {
	const create = `CREATE TABLE t (a int, b int, EXCLUDE USING btree (a WITH =) WHERE (b > 0));`
	tests := []struct{ name, after, message string }{
		{name: "drop element column", after: `ALTER TABLE t DROP COLUMN a;`, message: ".*constraint t_a_excl still refers to the column"},
		{name: "drop predicate column", after: `ALTER TABLE t DROP COLUMN b;`, message: ".*constraint t_a_excl still refers to the column"},
		{name: "rename element column", after: `ALTER TABLE t RENAME COLUMN a TO c;`, message: ".*constraint t_a_excl names the column.*"},
		{name: "rename predicate column", after: `ALTER TABLE t RENAME COLUMN b TO c;`, message: ".*constraint t_a_excl names the column.*"},
		{name: "drop backing index", after: `DROP INDEX t_a_excl;`, message: ".*index enforces constraint t_a_excl.*"},
		{name: "rename onto relation", after: `CREATE TABLE taken (id int); ALTER TABLE t RENAME CONSTRAINT t_a_excl TO taken;`, message: ".*name is already held.*"},
		{name: "rename onto check", after: `ALTER TABLE t ADD CONSTRAINT taken CHECK (a > 0); ALTER TABLE t RENAME CONSTRAINT t_a_excl TO taken;`, message: ".*name is already held.*"},
		{name: "comment removed constraint", after: `ALTER TABLE t DROP CONSTRAINT t_a_excl; COMMENT ON CONSTRAINT t_a_excl ON t IS 'missing';`, message: ".*names an object this schema does not declare"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			_, _, err := sqlschema.ReadWith(t.Context(), []byte(create+test.after), "postgres", nil, sqlschema.Options{Owners: exclusionSQLOwners()})
			c.Assert(err, qt.ErrorMatches, test.message)
		})
	}
}

func TestExclusionSQLAmendsEarlierFile(t *testing.T) {
	c := qt.New(t)
	opts := sqlschema.Options{Owners: exclusionSQLOwners()}
	base, _, err := sqlschema.ReadWith(t.Context(), []byte(`CREATE TABLE t (a int, EXCLUDE USING btree (a WITH =));`), "postgres", nil, opts)
	c.Assert(err, qt.IsNil)
	added, _, err := sqlschema.ReadWith(t.Context(), []byte(`ALTER TABLE t RENAME CONSTRAINT t_a_excl TO renamed; COMMENT ON CONSTRAINT renamed ON t IS 'from second file';`), "postgres", sqlschema.NewDocument(&base), opts)
	c.Assert(err, qt.IsNil)
	c.Assert(added.FeatureObjects.Len(), qt.Equals, 0)
	objects, err := base.FeatureObjects.All()
	c.Assert(err, qt.IsNil)
	c.Assert(objects, qt.HasLen, 1)
	c.Assert(objects[0].Ref.Name.Source, qt.Equals, "renamed")
	value, ok := objects[0].Value.(*pgtable.DesiredExclusion)
	c.Assert(ok, qt.IsTrue)
	c.Assert(value.Comment, qt.Equals, "from second file")
}

func TestExclusionSQLTiming(t *testing.T) {
	tests := []struct {
		name, clause, initially string
		deferrable              bool
	}{
		{name: "default method", clause: "EXCLUDE (a WITH =)"},
		{name: "implicit deferral", clause: "EXCLUDE (a WITH =) INITIALLY DEFERRED", initially: "deferred", deferrable: true},
		{name: "reversed deferral", clause: "EXCLUDE (a WITH =) INITIALLY DEFERRED DEFERRABLE", initially: "deferred", deferrable: true},
		{name: "repeated deferral", clause: "EXCLUDE (a WITH =) DEFERRABLE DEFERRABLE INITIALLY DEFERRED INITIALLY DEFERRED", initially: "deferred", deferrable: true},
		{name: "immediate", clause: "EXCLUDE (a WITH =) INITIALLY IMMEDIATE NOT DEFERRABLE", initially: "immediate"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			database, _, err := sqlschema.ReadWith(t.Context(), []byte("CREATE TABLE t (a int, "+test.clause+");"), "postgres", nil, sqlschema.Options{Owners: exclusionSQLOwners()})
			c.Assert(err, qt.IsNil)
			objects, err := database.FeatureObjects.All()
			c.Assert(err, qt.IsNil)
			c.Assert(objects, qt.HasLen, 1)
			value, ok := objects[0].Value.(*pgtable.DesiredExclusion)
			c.Assert(ok, qt.IsTrue)
			c.Assert(value.UsingMethod, qt.Equals, "btree")
			c.Assert(value.Deferrable, qt.Equals, test.deferrable)
			c.Assert(value.Initially, qt.Equals, test.initially)
		})
	}
}
