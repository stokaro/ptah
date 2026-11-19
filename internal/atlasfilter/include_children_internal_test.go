package atlasfilter

// White-box testing required: the child kinds below are held to
// includeChildTypes, the filter's unexported list of the selector types an
// include refuses because their objects ride along with a table. A black-box
// table would have to restate that list, and a restated list stays green when
// a child type joins the filter without a row.

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"
	"testing/fstest"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/core/goschema"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/feature/pgpolicy"
	"ptah.run/internal/builtintest"
	"ptah.run/internal/convert/goschematodb"
	"ptah.run/internal/sqlschema"
	"ptah.run/migration/planner"
	"ptah.run/migration/schemadiff"
)

// These tests hold a positive selection to its contract: the children of an
// included table ride along with it, on the database side and on the desired
// side alike. Each row is one child kind, declared twice on the same pair of
// tables, as SQL and as Go entities, once as the database holds it and once as
// the desired schema wants it. A comparison of the two plans the child's
// change. An include of its table must plan the same change, and an include of
// the other table must plan nothing.
//
// The database side is the desired-schema conversion a file-to-file diff
// reads, so the rows need no server; integration/include_children_e2e_test.go
// runs the same kinds through the shipped binaries against PostgreSQL.

// includeChildTable is the table every row's child belongs to, and
// includeOtherTable the one the control includes instead.
const (
	includeChildTable = "public.notes"
	includeOtherTable = "public.other"
)

// includeChildTablesSQL declares both tables. Each row adds its child after it.
const includeChildTablesSQL = `CREATE TABLE public.notes (id int PRIMARY KEY, owner text NOT NULL, parent_id int);
CREATE TABLE public.other (id int PRIMARY KEY);
`

// includeChildTouchSQL is the trigger function the trigger rows execute.
const includeChildTouchSQL = "CREATE FUNCTION public.touch() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RETURN NEW; END$$;\n"

// includeAuthUIDSQL is a function in a schema of its own, which no row's
// desired schema declares.
const includeAuthUIDSQL = "CREATE SCHEMA auth;\nCREATE FUNCTION auth.uid() RETURNS text LANGUAGE sql STABLE AS $$SELECT current_user::text$$;\n"

// notesEntity is the notes table, and the declarations beside it, as Go
// entities. Each field holds annotation text a row varies.
type notesEntity struct {
	// tableAttributes follow name="notes" on the table directive.
	tableAttributes string
	// tableDirectives are further directives on the notes struct.
	tableDirectives []string
	// ownerAttributes and parentAttributes follow the owner and parent_id
	// field directives.
	ownerAttributes, parentAttributes string
	// fields are further fields of the notes struct, as source.
	fields string
	// marker are directives on a struct of their own.
	marker []string
}

func (e notesEntity) source() string {
	var out strings.Builder
	out.WriteString("package entities\n\n")
	fmt.Fprintf(&out, "//ptah:schema:table name=\"notes\"%s\n", e.tableAttributes)
	for _, directive := range e.tableDirectives {
		out.WriteString(directive + "\n")
	}
	out.WriteString("type Note struct {\n")
	out.WriteString("\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n")
	fmt.Fprintf(&out, "\t//ptah:schema:field name=\"owner\" type=\"TEXT\" not_null=\"true\"%s\n\tOwner string\n", e.ownerAttributes)
	fmt.Fprintf(&out, "\t//ptah:schema:field name=\"parent_id\" type=\"INTEGER\"%s\n\tParentID *int\n", e.parentAttributes)
	out.WriteString(e.fields)
	out.WriteString("}\n\n")
	out.WriteString("//ptah:schema:table name=\"other\"\ntype Other struct {\n")
	out.WriteString("\t//ptah:schema:field name=\"id\" type=\"INTEGER\" primary=\"true\"\n\tID int\n}\n")
	if len(e.marker) > 0 {
		out.WriteString("\n" + strings.Join(e.marker, "\n") + "\ntype Marker struct{}\n")
	}
	return out.String()
}

const (
	goEnableRLS = `//ptah:schema:rls:enable table="notes"`
	goRoleAppRO = `//ptah:schema:role name="app_ro" login="false"`
)

func goPolicy(using string) string {
	return `//ptah:schema:rls:policy name="notes_owner" table="notes" for="SELECT" to="PUBLIC" using="` + using + `"`
}

func goTrigger(event string) string {
	return `//ptah:schema:trigger name="notes_touch" table="notes" timing="BEFORE" event="` + event + `" for="ROW" body="RETURN NEW;"`
}

func goGrant(privileges string) string {
	return `//ptah:schema:grant role="app_ro" privileges="` + privileges + `" on_table="notes"`
}

func goCheck(condition string) string {
	return `//ptah:schema:constraint name="notes_id_check" type="CHECK" check="` + condition + `"`
}

// includeChildRow is one child kind of the notes table.
type includeChildRow struct {
	name string
	// types are the include selector types that name an object of the kind,
	// each refused by the parser because the object rides along with its
	// table. A facet or a field of the table itself has none.
	types []string
	// liveSQL and desiredSQL follow includeChildTablesSQL on each side.
	liveSQL, desiredSQL string
	// liveGo and desiredGo are the same two sides as Go entities.
	liveGo, desiredGo notesEntity
}

var includeChildRows = []includeChildRow{
	{
		name:       "policy",
		types:      []string{"policy"},
		liveSQL:    "ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;\nCREATE POLICY notes_owner ON public.notes FOR SELECT USING (true);\n",
		desiredSQL: "ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;\nCREATE POLICY notes_owner ON public.notes FOR SELECT USING (owner = current_user);\n",
		liveGo:     notesEntity{marker: []string{goEnableRLS, goPolicy("true")}},
		desiredGo:  notesEntity{marker: []string{goEnableRLS, goPolicy("owner = current_user")}},
	},
	{
		name:       "row-level security enabled",
		desiredSQL: "ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;\n",
		desiredGo:  notesEntity{marker: []string{goEnableRLS}},
	},
	{
		name:       "row-level security forced",
		liveSQL:    "ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;\n",
		desiredSQL: "ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;\nALTER TABLE public.notes FORCE ROW LEVEL SECURITY;\n",
		liveGo:     notesEntity{marker: []string{goEnableRLS}},
		desiredGo:  notesEntity{marker: []string{goEnableRLS + ` force="true"`}},
	},
	{
		name:       "trigger",
		types:      []string{"trigger"},
		liveSQL:    includeChildTouchSQL + "CREATE TRIGGER notes_touch BEFORE INSERT ON public.notes FOR EACH ROW EXECUTE FUNCTION public.touch();\n",
		desiredSQL: includeChildTouchSQL + "CREATE TRIGGER notes_touch BEFORE UPDATE ON public.notes FOR EACH ROW EXECUTE FUNCTION public.touch();\n",
		liveGo:     notesEntity{marker: []string{goTrigger("INSERT")}},
		desiredGo:  notesEntity{marker: []string{goTrigger("UPDATE")}},
	},
	{
		name:       "grant",
		types:      []string{"grant"},
		liveSQL:    "GRANT SELECT ON public.notes TO app_ro;\n",
		desiredSQL: "GRANT SELECT, INSERT ON public.notes TO app_ro;\n",
		liveGo:     notesEntity{marker: []string{goRoleAppRO, goGrant("SELECT")}},
		desiredGo:  notesEntity{marker: []string{goRoleAppRO, goGrant("SELECT,INSERT")}},
	},
	{
		name:       "table comment",
		liveSQL:    "COMMENT ON TABLE public.notes IS 'a';\n",
		desiredSQL: "COMMENT ON TABLE public.notes IS 'b';\n",
		liveGo:     notesEntity{tableAttributes: ` comment="a"`},
		desiredGo:  notesEntity{tableAttributes: ` comment="b"`},
	},
	{
		name:       "column comment",
		liveSQL:    "COMMENT ON COLUMN public.notes.owner IS 'a';\n",
		desiredSQL: "COMMENT ON COLUMN public.notes.owner IS 'b';\n",
		liveGo:     notesEntity{ownerAttributes: ` comment="a"`},
		desiredGo:  notesEntity{ownerAttributes: ` comment="b"`},
	},
	{
		name:       "check constraint",
		types:      []string{"check", "constraint"},
		liveSQL:    "ALTER TABLE public.notes ADD CONSTRAINT notes_id_check CHECK (id > 0);\n",
		desiredSQL: "ALTER TABLE public.notes ADD CONSTRAINT notes_id_check CHECK (id > 1);\n",
		liveGo:     notesEntity{tableDirectives: []string{goCheck("id > 0")}},
		desiredGo:  notesEntity{tableDirectives: []string{goCheck("id > 1")}},
	},
	{
		name:       "unique constraint",
		types:      []string{"constraint"},
		desiredSQL: "ALTER TABLE public.notes ADD CONSTRAINT notes_owner_key UNIQUE (owner);\n",
		desiredGo:  notesEntity{tableDirectives: []string{`//ptah:schema:constraint name="notes_owner_key" type="UNIQUE" columns="owner"`}},
	},
	{
		name:       "foreign key",
		types:      []string{"foreign_key", "foreign-key", "fk", "constraint"},
		desiredSQL: "ALTER TABLE public.notes ADD CONSTRAINT notes_parent_fk FOREIGN KEY (parent_id) REFERENCES public.notes (id);\n",
		desiredGo:  notesEntity{parentAttributes: ` foreign="notes(id)" foreign_key_name="notes_parent_fk"`},
	},
	{
		name:       "index",
		types:      []string{"index"},
		desiredSQL: "CREATE INDEX notes_owner_idx ON public.notes (owner);\n",
		desiredGo:  notesEntity{fields: "\t//ptah:schema:index name=\"notes_owner_idx\" fields=\"owner\"\n\t_ int\n"},
	},
	{
		name:       "column",
		types:      []string{"column"},
		desiredSQL: "ALTER TABLE public.notes ADD COLUMN body text;\n",
		desiredGo:  notesEntity{fields: "\t//ptah:schema:field name=\"body\" type=\"TEXT\"\n\tBody *string\n"},
	},
}

// includeChildSides are the two sides of one row read from one source: the
// database side as the comparison reads it, and the desired side.
type includeChildSides struct {
	current *catalog.Database
	desired *schemamodel.Database
}

// includeChildSource is a source the comparison verbs take a desired schema
// from. read parses both sides of a row from it.
type includeChildSource struct {
	name string
	read func(ctx context.Context, row includeChildRow) (live, desired *schemamodel.Database, err error)
}

// includeChildSQL reads a row's SQL, and includeChildGo its Go entities.
var (
	includeChildSQL = includeChildSource{name: "sql", read: func(ctx context.Context, row includeChildRow) (*schemamodel.Database, *schemamodel.Database, error) {
		live, liveErr := readIncludeChildSQL(ctx, row.liveSQL)
		desired, desiredErr := readIncludeChildSQL(ctx, row.desiredSQL)
		return live, desired, errors.Join(liveErr, desiredErr)
	}}
	includeChildGo = includeChildSource{name: "go", read: func(_ context.Context, row includeChildRow) (*schemamodel.Database, *schemamodel.Database, error) {
		live, liveErr := readIncludeChildGo(row.liveGo)
		desired, desiredErr := readIncludeChildGo(row.desiredGo)
		return live, desired, errors.Join(liveErr, desiredErr)
	}}
)

// includeChildSources are the sources a row is read from.
var includeChildSources = []includeChildSource{includeChildSQL, includeChildGo}

// sides reads row from the source into the two sides of a comparison.
func (s includeChildSource) sides(c *qt.C, row includeChildRow) includeChildSides {
	c.Helper()
	live, desired, err := s.read(c.Context(), row)
	c.Assert(err, qt.IsNil)
	return includeChildSidesOf(c, live, desired)
}

func readIncludeChildSQL(ctx context.Context, statements string) (*schemamodel.Database, error) {
	runtime := builtintest.Runtime()
	db, _, err := sqlschema.ReadWith(ctx, []byte(includeChildTablesSQL+statements), "postgres", nil,
		sqlschema.Options{Owners: runtime.SQL(), Vocabulary: runtime.CoverageVocabulary(), Relations: runtime})
	return &db, err
}

func readIncludeChildGo(entity notesEntity) (*schemamodel.Database, error) {
	files := fstest.MapFS{"entities.go": &fstest.MapFile{Data: []byte(entity.source())}}
	return goschema.ParseFS(builtintest.Annotations(), files, ".")
}

// includeChildSidesOf converts live into the database side and claims
// about row-level security what the PostgreSQL reader claims after a read:
// complete knowledge of every policy and of every table's switches. A
// converted declaration leaves the switches of a table with policies to the
// owner's default, which only a read can observe, and a comparison refuses
// that as incomplete.
func includeChildSidesOf(c *qt.C, live, desired *schemamodel.Database) includeChildSides {
	c.Helper()
	current, err := goschematodb.ToDBSchema(c.Context(), live, "postgres", builtintest.Runtime())
	c.Assert(err, qt.IsNil)
	var kinds []schemaext.Kind
	for _, record := range current.FeatureCoverage.KindRecords() {
		kinds = append(kinds, record.Model.Kind)
	}
	kinds = slices.DeleteFunc(kinds, func(kind schemaext.Kind) bool {
		return kind == pgpolicy.PolicyKind || kind == pgpolicy.TableStateKind
	})
	read, err := pgpolicy.CompleteCoverage(schemaext.Observed)
	c.Assert(err, qt.IsNil)
	current.FeatureCoverage, err = current.FeatureCoverage.SelectKinds(kinds).Combine(read)
	c.Assert(err, qt.IsNil)
	return includeChildSides{current: current, desired: desired}
}

// includeChildScope is a positive selection of the named tables.
func includeChildScope(tables ...string) Scope {
	return Scope{Include: tables, Selections: builtintest.Runtime().Selections(), DefaultSchema: "public"}
}

// scoped projects both sides through scope.
func (s includeChildSides) scoped(c *qt.C, scope Scope) includeChildSides {
	c.Helper()
	current, err := ScopeDatabase(s.current, scope)
	c.Assert(err, qt.IsNil)
	desired, err := ScopeGenerated(s.desired, scope)
	c.Assert(err, qt.IsNil)
	return includeChildSides{current: current, desired: desired}
}

// plan is the SQL a comparison of the two sides plans, statement by
// statement.
func (s includeChildSides) plan(c *qt.C) []string {
	c.Helper()
	runtime := builtintest.Runtime()
	diff, err := schemadiff.CompareWithDialect(c.Context(), s.desired, s.current, "postgres", runtime)
	c.Assert(err, qt.IsNil)
	statements, err := planner.GenerateSchemaDiffSQLStatements(c.Context(), runtime, diff, "postgres")
	c.Assert(err, qt.IsNil)
	return statements
}

// TestScopeKeepsTheChildrenOfAnIncludedTable_HappyPath includes the table a
// row's child belongs to: the plan is the plan of the unscoped comparison, so
// the child stayed on both sides. A side that lost it alone would plan its
// creation or its removal instead, and both sides losing it would plan
// nothing.
func TestScopeKeepsTheChildrenOfAnIncludedTable_HappyPath(t *testing.T) {
	for _, row := range includeChildRows {
		for _, source := range includeChildSources {
			t.Run(row.name+"/"+source.name, func(t *testing.T) {
				c := qt.New(t)
				sides := source.sides(c, row)
				unscoped := sides.plan(c)
				c.Assert(unscoped, qt.Not(qt.HasLen), 0, qt.Commentf("the row declares no difference"))

				scoped := sides.scoped(c, includeChildScope(includeChildTable)).plan(c)

				c.Assert(scoped, qt.DeepEquals, unscoped)
			})
		}
	}
}

// TestScopeLeavesOutTheChildrenOfAnotherTable_HappyPath is the control for the
// test above: an include of the other table takes no child of notes along on
// either side, so the same two sides plan nothing.
func TestScopeLeavesOutTheChildrenOfAnotherTable_HappyPath(t *testing.T) {
	for _, row := range includeChildRows {
		for _, source := range includeChildSources {
			t.Run(row.name+"/"+source.name, func(t *testing.T) {
				c := qt.New(t)
				sides := source.sides(c, row)

				scoped := sides.scoped(c, includeChildScope(includeOtherTable)).plan(c)

				c.Assert(scoped, qt.HasLen, 0)
			})
		}
	}
}

// includeUnselectedRows hold objects an include of notes does not select,
// which the database side holds and the desired side leaves out or changes.
// They are SQL, the source a scoped comparison against a database holding
// objects of its own reads.
var includeUnselectedRows = []struct {
	name string
	// liveSQL and desiredSQL follow includeChildTablesSQL on each side.
	liveSQL, desiredSQL string
	// unselected is a statement the unscoped plan holds for an object the
	// include leaves out.
	unselected string
	// scoped is the plan of an include of notes, its statements joined by
	// newlines.
	scoped string
}{
	{
		name:       "function outside the selection",
		liveSQL:    includeAuthUIDSQL,
		unselected: `DROP FUNCTION IF EXISTS "auth"."uid"`,
	},
	{
		name:       "policy on another table",
		liveSQL:    "ALTER TABLE public.other ENABLE ROW LEVEL SECURITY;\nCREATE POLICY other_all ON public.other FOR SELECT USING (true);\n",
		desiredSQL: "ALTER TABLE public.other ENABLE ROW LEVEL SECURITY;\nCREATE POLICY other_all ON public.other FOR SELECT USING (id > 0);\n",
		unselected: `ALTER POLICY "other_all"`,
	},
	{
		name: "policy on notes calling a function outside the selection",
		liveSQL: includeAuthUIDSQL +
			"ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;\nCREATE POLICY notes_owner ON public.notes FOR SELECT USING (true);\n",
		desiredSQL: "ALTER TABLE public.notes ENABLE ROW LEVEL SECURITY;\nCREATE POLICY notes_owner ON public.notes FOR SELECT USING (owner = auth.uid());\n",
		unselected: `DROP FUNCTION IF EXISTS "auth"."uid"`,
		scoped:     "ALTER POLICY \"notes_owner\" ON \"public\".\"notes\" TO PUBLIC\n    USING (owner = auth.uid())",
	},
}

// TestScopeLeavesOutWhatItDoesNotSelect_HappyPath includes notes on sides
// that differ in objects the include does not select. The unscoped plan
// changes them, and the scoped plan leaves them out: it changes nothing but
// notes.
func TestScopeLeavesOutWhatItDoesNotSelect_HappyPath(t *testing.T) {
	for _, row := range includeUnselectedRows {
		t.Run(row.name, func(t *testing.T) {
			c := qt.New(t)
			sides := includeChildSQL.sides(c, includeChildRow{liveSQL: row.liveSQL, desiredSQL: row.desiredSQL})
			unscoped := sides.plan(c)

			scoped := sides.scoped(c, includeChildScope(includeChildTable)).plan(c)

			c.Assert(strings.Join(unscoped, "\n"), qt.Contains, row.unselected)
			c.Assert(strings.Join(scoped, "\n"), qt.Equals, row.scoped)
		})
	}
}

// TestScopeIncludingAChildByItsOwnName_FailurePath names the policy of the
// policy row by its own name, bare and qualified, the spelling an include
// refuses as `notes_owner[type=policy]`. An include names a table's child
// only through its table, and these name no table, so the selection matched
// nothing on either side and keeps no policy.
func TestScopeIncludingAChildByItsOwnName_FailurePath(t *testing.T) {
	for _, source := range includeChildSources {
		t.Run(source.name, func(t *testing.T) {
			c := qt.New(t)
			sides := source.sides(c, includeChildRows[0])
			scope := includeChildScope("notes_owner", "public.notes_owner")

			current, currentErr := ScopeDatabase(sides.current, scope)
			desired, desiredErr := ScopeGenerated(sides.desired, scope)

			var empty *EmptySelectionError
			c.Assert(currentErr, qt.ErrorAs, &empty)
			c.Assert(desiredErr, qt.ErrorAs, &empty)
			c.Assert(current.FeatureObjects.Refs(), qt.HasLen, 0)
			c.Assert(desired.FeatureObjects.Refs(), qt.HasLen, 0)
		})
	}
}

// TestIncludeChildRowsNameEveryChildType holds the rows to the filter's own
// list: the selector types the rows name are the types an include refuses as
// a child, the owners' among them, such as the policy type PostgreSQL's
// policy owner selects. A child type added to the filter without a row, or a
// row naming a type the filter does not refuse, fails here.
func TestIncludeChildRowsNameEveryChildType(t *testing.T) {
	c := qt.New(t)
	var named []string
	for _, row := range includeChildRows {
		named = append(named, row.types...)
	}

	c.Assert(slices.Compact(slices.Sorted(slices.Values(named))), qt.DeepEquals, slices.Sorted(maps.Keys(includeChildTypes)))
}
