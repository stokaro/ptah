package renderer_test

import (
	"fmt"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/goschema"
	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer"
)

// routingDialects is every canonical engine `--dialect` accepts. The grid below
// is this list crossed with routedObjectRows, and every cell has to carry an
// answer.
var routingDialects = []string{
	platform.Postgres, platform.CockroachDB, platform.YugabyteDB, platform.Spanner,
	platform.ClickHouse, platform.MySQL, platform.MariaDB, platform.SQLServer, platform.SQLite,
}

// routedObjectRows is one row per declared object kind, with the name the
// fixture gives that object. Each name is unique across the fixture, so a
// mention of it in the rendered SQL can only be about that object.
var routedObjectRows = []struct {
	kind   string
	object string
}{
	{kind: "sequence", object: "seq_probe"},
	{kind: "domain", object: "domain_probe"},
	{kind: "role", object: "role_probe"},
	{kind: "table", object: "table_probe"},
	{kind: "view", object: "view_probe"},
	{kind: "function", object: "func_probe"},
	{kind: "trigger", object: "trigger_probe"},
	{kind: "grant", object: "grant_probe"},
}

// routedObjectSchema declares one object of every kind in routedObjectRows.
//
// Materialized views are deliberately absent. Four of these renderers refuse one
// with an ERROR rather than a comment, on purpose (stokaro/ptah#931 item 3), and
// an error aborts the whole render -- so including one would decide this grid
// before any other kind was reached. That refusal is already pinned by
// TestRender_MaterializedViewIsRefusedWhereApplyRefusesIt.
//
// The grant names its own role rather than role_probe so that the grant row and
// the role row cannot be satisfied by the same mention.
func routedObjectSchema() *goschema.Database {
	start := int64(1000)
	return &goschema.Database{
		Sequences: []goschema.Sequence{{Name: "seq_probe", AsType: "bigint", Start: &start}},
		Domains:   []goschema.Domain{{Name: "domain_probe", BaseType: "TEXT"}},
		Roles:     []goschema.Role{{Name: "role_probe", Login: true, Inherit: true}},
		Tables:    []goschema.Table{{StructName: "T", Name: "table_probe"}},
		Fields: []goschema.Field{
			{StructName: "T", Name: "id", Type: "BIGINT", Primary: true},
			{StructName: "T", Name: "touched", Type: "TIMESTAMP", Nullable: true},
		},
		Views:     []goschema.View{{StructName: "V", Name: "view_probe", Body: "SELECT id FROM table_probe"}},
		Functions: []goschema.Function{{Name: "func_probe", Returns: "integer", Language: "sql", Body: "SELECT 1;"}},
		Triggers: []goschema.Trigger{{
			StructName: "TR", Name: "trigger_probe", Table: "table_probe",
			Timing: "AFTER", Event: "INSERT", ForEach: "ROW", Body: "SELECT 1",
		}},
		Grants: []goschema.Grant{{
			StructName: "G", Role: "grant_probe", Privileges: []string{"SELECT"}, OnTable: "table_probe",
		}},
	}
}

// routedObjectAnswer classifies what one rendered schema says about one object:
//
//	"ddl"    a statement a server would execute names it
//	"named"  only a comment names it -- the target declines the object and says so
//	"refused" rendering fails before SQL because the target cannot converge it
//	"silent"  nothing in the output mentions it at all
//
// The comment/statement split is the whole measurement. Every renderer here
// writes its refusal as a comment that repeats the object's DDL keywords, so a
// plain substring search over the output cannot tell the two apart, and
// "silent" -- the defect -- looks exactly like a target that refused.
func routedObjectAnswer(sql, object string) string {
	executable, commented := false, false
	for line := range strings.SplitSeq(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.Contains(trimmed, object) {
			continue
		}
		if strings.HasPrefix(trimmed, "--") {
			commented = true
			continue
		}
		executable = true
	}
	return map[[2]bool]string{
		{true, false}:  "ddl",
		{true, true}:   "ddl",
		{false, true}:  "named",
		{false, false}: "silent",
	}[[2]bool{executable, commented}]
}

// routedObjectCell is one (dialect, object kind) cell of the grid.
type routedObjectCell struct {
	dialect string
	kind    string
	object  string
	answer  string
}

// routedObjectGrid renders the fixture once per dialect and classifies every
// declared object in it.
func routedObjectGrid(c *qt.C) []routedObjectCell {
	c.Helper()

	cells := make([]routedObjectCell, 0, len(routingDialects)*len(routedObjectRows))
	for _, dialect := range routingDialects {
		database := routedObjectSchema()
		roleRefused := dialect == platform.MySQL || dialect == platform.MariaDB
		if roleRefused {
			database.Roles = nil
		}
		adaptForClickHouse(database, dialect)
		adaptForSQLServer(database, dialect)
		statements, err := renderer.GetOrderedCreateStatements(database, dialect)
		c.Assert(err, qt.IsNil, qt.Commentf("render failed for %s", dialect))
		sql := strings.Join(statements, "\n")
		for _, row := range routedObjectRows {
			if roleRefused && row.kind == "role" {
				_, err := renderer.GetOrderedCreateStatements(&goschema.Database{
					Roles: []goschema.Role{{Name: row.object}},
				}, dialect)
				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				cells = append(cells, routedObjectCell{
					dialect: dialect,
					kind:    row.kind,
					object:  row.object,
					answer:  "refused",
				})
				continue
			}
			cells = append(cells, routedObjectCell{
				dialect: dialect,
				kind:    row.kind,
				object:  row.object,
				answer:  routedObjectAnswer(sql, row.object),
			})
		}
	}
	return cells
}

// cellsAnswering returns one printable line per cell whose answer is the one
// given, so a failure names the objects and the dialects rather than a count.
func cellsAnswering(cells []routedObjectCell, answer string) []string {
	matching := slices.DeleteFunc(slices.Clone(cells), func(cell routedObjectCell) bool {
		return cell.answer != answer
	})
	lines := make([]string, 0, len(matching))
	for _, cell := range matching {
		lines = append(lines, fmt.Sprintf("%-12s %-9s %s", cell.dialect, cell.kind, cell.object))
	}
	return lines
}

// TestRender_NoDialectLosesADeclaredObject is the completion criterion for
// stokaro/ptah#929 item 5: every object a schema declares reaches its target's
// renderer, and the renderer either emits a statement for it or names it.
// Nothing disappears.
//
// Measured on the state this test was written against, one fixture declaring a
// sequence, a domain, a role, a table, a view and a function rendered offline
// for every dialect spelling `--dialect` lists: fifteen objects were SILENTLY
// absent at exit 0 -- the domain on clickhouse; the domain, role and function on
// mysql and on mariadb; and the sequence, domain, role and function on both
// sqlserver and sqlite. The cause was not the renderers, which had an arm for
// each of those kinds all along. It was the converter deleting the node first,
// gated on a list of dialect names, so there was nothing left for a renderer to
// report.
//
// The assertion is on the list of losing cells rather than on a count, so a
// regression prints which object vanished on which engine.
func TestRender_NoDialectLosesADeclaredObject(t *testing.T) {
	c := qt.New(t)

	cells := routedObjectGrid(c)

	// Control: the grid really covers every dialect and every kind. A fixture or
	// a classifier that produced no cells would satisfy the assertion below while
	// measuring nothing.
	c.Assert(cells, qt.HasLen, len(routingDialects)*len(routedObjectRows))

	silent := cellsAnswering(cells, "silent")
	c.Assert(silent, qt.HasLen, 0,
		qt.Commentf("%d of %d declared objects are absent with no diagnostic:\n%s",
			len(silent), len(cells), strings.Join(silent, "\n")))
}

// TestRender_TheRoutingGridDistinguishesItsAnswers is the control for the test
// above.
//
// "No cell is silent" is satisfied by a classifier that never returns "silent",
// and by a renderer that answered every kind on every engine with a comment.
// Both edges are pinned here: the assertion before the table requires a named
// refusal to exist at all, and the rows name the cells that must be executable
// DDL -- every kind on PostgreSQL, and the table and the view on every engine --
// so the grid cannot pass by refusing everything.
func TestRender_TheRoutingGridDistinguishesItsAnswers(t *testing.T) {
	c := qt.New(t)

	cells := routedObjectGrid(c)

	// The floor the rows below cannot state as a slice: at least one cell has to
	// be a named refusal, or the classifier is reading every output as DDL and
	// the "no cell is silent" test above is measuring one answer, not three.
	c.Assert(len(cellsAnswering(cells, "named")) > 0, qt.IsTrue,
		qt.Commentf("no cell is a named refusal; the classifier reads everything as DDL"))

	tests := []struct {
		name string
		// cells is the slice of the grid the row is about, and want is every
		// line in it that answers. Naming the lines rather than counting them
		// is what makes a regression say which object moved on which engine.
		cells  []routedObjectCell
		answer string
		want   []string
	}{
		{
			name:   "postgres emits every kind",
			cells:  dialectCells(cells, platform.Postgres),
			answer: "ddl",
			want: []string{
				"postgres     sequence  seq_probe",
				"postgres     domain    domain_probe",
				"postgres     role      role_probe",
				"postgres     table     table_probe",
				"postgres     view      view_probe",
				"postgres     function  func_probe",
				"postgres     trigger   trigger_probe",
				"postgres     grant     grant_probe",
			},
		},
		{
			// ClickHouse used to answer "named" for both of these. It renders
			// them now (stokaro/ptah#1025), and naming the cells is what makes
			// a regression say which object moved rather than shifting a count.
			name:   "clickhouse emits roles and grants",
			cells:  dialectCells(cells, platform.ClickHouse),
			answer: "ddl",
			want: []string{
				"clickhouse   role      role_probe",
				"clickhouse   table     table_probe",
				"clickhouse   view      view_probe",
				"clickhouse   grant     grant_probe",
			},
		},
		{
			name:   "mysql family refuses roles",
			cells:  kindCells(cells, "role"),
			answer: "refused",
			want: []string{
				"mysql        role      role_probe",
				"mariadb      role      role_probe",
			},
		},
		{
			name:   "the table is executable everywhere",
			cells:  kindCells(cells, "table"),
			answer: "ddl",
			want: []string{
				"postgres     table     table_probe",
				"cockroachdb  table     table_probe",
				"yugabytedb   table     table_probe",
				"spanner      table     table_probe",
				"clickhouse   table     table_probe",
				"mysql        table     table_probe",
				"mariadb      table     table_probe",
				"sqlserver    table     table_probe",
				"sqlite       table     table_probe",
			},
		},
		{
			name:   "the view is executable everywhere",
			cells:  kindCells(cells, "view"),
			answer: "ddl",
			want: []string{
				"postgres     view      view_probe",
				"cockroachdb  view      view_probe",
				"yugabytedb   view      view_probe",
				"spanner      view      view_probe",
				"clickhouse   view      view_probe",
				"mysql        view      view_probe",
				"mariadb      view      view_probe",
				"sqlserver    view      view_probe",
				"sqlite       view      view_probe",
			},
		},
		{
			name:   "sqlite refuses the five kinds it has no object for",
			cells:  dialectCells(cells, platform.SQLite),
			answer: "named",
			want: []string{
				"sqlite       sequence  seq_probe",
				"sqlite       domain    domain_probe",
				"sqlite       role      role_probe",
				"sqlite       function  func_probe",
				"sqlite       grant     grant_probe",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(cellsAnswering(test.cells, test.answer), qt.DeepEquals, test.want)
		})
	}
}

// dialectCells and kindCells slice the grid along one axis.
func dialectCells(cells []routedObjectCell, dialect string) []routedObjectCell {
	return slices.DeleteFunc(slices.Clone(cells), func(cell routedObjectCell) bool {
		return cell.dialect != dialect
	})
}

func kindCells(cells []routedObjectCell, kind string) []routedObjectCell {
	return slices.DeleteFunc(slices.Clone(cells), func(cell routedObjectCell) bool {
		return cell.kind != kind
	})
}

// TestRender_SQLServerGeneratesTheSequenceItUsedOnlyToName pins the answer to
// the one cell where naming the skip and telling the truth pulled apart.
//
// SQL Server has had CREATE SEQUENCE since 2012. The renderer's refusal used to
// read "CREATE SEQUENCE ... is not supported", so routing the node there would
// have replaced a silent omission with a false claim about the engine -- and
// that is exactly why the converter withheld it, which is how the omission
// survived. Naming Ptah's generator instead of the engine was the first half
// of the answer; the second is that the generator now has the path, so the
// declared sequence becomes a statement the server executes
// (stokaro/ptah#1626).
//
// The old skip sentence must be gone, not merely joined: a target that both
// emits the statement and reports it skipped is telling the reader two
// different things about one object.
func TestRender_SQLServerGeneratesTheSequenceItUsedOnlyToName(t *testing.T) {
	c := qt.New(t)

	database := routedObjectSchema()
	adaptForSQLServer(database, platform.SQLServer)
	statements, err := renderer.GetOrderedCreateStatements(database, platform.SQLServer)
	c.Assert(err, qt.IsNil)
	sql := strings.Join(statements, "\n")

	c.Assert(sql, qt.Contains, `CREATE SEQUENCE [seq_probe] AS bigint START WITH 1000;`)
	c.Assert(sql, qt.Not(qt.Contains), `-- SQLSERVER: CREATE SEQUENCE "seq_probe" is not generated`)
	c.Assert(sql, qt.Not(qt.Contains), `-- SQLSERVER: CREATE SEQUENCE "seq_probe" is not supported`)
	c.Assert(routedObjectAnswer(sql, "seq_probe"), qt.Equals, "ddl")
}

// TestRender_MySQLFamilyRefusesRolesBeforeSQL pins that a role declared for a
// MySQL-family target fails closed.
//
// Both engines host roles, but Ptah does not read or compare their role model.
// Reporting success after emitting only a comment loses declared state, so the
// safe answer is an error before any statement is returned.
func TestRender_MySQLFamilyRefusesRolesBeforeSQL(t *testing.T) {
	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)
			statements, err := renderer.GetOrderedCreateStatements(routedObjectSchema(), dialect)

			c.Assert(statements, qt.HasLen, 0)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Check(err, qt.ErrorMatches,
				".*"+dialect+": CREATE ROLE role_probe: Ptah does not read or compare MySQL-family role state.*")
		})
	}
}

// TestValidateSchema_MySQLFamilyRefusesRoles keeps the public validation-only
// entry points aligned with complete rendering. A role cannot pass validation
// and then fail only when a caller asks for SQL.
func TestValidateSchema_MySQLFamilyRefusesRoles(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		validate func(*goschema.Database, string) error
	}{
		{
			name: "mysql default capabilities", dialect: platform.MySQL,
			validate: renderer.ValidateSchema,
		},
		{
			name: "mariadb default capabilities", dialect: platform.MariaDB,
			validate: renderer.ValidateSchema,
		},
		{
			name: "mysql capability override", dialect: platform.MySQL,
			validate: func(database *goschema.Database, dialect string) error {
				return renderer.ValidateSchemaWithCapabilities(
					database,
					dialect,
					capability.ForDialect(dialect).With(capability.RoleManagement, true),
				)
			},
		},
		{
			name: "mariadb capability override", dialect: platform.MariaDB,
			validate: func(database *goschema.Database, dialect string) error {
				return renderer.ValidateSchemaWithCapabilities(
					database,
					dialect,
					capability.ForDialect(dialect).With(capability.RoleManagement, true),
				)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := test.validate(
				&goschema.Database{Roles: []goschema.Role{{Name: "app_user"}}},
				test.dialect,
			)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches,
				".*"+test.dialect+": CREATE ROLE app_user: Ptah does not read or compare MySQL-family role state.*")
		})
	}
}

// TestValidateSchema_MySQLFamilyRoleRefusalIsNarrow pins both sides of the
// validation gate: role-free MySQL-family schemas still validate, and the
// PostgreSQL role model remains supported.
func TestValidateSchema_MySQLFamilyRoleRefusalIsNarrow(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		database *goschema.Database
	}{
		{
			name: "mysql without roles", dialect: platform.MySQL,
			database: &goschema.Database{Tables: []goschema.Table{{StructName: "T", Name: "t"}}},
		},
		{
			name: "mariadb without roles", dialect: platform.MariaDB,
			database: &goschema.Database{Tables: []goschema.Table{{StructName: "T", Name: "t"}}},
		},
		{
			name: "postgres with an application role", dialect: platform.Postgres,
			database: &goschema.Database{Roles: []goschema.Role{{Name: "app_user"}}},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := renderer.ValidateSchema(test.database, test.dialect)

			c.Assert(err, qt.IsNil)
		})
	}
}

// TestValidateSchema_MySQLFamilyRoleRefusalNamesTheSortedFirstRole pins WHICH
// role the sentence names when a schema declares several.
//
// Two gates answer the same schema. This one runs before a caller asks for SQL;
// the MySQL planner runs after, and it sorts diff.RolesAdded, so the CREATE ROLE
// node it renders first -- and refuses on -- is the alphabetically first role.
// Naming the parse-order role here made the two gates disagree about the same
// schema: the integration fixture declares app_user, admin_user, readonly_user
// and got "app_user" from validation where the planner says "admin_user". It
// also moved the name whenever a declaration was reordered, which is not a
// property of the schema.
func TestValidateSchema_MySQLFamilyRoleRefusalNamesTheSortedFirstRole(t *testing.T) {
	declarationOrders := [][]string{
		// The 016-roles integration fixture's own order.
		{"app_user", "admin_user", "readonly_user"},
		{"readonly_user", "app_user", "admin_user"},
		{"admin_user", "app_user", "readonly_user"},
	}

	for _, dialect := range []string{platform.MySQL, platform.MariaDB} {
		for _, order := range declarationOrders {
			t.Run(dialect+"/"+strings.Join(order, ","), func(t *testing.T) {
				c := qt.New(t)
				roles := make([]goschema.Role, 0, len(order))
				for _, name := range order {
					roles = append(roles, goschema.Role{Name: name})
				}

				err := renderer.ValidateSchema(&goschema.Database{Roles: roles}, dialect)

				c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
				c.Assert(err, qt.ErrorMatches,
					".*"+dialect+": CREATE ROLE admin_user: Ptah does not read or compare MySQL-family role state.*")
			})
		}
	}
}

// adaptForClickHouse rewrites the declarations in the fixture that ClickHouse
// represents differently, so the grid measures ROUTING rather than stopping at
// a refusal.
//
// ClickHouse renders roles and grants for real (stokaro/ptah#1025), but a role
// there carries no attributes at all, a grant scope is a two-part pattern, and
// a grant must name a role the same schema declares: the fixture's LOGIN, its
// bare `table_probe` and its undeclared `grant_probe` grantee are all refused.
// Nulling the declarations out — the adaptation MySQL and MariaDB get — would
// be the wrong answer here, because it would record ClickHouse as not routing
// objects it does route. The refusals themselves are pinned in
// internal/clickhouserbac, and
// TestRender_ClickHouseRefusesTheUnrepresentableDeclaration below keeps them
// reachable from this fixture.
//
// Declaring the grantee costs this grid one property, on ClickHouse alone: the
// grant cell can now be satisfied by the `CREATE ROLE grant_probe` line rather
// than by a GRANT, which is the confusion the two distinct probe names exist to
// prevent everywhere else. There is no way to keep it — a GRANT names its
// grantee, and on ClickHouse the grantee must be declared. The property is held
// instead by core/renderer/internal/dialects/clickhouse's own tests, which
// assert the rendered GRANT statement rather than a mention of the name.
func adaptForClickHouse(database *goschema.Database, dialect string) {
	if dialect != platform.ClickHouse {
		return
	}
	for i := range database.Roles {
		database.Roles[i].Login = false
	}
	for i := range database.Grants {
		database.Grants[i].OnTable = "public." + database.Grants[i].OnTable
		database.Roles = append(database.Roles, goschema.Role{
			Name: database.Grants[i].Role, Inherit: true,
		})
	}
}

// adaptForSQLServer strips the role attributes a SQL Server DATABASE role does
// not have.
//
// A database role is a permission container inside one database: it cannot log
// in, and `CREATE ROLE [r] LOGIN` is `Incorrect syntax near 'LOGIN'` on
// 17.0.4075.5. The renderer refuses the declaration rather than creating a
// principal that cannot do what the author wrote, so the grid -- which asks a
// different question, whether any declared object is lost -- hands it a role
// the target can represent. The control below is what proves the un-adapted
// fixture is refused rather than quietly rendered (stokaro/ptah#1698).
func adaptForSQLServer(database *goschema.Database, dialect string) {
	if dialect != platform.SQLServer {
		return
	}
	for i := range database.Roles {
		database.Roles[i].Login = false
		database.Roles[i].Password = ""
		database.Roles[i].Superuser = false
		database.Roles[i].CreateDB = false
		database.Roles[i].CreateRole = false
		database.Roles[i].Replication = false
	}
}

// TestRender_SQLServerRefusesARoleThatWantsToLogIn is the control on the
// adaptation above.
//
// The permanent-diff hazard is the reason this refuses rather than warns: the
// reader can only ever report those attributes false, so a comment-and-create
// answer would report the same pending change on every run forever.
func TestRender_SQLServerRefusesARoleThatWantsToLogIn(t *testing.T) {
	c := qt.New(t)

	_, err := renderer.GetOrderedCreateStatements(routedObjectSchema(), platform.SQLServer)

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, "declares LOGIN")
	c.Assert(err.Error(), qt.Contains, "server-level LOGIN")
}

// TestRender_SQLServerCreatesARoleWithoutAttributes is that control's own
// control: a renderer that refused every role would satisfy the row above and
// would never create one.
func TestRender_SQLServerCreatesARoleWithoutAttributes(t *testing.T) {
	c := qt.New(t)
	database := routedObjectSchema()
	adaptForSQLServer(database, platform.SQLServer)

	statements, err := renderer.GetOrderedCreateStatements(database, platform.SQLServer)

	c.Assert(err, qt.IsNil)
	c.Assert(strings.Join(statements, "\n"), qt.Contains, "CREATE ROLE [role_probe];")
}

// TestRender_ClickHouseRefusesTheUnrepresentableDeclaration is the control on
// the adaptation above: the grid passes because the fixture was adapted, and
// this test is what proves the un-adapted fixture is refused rather than
// quietly rendered.
func TestRender_ClickHouseRefusesTheUnrepresentableDeclaration(t *testing.T) {
	c := qt.New(t)

	_, err := renderer.GetOrderedCreateStatements(routedObjectSchema(), platform.ClickHouse)

	c.Assert(err, qt.IsNotNil)
	c.Assert(err.Error(), qt.Contains, "declares login")
	c.Assert(err.Error(), qt.Contains, "with no database")
}
