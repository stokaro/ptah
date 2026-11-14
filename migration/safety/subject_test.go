package safety_test

import (
	"context"
	"fmt"
	"go/types"
	"slices"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/go-extras/go-kit/must"
	"golang.org/x/tools/go/packages"

	"ptah.run/core/ast"
	"ptah.run/core/objectidentity"
	"ptah.run/core/platform/identifier"
	"ptah.run/core/schemaext"
	"ptah.run/dialect/mssql/mssqlproperty"
	"ptah.run/engine/builtin"
	"ptah.run/feature/pgpolicy"
	"ptah.run/feature/synonym"
	"ptah.run/migration/safety"
)

// changePayload is an owner operation that names its object only through the
// schema-change report.
type changePayload struct{ name string }

func (*changePayload) Kind() schemaext.Kind                   { return "example.org/change" }
func (p *changePayload) CloneExtension() ast.ExtensionPayload { return &changePayload{name: p.name} }
func (p *changePayload) SchemaChange() ast.ExtensionChange {
	return ast.ExtensionChange{Action: ast.ExtensionModify, Name: p.name}
}

// omissionPayload is an owner operation that names its object only through
// the skip line a target without the capability writes.
type omissionPayload struct{ name string }

func (*omissionPayload) Kind() schemaext.Kind { return "example.org/omission" }
func (p *omissionPayload) CloneExtension() ast.ExtensionPayload {
	return &omissionPayload{name: p.name}
}
func (p *omissionPayload) OmissionSubject() (kind, name string) { return "widget", p.name }

// subjectRow is one statement node and the object the safety report names for
// it.
type subjectRow struct {
	node    ast.Node
	subject string
}

// namedSubjects holds a row for every statement node kind that names what it
// acts on, and for each way an owner operation names its object.
var namedSubjects = []subjectRow{
	{node: &ast.AlterIndexNode{Name: "users_email_idx"}, subject: "users_email_idx"},
	{node: &ast.AlterRoleNode{Name: "reader"}, subject: "reader"},
	{node: &ast.AlterSequenceNode{Schema: "app", Name: "order_seq"}, subject: "app.order_seq"},
	{node: &ast.AlterSequenceNode{Name: "order_seq"}, subject: "order_seq"},
	{node: &ast.AlterSerialSequenceNode{Table: "orders", Column: "id"}, subject: "orders.id"},
	{node: &ast.AlterTableDisableRLSNode{Table: "orders"}, subject: "orders"},
	{node: &ast.AlterTableEnableRLSNode{Table: "orders"}, subject: "orders"},
	{node: &ast.AlterTableForceRLSNode{Table: "orders"}, subject: "orders"},
	{node: &ast.AlterTableNode{Name: "products"}, subject: "products"},
	{node: &ast.AlterTypeNode{Name: "status"}, subject: "status"},
	{node: &ast.AlterViewOptionsNode{Name: "active_users"}, subject: "active_users"},
	{node: &ast.CreateDatabaseNode{Name: "app"}, subject: "app"},
	{node: &ast.CreateFunctionNode{Name: "total"}, subject: "total"},
	{node: &ast.CreateMaterializedViewNode{Name: "daily"}, subject: "daily"},
	{node: &ast.CreatePolicyNode{Name: "tenant"}, subject: "tenant"},
	{node: &ast.CreateRoleNode{Name: "reader"}, subject: "reader"},
	{node: &ast.CreateSchemaNode{Name: "app"}, subject: "app"},
	{node: &ast.CreateSequenceNode{Schema: "app", Name: "order_seq"}, subject: "app.order_seq"},
	{node: &ast.CreateTableNode{Name: "users"}, subject: "users"},
	{node: &ast.CreateTriggerNode{Name: "audit", Table: "users"}, subject: "audit"},
	{node: &ast.CreateTypeNode{Name: "status"}, subject: "status"},
	{node: &ast.CreateViewNode{Name: "active_users"}, subject: "active_users"},
	{node: &ast.DropFunctionNode{Name: "total"}, subject: "total"},
	{node: &ast.DropIndexNode{Name: "users_email_idx"}, subject: "users_email_idx"},
	{node: &ast.DropMaterializedViewNode{Name: "daily"}, subject: "daily"},
	{node: &ast.DropPolicyNode{Name: "tenant"}, subject: "tenant"},
	{node: &ast.DropRoleNode{Name: "reader"}, subject: "reader"},
	{node: &ast.DropSequenceNode{Schema: "app", Name: "order_seq"}, subject: "app.order_seq"},
	{node: &ast.DropTableNode{Name: "legacy"}, subject: "legacy"},
	{node: &ast.DropTriggerNode{Name: "audit", Table: "users"}, subject: "audit"},
	{node: &ast.DropTypeNode{Name: "status"}, subject: "status"},
	{node: &ast.DropViewNode{Name: "active_users"}, subject: "active_users"},
	{node: &ast.EnumNode{Name: "status"}, subject: "status"},
	{node: &ast.GrantPrivilegeNode{Role: "reader", ObjectName: "users"}, subject: "users"},
	{node: &ast.GrantRoleMembershipNode{Role: "readers", Member: "app"}, subject: "readers"},
	{node: &ast.IndexNode{Name: "users_email_idx"}, subject: "users_email_idx"},
	{node: &ast.ObjectCommentNode{Object: ast.CommentedIndex, Name: "users_email_idx"}, subject: "users_email_idx"},
	{node: &ast.RefreshMaterializedViewNode{Name: "daily"}, subject: "daily"},
	{node: &ast.RevokePrivilegeNode{Role: "reader", ObjectName: "users"}, subject: "users"},
	{node: &ast.RevokeRoleMembershipNode{Role: "readers", Member: "app"}, subject: "readers"},
	{node: &ast.RoutineNode{Name: "archive"}, subject: "archive"},
	{node: &ast.UpsertNode{Table: "settings"}, subject: "settings"},
	{node: &ast.RawSQLNode{SQL: `PRAGMA foreign_keys = off`}, subject: "foreign_keys"},
	{node: &ast.RawSQLNode{SQL: `ALTER TABLE "new_products" RENAME TO "products"`}, subject: "new_products"},
	{node: &ast.OpaqueRoutineNode{SQL: "CREATE PROCEDURE archive() BEGIN SELECT 1; END", Dialect: "mysql"}, subject: "archive"},
	{node: &ast.ExtensionStatement{Payload: &scopedPayload{
		subject: objectidentity.NewBuilder(identifier.ForDialect("postgres")).SchemaScopedParts("example.org/scoped", "jobs", "route.daily"),
	}}, subject: `jobs."route.daily"`},
	{node: &ast.ExtensionStatement{Payload: &changePayload{name: "app.widgets"}}, subject: "app.widgets"},
	{node: &ast.ExtensionStatement{Payload: &omissionPayload{name: "metrics"}}, subject: "metrics"},
	{node: &ast.ExtensionAlterOperation{Payload: &changePayload{name: "orders_idx"}}, subject: "orders_idx"},
	{node: &ast.ExtensionStatement{Payload: &pgpolicy.TableStateOperation{Schema: "app", Table: "orders", Change: pgpolicy.TableStateChange{
		Before: &pgpolicy.ObservedTableState{Enabled: true}, After: &pgpolicy.DesiredTableState{},
	}}}, subject: "app.orders"},
	{node: synonymOperation(synonym.Create, "Sales", "OrdersAlias"), subject: "Sales.OrdersAlias"},
	{node: synonymOperation(synonym.Drop, "", "orders_alias"), subject: "orders_alias"},
	{node: synonymOperation(synonym.Retarget, "dbo", "orders_alias"), subject: "dbo.orders_alias"},
	{node: propertyOperation(mssqlproperty.Add, mssqlproperty.Property{Schema: "dbo", Table: "Orders", Column: "Id"}), subject: "dbo.Orders.Id"},
	{node: propertyOperation(mssqlproperty.Update, mssqlproperty.Property{Schema: "dbo", Table: "orders"}), subject: "dbo.orders"},
	{node: propertyOperation(mssqlproperty.Drop, mssqlproperty.Property{Schema: "sales"}), subject: "sales"},
}

// synonymOperation is the statement that creates, drops or retargets the
// synonym schema.name.
func synonymOperation(action synonym.Action, schema, name string) ast.Node {
	return &ast.ExtensionStatement{Payload: &synonym.Operation{Action: action, Synonym: synonym.DesiredSynonym{
		Synonym: synonym.Synonym{Schema: schema, Name: name, Target: "dbo.orders"},
	}}}
}

// propertyOperation is the statement that runs action for an MS_Description
// property on the object address names by its schema, table and column.
func propertyOperation(action mssqlproperty.Action, address mssqlproperty.Property) ast.Node {
	address.Name, address.Value = "MS_Description", "documented"
	return &ast.ExtensionStatement{Payload: &mssqlproperty.Operation{
		Action:   action,
		Property: mssqlproperty.DesiredProperty{Property: address},
	}}
}

// namelessSubjects holds a row for every statement node kind that names no
// object, and for the shapes of a naming kind that carry no name.
var namelessSubjects = []subjectRow{
	{node: &ast.CommentNode{Text: "rebuild products"}},
	{node: &ast.DoBlockNode{SQL: "DO $$ BEGIN PERFORM 1; END $$"}},
	{node: &ast.StatementList{Statements: []ast.Node{&ast.CreateTableNode{Name: "users"}}}},
	{node: &ast.RawSQLNode{SQL: "SELECT 1"}},
	{node: &ast.RawSQLNode{SQL: "CREATE INDEX ON users (email)"}},
	{node: &ast.RawSQLNode{SQL: "CREATE TABLE users (id int); CREATE TABLE orders (id int)"}},
	{node: &ast.OpaqueRoutineNode{SQL: "CALL archive()", Dialect: "postgres"}},
	{node: &ast.ExtensionStatement{}},
	{node: &ast.ExtensionStatement{Payload: &unknownPayload{}}},
	{node: &ast.ExtensionAlterOperation{Payload: (*changePayload)(nil)}},
	{node: propertyOperation(mssqlproperty.Add, mssqlproperty.Property{})},
	{node: (*ast.CreateTableNode)(nil)},
	{node: (*ast.AlterTableNode)(nil)},
	{node: nil},
}

// partNodes are the node kinds that exist only inside a statement: a column,
// a constraint, an enum's definition and the operations of an ALTER TABLE and
// an ALTER TYPE. The safety report rates the statement that carries one,
// under that statement's subject, and never reaches one alone.
var partNodes = []ast.Node{
	&ast.AddColumnOperation{}, &ast.AddConstraintOperation{}, &ast.AddEnumValueOperation{},
	&ast.AddIndexOperation{}, &ast.AlterColumnOperation{}, &ast.AlterConstraintOperation{},
	&ast.AlterGeneratedColumnExpressionOperation{}, &ast.AlterIndexVisibilityOperation{},
	&ast.ColumnNode{}, &ast.ConstraintNode{}, &ast.DropColumnOperation{}, &ast.DropConstraintOperation{},
	&ast.EnumTypeDef{}, &ast.ModifyColumnOperation{}, &ast.RenameColumnOperation{},
	&ast.RenameConstraintOperation{}, &ast.RenameEnumValueOperation{}, &ast.RenameIndexOperation{},
	&ast.RenameTableOperation{}, &ast.RenameTypeOperation{}, &ast.ReplaceIndexOperation{},
	&ast.SetCommentOperation{}, &ast.SetConstraintCommentOperation{}, &ast.ValidateConstraintOperation{},
}

// nodeKindFloor is the least number of node kinds the source walk below may
// find. A walk that stopped matching reports a short list rather than an
// error, and a short list would compare equal to tables that shrank with it.
const nodeKindFloor = 70

func TestAssess_NamesWhatEachStatementActsOn(t *testing.T) {
	for _, row := range namedSubjects {
		t.Run(fmt.Sprintf("%T/%s", row.node, row.subject), func(t *testing.T) {
			c := qt.New(t)

			assessments := safety.Assess([]ast.Node{row.node})

			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Subject, qt.Equals, row.subject)
		})
	}
}

func TestAssess_LeavesTheSubjectEmptyWhereTheStatementNamesNoObject(t *testing.T) {
	for i, row := range namelessSubjects {
		t.Run(fmt.Sprintf("%d/%T", i, row.node), func(t *testing.T) {
			c := qt.New(t)

			assessments := safety.Assess([]ast.Node{row.node})

			c.Assert(assessments, qt.HasLen, 1)
			c.Assert(assessments[0].Subject, qt.Equals, "")
		})
	}
}

// TestAssess_SubjectTablesCoverEveryNodeKind reads the node kinds out of the
// ast package's source and holds the tables above to them, so a kind added
// without a row fails here rather than reaching the report unmeasured.
func TestAssess_SubjectTablesCoverEveryNodeKind(t *testing.T) {
	c := qt.New(t)

	declared := declaredNodeKinds(c)

	c.Assert(len(declared) >= nodeKindFloor, qt.IsTrue, qt.Commentf(
		"the source walk found %d node kinds and the floor is %d", len(declared), nodeKindFloor))
	c.Assert(coveredNodeKinds(), qt.DeepEquals, declared)
}

func TestAssessRendered_NamesWhatEachRenderedStatementActsOn(t *testing.T) {
	tests := []struct {
		dialect string
		nodes   []ast.Node
		want    []string
	}{
		{dialect: "sqlite", nodes: []ast.Node{ast.NewCreateTable("users"), ast.NewCreateView("active_users").SetBody("SELECT 1")},
			want: []string{"users", "active_users"}},
		{dialect: "sqlite", nodes: []ast.Node{
			ast.NewRawSQL("PRAGMA foreign_keys = off"),
			ast.NewRawSQL(`INSERT INTO "new_products" ("id") SELECT "id" FROM "products"`),
			ast.NewRawSQL(`ALTER TABLE "new_products" RENAME TO "products"`),
		}, want: []string{"foreign_keys", "new_products", "new_products"}},
		{dialect: "postgres", nodes: []ast.Node{ast.NewRawSQL(`UPDATE ONLY "app"."users" SET "a" = 1; DELETE FROM ONLY users`)},
			want: []string{"app.users", "users"}},
		{dialect: "postgres", nodes: []ast.Node{ast.NewRawSQL(`CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS "a""b" ON t (c)`)},
			want: []string{`a"b`}},
		{dialect: "postgres", nodes: []ast.Node{ast.NewRawSQL(`COMMENT ON COLUMN "public"."users"."name" IS 'x'`)},
			want: []string{"public.users.name"}},
		{dialect: "postgres", nodes: []ast.Node{ast.NewRawSQL("TRUNCATE TABLE ONLY users; REFRESH MATERIALIZED VIEW CONCURRENTLY totals")},
			want: []string{"users", "totals"}},
		{dialect: "postgres", nodes: []ast.Node{&ast.StatementList{Statements: []ast.Node{
			ast.NewRawSQL("CREATE OR REPLACE FUNCTION app.total() RETURNS int LANGUAGE sql AS 'SELECT 1'"),
			ast.NewRawSQL("DROP TRIGGER IF EXISTS audit ON users"),
		}}}, want: []string{"app.total", "audit"}},
		{dialect: "mysql", nodes: []ast.Node{ast.NewRawSQL("INSERT IGNORE INTO `Orders` (id) VALUES (1)")},
			want: []string{"Orders"}},
		{dialect: "mysql", nodes: []ast.Node{ast.NewRawSQL(
			"UPDATE LOW_PRIORITY IGNORE orders SET a = 1; DELETE LOW_PRIORITY QUICK IGNORE FROM orders WHERE a = 1",
		)}, want: []string{"orders", "orders"}},
		{dialect: "mysql", nodes: []ast.Node{ast.NewRawSQL("UPDATE orders, items SET orders.a = items.a")},
			want: []string{"orders, items"}},
		{dialect: "sqlite", nodes: []ast.Node{ast.NewRawSQL("UPDATE OR IGNORE products SET a = 1")},
			want: []string{"products"}},
		{dialect: "postgres", nodes: []ast.Node{ast.NewRawSQL("DROP TABLE IF EXISTS a, app.b CASCADE; TRUNCATE TABLE ONLY a, b")},
			want: []string{"a, app.b", "a, b"}},
		{dialect: "postgres", nodes: []ast.Node{ast.NewRawSQL("CREATE SCHEMA app AUTHORIZATION bob")},
			want: []string{"app"}},
		{dialect: "sqlserver", nodes: []ast.Node{ast.NewRawSQL("CREATE SYNONYM [dbo].[orders_alias] FOR [dbo].[orders]")},
			want: []string{"dbo.orders_alias"}},
		{dialect: "postgres", nodes: []ast.Node{&ast.ExtensionStatement{Payload: &pgpolicy.TableStateOperation{
			Schema: "app", Table: "orders", Change: pgpolicy.TableStateChange{
				Before: &pgpolicy.ObservedTableState{Enabled: true}, After: &pgpolicy.DesiredTableState{},
				Access: schemaext.AccessEffect{Access: schemaext.AccessWidens, Reason: "disabling row security admits every row"},
			},
		}}}, want: []string{"app.orders"}},
	}
	runtime := must.Must(builtin.New())

	for _, test := range tests {
		t.Run(test.dialect+"/"+strings.Join(test.want, ","), func(t *testing.T) {
			c := qt.New(t)

			assessments, err := safety.AssessRendered(context.Background(), runtime, test.nodes, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(assessmentSubjects(assessments), qt.DeepEquals, test.want)
		})
	}
}

func TestAssessRendered_LeavesTheSubjectEmptyWhereTheStatementNamesNoObject(t *testing.T) {
	tests := []struct {
		dialect   string
		statement string
	}{
		{dialect: "postgres", statement: "DO $$ BEGIN PERFORM 1; END $$"},
		{dialect: "postgres", statement: "CREATE INDEX ON users (email)"},
		{dialect: "postgres", statement: "LOCK TABLE users IN ACCESS EXCLUSIVE MODE"},
		{dialect: "postgres", statement: "CREATE EVENT TRIGGER audit ON ddl_command_start EXECUTE FUNCTION f()"},
		{dialect: "sqlserver", statement: "EXEC sp_rename 'dbo.a', 'b'"},
		{dialect: "sqlserver", statement: "DROP TABLE [dbo].[order items]"},
		{dialect: "sqlserver", statement: "UPDATE TOP (10) orders SET a = 1"},
		{dialect: "sqlserver", statement: "DELETE TOP (10) FROM orders"},
		{dialect: "postgres", statement: "CREATE USER MAPPING FOR bob SERVER files"},
		{dialect: "postgres", statement: "DROP USER MAPPING IF EXISTS FOR bob SERVER files"},
		{dialect: "postgres", statement: "CREATE SCHEMA AUTHORIZATION bob"},
		{dialect: "postgres", statement: "CREATE SCHEMA IF NOT EXISTS AUTHORIZATION bob"},
		{dialect: "sqlite", statement: "SELECT 1"},
	}
	runtime := must.Must(builtin.New())

	for _, test := range tests {
		t.Run(test.dialect+"/"+test.statement, func(t *testing.T) {
			c := qt.New(t)

			assessments, err := safety.AssessRendered(context.Background(), runtime, []ast.Node{ast.NewRawSQL(test.statement)}, test.dialect)

			c.Assert(err, qt.IsNil)
			c.Assert(assessmentSubjects(assessments), qt.DeepEquals, []string{""})
		})
	}
}

// nameless is a pair of assessments whose first names no object, the shape
// the report prints a dash for.
var nameless = []safety.StatementAssessment{
	{Index: 1, NodeType: "*ast.DoBlockNode", Severity: safety.Safe, Reason: "runs a block", Statement: "DO $$ BEGIN END $$"},
	{Index: 2, NodeType: "*ast.CreateTableNode", Subject: "users", Severity: safety.Safe, Reason: "adds a table", Statement: "CREATE TABLE users ()"},
}

func TestRenderText_PrintsADashWhereTheStatementNamesNoObject(t *testing.T) {
	c := qt.New(t)

	var out strings.Builder
	err := safety.RenderText(&out, nameless)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Equals, "Safety classification:\n"+
		"  #  severity      subject                  reason\n"+
		"  1  safe         -                        runs a block\n"+
		"  2  safe         users                    adds a table\n")
}

func TestRenderHTML_PrintsADashWhereTheStatementNamesNoObject(t *testing.T) {
	c := qt.New(t)

	var page strings.Builder
	err := safety.RenderHTML(&page, nameless)

	c.Assert(err, qt.IsNil)
	c.Assert(page.String(), qt.Contains, `<td class="name">-</td>`)
	c.Assert(page.String(), qt.Contains, `<td class="name">users</td>`)
	c.Assert(page.String(), qt.Not(qt.Contains), "*ast.")
}

func assessmentSubjects(assessments []safety.StatementAssessment) []string {
	subjects := make([]string, 0, len(assessments))
	for _, assessment := range assessments {
		subjects = append(subjects, assessment.Subject)
	}
	return subjects
}

// coveredNodeKinds is the sorted set of node kinds the tables above describe.
func coveredNodeKinds() []string {
	var kinds []string
	for _, row := range slices.Concat(namedSubjects, namelessSubjects) {
		kinds = append(kinds, fmt.Sprintf("%T", row.node))
	}
	for _, node := range partNodes {
		kinds = append(kinds, fmt.Sprintf("%T", node))
	}
	kinds = slices.DeleteFunc(kinds, func(kind string) bool { return kind == "<nil>" })
	slices.Sort(kinds)
	return slices.Compact(kinds)
}

// declaredNodeKinds lists, as `%T` prints them, the named types of the ast
// package whose pointer implements ast.Node.
func declaredNodeKinds(c *qt.C) []string {
	c.Helper()

	pkgs, err := packages.Load(&packages.Config{Mode: packages.NeedName | packages.NeedTypes}, "ptah.run/core/ast")
	c.Assert(err, qt.IsNil)
	c.Assert(pkgs, qt.HasLen, 1)
	c.Assert(pkgs[0].Errors, qt.HasLen, 0)

	scope := pkgs[0].Types.Scope()
	node, ok := scope.Lookup("Node").Type().Underlying().(*types.Interface)
	c.Assert(ok, qt.IsTrue)

	var kinds []string
	for _, name := range scope.Names() {
		typeName, isType := scope.Lookup(name).(*types.TypeName)
		if !isType || typeName.IsAlias() || types.IsInterface(typeName.Type()) {
			continue
		}
		if types.Implements(types.NewPointer(typeName.Type()), node) {
			kinds = append(kinds, "*ast."+name)
		}
	}
	slices.Sort(kinds)
	return kinds
}
