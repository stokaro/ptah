package mysql_test

import (
	"reflect"
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/renderer"
)

// The MySQL renderer is a thin wrapper: it holds the shared mysqllike renderer
// in a named field, names four node kinds in its dispatch and hands every other
// kind to that field. A case dropped from the dispatch still renders, because
// the shared renderer answers the kind too -- with its own lowercase dialect
// name, at exit 0, and only in the text.
//
// These censuses drive each node kind through renderer.RenderSQL, which is the
// entry point `ptah schema render` uses: it accepts the node onto the
// validating renderer, which hands it to this wrapper's dispatch.

// mysqlCensusRow is one node kind and what this target makes of it.
type mysqlCensusRow struct {
	// kind is the node type name, without the ast. prefix. It labels the
	// subtest and is what the two controls below count and check against the
	// node beside it.
	kind string
	node ast.Node
	want string
}

// mysqlRenderedKinds lists every node kind MySQL answers without an error, with
// the exact SQL or refusal comment it writes.
//
// A refusal comment is output, not a failure: the render continues and the
// caller gets a line naming what was skipped. The kinds that abort the render
// instead are in mysqlRefusedKinds.
func mysqlRenderedKinds() []mysqlCensusRow {
	return []mysqlCensusRow{
		{
			kind: "AlterSequenceNode",
			node: ast.NewAlterSequence("seq1"),
			want: "-- ALTER SEQUENCE seq1 not supported in mysql\n",
		},
		{
			kind: "AlterTableDisableRLSNode",
			node: ast.NewAlterTableDisableRLS("users"),
			want: "-- MYSQL: DISABLE ROW LEVEL SECURITY on users is not generated for this target; skipped.\n",
		},
		{
			// Wrapper-owned. The capitalized dialect name is what marks this
			// line as the wrapper's rather than the shared renderer's.
			kind: "AlterTableEnableRLSNode",
			node: ast.NewAlterTableEnableRLS("users"),
			want: "-- ALTER TABLE users ENABLE ROW LEVEL SECURITY not supported in MySQL\n",
		},
		{
			kind: "AlterTableNode",
			node: &ast.AlterTableNode{
				Name:       "users",
				Operations: []ast.AlterOperation{&ast.AddColumnOperation{Column: ast.NewColumn("email", "VARCHAR(255)")}},
			},
			want: "-- ALTER statements: --\nALTER TABLE `users` ADD COLUMN `email` VARCHAR(255);\n\n",
		},
		{
			kind: "AlterTypeNode",
			node: ast.NewAlterType("mood"),
			want: "-- MYSQL does not support ALTER TYPE - type changes are handled through ALTER TABLE MODIFY COLUMN\n",
		},
		{
			// A column renders as part of the table that owns it, so a column
			// on its own writes nothing. The empty string is the measurement:
			// a handler that started emitting a bare column definition here
			// would be a change, not a fix.
			kind: "ColumnNode",
			node: ast.NewColumn("email", "VARCHAR(255)"),
			want: "",
		},
		{
			kind: "CommentNode",
			node: ast.NewComment("a note"),
			want: "-- a note --\n",
		},
		{
			// Same as ColumnNode: a constraint belongs to its table.
			kind: "ConstraintNode",
			node: ast.NewUniqueConstraint("uq_users_email", "email"),
			want: "",
		},
		{
			kind: "CreateContinuousAggregateNode",
			node: ast.NewCreateContinuousAggregate("agg1", "SELECT 1"),
			want: "-- Continuous aggregate agg1 not supported in mysql\n",
		},
		{
			kind: "CreateDatabaseNode",
			node: ast.NewCreateDatabase("appdb"),
			want: "CREATE DATABASE `appdb`;\n",
		},
		{
			// The wrapper forwards this one deliberately: the engine has
			// CREATE FUNCTION, so a `not supported` claim here would be wrong.
			kind: "CreateFunctionNode",
			node: &ast.CreateFunctionNode{Name: "touch", Returns: "int", Volatility: "IMMUTABLE", Body: "RETURN 1"},
			want: "CREATE FUNCTION `touch`() RETURNS int DETERMINISTIC RETURN 1;\n",
		},
		{
			kind: "CreateHypertableNode",
			node: ast.NewCreateHypertable("events", "ts"),
			want: "-- Hypertable events not supported in mysql\n",
		},
		{
			// Wrapper-owned.
			kind: "CreatePolicyNode",
			node: ast.NewCreatePolicy("p1", "users"),
			want: "-- CREATE POLICY p1 not supported in MySQL\n",
		},
		{
			kind: "CreateRoleNode",
			node: ast.NewCreateRole("app_role"),
			want: "CREATE ROLE IF NOT EXISTS `app_role`;\n",
		},
		{
			kind: "CreateSchemaNode",
			node: ast.NewCreateSchema("app"),
			want: "CREATE SCHEMA `app`;\n",
		},
		{
			// The capability split: one shared body refuses here and renders
			// on MariaDB. TestSequenceCapabilitySplit in the mysqllike suite
			// holds the pair.
			kind: "CreateSequenceNode",
			node: ast.NewCreateSequence("seq1"),
			want: "-- CREATE SEQUENCE seq1 not supported in mysql\n",
		},
		{
			kind: "CreateSynonymNode",
			node: &ast.CreateSynonymNode{Name: "s1", Target: "users"},
			want: "-- Synonym s1 not supported in mysql\n",
		},
		{
			kind: "CreateTableNode",
			node: mysqlCensusTable(),
			want: "-- MYSQL TABLE: users --\nCREATE TABLE `users` (\n  `id` INT\n);\n\n",
		},
		{
			kind: "CreateTriggerNode",
			node: &ast.CreateTriggerNode{
				Name: "trg1", Table: "users", Timing: "BEFORE", Event: "INSERT", Body: "SET NEW.id = NEW.id",
			},
			want: "CREATE TRIGGER `trg1` BEFORE INSERT ON `users` FOR EACH ROW SET NEW.id = NEW.id;\n",
		},
		{
			kind: "CreateTypeNode",
			node: ast.NewCreateType("mood", ast.NewEnumTypeDef("happy", "sad")),
			want: "-- MYSQL: CREATE TYPE mood is not generated for this target; skipped.\n",
		},
		{
			kind: "CreateViewNode",
			node: &ast.CreateViewNode{Name: "v1", Body: "SELECT 1"},
			want: "CREATE VIEW `v1` AS\nSELECT 1\n;\n",
		},
		{
			kind: "DefaultPrivilegeNode",
			node: ast.NewDefaultPrivilege("owner", "app", "TABLE", "app_role", []ast.DefaultPrivilege{{Privilege: "SELECT"}}),
			want: "-- MYSQL: default privilege app_role is not generated for this target; skipped.\n",
		},
		{
			kind: "DropContinuousAggregateNode",
			node: ast.NewDropContinuousAggregate("agg1"),
			want: "-- Continuous aggregate agg1 not supported in mysql\n",
		},
		{
			// Wrapper-owned.
			kind: "DropExtensionNode",
			node: ast.NewDropExtension("pg_trgm"),
			want: "-- DROP EXTENSION pg_trgm not supported in MySQL\n",
		},
		{
			kind: "DropFunctionNode",
			node: ast.NewDropFunction("touch"),
			want: "DROP FUNCTION `touch`;\n",
		},
		{
			kind: "DropIndexNode",
			node: &ast.DropIndexNode{Name: "idx_users_email", Table: "users"},
			want: "DROP INDEX `idx_users_email` ON `users`;\n",
		},
		{
			kind: "DropPolicyNode",
			node: ast.NewDropPolicy("p1", "users"),
			want: "-- MYSQL: DROP POLICY p1 is not generated for this target; skipped.\n",
		},
		{
			kind: "DropRoleNode",
			node: ast.NewDropRole("app_role"),
			want: "DROP ROLE `app_role`;\n",
		},
		{
			kind: "DropSequenceNode",
			node: ast.NewDropSequence("seq1"),
			want: "-- DROP SEQUENCE seq1 not supported in mysql\n",
		},
		{
			kind: "DropSynonymNode",
			node: ast.NewDropSynonym("s1"),
			want: "-- DROP SYNONYM s1 not supported in mysql\n",
		},
		{
			kind: "DropTableNode",
			node: ast.NewDropTable("users"),
			want: "DROP TABLE `users`;\n",
		},
		{
			kind: "DropTriggerNode",
			node: &ast.DropTriggerNode{Name: "trg1", Table: "users"},
			want: "DROP TRIGGER `trg1`;\n",
		},
		{
			kind: "DropTypeNode",
			node: ast.NewDropType("mood"),
			want: "-- MYSQL: DROP TYPE mood is not generated for this target; skipped.\n",
		},
		{
			kind: "DropViewNode",
			node: ast.NewDropView("v1"),
			want: "DROP VIEW `v1`;\n",
		},
		{
			// MySQL spells an enumeration inline on the column, so a standalone
			// enum node has nothing of its own to write.
			kind: "EnumNode",
			node: ast.NewEnum("mood", "happy", "sad"),
			want: "",
		},
		{
			kind: "ExtendedPropertyNode",
			node: ast.NewExtendedProperty(ast.ExtendedPropertyAdd, "MS_Description"),
			want: "-- EXTENDED PROPERTY MS_Description not supported in mysql\n",
		},
		{
			// Wrapper-owned.
			kind: "ExtensionNode",
			node: ast.NewExtension("pg_trgm"),
			want: "-- Extension pg_trgm not supported in MySQL\n",
		},
		{
			kind: "GrantPrivilegeNode",
			node: ast.NewGrantPrivilege("app_role", "TABLE", "users", []string{"SELECT"}),
			want: "GRANT SELECT ON `users` TO `app_role`;\n",
		},
		{
			kind: "IndexNode",
			node: ast.NewIndex("idx_users_email", "users", "email"),
			want: "CREATE INDEX `idx_users_email` ON `users` (`email`);\n",
		},
		{
			kind: "RawSQLNode",
			node: ast.NewRawSQL("SELECT 1;"),
			want: "SELECT 1;\n",
		},
		{
			kind: "RevokeDefaultPrivilegeNode",
			node: ast.NewRevokeDefaultPrivilege("owner", "app", "TABLE", "app_role", []string{"SELECT"}),
			want: "-- MYSQL: default privilege app_role is not generated for this target; skipped.\n",
		},
		{
			kind: "RevokePrivilegeNode",
			node: ast.NewRevokePrivilege("app_role", "TABLE", "users", []string{"SELECT"}),
			want: "REVOKE SELECT ON `users` FROM `app_role`;\n",
		},
	}
}

// mysqlRefusedKinds lists every node kind that aborts a MySQL render, with the
// message it aborts on. A refusal returns no SQL at all.
func mysqlRefusedKinds() []mysqlCensusRow {
	return []mysqlCensusRow{
		{
			kind: "AlterMaterializedViewRefreshNode",
			node: ast.NewAlterMaterializedViewRefresh("mv1", &ast.MatViewRefreshSpec{Mode: "EVERY", Interval: "1 HOUR"}),
			want: "unsupported feature: mysql: ALTER MATERIALIZED VIEW REFRESH mv1: materialized views are not " +
				"supported by MySQL or MariaDB; remove matview definitions for this target",
		},
		{
			kind: "AlterRoleNode",
			node: ast.NewAlterRole("app_role"),
			want: `unsupported feature: mysql: role "app_role" declares an altered attribute, which a role does ` +
				"not carry here; a principal that logs in is a USER here, and Ptah does not manage users",
		},
		{
			kind: "CreateMaterializedViewNode",
			node: &ast.CreateMaterializedViewNode{Name: "mv1", Body: "SELECT 1"},
			want: "unsupported feature: mysql: CREATE MATERIALIZED VIEW mv1: materialized views are not " +
				"supported by MySQL or MariaDB; remove matview definitions for this target",
		},
		{
			kind: "DropMaterializedViewNode",
			node: ast.NewDropMaterializedView("mv1"),
			want: "unsupported feature: mysql: DROP MATERIALIZED VIEW mv1: materialized views are not " +
				"supported by MySQL or MariaDB; remove matview definitions for this target",
		},
		{
			kind: "RefreshMaterializedViewNode",
			node: ast.NewRefreshMaterializedView("mv1"),
			want: "unsupported feature: mysql: REFRESH MATERIALIZED VIEW mv1: materialized views are not " +
				"supported by MySQL or MariaDB; remove matview definitions for this target",
		},
		{
			kind: "UpsertNode",
			node: &ast.UpsertNode{
				Table: "users", InsertColumns: []string{"id"}, Values: []string{"1"}, MatchColumns: []string{"id"},
			},
			want: "unsupported feature: mysql: upsert rendering is not implemented",
		},
	}
}

// mysqlCensusTable is the CREATE TABLE fixture, built where a composite literal
// cannot express it.
func mysqlCensusTable() *ast.CreateTableNode {
	table := ast.NewCreateTable("users")
	table.AddColumn(ast.NewColumn("id", "INT"))
	return table
}

// TestMySQLDispatch_EveryNodeKindCensus_HappyPath pins what each node kind
// renders on MySQL through the public render path.
//
// The value of the row is the whole output, not a fragment: a forward that
// stops forwarding writes nothing, and a substring check on an empty string is
// the one assertion that shape can still satisfy.
func TestMySQLDispatch_EveryNodeKindCensus_HappyPath(t *testing.T) {
	for _, row := range mysqlRenderedKinds() {
		t.Run(row.kind, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.MySQL, row.node)

			c.Assert(err, qt.IsNil)
			c.Assert(sql, qt.Equals, row.want)
		})
	}
}

// TestMySQLDispatch_EveryNodeKindCensus_FailurePath pins the node kinds MySQL
// cannot render standalone. Each aborts the render rather than writing a
// skipped comment, and the census records that outcome rather than leaving the
// kind out.
func TestMySQLDispatch_EveryNodeKindCensus_FailurePath(t *testing.T) {
	for _, row := range mysqlRefusedKinds() {
		t.Run(row.kind, func(t *testing.T) {
			c := qt.New(t)

			sql, err := renderer.RenderSQL(platform.MySQL, row.node)

			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err, qt.ErrorMatches, row.want)
			c.Assert(sql, qt.Equals, "")
		})
	}
}

// censusKindFloor is how many node kinds the two censuses answer for.
//
// The count is pinned because a table that quietly lost a row reports nothing
// and reads as success, and because ast.Visitor carries one method and holds no
// list of node kinds to compare the census against. It is a ratchet: a kind the
// census starts answering for raises it, and a kind it stops answering for
// reddens this test.
//
// What this number does not measure is whether a node kind reaches any renderer
// at all. [ptah.run/internal/astrouteguard] derives the whole corpus from
// core/ast and owns that question for every dialect at once.
const censusKindFloor = 48

// censusRows is the two censuses joined, which is the set this file answers for.
func censusRows() []mysqlCensusRow {
	return append(mysqlRenderedKinds(), mysqlRefusedKinds()...)
}

// TestMySQLDispatch_TheCensusCoversEveryNodeKind is the floor under the two
// censuses: a table that quietly lost a row reports nothing and reads as
// success.
func TestMySQLDispatch_TheCensusCoversEveryNodeKind(t *testing.T) {
	c := qt.New(t)

	rows := censusRows()
	covered := make([]string, 0, len(rows))
	for _, row := range rows {
		covered = append(covered, row.kind)
	}
	slices.Sort(covered)
	distinct := slices.Compact(slices.Clone(covered))

	c.Assert(distinct, qt.DeepEquals, covered,
		qt.Commentf("a node kind is censused twice, so the count covers fewer kinds than it reports"))
	c.Assert(len(distinct) >= censusKindFloor, qt.IsTrue,
		qt.Commentf("the census answers for %d node kinds, below the floor of %d", len(distinct), censusKindFloor))
}

// TestMySQLDispatch_EachCensusRowNamesItsNode keeps the kind label honest.
//
// The label is what the floor above counts, so a label that drifted from the
// node beside it leaves the count intact while the census stopped answering for
// the kind it claims.
func TestMySQLDispatch_EachCensusRowNamesItsNode(t *testing.T) {
	for _, row := range censusRows() {
		t.Run(row.kind, func(t *testing.T) {
			c := qt.New(t)

			c.Assert(reflect.TypeOf(row.node).Elem().Name(), qt.Equals, row.kind)
		})
	}
}
