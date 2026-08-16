package clickhouse_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/ast"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/core/ptaherr"
	"go.5x5.cz/ptah/core/renderer/internal/dialects/clickhouse"
)

// renderRBAC renders one node and returns the buffer alongside the error.
//
// The shared render helper calls t.Fatalf on an error, so it cannot answer the
// question every refusal below has to answer: that the renderer refused BEFORE
// writing anything. A refusal that emits half a statement and then errors would
// pass an error-only assertion, and the half statement is what a caller
// collecting output into a migration file would keep.
func renderRBAC(t *testing.T, node ast.Node) (string, error) {
	t.Helper()
	r := clickhouse.New()
	r.Reset()
	err := node.Accept(r)
	return r.Output(), err
}

// TestVisitCreateRole_HappyPath pins the whole of what a ClickHouse CREATE ROLE
// may say.
//
// Every assertion is exact equality rather than a substring, because the claim
// is as much about what is absent as about what is present: system.roles is
// (name, id, storage), so an attribute of the node that reached the output would
// be a statement the server refuses with a syntax error.
func TestVisitCreateRole_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.CreateRoleNode
		want string
	}{
		{
			name: "the IF NOT EXISTS guard is unconditional",
			node: ast.NewCreateRole("analyst"),
			want: "CREATE ROLE IF NOT EXISTS `analyst`;\n",
		},
		{
			name: "a comment becomes the leading line",
			node: ast.NewCreateRole("analyst").SetComment("read-only reporting"),
			want: "-- read-only reporting\nCREATE ROLE IF NOT EXISTS `analyst`;\n",
		},
		{
			name: "a comment spanning lines is folded into one",
			node: ast.NewCreateRole("analyst").SetComment("reporting\nread-only"),
			want: "-- reporting read-only\nCREATE ROLE IF NOT EXISTS `analyst`;\n",
		},
		{
			name: "a percent verb in a comment is prose, not a format string",
			node: ast.NewCreateRole("analyst").SetComment("100% of the rows"),
			want: "-- 100% of the rows\nCREATE ROLE IF NOT EXISTS `analyst`;\n",
		},
		{
			name: "a name that needs quoting keeps its bytes",
			node: ast.NewCreateRole("web app"),
			want: "CREATE ROLE IF NOT EXISTS `web app`;\n",
		},
		{
			// Inherit is false here because it is the Go zero value, not because
			// anybody declared inherit="false". The two are indistinguishable at
			// this boundary, so the visitor renders rather than refuses, and
			// clickhouserbac.ValidateDeclared makes the distinction one layer up
			// where goschema.Role still carries the parser's default of true.
			name: "a zero-valued node renders",
			node: &ast.CreateRoleNode{Name: "analyst"},
			want: "CREATE ROLE IF NOT EXISTS `analyst`;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, tt.want)
		})
	}
}

// TestVisitCreateRole_FailurePath pins the attributes that are refused rather
// than dropped.
//
// Dropping them is the failure mode worth refusing: a role rendered from a
// declaration carrying PASSWORD would apply cleanly and leave an operator
// believing a credential was set on a principal that cannot hold one.
func TestVisitCreateRole_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.CreateRoleNode
		want string
	}{
		{
			name: "login",
			node: ast.NewCreateRole("analyst").SetLogin(true),
			want: `CREATE ROLE analyst declares login: a ClickHouse role carries no attributes`,
		},
		{
			name: "password",
			node: ast.NewCreateRole("analyst").SetPassword("scram-sha-256$4096:x"),
			want: `CREATE ROLE analyst declares password: a ClickHouse role carries no attributes`,
		},
		{
			name: "superuser",
			node: ast.NewCreateRole("analyst").SetSuperuser(true),
			want: `CREATE ROLE analyst declares superuser: a ClickHouse role carries no attributes`,
		},
		{
			name: "createdb",
			node: ast.NewCreateRole("analyst").SetCreateDB(true),
			want: `CREATE ROLE analyst declares createdb: a ClickHouse role carries no attributes`,
		},
		{
			name: "createrole",
			node: ast.NewCreateRole("analyst").SetCreateRole(true),
			want: `CREATE ROLE analyst declares createrole: a ClickHouse role carries no attributes`,
		},
		{
			name: "replication",
			node: ast.NewCreateRole("analyst").SetReplication(true),
			want: `CREATE ROLE analyst declares replication: a ClickHouse role carries no attributes`,
		},
		{
			// One sentence for three attributes, so a declaration carrying all
			// of them is one refused run rather than three.
			name: "every declared attribute is named at once",
			node: ast.NewCreateRole("analyst").SetLogin(true).SetSuperuser(true).SetReplication(true),
			want: `CREATE ROLE analyst declares login, superuser, replication`,
		},
		{
			name: "no name",
			node: ast.NewCreateRole(""),
			want: `CREATE ROLE names no role`,
		},
		{
			name: "a name that is only whitespace",
			node: ast.NewCreateRole("   "),
			want: `CREATE ROLE names no role`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, tt.want)
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestVisitCreateRole_RefusalNeverPrintsThePassword is the control the table
// above cannot state as a row.
//
// The refusal travels to stderr, into a plan file, and into whatever collects
// them, so naming the attribute has to be enough: interpolating the value would
// turn a safety refusal into a credential disclosure.
func TestVisitCreateRole_RefusalNeverPrintsThePassword(t *testing.T) {
	c := qt.New(t)

	secret := "correct-horse-battery-staple"
	out, err := renderRBAC(t, ast.NewCreateRole("analyst").SetPassword(secret))

	c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
	c.Assert(err.Error(), qt.Contains, "declares password")
	c.Assert(err.Error(), qt.Not(qt.Contains), secret)
	c.Assert(out, qt.Equals, "")
}

// TestVisitDropRole_HappyPath pins the guard as unconditional.
//
// node.IfExists is a PostgreSQL-shaped choice between a drop that tolerates an
// absent role and one that fails on it. Both render the tolerant form here,
// because the failing form can only abort a run at a statement whose goal the
// server has already met.
func TestVisitDropRole_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.DropRoleNode
		want string
	}{
		{
			name: "without the node's IfExists flag",
			node: ast.NewDropRole("analyst"),
			want: "DROP ROLE IF EXISTS `analyst`;\n",
		},
		{
			name: "with the node's IfExists flag",
			node: ast.NewDropRole("analyst").SetIfExists(),
			want: "DROP ROLE IF EXISTS `analyst`;\n",
		},
		{
			name: "a comment becomes the leading line",
			node: ast.NewDropRole("analyst").SetComment("superseded by reader"),
			want: "-- superseded by reader\nDROP ROLE IF EXISTS `analyst`;\n",
		},
		{
			name: "a name that needs quoting keeps its bytes",
			node: ast.NewDropRole("web app"),
			want: "DROP ROLE IF EXISTS `web app`;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, tt.want)
		})
	}
}

func TestVisitDropRole_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.DropRoleNode
		want string
	}{
		{
			name: "no name",
			node: ast.NewDropRole(""),
			want: `DROP ROLE names no role`,
		},
		{
			name: "a name that is only whitespace",
			node: ast.NewDropRole(" "),
			want: `DROP ROLE names no role`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, tt.want)
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestVisitAlterRole_FailurePath has no happy-path counterpart on purpose.
//
// A ClickHouse role has nothing an ALTER could change, so there is no input that
// renders. The assertion that the buffer stays empty is the part that matters:
// the previous implementation wrote a `-- CLICKHOUSE: ALTER ROLE ... is not
// supported` comment and returned nil, which is a migration reporting success
// for a change it never made.
func TestVisitAlterRole_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.AlterRoleNode
		want string
	}{
		{
			name: "with no operations",
			node: ast.NewAlterRole("analyst"),
			want: `ALTER ROLE analyst: a ClickHouse role carries no attributes to alter`,
		},
		{
			name: "with a password operation",
			node: ast.NewAlterRole("analyst").AddOperation(&ast.SetPasswordOperation{Password: "x"}),
			want: `ALTER ROLE analyst: a ClickHouse role carries no attributes to alter`,
		},
		{
			name: "with a login operation",
			node: ast.NewAlterRole("reader").AddOperation(&ast.SetLoginOperation{Login: true}),
			want: `ALTER ROLE reader: a ClickHouse role carries no attributes to alter`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, tt.want)
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestVisitGrantPrivilege_HappyPath pins the statement byte for byte.
//
// The scope is the measurement worth pinning: ClickHouse takes a two-part
// pattern and no object-type keyword, so `db`.`t` and `db`.* are the only two
// shapes, and the wildcard is written bare because quoting it would name a table
// literally called `*`.
func TestVisitGrantPrivilege_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.GrantPrivilegeNode
		want string
	}{
		{
			name: "a table scope",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT"}),
			want: "GRANT SELECT ON `shop`.`orders` TO `analyst`;\n",
		},
		{
			name: "a database scope",
			node: ast.NewGrantPrivilege("analyst", "SCHEMA", "shop", []string{"SELECT"}),
			want: "GRANT SELECT ON `shop`.* TO `analyst`;\n",
		},
		{
			name: "DATABASE is the same object as SCHEMA",
			node: ast.NewGrantPrivilege("analyst", "DATABASE", "shop", []string{"SELECT"}),
			want: "GRANT SELECT ON `shop`.* TO `analyst`;\n",
		},
		{
			name: "the object type is read case-insensitively",
			node: ast.NewGrantPrivilege("analyst", "table", "shop.orders", []string{"SELECT"}),
			want: "GRANT SELECT ON `shop`.`orders` TO `analyst`;\n",
		},
		{
			name: "privileges keep the order they were declared in",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"INSERT", "SELECT", "ALTER UPDATE"}),
			want: "GRANT INSERT, SELECT, ALTER UPDATE ON `shop`.`orders` TO `analyst`;\n",
		},
		{
			name: "a privilege spelled in lower case is passed through",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"select"}),
			want: "GRANT select ON `shop`.`orders` TO `analyst`;\n",
		},
		{
			name: "surrounding whitespace on a privilege is trimmed",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"  SELECT  "}),
			want: "GRANT SELECT ON `shop`.`orders` TO `analyst`;\n",
		},
		{
			name: "with grant option",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT"}).SetWithOption(true),
			want: "GRANT SELECT ON `shop`.`orders` TO `analyst` WITH GRANT OPTION;\n",
		},
		{
			name: "a comment becomes the leading line",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT"}).SetComment("dashboard reads"),
			want: "-- dashboard reads\nGRANT SELECT ON `shop`.`orders` TO `analyst`;\n",
		},
		{
			name: "identifiers that need quoting keep their bytes",
			node: ast.NewGrantPrivilege("web app", "TABLE", "shop 1.order items", []string{"SELECT"}),
			want: "GRANT SELECT ON `shop 1`.`order items` TO `web app`;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, tt.want)
		})
	}
}

// TestVisitGrantPrivilege_ObjectTypeIsInterpretedNeverEmitted states the claim
// the exact assertions above satisfy without naming.
//
// ClickHouse has no object-type keyword: `GRANT SELECT ON TABLE shop.orders TO
// analyst` does not parse. The node's ObjectType decides which of the two scope
// shapes is written and then has no spelling of its own left.
func TestVisitGrantPrivilege_ObjectTypeIsInterpretedNeverEmitted(t *testing.T) {
	tests := []struct {
		name       string
		objectType string
		objectName string
		want       string
	}{
		{
			name:       "a table scope",
			objectType: "TABLE",
			objectName: "shop.orders",
			want:       "GRANT SELECT ON `shop`.`orders` TO `analyst`;\n",
		},
		{
			name:       "a database scope",
			objectType: "SCHEMA",
			objectName: "shop",
			want:       "GRANT SELECT ON `shop`.* TO `analyst`;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, ast.NewGrantPrivilege("analyst", tt.objectType, tt.objectName, []string{"SELECT"}))
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, tt.want)
			c.Assert(out, qt.Not(qt.Contains), "ON TABLE")
			c.Assert(out, qt.Not(qt.Contains), "ON SCHEMA")
		})
	}
}

// TestVisitGrantPrivilege_FailurePath covers the three ways a PostgreSQL-shaped
// grant node fails to describe a ClickHouse grant: a scope this renderer cannot
// resolve, an object ClickHouse does not have, and a privilege that is not
// privilege syntax.
//
// The scope rows matter most. A renderer is offline, so there is no current
// database to attach an unqualified table name to; resolving it against a guess
// would make the grant land wherever the session happened to point when the
// migration was applied.
func TestVisitGrantPrivilege_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.GrantPrivilegeNode
		want string
	}{
		{
			name: "no role",
			node: ast.NewGrantPrivilege("", "TABLE", "shop.orders", []string{"SELECT"}),
			want: `GRANT names no role`,
		},
		{
			name: "no privilege",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", nil),
			want: `GRANT to role "analyst" names no privilege`,
		},
		{
			name: "a privilege that is only whitespace",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"   "}),
			want: `names privilege "   "`,
		},
		{
			name: "an unqualified table name",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "orders", []string{"SELECT"}),
			want: `names table "orders" with no database: qualify it as database.table`,
		},
		{
			name: "a three-part table name",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "cluster.shop.orders", []string{"SELECT"}),
			want: `a ClickHouse scope has at most two parts`,
		},
		{
			name: "a wildcard table",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.*", []string{"SELECT"}),
			want: `not wildcard scopes`,
		},
		{
			name: "a wildcard database",
			node: ast.NewGrantPrivilege("analyst", "SCHEMA", "*", []string{"SELECT"}),
			want: `not wildcard scopes`,
		},
		{
			name: "the global scope",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "*.*", []string{"SELECT"}),
			want: `not wildcard scopes`,
		},
		{
			name: "a sequence",
			node: ast.NewGrantPrivilege("analyst", "SEQUENCE", "order_number_seq", []string{"USAGE"}),
			want: `ClickHouse has no sequences`,
		},
		{
			name: "an object type ClickHouse cannot scope a grant to",
			node: ast.NewGrantPrivilege("analyst", "FUNCTION", "bump", []string{"EXECUTE"}),
			want: `names object type "FUNCTION": a ClickHouse grant is scoped to a database or a table`,
		},
		{
			name: "no object type",
			node: ast.NewGrantPrivilege("analyst", "", "shop.orders", []string{"SELECT"}),
			want: `names object type ""`,
		},
		{
			name: "no object name",
			node: ast.NewGrantPrivilege("analyst", "SCHEMA", "", []string{"SELECT"}),
			want: `names no object`,
		},
		{
			// A privilege is keyword syntax, so it reaches the statement
			// unquoted and there is nothing to escape it with. These four rows
			// are the injection control for the one field this file cannot
			// quote.
			name: "a privilege carrying a second statement",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT; DROP TABLE shop.orders"}),
			want: `names privilege "SELECT; DROP TABLE shop.orders"`,
		},
		{
			name: "a privilege carrying a backtick",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT` ON *.* TO `evil"}),
			want: "names privilege \"SELECT` ON *.* TO `evil\"",
		},
		{
			name: "a privilege carrying a newline",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT\nGRANT SELECT ON *.* TO evil"}),
			want: `a ClickHouse privilege is keyword syntax and cannot be quoted`,
		},
		{
			name: "a column-scoped privilege",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT(id)"}),
			want: `names privilege "SELECT(id)"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, tt.want)
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestVisitRevokePrivilege_HappyPath pins the downgrade as one statement.
//
// `REVOKE GRANT OPTION FOR ...` takes grant_option from 1 to 0 and leaves no
// is_partial_revoke row behind, measured on both declared lines. Rendering it as
// a revoke followed by a re-grant would leave the role with no privilege at all
// whenever the second statement failed.
func TestVisitRevokePrivilege_HappyPath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.RevokePrivilegeNode
		want string
	}{
		{
			name: "a table scope",
			node: ast.NewRevokePrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT"}),
			want: "REVOKE SELECT ON `shop`.`orders` FROM `analyst`;\n",
		},
		{
			name: "a database scope",
			node: ast.NewRevokePrivilege("analyst", "SCHEMA", "shop", []string{"SELECT"}),
			want: "REVOKE SELECT ON `shop`.* FROM `analyst`;\n",
		},
		{
			name: "the grant option alone",
			node: ast.NewRevokePrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT"}).SetGrantOptionFor(true),
			want: "REVOKE GRANT OPTION FOR SELECT ON `shop`.`orders` FROM `analyst`;\n",
		},
		{
			name: "the grant option on a database scope with two privileges",
			node: ast.NewRevokePrivilege("analyst", "SCHEMA", "shop", []string{"SELECT", "INSERT"}).SetGrantOptionFor(true),
			want: "REVOKE GRANT OPTION FOR SELECT, INSERT ON `shop`.* FROM `analyst`;\n",
		},
		{
			name: "SetGrantOptionFor(false) revokes the privilege itself",
			node: ast.NewRevokePrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT"}).SetGrantOptionFor(false),
			want: "REVOKE SELECT ON `shop`.`orders` FROM `analyst`;\n",
		},
		{
			name: "a comment becomes the leading line",
			node: ast.NewRevokePrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT"}).SetComment("dashboard retired"),
			want: "-- dashboard retired\nREVOKE SELECT ON `shop`.`orders` FROM `analyst`;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, tt.want)
		})
	}
}

// TestVisitRevokePrivilege_FailurePath is the revoke half of the grant table.
//
// It is written out rather than shared, because a revoke that quietly rendered
// nothing is worse than a grant that did: revoking on ClickHouse is already a
// silent no-op when the privilege was never granted, so a missing REVOKE cannot
// be detected by applying it.
func TestVisitRevokePrivilege_FailurePath(t *testing.T) {
	tests := []struct {
		name string
		node *ast.RevokePrivilegeNode
		want string
	}{
		{
			name: "no role",
			node: ast.NewRevokePrivilege("", "TABLE", "shop.orders", []string{"SELECT"}),
			want: `REVOKE names no role`,
		},
		{
			name: "no privilege",
			node: ast.NewRevokePrivilege("analyst", "TABLE", "shop.orders", nil),
			want: `REVOKE to role "analyst" names no privilege`,
		},
		{
			name: "no privilege with the grant option asked for",
			node: ast.NewRevokePrivilege("analyst", "TABLE", "shop.orders", nil).SetGrantOptionFor(true),
			want: `REVOKE to role "analyst" names no privilege`,
		},
		{
			name: "an unqualified table name",
			node: ast.NewRevokePrivilege("analyst", "TABLE", "orders", []string{"SELECT"}),
			want: `names table "orders" with no database: qualify it as database.table`,
		},
		{
			name: "a wildcard database",
			node: ast.NewRevokePrivilege("analyst", "SCHEMA", "*", []string{"SELECT"}),
			want: `not wildcard scopes`,
		},
		{
			name: "an object type ClickHouse cannot scope a grant to",
			node: ast.NewRevokePrivilege("analyst", "SEQUENCE", "order_number_seq", []string{"USAGE"}),
			want: `ClickHouse has no sequences`,
		},
		{
			name: "a privilege carrying a second statement",
			node: ast.NewRevokePrivilege("analyst", "TABLE", "shop.orders", []string{"SELECT; DROP TABLE shop.orders"}),
			want: `names privilege "SELECT; DROP TABLE shop.orders"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.ErrorIs, ptaherr.ErrUnsupportedFeature)
			c.Assert(err.Error(), qt.Contains, tt.want)
			c.Assert(out, qt.Equals, "")
		})
	}
}

// TestRBACIdentifiersCannotBreakOutOfTheStatement is the injection control for
// every identifier these five visitors write.
//
// Exact equality IS the control. Each row feeds a name carrying the two
// characters that could end a statement early — a backtick and a newline — and
// the expected string shows where they land: between the quotes, with every
// backtick doubled, so the payload is part of one identifier and the statement
// stays one statement. A renderer that concatenated the name instead would
// produce output this table cannot spell.
func TestRBACIdentifiersCannotBreakOutOfTheStatement(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{
			name: "a backtick in a created role name is doubled",
			node: ast.NewCreateRole("a`b"),
			want: "CREATE ROLE IF NOT EXISTS `a``b`;\n",
		},
		{
			name: "a role name carrying a statement stays one identifier",
			node: ast.NewCreateRole("r`; DROP TABLE shop.orders; --"),
			want: "CREATE ROLE IF NOT EXISTS `r``; DROP TABLE shop.orders; --`;\n",
		},
		{
			name: "a newline in a role name stays inside the quotes",
			node: ast.NewCreateRole("r\nGRANT SELECT ON *.* TO evil"),
			want: "CREATE ROLE IF NOT EXISTS `r\nGRANT SELECT ON *.* TO evil`;\n",
		},
		{
			name: "a dropped role name is quoted the same way",
			node: ast.NewDropRole("a`b"),
			want: "DROP ROLE IF EXISTS `a``b`;\n",
		},
		{
			name: "a grantee carrying a second grantee stays one grantee",
			node: ast.NewGrantPrivilege("a`, `evil", "TABLE", "shop.orders", []string{"SELECT"}),
			want: "GRANT SELECT ON `shop`.`orders` TO `a``, ``evil`;\n",
		},
		{
			name: "a table name carrying a backtick is doubled",
			node: ast.NewGrantPrivilege("analyst", "TABLE", "shop.or`ders", []string{"SELECT"}),
			want: "GRANT SELECT ON `shop`.`or``ders` TO `analyst`;\n",
		},
		{
			name: "a database name carrying a backtick is doubled",
			node: ast.NewGrantPrivilege("analyst", "SCHEMA", "sh`op", []string{"SELECT"}),
			want: "GRANT SELECT ON `sh``op`.* TO `analyst`;\n",
		},
		{
			name: "a revoked grantee is quoted the same way",
			node: ast.NewRevokePrivilege("a`, `evil", "TABLE", "shop.orders", []string{"SELECT"}),
			want: "REVOKE SELECT ON `shop`.`orders` FROM `a``, ``evil`;\n",
		},
		{
			// The comment is the other place a newline could end a line early,
			// and the fold is what keeps the rest of the sentence commented.
			name: "a comment carrying a statement stays commented",
			node: ast.NewCreateRole("analyst").SetComment("note\nDROP TABLE shop.orders;"),
			want: "-- note DROP TABLE shop.orders;\nCREATE ROLE IF NOT EXISTS `analyst`;\n",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)
			out, err := renderRBAC(t, tt.node)
			c.Assert(err, qt.IsNil)
			c.Assert(out, qt.Equals, tt.want)
		})
	}
}

// TestRBACVisitors_WithoutRoleManagementNameTheRefusal is the capability gate's
// own test.
//
// ClickHouse24 carries RoleManagement, so every other test in this file renders
// SQL and none of them would notice the gate being deleted. A target whose
// capability set withholds the key must get the same named refusal every other
// withheld kind gets — a comment naming the object, not silence and not an
// error — because that is what a reader looking for a missing object finds.
func TestRBACVisitors_WithoutRoleManagementNameTheRefusal(t *testing.T) {
	tests := []struct {
		name string
		node ast.Node
		want string
	}{
		{name: "create role", node: &ast.CreateRoleNode{Name: "r"}, want: `-- CLICKHOUSE: CREATE ROLE "r" is not supported`},
		{name: "drop role", node: &ast.DropRoleNode{Name: "r"}, want: `-- CLICKHOUSE: DROP ROLE "r" is not supported`},
		{name: "alter role", node: &ast.AlterRoleNode{Name: "r"}, want: `-- CLICKHOUSE: ALTER ROLE "r" is not supported`},
		{
			name: "grant",
			node: &ast.GrantPrivilegeNode{Role: "r", Privileges: []string{"SELECT"}, ObjectType: "TABLE", ObjectName: "db.t"},
			want: `-- CLICKHOUSE: GRANT "r" is not supported`,
		},
		{
			name: "revoke",
			node: &ast.RevokePrivilegeNode{Role: "r", Privileges: []string{"SELECT"}, ObjectType: "TABLE", ObjectName: "db.t"},
			want: `-- CLICKHOUSE: REVOKE "r" is not supported`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			renderer := clickhouse.NewWithCapabilities(
				capability.ClickHouse24().With(capability.RoleManagement, false),
			)
			err := test.node.Accept(renderer)

			c.Assert(err, qt.IsNil)
			c.Assert(renderer.Output(), qt.Contains, test.want)
		})
	}
}
