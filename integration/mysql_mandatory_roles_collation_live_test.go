//go:build integration

package integration_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"

	"ptah.run/catalog"
	"ptah.run/dbschema"
	"ptah.run/internal/dbtarget"
)

// TestMySQLMandatoryRolesReadWithSessionCollation keeps the account identities
// in mandatory_roles when the session's utf8mb4 default makes MySQL's
// APPLICABLE_ROLES view fail. Logins can be mandatory roles, and user names are
// case-sensitive. A host spelled in another case than the account's is left
// out: the server makes that spelling active, and it holds nothing.
func TestMySQLMandatoryRolesReadWithSessionCollation(t *testing.T) {
	c := qt.New(t)
	server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
	database := server.database(c, "mandatory_collation")
	suffix := fmt.Sprintf("_%d", time.Now().UnixNano()%1_000_000_000_000)
	accounts := []string{
		granteeIdentifier("mand" + suffix),
		granteeIdentifier("Mand" + suffix),
		granteeIdentifier("mloc"+suffix) + "@'localhost'",
		granteeIdentifier("mlog" + suffix),
		granteeIdentifier("m,x@'" + suffix),
	}
	c.Cleanup(func() {
		_, err := server.admin.ExecContext(context.Background(), "DROP USER IF EXISTS "+strings.Join(accounts, ", "))
		c.Check(err, qt.IsNil)
	})
	for _, statement := range []string{
		"CREATE ROLE " + strings.Join(accounts[:3], ", ") + ", " + accounts[4],
		"CREATE USER " + accounts[3],
	} {
		_, err := server.admin.ExecContext(c.Context(), statement)
		c.Assert(err, qt.IsNil)
	}
	withMandatoryRoles(c, server, strings.Join([]string{
		accounts[0], accounts[1], granteeIdentifier("mloc"+suffix) + "@'LOCALHOST'",
		accounts[3], accounts[4], "missing" + suffix, accounts[0],
	}, ", "))
	target := server.url("mysql", database) + "?default_collation_for_utf8mb4=utf8mb4_general_ci"
	conn, err := dbschema.ConnectToDatabase(c.Context(), target)
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)

	read, err := conn.Reader().ReadSchemaContext(c.Context())

	c.Assert(err, qt.IsNil)
	mandatory := slices.DeleteFunc(read.RoleMemberships, func(membership catalog.RoleMembership) bool {
		return membership.Member != "PUBLIC"
	})
	c.Assert(mandatory, qt.ContentEquals, []catalog.RoleMembership{
		{Role: "mand" + suffix, Member: "PUBLIC"},
		{Role: "Mand" + suffix, Member: "PUBLIC"},
		{Role: "mlog" + suffix, Member: "PUBLIC"},
		{Role: "m,x@'" + suffix, Member: "PUBLIC"},
	})
}

// TestMySQLMandatoryRoleHostIdentity keeps the first host spelling among
// equal-length case variants, as MySQL does, and records the account only
// where that spelling is the account's own: a spelling in another case, or a
// Unicode variant of another byte length, holds nothing. A host with a
// trailing space or accent is not the stored account. These distinctions
// decide which role MySQL activates.
func TestMySQLMandatoryRoleHostIdentity(t *testing.T) {
	tests := []struct {
		name        string
		hosts, want []string
	}{
		{name: "uppercase first and Unicode", hosts: []string{"K", "k", "K", "K"}},
		{name: "lowercase first", hosts: []string{"k", "K"}, want: []string{"k"}},
		{name: "padding and accents do not match", hosts: []string{"k ", "ḱ", "missing"}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
			database := server.database(c, "mandatory_host")
			role := fmt.Sprintf("mh_%d", time.Now().UnixNano()%1_000_000_000_000)
			account := granteeIdentifier(role) + "@'k'"
			c.Cleanup(func() {
				_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+account)
				c.Check(err, qt.IsNil)
			})
			_, err := server.admin.ExecContext(c.Context(), "CREATE ROLE "+account)
			c.Assert(err, qt.IsNil)
			var named []string
			for _, host := range test.hosts {
				named = append(named, granteeIdentifier(role)+"@"+granteeIdentifier(host))
			}
			withMandatoryRoles(c, server, strings.Join(named, ", "))
			want := make([]catalog.RoleMembership, 0, len(test.want))
			for _, host := range test.want {
				want = append(want, catalog.RoleMembership{Role: role + "@" + host, Member: "PUBLIC"})
			}

			got := readMandatoryMemberships(c, server.url("mysql", database))

			c.Assert(got, qt.ContentEquals, want)
		})
	}
}

// TestMySQLMandatoryRoleQuotedListGrammar checks the account list against the
// server cache on its normal session, then reads it through a session whose
// collation prevents querying that view. Double quotes protect list commas
// and spaces from MySQL 9.7 onward; earlier servers parse them differently.
func TestMySQLMandatoryRoleQuotedListGrammar(t *testing.T) {
	for _, prefix := range []string{"mq,x", "mq x"} {
		t.Run(prefix, func(t *testing.T) {
			c := qt.New(t)
			server := newMySQLFamilyServer(c, dbtarget.MySQLAdmin)
			database := server.database(c, "mandatory_quotes")
			role := fmt.Sprintf("%s_%d", prefix, time.Now().UnixNano()%1_000_000_000_000)
			account := granteeIdentifier(role) + "@'h'"
			c.Cleanup(func() {
				_, err := server.admin.ExecContext(context.Background(), "DROP ROLE IF EXISTS "+account)
				c.Check(err, qt.IsNil)
			})
			_, err := server.admin.ExecContext(c.Context(), "CREATE ROLE "+account)
			c.Assert(err, qt.IsNil)
			withMandatoryRoles(c, server, `"`+role+`"@"h"`)
			want := mandatoryMembershipsInView(c, server)

			got := readMandatoryMemberships(c, server.url("mysql", database))

			c.Assert(got, qt.ContentEquals, want)
		})
	}
}

func readMandatoryMemberships(c *qt.C, target string) []catalog.RoleMembership {
	c.Helper()
	conn, err := dbschema.ConnectToDatabase(c.Context(), target+"?default_collation_for_utf8mb4=utf8mb4_general_ci")
	c.Assert(err, qt.IsNil)
	defer dbschema.CloseAndWarn(conn)
	read, err := conn.Reader().ReadSchemaContext(c.Context())
	c.Assert(err, qt.IsNil)
	return slices.DeleteFunc(read.RoleMemberships, func(membership catalog.RoleMembership) bool {
		return membership.Member != "PUBLIC"
	})
}

func mandatoryMembershipsInView(c *qt.C, server mysqlFamilyServer) []catalog.RoleMembership {
	c.Helper()
	rows, err := server.admin.QueryContext(c.Context(), `
		SELECT ROLE_NAME, ROLE_HOST FROM information_schema.APPLICABLE_ROLES WHERE IS_MANDATORY = 'YES'`)
	c.Assert(err, qt.IsNil)
	defer rows.Close()
	memberships := make([]catalog.RoleMembership, 0)
	for rows.Next() {
		var user, host string
		c.Assert(rows.Scan(&user, &host), qt.IsNil)
		memberships = append(memberships, catalog.RoleMembership{Role: user + "@" + host, Member: "PUBLIC"})
	}
	c.Assert(rows.Err(), qt.IsNil)
	return memberships
}
