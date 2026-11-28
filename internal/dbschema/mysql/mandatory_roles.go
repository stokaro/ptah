package mysql

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"

	"ptah.run/catalog"
	"ptah.run/internal/mysqlgrants"
	"ptah.run/internal/mysqlstored"
)

// mandatoryRoles is what a read established about MySQL's mandatory_roles.
type mandatoryRoles struct {
	// memberships are PUBLIC's memberships in the roles the setting names,
	// each role named as a membership names it.
	memberships []catalog.RoleMembership
	// unread says why the read could not establish which roles the setting
	// names, and which names it found, and is nil where it could.
	unread *catalog.EveryAccountUnread
}

// publicRoleHidesMandatory is the cause a read records where a MySQL role
// named PUBLIC beside the mandatory roles leaves no grantee to state that
// every account holds them.
const publicRoleHidesMandatory = "a role named PUBLIC hides which roles every account holds, " +
	"since each membership a read records for PUBLIC is that role's"

// addTo adds the memberships of PUBLIC in the mandatory roles to schema's
// memberships, and records in schema whether every account then holds roles
// whose memberships the read does not state: where the mandatory roles were
// not read, and where a MySQL role is named PUBLIC
// ([catalog.Database.PublicIsRole]). That role's described memberships list
// it as a member, so a membership of PUBLIC is the role's, and none is left to
// state what every account holds: `ptah db read` would print GRANT of each
// mandatory role to the role, and a comparison would hold it. The memberships
// are then left as they are.
func (m mandatoryRoles) addTo(schema *catalog.Database) {
	switch {
	case m.unread != nil:
		schema.EveryAccountHoldsUnread = m.unread
	case schema.PublicIsRole && len(m.memberships) > 0:
		roles := make([]string, 0, len(m.memberships))
		for _, membership := range m.memberships {
			roles = append(roles, membership.Role)
		}
		schema.EveryAccountHoldsUnread = &catalog.EveryAccountUnread{Roles: roles, Cause: publicRoleHidesMandatory}
	case schema.PublicIsRole:
	default:
		schema.RoleMemberships = append(schema.RoleMemberships, m.memberships...)
	}
}

// readMandatoryRoles reports each account MySQL's mandatory_roles names as
// held by PUBLIC, every account's grantee. A mandatory role can be activated
// by every account, and its grants therefore contribute to every role's reach.
// MariaDB has no mandatory_roles setting.
//
// APPLICABLE_ROLES compares incompatible collations inside its definition
// when default_collation_for_utf8mb4 is utf8mb4_general_ci. Reading the setting
// and checking mysql.user avoids that view without changing the session.
// The parser follows the server's account-list grammar, including its change
// to double-quoted lists in MySQL 9.7.
//
// A setting, a version or an account the read cannot read leaves the
// mandatory roles unread rather than failing the read, with the cause: the
// security analysis then says it cannot measure a role's reach, `ptah db
// read` prints a note, and a table revoke is refused. A cancellation still
// fails it.
func (r *Reader) readMandatoryRoles(ctx context.Context, names *mysqlgrants.Names) (mandatoryRoles, error) {
	if names.MariaDB {
		return mandatoryRoles{}, nil
	}
	var value, version string
	if err := r.db.QueryRowContext(ctx, "SELECT @@GLOBAL.mandatory_roles, VERSION()").Scan(&value, &version); err != nil {
		return mandatoryRoles{unread: &catalog.EveryAccountUnread{
			Cause: fmt.Sprintf("the read could not read @@GLOBAL.mandatory_roles: %v", err),
		}}, ctx.Err()
	}
	if value == "" {
		return mandatoryRoles{}, nil
	}
	versionID := mysqlstored.VersionID(version)
	if versionID == 0 {
		return mandatoryRoles{unread: &catalog.EveryAccountUnread{
			Cause: fmt.Sprintf("the server version %q does not say which account-list grammar reads "+
				"@@GLOBAL.mandatory_roles = %q", version, value),
		}}, nil
	}
	// MySQL keeps the first occurrence with equal user bytes and equal host
	// byte lengths whose hosts compare without case. The stored host supplies
	// that comparison's key; the membership keeps the setting's spelling.
	type accountKey struct {
		user, storedHost string
		hostBytes        int
	}
	seen := make(map[accountKey]bool)
	var memberships []catalog.RoleMembership
	accounts := mysqlgrants.MandatoryAccounts(value, versionID >= 90700)
	for _, role := range accounts {
		storedHost, err := r.mandatoryRoleHost(ctx, role)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return mandatoryRoles{unread: &catalog.EveryAccountUnread{
				Roles: mandatoryNames(accounts),
				Cause: fmt.Sprintf("the read could not look up the mandatory role %s in mysql.user: %v", role.Grantee(), err),
			}}, ctx.Err()
		}
		key := accountKey{user: role.User, storedHost: storedHost, hostBytes: len(role.Host)}
		if seen[key] {
			continue
		}
		seen[key] = true
		// The account the server makes active is the setting's spelling, and
		// it holds the stored account's privileges only where the hosts match
		// byte for byte. Measured on MySQL 8.4.13 and 26.10.0 with the role
		// 'mloc'@'localhost' reading a table: `mloc`@LOCALHOST makes
		// `mloc`@`LOCALHOST` active, and a login with it active is refused the
		// table with Error 1142. With `mh`@`K`, `mh`@`k` the server keeps
		// the first spelling alone, and the login is refused the table of
		// 'mh'@'k' the same way. Such an account holds nothing, so PUBLIC is
		// not recorded holding it.
		if storedHost != role.Host {
			continue
		}
		memberships = append(memberships, catalog.RoleMembership{Role: names.Name(role), Member: publicGrantee})
	}
	slices.SortFunc(memberships, func(a, b catalog.RoleMembership) int { return cmp.Compare(a.Role, b.Role) })
	return mandatoryRoles{memberships: memberships}, nil
}

// mandatoryNames is each account the setting names, named as a membership
// names it ([mysqlgrants.Options.PrincipalName]), sorted and without repeats.
// The read stopped before it could tell which of them exist, so a name may be
// one no account has.
func mandatoryNames(accounts []mysqlgrants.Account) []string {
	names := make([]string, 0, len(accounts))
	for _, account := range accounts {
		names = append(names, mysqlgrants.Options{}.PrincipalName(account))
	}
	slices.Sort(names)
	return slices.Compact(names)
}

// mandatoryRoleHost finds the account as MySQL's mandatory-role cache does:
// user bytes match exactly, while hosts use the system character set's case
// table without padding. Explicit conversion avoids the connection's default
// collation and an invalid conversion of a Unicode host to mysql.user's ASCII
// column. The general_ci case table matches the server's system character set;
// a binary comparison of the lowered strings keeps accents and trailing spaces.
//
// The stored host is only an existence and duplicate key. Replacing the
// setting's host with it changes the activated role's identity: a role on k
// can be found as K, yet activating K does not grant the privileges of k.
func (r *Reader) mandatoryRoleHost(ctx context.Context, role mysqlgrants.Account) (string, error) {
	var host string
	err := r.db.QueryRowContext(ctx, `
		SELECT Host FROM mysql.user
		WHERE CAST(User AS BINARY) = CAST(? AS BINARY)
		  AND CAST(LOWER(CONVERT(Host USING utf8mb4) COLLATE utf8mb4_general_ci) AS BINARY)
		    = CAST(LOWER(CONVERT(? USING utf8mb4) COLLATE utf8mb4_general_ci) AS BINARY)`,
		role.User, role.Host,
	).Scan(&host)
	return host, err
}
