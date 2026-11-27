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
func (r *Reader) readMandatoryRoles(ctx context.Context, names *mysqlgrants.Names) ([]catalog.RoleMembership, error) {
	if names.MariaDB {
		return nil, nil
	}
	var value, version string
	if err := r.db.QueryRowContext(ctx, "SELECT @@GLOBAL.mandatory_roles, VERSION()").Scan(&value, &version); err != nil {
		return nil, err
	}
	if value == "" {
		return nil, nil
	}
	versionID := mysqlstored.VersionID(version)
	if versionID == 0 {
		return nil, fmt.Errorf("cannot read mandatory roles for unrecognized MySQL version %q", version)
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
	for _, role := range mysqlgrants.MandatoryAccounts(value, versionID >= 90700) {
		storedHost, err := r.mandatoryRoleHost(ctx, role)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		key := accountKey{user: role.User, storedHost: storedHost, hostBytes: len(role.Host)}
		if seen[key] {
			continue
		}
		seen[key] = true
		name, err := names.Name(role)
		if err != nil {
			return nil, err
		}
		memberships = append(memberships, catalog.RoleMembership{Role: name, Member: publicGrantee})
	}
	slices.SortFunc(memberships, func(a, b catalog.RoleMembership) int { return cmp.Compare(a.Role, b.Role) })
	return memberships, nil
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
