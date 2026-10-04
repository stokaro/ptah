package ydb

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"

	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb"
	"github.com/ydb-platform/ydb-go-genproto/protos/Ydb_Scheme"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
	"ptah.run/core/platform"
	"ptah.run/internal/sqlident"
	"ptah.run/internal/ydbacl"
)

// Principals is what a YDB database reports of its users, groups and
// memberships.
type Principals struct {
	// Database is the absolute path of the database that holds them, such as
	// /local. Users and groups belong to a database, not to a directory in it.
	Database string
	// Users are the users, each with whether it may log in.
	Users []User
	// Groups are the group names.
	Groups []string
	// Memberships are the members of each group.
	Memberships []Membership
}

// User is one YDB user.
type User struct {
	// Name is the user's name.
	Name string
	// Enabled reports whether the user may log in: false for a user created
	// or altered with NOLOGIN, which YDB reports as IsEnabled.
	Enabled bool
}

// Membership is one member of one group: a user or another group.
type Membership struct {
	// Group is the group that has the member.
	Group string
	// Member is the user or group that holds the group's permissions.
	Member string
}

// ErrPrincipalsRefused reports a database that would not let the read see its
// users, groups and memberships, which YDB reports only through .sys/auth_*.
// Measured on 25.1.4.7 and 26.2.1.14: a user who may list the database and may
// not read its rows -- a member of METADATA-READERS alone, or a user granted
// LIST directly -- is answered ABORTED with `Failed to resolve table ...
// status: AccessDenied`, and a member of DATA-READERS reads them. A user who
// may not list the database fails the read before it gets here.
var ErrPrincipalsRefused = errors.New("the server refused to report its users and groups")

// readPrincipals reads the three views through run, which executes one
// read-only query and returns its result set. The query for users names its
// columns, so the password hash .sys/auth_users also holds never leaves the
// server.
func readPrincipals(
	ctx context.Context,
	database string,
	run func(context.Context, string) (*Ydb.ResultSet, error),
) (Principals, error) {
	principals := Principals{Database: "/" + strings.Trim(database, "/")}
	users, err := run(ctx, "SELECT Sid, IsEnabled FROM "+systemView(database, "auth_users"))
	if err != nil {
		return Principals{}, err
	}
	for _, row := range users.GetRows() {
		principals.Users = append(principals.Users, User{
			Name:    textOf(row, users, "Sid"),
			Enabled: boolOf(row, users, "IsEnabled"),
		})
	}
	groups, err := run(ctx, "SELECT Sid FROM "+systemView(database, "auth_groups"))
	if err != nil {
		return Principals{}, err
	}
	for _, row := range groups.GetRows() {
		principals.Groups = append(principals.Groups, textOf(row, groups, "Sid"))
	}
	members, err := run(ctx, "SELECT GroupSid, MemberSid FROM "+systemView(database, "auth_group_members"))
	if err != nil {
		return Principals{}, err
	}
	for _, row := range members.GetRows() {
		principals.Memberships = append(principals.Memberships, Membership{
			Group:  textOf(row, members, "GroupSid"),
			Member: textOf(row, members, "MemberSid"),
		})
	}
	return principals, nil
}

// cell is the value of the named column of row, or nil when the result set
// has no such column.
func cell(row *Ydb.Value, set *Ydb.ResultSet, column string) *Ydb.Value {
	for i, meta := range set.GetColumns() {
		if meta.GetName() == column && i < len(row.GetItems()) {
			return row.GetItems()[i]
		}
	}
	return nil
}

// textOf is a Utf8 column's value, and "" for NULL.
func textOf(row *Ydb.Value, set *Ydb.ResultSet, column string) string {
	return cell(row, set, column).GetTextValue()
}

// boolOf is a Bool column's value, and false for NULL.
func boolOf(row *Ydb.Value, set *Ydb.ResultSet, column string) bool {
	return cell(row, set, column).GetBoolValue()
}

// isPrincipalsRefusal reports the answer a read of .sys/auth_* gets from an
// account that may not read it; see [ErrPrincipalsRefused]. Any other ABORTED
// is not a refusal, and fails the read.
func isPrincipalsRefusal(status Ydb.StatusIds_StatusCode, issues string) bool {
	return status == Ydb.StatusIds_ABORTED && strings.Contains(issues, "AccessDenied")
}

// systemView is the quoted absolute path of a system view of database.
func systemView(database, view string) string {
	return sqlident.Quote(platform.YDB, strings.TrimRight(database, "/")+"/.sys/"+view)
}

// directoryAccess records the owner and the permission entries of the
// directory schema, relative to the database root: the database itself for
// the root, read whatever the scope because a grant on the database belongs
// to no directory, and a directory the read covers otherwise.
func (r *Reader) directoryAccess(schema string, self *Ydb_Scheme.Entry, db *catalog.Database) {
	if self == nil {
		return
	}
	if schema == "" {
		r.recordAccess(self, ydbacl.ObjectDatabase, "", "", db)
		return
	}
	if r.inScope(schema) {
		r.recordAccess(self, ydbacl.ObjectDirectory, "", schema, db)
	}
}

// tableAccess records the owner and the permission entries of the table name
// in the directory schema.
func (r *Reader) tableAccess(schema, name string, self *Ydb_Scheme.Entry, db *catalog.Database) {
	if self != nil {
		r.recordAccess(self, ydbacl.ObjectTable, schema, name, db)
	}
}

// ownerKinds names an object type as [catalog.ObjectOwner] spells it.
var ownerKinds = map[string]string{
	ydbacl.ObjectDatabase:  "database",
	ydbacl.ObjectDirectory: "schema",
	ydbacl.ObjectTable:     "table",
}

// recordAccess records one object's owner and one grant per permission each
// of its entries names. An entry can name several permissions -- one YDB
// created with a mask no single name covers is reported as the names it is
// made of -- and each is a grant of its own, as the GRANT that made it would
// be.
func (r *Reader) recordAccess(entry *Ydb_Scheme.Entry, objectType, schema, name string, db *catalog.Database) {
	if owner := entry.GetOwner(); owner != "" {
		db.ObjectOwners = append(db.ObjectOwners, catalog.ObjectOwner{
			Kind: ownerKinds[objectType], Schema: schema, Name: name, Owner: owner,
		})
	}
	seen := make(map[string]bool)
	for _, permission := range entry.GetPermissions() {
		for _, permissionName := range permission.GetPermissionNames() {
			key := permission.GetSubject() + "\x00" + permissionName
			if seen[key] {
				continue
			}
			seen[key] = true
			db.Grants = append(db.Grants, catalog.Grant{
				Role:       permission.GetSubject(),
				Privilege:  permissionName,
				ObjectType: objectType,
				Schema:     schema,
				ObjectName: name,
			})
		}
	}
}

// principals reads the users, groups and memberships into db, and settles
// whether each object's owner can log in.
//
// Some principals are recorded as existing and left out of the description,
// as a PostgreSQL read leaves out the roles the server reserves. The
// description is what a replay creates, and each of these would make it fail
// or create what nothing in the database made:
//
//   - a principal whose name YDB refuses in CREATE USER and CREATE GROUP,
//     which the cluster made rather than SQL: the groups a default cluster
//     creates, such as ADMINS and DATA-READERS, are spelled that way;
//   - the owner of the database, which holds the database before any
//     statement runs in it. Measured on 26.2.1.14 and 25.1.4.7, local-ydb's
//     database /local is owned by its user root, and a checkpoint that
//     described root failed its replay with `User already exists` at
//     `CREATE USER root`;
//   - every principal, when the read is of a dev realm. A realm is a directory
//     that stands in for a database, and the users and groups are those of the
//     database that holds it.
//
// Memberships and grants are described whoever they name, because a
// declaration can name such a principal: a user can be made a member of
// DATA-READERS.
//
// A database that refuses the read is recorded as such rather than failing it:
// an account that may describe the tables and may not read .sys sees the rest
// of the database, and the comparison then plans no user or group it cannot
// see. Any other error fails the read.
func (r *Reader) principals(ctx context.Context, source Source, db *catalog.Database) error {
	principals, err := source.Principals(ctx)
	if errors.Is(err, ErrPrincipalsRefused) {
		db.NotDescribed = db.NotDescribed.With(coverage.Refused(coverage.Role))
		return nil
	}
	if err != nil {
		return fmt.Errorf("read the YDB users and groups: %w", err)
	}
	realm := principals.Database != r.database
	owner := databaseOwner(db)
	// add describes role, or records it as existing beside the description.
	// Its password state stays unknown: YDB reports a password hash, which
	// the read never asks for.
	add := func(role catalog.Role) {
		if realm || role.Name == owner || ydbacl.CheckName(role.Name) != nil {
			db.RolesOutOfScope = append(db.RolesOutOfScope, role)
			return
		}
		db.Roles = append(db.Roles, role)
	}
	canLogin := make(map[string]bool, len(principals.Users))
	for _, user := range principals.Users {
		canLogin[user.Name] = user.Enabled
		add(catalog.Role{Name: user.Name, Login: user.Enabled, Inherit: true})
	}
	for _, group := range principals.Groups {
		add(catalog.Role{Name: group, Inherit: true, Group: true})
	}
	for _, membership := range principals.Memberships {
		db.RoleMemberships = append(db.RoleMemberships,
			catalog.RoleMembership{Role: membership.Group, Member: membership.Member})
	}
	slices.SortFunc(db.Roles, func(a, b catalog.Role) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(db.RolesOutOfScope, func(a, b catalog.Role) int { return strings.Compare(a.Name, b.Name) })
	slices.SortFunc(db.RoleMemberships, func(a, b catalog.RoleMembership) int {
		return strings.Compare(a.Role+"\x00"+a.Member, b.Role+"\x00"+b.Member)
	})
	for i := range db.ObjectOwners {
		db.ObjectOwners[i].OwnerCanLogin = canLogin[db.ObjectOwners[i].Owner]
	}
	return nil
}

// databaseOwner is the owner of the root the read walked, which
// [Reader.directoryAccess] records as the database's owner, or "" when the
// root reported none.
func databaseOwner(db *catalog.Database) string {
	for _, owner := range db.ObjectOwners {
		if owner.Kind == ownerKinds[ydbacl.ObjectDatabase] {
			return owner.Owner
		}
	}
	return ""
}
