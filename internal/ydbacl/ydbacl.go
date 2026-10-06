// Package ydbacl holds the answers about YDB's access model that the
// declaration gate, the renderer, the reader, the schema comparison and the
// planner must give alike: which YDB permission a declared privilege names,
// which names YDB takes for a user or a group, how a GRANT names the object it
// is about, and how a declared password is written.
//
// YDB's access model is not PostgreSQL's with other keywords. Every fact below
// was measured on local-ydb 25.1.4.7 and 26.2.1.14, and the statements this
// package spells were measured on 25.2.1.24, 25.3.1.25, 25.4.1.15 and
// 26.1.1.22 too:
//
//   - A principal is a user or a group, made by CREATE USER or CREATE GROUP,
//     and the two share one namespace (`Account already exists`). A group
//     never logs in and is the only principal with members; a member can be a
//     user or another group.
//   - A permission is an entry in an object's access list: a subject and one
//     permission name, such as ydb.granular.select_row. GRANT adds one entry
//     per permission it names, and REVOKE removes the entry with exactly that
//     name and no other: revoking ydb.generic.read leaves ydb.granular.select_row
//     in place, and the reverse. The keyword spellings GRANT takes (SELECT ROW,
//     SELECT, ALL) are other names for one permission each; see [Permission].
//   - WITH GRANT OPTION is not a flag on an entry. It adds a second entry,
//     ydb.access.grant, and REVOKE GRANT OPTION FOR SELECT removes both the
//     grant entry and the SELECT one.
//   - A permission on a directory is inherited by everything under it, which
//     is why YDB has no default privileges.
package ydbacl

import (
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/ydbtype"
)

// Permission names YDB takes in GRANT and reports in an object's access list,
// with the keyword spelling GRANT takes for each. Measured on 25.1.4.7 and
// 26.2.1.14 alike: each keyword stores exactly the permission beside it, and
// `GRANT 'name' ...` stores the name as written. A name is case-sensitive:
// `'YDB.GENERIC.LIST'` answers `Unknown permission name`.
var permissionKeywords = []struct{ name, keyword string }{
	{"ydb.database.connect", "CONNECT"},
	{"ydb.database.create", "CREATE"},
	{"ydb.database.drop", "DROP"},
	{"ydb.granular.select_row", "SELECT ROW"},
	{"ydb.granular.update_row", "UPDATE ROW"},
	{"ydb.granular.erase_row", "ERASE ROW"},
	{"ydb.granular.read_attributes", "SELECT ATTRIBUTES"},
	{"ydb.granular.write_attributes", "MODIFY ATTRIBUTES"},
	{"ydb.granular.create_directory", "CREATE DIRECTORY"},
	{"ydb.granular.create_table", "CREATE TABLE"},
	{"ydb.granular.create_queue", "CREATE QUEUE"},
	{"ydb.granular.remove_schema", "REMOVE SCHEMA"},
	{"ydb.granular.describe_schema", "DESCRIBE SCHEMA"},
	{"ydb.granular.alter_schema", "ALTER SCHEMA"},
	{"ydb.access.grant", "GRANT"},
	{"ydb.generic.read", "SELECT"},
	{"ydb.generic.write", "INSERT"},
	{"ydb.generic.list", "LIST"},
	{"ydb.generic.use_legacy", "USE LEGACY"},
	{"ydb.generic.use", "USE"},
	{"ydb.generic.manage", "MANAGE"},
	{"ydb.generic.full_legacy", "FULL LEGACY"},
	{"ydb.generic.full", "FULL"},
	{"ydb.tables.modify", "MODIFY TABLES"},
	{"ydb.tables.read", "SELECT TABLES"},
}

// GrantPermission is the permission WITH GRANT OPTION adds beside the one it
// is written with.
const GrantPermission = "ydb.access.grant"

// Permissions returns every permission name Ptah declares on YDB, sorted.
func Permissions() []string {
	names := make([]string, 0, len(permissionKeywords))
	for _, permission := range permissionKeywords {
		names = append(names, permission.name)
	}
	slices.Sort(names)
	return names
}

// Permission returns the YDB permission a declared privilege names, and false
// for one that names none.
//
// A privilege is a permission name, in any case, or a keyword GRANT takes,
// with any spacing: `ydb.granular.select_row`, `YDB.GRANULAR.SELECT_ROW` and
// `select row` all name ydb.granular.select_row. ALL and ALL PRIVILEGES name
// ydb.generic.full, which is what GRANT ALL stores. A declaration's
// privileges are upper-cased before they get here, and YDB's names are all
// lower case, so the name is matched without regard to case.
func Permission(privilege string) (string, bool) {
	normalized := strings.Join(strings.Fields(strings.ToUpper(privilege)), " ")
	switch normalized {
	case "":
		return "", false
	case "ALL", "ALL PRIVILEGES":
		return "ydb.generic.full", true
	}
	for _, permission := range permissionKeywords {
		if normalized == permission.keyword || normalized == strings.ToUpper(permission.name) {
			return permission.name, true
		}
	}
	return "", false
}

// LiteralPermission reads a quoted YQL permission name. Full permission names
// are case-sensitive; short aliases are case-insensitive and use underscores
// where keyword spellings use spaces. ALL is a keyword, not a short alias.
// This shares the keyword table because every alias names the same entry.
func LiteralPermission(value string) (string, bool) {
	for _, permission := range permissionKeywords {
		if value == permission.name || strings.EqualFold(value, strings.ReplaceAll(permission.keyword, " ", "_")) {
			return permission.name, true
		}
	}
	return "", false
}

// errName is the reason a user or group name is refused.
var errName = errors.New("YDB takes a user or group name of lower-case ASCII letters and digits only")

// CheckName returns nil when YDB takes name for a user or a group, and an
// error saying why it does not otherwise.
//
// Measured on 25.1.4.7 through 26.2.1.14: CREATE USER and CREATE GROUP answer
// `Name is not allowed` for a name holding `_`, `-`, `.`, `@` or an upper-case
// letter, and take one of lower-case letters and digits, a leading digit and
// 300 characters included. The groups a cluster creates for itself, such as
// ADMINS and DATA-READERS, have names this rule refuses, which is how a name
// tells a group SQL made from one the server made.
func CheckName(name string) error {
	if name == "" {
		return fmt.Errorf("%w, and the name is empty", errName)
	}
	for _, char := range name {
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') {
			return fmt.Errorf("%w: %q holds %q", errName, name, char)
		}
	}
	return nil
}

// IsPasswordHash reports whether a declared password is a password hash YDB
// takes with CREATE USER ... HASH rather than a password: a JSON object with
// the hash, its salt and its type, the form `.sys/auth_users` reports on 25.1.
//
// Measured on 25.1.4.7 and 26.2.1.14: both take that object with HASH. A
// password cannot be mistaken for it: YDB refuses a password holding a quote,
// a colon or a comma (`Password contains unacceptable characters`).
func IsPasswordHash(password string) bool {
	var hash struct {
		Hash *string `json:"hash"`
		Salt *string `json:"salt"`
		Type *string `json:"type"`
	}
	trimmed := strings.TrimSpace(password)
	if !strings.HasPrefix(trimmed, "{") {
		return false
	}
	if err := json.Unmarshal([]byte(trimmed), &hash); err != nil {
		return false
	}
	return hash.Hash != nil && hash.Salt != nil && hash.Type != nil
}

// PasswordClause is the clause CREATE USER and ALTER USER set a declared
// password with: HASH for a password hash, PASSWORD otherwise, the value
// written as a YQL string. YQL reads backslash escapes in a string, so the
// value is escaped the way every other YQL string Ptah writes is.
func PasswordClause(password string) string {
	if IsPasswordHash(password) {
		return "HASH " + ydbtype.StringLiteral(strings.TrimSpace(password))
	}
	return "PASSWORD " + ydbtype.StringLiteral(password)
}
