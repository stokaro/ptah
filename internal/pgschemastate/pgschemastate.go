// Package pgschemastate plans the statements that give a PostgreSQL-family
// schema the owner, access list and comment a [dbreset.SchemaState] records:
// [Create] for a schema a dev database reset creates again, and [Return] for
// one it empties where it stands.
//
// A reset of a dev database keeps the schema a URL resolves to, and "public",
// and drops the rest. Each kept schema is the dev database's environment, so
// the reset gives it back what it held when the run claimed the database, not
// what the run left on it. A run can change a schema's privileges and comment
// as surely as its tables: a schema document that revokes USAGE on public from
// PUBLIC does, when a diff creates it on the dev database. Recreated with what
// that run left, measured on PostgreSQL 18, public held no USAGE for PUBLIC
// when the next diff created a document there, and that diff planned
// `GRANT USAGE ON SCHEMA "public" TO PUBLIC` for a pair the dev database had
// reported synced before.
package pgschemastate

import (
	"cmp"
	"slices"

	"ptah.run/core/platform"
	"ptah.run/internal/dbreset"
	"ptah.run/internal/sqlident"
)

// publicGrantee is the grantee that stands for every role. It is a keyword in
// GRANT and REVOKE, and is never quoted.
const publicGrantee = "PUBLIC"

// ownerPrivileges are the privileges the owner of a schema holds when the
// schema has no access list of its own.
var ownerPrivileges = []string{"CREATE", "USAGE"}

// Create returns the statements that create schema state.Name with the owner,
// access list and comment state records.
func Create(state dbreset.SchemaState) []string {
	statements := []string{"CREATE SCHEMA " + quote(state.Name) + " AUTHORIZATION " + quote(state.Owner)}
	if !state.DefaultAccess {
		created := dbreset.SchemaState{Name: state.Name, Owner: state.Owner, DefaultAccess: true}
		statements = append(statements, accessStatements(state, created)...)
	}
	return append(statements, commentStatement(state))
}

// Return returns the statements that give schema current.Name, as current
// describes it, the owner, access list and comment claim records, and none when
// the two agree. A grant current holds that claim does not is revoked, and a
// schema whose access list was its default compares as its owner holding every
// privilege, so a schema that differs from its claim only in how that is
// spelled is left alone.
func Return(claim, current dbreset.SchemaState) []string {
	var statements []string
	if current.Owner != claim.Owner {
		// ALTER SCHEMA ... OWNER TO moves the old owner's privileges to the new
		// one, so current is read as the new owner holding them.
		statements = append(statements, "ALTER SCHEMA "+quote(claim.Name)+" OWNER TO "+quote(claim.Owner))
		current = withOwner(current, claim.Owner)
	}
	if !sameAccess(current, claim) {
		statements = append(statements, accessStatements(claim, current)...)
	}
	if current.Comment != claim.Comment {
		statements = append(statements, commentStatement(claim))
	}
	return statements
}

// accessStatements revokes every privilege current grants, from each grantee
// either state names, PUBLIC and the owner included, and grants the privileges
// want records.
func accessStatements(want, current dbreset.SchemaState) []string {
	grantees := []string{publicGrantee, want.Owner}
	for _, privilege := range slices.Concat(effective(current), effective(want)) {
		if !slices.Contains(grantees, privilege.Grantee) {
			grantees = append(grantees, privilege.Grantee)
		}
	}
	statements := make([]string, 0, len(grantees)+len(want.Privileges))
	for _, grantee := range grantees {
		statements = append(statements, "REVOKE ALL PRIVILEGES ON SCHEMA "+quote(want.Name)+" FROM "+role(grantee))
	}
	for _, privilege := range effective(want) {
		statement := "GRANT " + privilege.Privilege + " ON SCHEMA " + quote(want.Name) + " TO " + role(privilege.Grantee)
		if privilege.GrantOption {
			statement += " WITH GRANT OPTION"
		}
		statements = append(statements, statement)
	}
	return statements
}

// effective is the access list state grants: its own, in the order it records
// it, or its owner holding every privilege when it holds the default one.
func effective(state dbreset.SchemaState) []dbreset.SchemaPrivilege {
	if !state.DefaultAccess {
		return slices.Clone(state.Privileges)
	}
	privileges := make([]dbreset.SchemaPrivilege, 0, len(ownerPrivileges))
	for _, privilege := range ownerPrivileges {
		privileges = append(privileges, dbreset.SchemaPrivilege{Grantee: state.Owner, Privilege: privilege})
	}
	return privileges
}

// sameAccess reports whether a and b grant the same privileges, in whatever
// order they record them.
func sameAccess(a, b dbreset.SchemaState) bool {
	return slices.Equal(sortedAccess(a), sortedAccess(b))
}

func sortedAccess(state dbreset.SchemaState) []dbreset.SchemaPrivilege {
	privileges := effective(state)
	slices.SortFunc(privileges, func(a, b dbreset.SchemaPrivilege) int {
		return cmp.Or(cmp.Compare(a.Grantee, b.Grantee), cmp.Compare(a.Privilege, b.Privilege),
			compareBool(a.GrantOption, b.GrantOption))
	})
	return privileges
}

// withOwner is state after ALTER SCHEMA ... OWNER TO owner, which moves the
// old owner's privileges to owner.
func withOwner(state dbreset.SchemaState, owner string) dbreset.SchemaState {
	privileges := slices.Clone(state.Privileges)
	for position := range privileges {
		if privileges[position].Grantee == state.Owner {
			privileges[position].Grantee = owner
		}
	}
	state.Owner, state.Privileges = owner, privileges
	return state
}

// commentStatement sets the comment state records, removing the schema's
// comment when it records none.
func commentStatement(state dbreset.SchemaState) string {
	comment := "NULL"
	if state.Comment != "" {
		comment = sqlident.StringLiteral(state.Comment)
	}
	return "COMMENT ON SCHEMA " + quote(state.Name) + " IS " + comment
}

// quote quotes an identifier as the PostgreSQL family does.
func quote(name string) string {
	return sqlident.Quote(platform.Postgres, name)
}

// role spells a grantee in GRANT and REVOKE: PUBLIC as the keyword, and any
// other role quoted.
func role(grantee string) string {
	if grantee == publicGrantee {
		return publicGrantee
	}
	return quote(grantee)
}

func compareBool(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}
