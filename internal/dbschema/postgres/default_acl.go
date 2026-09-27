package postgres

import "ptah.run/internal/aclitem"

// This file holds what every statement over pg_default_acl shares: the SQL
// fragments the reader's reads and the writer's cleanup select with, and the Go
// that turns what they select into grantees and statements. Each fragment is a
// function of the alias the caller gives the relation, so a statement cannot
// spell one of them its own way.

// undescribedDefaultACL is the predicate for a pg_default_acl row no
// declaration can name: no schema, or no grantor.
//
// It is one function because two reads depend on it agreeing with itself. The
// described read takes its complement and the undescribed read takes it, so a
// row is either in the description or in the note, never both and never
// neither. Written as two predicates, the first shape added to one of them
// would silently drop out of both (stokaro/ptah#3770).
func undescribedDefaultACL(alias string) string {
	return "(" + alias + ".defaclnamespace = 0 OR " + alias + ".defaclrole = 0)"
}

// defaultACLGrantor selects a row's grantor: the role's name, or the empty
// string for role 0.
//
// Role 0 is how CockroachDB records FOR ALL ROLES, measured on v25.4.16,
// v26.2.7 and v26.3.1, and pg_get_userbyid answers `unknown (OID=0)` for it. A
// statement built from that name is refused with `role/user "unknown (oid=0)"
// does not exist`, and `ptah db drop-all` stops halfway through a CockroachDB
// database, after its tables are gone (stokaro/ptah#3770). No role has an
// empty name, so the empty string cannot be mistaken for one. PostgreSQL and
// YugabyteDB never record role 0: both refuse FOR ALL ROLES as a syntax error.
func defaultACLGrantor(alias string) string {
	role := alias + ".defaclrole"
	return `CASE WHEN ` + role + ` = 0 THEN '' ELSE pg_get_userbyid(` + role + `) END`
}

// defaultACLObjectType turns pg_default_acl's one-character object class into
// the keyword a statement writes.
//
// Every statement over the relation uses it, so a class is named the same way
// in the description, the note and the cleanup. SCHEMAS and LARGE OBJECTS are
// global only -- PostgreSQL 18.6 refuses IN SCHEMA for both -- so only the
// undescribed read meets them. A code this list does not know is kept as the
// catalog spells it, which is enough to report it and says nothing false.
func defaultACLObjectType(alias string) string {
	column := alias + ".defaclobjtype"
	return `CASE ` + column + `
					WHEN 'r' THEN 'TABLES'
					WHEN 'S' THEN 'SEQUENCES'
					WHEN 'f' THEN 'FUNCTIONS'
					WHEN 'T' THEN 'TYPES'
					WHEN 'n' THEN 'SCHEMAS'
					WHEN 'L' THEN 'LARGE OBJECTS'
					ELSE ` + column + `::text
				END`
}

// defaultACLList selects a row's ACL as the JSON array array_to_json renders,
// for [aclitem.ParseJSON] to read.
//
// No statement here explodes the ACL with aclexplode, because not every line
// answers it. CockroachDB v25.4.16 stores defaclacl as text[], and aclexplode
// over it answers no rows: a read built on it finds no default privilege, and a
// cleanup built on it revokes none (stokaro/ptah#3802). array_to_json answers
// on PostgreSQL 18.6, YugabyteDB 2026.1.2 and CockroachDB v25.4.16, v26.2.7 and
// v26.3.1, and each element has the one shape [aclitem.Parse] reads. A NULL
// list reads as an empty one.
func defaultACLList(alias string) string {
	return "COALESCE(array_to_json(" + alias + ".defaclacl)::text, '[]')"
}

// defaultACLPublic is the grantee name a description gives the empty grantee
// of an ACL item, as aclexplode names grantee 0.
const defaultACLPublic = "PUBLIC"

// defaultACLGranteeName is an item's grantee as a description names it: the
// role, or PUBLIC.
func defaultACLGranteeName(item aclitem.Item) string {
	if item.Grantee == "" {
		return defaultACLPublic
	}
	return item.Grantee
}

// defaultACLGranteeClause is an item's grantee as a statement writes it: the
// quoted role, or the keyword PUBLIC. A role named "PUBLIC" stays quoted, so
// the statement cannot mistake it for the keyword.
func defaultACLGranteeClause(item aclitem.Item) string {
	if item.Grantee == "" {
		return defaultACLPublic
	}
	return quoteIdent(item.Grantee)
}

// defaultACLGrantorClause is the clause of ALTER DEFAULT PRIVILEGES that names
// the grantor [defaultACLGrantor] selected: FOR ROLE and the role, or FOR ALL
// ROLES for role 0.
func defaultACLGrantorClause(grantor string) string {
	if grantor == "" {
		return "FOR ALL ROLES"
	}
	return "FOR ROLE " + quoteIdent(grantor)
}

// defaultACLGrantorName is the grantor [defaultACLGrantor] selected as a label,
// for a name a message shows: the role, or `all roles` for role 0.
func defaultACLGrantorName(grantor string) string {
	if grantor == "" {
		return "all roles"
	}
	return grantor
}
