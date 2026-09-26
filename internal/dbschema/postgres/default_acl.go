package postgres

// This file holds the SQL fragments every statement over pg_default_acl shares:
// the reader's two reads and the writer's cleanup. Each is a function of the
// alias the caller gives the relation, so a statement cannot spell one of them
// its own way.

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

// defaultACLGrantorClause is the clause of ALTER DEFAULT PRIVILEGES that names
// a row's grantor: FOR ROLE and the role, or FOR ALL ROLES for role 0.
//
// Role 0 is how CockroachDB records FOR ALL ROLES, measured on v25.4.16,
// v26.2.7 and v26.3.1, and pg_get_userbyid answers `unknown (OID=0)` for it.
// Built from that name, the statement is refused with `role/user
// "unknown (oid=0)" does not exist`, and `ptah db drop-all` stops halfway
// through a CockroachDB database, after its tables are gone
// (stokaro/ptah#3770). PostgreSQL and YugabyteDB never record role 0: both
// refuse FOR ALL ROLES as a syntax error.
func defaultACLGrantorClause(alias string) string {
	role := alias + ".defaclrole"
	return `CASE WHEN ` + role + ` = 0 THEN 'FOR ALL ROLES'
					ELSE format('FOR ROLE %I', pg_get_userbyid(` + role + `))
				END`
}

// defaultACLGrantorName is the grantor as a label, for a name a message or a
// selector shows: the role, or `all roles` for role 0. See
// [defaultACLGrantorClause] for why role 0 needs a spelling of its own.
func defaultACLGrantorName(alias string) string {
	role := alias + ".defaclrole"
	return `CASE WHEN ` + role + ` = 0 THEN 'all roles' ELSE pg_get_userbyid(` + role + `) END`
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
