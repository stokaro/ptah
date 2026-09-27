// Package pgdefaultacl reads pg_default_acl, the catalog of what ALTER DEFAULT
// PRIVILEGES established, and spells the statement that takes a default back.
//
// It is shared by the PostgreSQL reader and writer in
// internal/dbschema/postgres and by internal/schemaclean. The plan `ptah-compat
// schema clean` prints, the statements a narrowed plan runs and the cleanup
// `ptah db drop-all` runs are therefore one revoke read one way. Two readers of
// the relation drift: a copy that exploded the ACL with aclexplode revoked
// nothing on CockroachDB v25.4, and one that named role 0 wrote a role nobody
// has (stokaro/ptah#3832).
//
// The SQL fragments are functions of the alias a statement gives the relation,
// so a statement cannot spell one of them its own way.
//
// CockroachDB is read differently in two places. Its cleanup takes the revokes
// from SHOW DEFAULT PRIVILEGES through [ReadRevokesFromShow], and [Refused]
// and [Readable] recognize the one refusal it answers pg_default_acl with, so
// the reader can record default privileges as not inspected instead of
// failing (stokaro/ptah#3816).
package pgdefaultacl

import (
	"cmp"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/internal/aclitem"
	"ptah.run/internal/sqlident"
)

// UndescribedSQL is the predicate for a pg_default_acl row no declaration can
// name: no grantor, or no schema and an object class Ptah does not model.
//
// It is one function because the reads depend on it agreeing with itself. The
// described reads take its complement -- the schema-scoped read and
// [GlobalSQL] -- and the undescribed read takes it, so a row is either in the
// description or in the note, never both and never neither. Written as two
// predicates, the first shape added to one of them would silently drop out of
// both (stokaro/ptah#3770).
func UndescribedSQL(alias string) string {
	return "(" + alias + ".defaclrole = 0 OR (" + alias + ".defaclnamespace = 0 AND " +
		alias + ".defaclobjtype NOT IN ('r', 'S', 'f', 'T', 'n', 'L')))"
}

// GrantorSQL selects a row's grantor: the role's name, or the empty string for
// role 0.
//
// Role 0 is how CockroachDB records FOR ALL ROLES, measured on v25.4.16,
// v26.2.7 and v26.3.1, and pg_get_userbyid answers `unknown (OID=0)` for it. A
// statement built from that name is refused with `role/user "unknown (oid=0)"
// does not exist`, and `ptah db drop-all` stops halfway through a CockroachDB
// database, after its tables are gone (stokaro/ptah#3770). No role has an
// empty name, so the empty string cannot be mistaken for one. PostgreSQL and
// YugabyteDB never record role 0: both refuse FOR ALL ROLES as a syntax error.
func GrantorSQL(alias string) string {
	role := alias + ".defaclrole"
	return `CASE WHEN ` + role + ` = 0 THEN '' ELSE pg_get_userbyid(` + role + `) END`
}

// ObjectTypeSQL turns pg_default_acl's one-character object class into the
// keyword a statement writes.
//
// Every statement over the relation uses it, so a class is named the same way
// in the description, the note and the cleanup. SCHEMAS and LARGE OBJECTS are
// global only -- PostgreSQL 18.6 refuses IN SCHEMA for both, and LARGE OBJECTS
// is PostgreSQL 18's -- so only the global read meets them. A code this list
// does not know is kept as the catalog spells it, which is enough to report it
// and says nothing false.
func ObjectTypeSQL(alias string) string {
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

// ListSQL selects a row's ACL as the JSON array array_to_json renders, for
// [aclitem.ParseJSON] to read.
//
// No statement explodes the ACL with aclexplode, because not every line
// answers it. CockroachDB v25.4.16 stores defaclacl as text[], and aclexplode
// over it answers no rows: a read built on it finds no default privilege, and a
// cleanup built on it revokes none (stokaro/ptah#3802). array_to_json answers
// on PostgreSQL 18.6, YugabyteDB 2026.1.2 and CockroachDB v25.4.16, v26.2.7 and
// v26.3.1, and each element has the one shape [aclitem.Parse] reads. A NULL
// list reads as an empty one.
func ListSQL(alias string) string {
	return "COALESCE(array_to_json(" + alias + ".defaclacl)::text, '[]')"
}

// ScopedClassesSQL is the object-class filter every read of schema-scoped
// rows applies: the four classes IN SCHEMA can name. SCHEMAS and LARGE OBJECTS
// are global only.
func ScopedClassesSQL(alias string) string {
	return alias + ".defaclobjtype IN ('r', 'S', 'f', 'T')"
}

// Public is the grantee name a description gives the empty grantee of an ACL
// item, as aclexplode names grantee 0.
const Public = "PUBLIC"

// GranteeName is an item's grantee as a description names it: the role, or
// [Public].
func GranteeName(item aclitem.Item) string {
	if item.Grantee == "" {
		return Public
	}
	return item.Grantee
}

// Querier is the part of a database handle the revoke read uses. A
// transaction, a pool and a dbschema.DatabaseConnection all satisfy it.
type Querier interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
}

// Revoke takes back what one grantee receives by default in one schema, for one
// grantor and one object class.
//
// A global default is not taken back to nothing but to the built-in default,
// so [ReadGlobalResets] writes two for a grantee the built-in default gives
// something: the revoke, and a grant of that back, in that order.
type Revoke struct {
	// Schema is the schema the default applies in, or the empty string for a
	// global default, which applies in every schema.
	Schema string
	// Grantor is the role whose new objects the default applies to, or the
	// empty string for CockroachDB's FOR ALL ROLES.
	Grantor string
	// Class is pg_default_acl's one-character object class: r, S, f or T, and
	// for a global default n or L too.
	Class string
	// Grantee is the role receiving the default, or the empty string for
	// PUBLIC.
	Grantee string
	// Statement is the ALTER DEFAULT PRIVILEGES ... REVOKE ALL that takes it
	// back, or for a global default the GRANT that restores the built-in
	// default, with every identifier quoted.
	Statement string
}

// Name labels the revoke for a message or a plan: [Revoke.GrantorName], the
// class and [Revoke.GranteeName], separated by slashes.
func (r Revoke) Name() string {
	return r.GrantorName() + "/" + r.Class + "/" + r.GranteeName()
}

// GrantorName is the grantor as a message names it: the role, or `all roles`
// for role 0, as the note on `db read` names it.
func (r Revoke) GrantorName() string {
	return grantorLabel(r.Grantor)
}

// GranteeName is the grantee as a description names it: the role, or
// [Public].
func (r Revoke) GranteeName() string {
	return GranteeName(aclitem.Item{Grantee: r.Grantee})
}

// grantorLabel is a grantor as a message names it: the role, or `all roles`
// for role 0.
func grantorLabel(grantor string) string {
	if grantor == "" {
		return "all roles"
	}
	return grantor
}

// ReadRevokes returns one revoke per grantee of each default privilege set in
// schemas, ordered by schema, name and statement, byte by byte.
//
// Only schema-scoped rows of the four classes IN SCHEMA can name are read; a
// default set without IN SCHEMA applies in every schema, so a cleanup scoped to
// some of them does not own it. A row whose ACL does not parse is an error
// rather than a skip: a skipped grantee is a default the cleanup leaves behind
// while reporting a clean schema.
//
// The caller decides whether the server has pg_default_acl at all. A missing
// relation is a parse failure, so a server without one must not be asked.
func ReadRevokes(ctx context.Context, q Querier, schemas []string) ([]Revoke, error) {
	if len(schemas) == 0 {
		return nil, nil
	}
	query := `
		SELECT
			n.nspname,
			` + GrantorSQL("d") + ` AS grantor,
			d.defaclobjtype,
			` + ObjectTypeSQL("d") + ` AS object_type,
			` + ListSQL("d") + ` AS acl
		FROM pg_default_acl d
		JOIN pg_namespace n ON n.oid = d.defaclnamespace
		WHERE ` + schemaPredicate(len(schemas)) + `
		AND ` + ScopedClassesSQL("d")
	args := make([]any, len(schemas))
	for i, schema := range schemas {
		args[i] = schema
	}
	rows, err := q.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to query default privileges: %w", err)
	}
	defer rows.Close()

	var revokes []Revoke
	for rows.Next() {
		var schema, grantor, class, objectType, acl string
		if err := rows.Scan(&schema, &grantor, &class, &objectType, &acl); err != nil {
			return nil, fmt.Errorf("failed to scan default privileges: %w", err)
		}
		items, err := aclitem.ParseJSON(acl)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to read the default privileges %s set on %s in schema %s: %w",
				grantorLabel(grantor), objectType, schema, err,
			)
		}
		for _, item := range items {
			revokes = append(revokes, Revoke{
				Schema:    schema,
				Grantor:   grantor,
				Class:     class,
				Grantee:   item.Grantee,
				Statement: revokeStatement(schema, grantor, objectType, item.Grantee),
			})
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate default privileges: %w", err)
	}
	sortRevokes(revokes)
	return revokes, nil
}

// Refused reports whether err is CockroachDB refusing to produce a row of
// pg_default_acl.
//
// CockroachDB v26.2.7 writes a role name that needs quoting, such as one with a
// dash, into the aclitem text unquoted, then refuses to read the text back:
// `missing "=" sign: "r-dash=U*/"` at SQLSTATE 22P02 (stokaro/ptah#3816). It
// does so for whichever row names such a role, so every read of the relation
// that reaches that row fails, whatever its columns and its filter; a read that
// stops before the row succeeds. v25.4.16 writes the name unquoted into a
// text[] it never parses, and v26.3.1 quotes it, so neither refuses.
func Refused(err error) bool {
	var state interface{ SQLState() string }
	return errors.As(err, &state) && state.SQLState() == invalidTextRepresentation &&
		strings.Contains(err.Error(), `missing "=" sign`)
}

// invalidTextRepresentation is SQLSTATE 22P02.
const invalidTextRepresentation = "22P02"

// Readable reports whether the server produces every row of pg_default_acl.
// It reads the whole relation, because the refusal [Refused] recognizes comes
// with the row that names the role, not with the statement: measured on
// CockroachDB v26.2.7, `SELECT 1 FROM pg_default_acl LIMIT 1` answers and the
// same statement without the limit fails after its first row. Any other error
// is returned.
func Readable(ctx context.Context, q Querier) (bool, error) {
	rows, err := q.QueryContext(ctx, "SELECT 1 FROM pg_default_acl")
	if err != nil {
		return refusedOr(err)
	}
	defer rows.Close()
	for rows.Next() {
		continue
	}
	if err := rows.Err(); err != nil {
		return refusedOr(err)
	}
	return true, nil
}

// refusedOr answers Readable for a failed read: false for the refusal, and the
// error for anything else.
func refusedOr(err error) (bool, error) {
	if Refused(err) {
		return false, nil
	}
	return false, fmt.Errorf("failed to read pg_default_acl: %w", err)
}

// showClasses maps the object_type SHOW DEFAULT PRIVILEGES writes to the
// pg_default_acl class code and the keyword a statement writes. SCHEMAS is
// missing because IN SCHEMA cannot name it.
var showClasses = map[string]struct{ class, keyword string }{
	"tables":    {class: "r", keyword: "TABLES"},
	"sequences": {class: "S", keyword: "SEQUENCES"},
	"routines":  {class: "f", keyword: "FUNCTIONS"},
	"types":     {class: "T", keyword: "TYPES"},
}

// ReadRevokesFromShow is [ReadRevokes] for CockroachDB, read through
// CockroachDB's SHOW DEFAULT PRIVILEGES rather than pg_default_acl.
//
// A revoke needs only who holds a default, never which privileges, and SHOW
// names each grantee as it is, on every declared line: v25.4.16, v26.2.7 and
// v26.3.1. pg_default_acl cannot say as much. On v26.2.7 it refuses every read
// once a default names a role that needs quoting (see [Refused]), so a cleanup
// that read it could revoke nothing; and it leaves out a grantee that holds
// only a privilege PostgreSQL does not have, such as DROP, whose default a
// cleanup from it would leave in place.
//
// SHOW takes one schema per statement and a list of roles, and names the FOR
// ALL ROLES defaults only when asked for them, so each schema costs two
// statements: one for every role pg_roles lists, and one for FOR ALL ROLES. A
// role or a schema that holds nothing answers no rows. An object type this
// function does not know is an error rather than a skip, for the reason
// [ReadRevokes] gives.
func ReadRevokesFromShow(ctx context.Context, q Querier, schemas []string) ([]Revoke, error) {
	if len(schemas) == 0 {
		return nil, nil
	}
	roles, err := readRoleNames(ctx, q)
	if err != nil {
		return nil, err
	}
	seen := make(map[Revoke]bool)
	var revokes []Revoke
	for _, schema := range schemas {
		statements := []string{"SHOW DEFAULT PRIVILEGES FOR ALL ROLES IN SCHEMA " + quote(schema)}
		if len(roles) > 0 {
			statements = append(statements,
				"SHOW DEFAULT PRIVILEGES FOR ROLE "+strings.Join(roles, ", ")+" IN SCHEMA "+quote(schema))
		}
		for _, statement := range statements {
			found, err := readShownRevokes(ctx, q, statement, schema)
			if err != nil {
				return nil, err
			}
			for _, revoke := range found {
				if !seen[revoke] {
					seen[revoke] = true
					revokes = append(revokes, revoke)
				}
			}
		}
	}
	sortRevokes(revokes)
	return revokes, nil
}

// readRoleNames lists every role, quoted for a FOR ROLE list.
func readRoleNames(ctx context.Context, q Querier) ([]string, error) {
	rows, err := q.QueryContext(ctx, "SELECT rolname FROM pg_roles ORDER BY rolname")
	if err != nil {
		return nil, fmt.Errorf("failed to query roles for default privileges: %w", err)
	}
	defer rows.Close()
	var roles []string
	for rows.Next() {
		var role string
		if err := rows.Scan(&role); err != nil {
			return nil, fmt.Errorf("failed to scan roles for default privileges: %w", err)
		}
		roles = append(roles, quote(role))
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate roles for default privileges: %w", err)
	}
	return roles, nil
}

// readShownRevokes runs one SHOW DEFAULT PRIVILEGES statement and returns one
// revoke per grantor, class and grantee it names, privilege rows folded. A FOR
// ALL ROLES row carries a NULL role, measured on v25.4.16, v26.2.7 and v26.3.1,
// which reads as the empty grantor [Revoke] gives role 0; SHOW spells PUBLIC
// `public`.
func readShownRevokes(ctx context.Context, q Querier, statement, schema string) ([]Revoke, error) {
	rows, err := q.QueryContext(ctx, statement)
	if err != nil {
		return nil, fmt.Errorf("failed to query default privileges in schema %s: %w", schema, err)
	}
	defer rows.Close()
	var revokes []Revoke
	for rows.Next() {
		var role sql.NullString
		var forAllRoles, grantable bool
		var objectType, grantee, privilege string
		// Scanned for the column count; a revoke needs only who holds a
		// default, so the privilege and its grant option go unused.
		if err := rows.Scan(&role, &forAllRoles, &objectType, &grantee, &privilege, &grantable); err != nil {
			return nil, fmt.Errorf("failed to scan default privileges in schema %s: %w", schema, err)
		}
		class, known := showClasses[objectType]
		if !known {
			return nil, fmt.Errorf(
				"failed to read the default privileges in schema %s: object type %q is not one IN SCHEMA can name",
				schema, objectType,
			)
		}
		grantor := role.String
		if grantee == "public" {
			grantee = ""
		}
		revokes = append(revokes, Revoke{
			Schema:    schema,
			Grantor:   grantor,
			Class:     class.class,
			Grantee:   grantee,
			Statement: revokeStatement(schema, grantor, class.keyword, grantee),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate default privileges in schema %s: %w", schema, err)
	}
	return revokes, nil
}

// sortRevokes orders revokes by schema, name and statement, byte by byte.
func sortRevokes(revokes []Revoke) {
	slices.SortFunc(revokes, func(a, b Revoke) int {
		return cmp.Or(
			strings.Compare(a.Schema, b.Schema),
			strings.Compare(a.Name(), b.Name()),
			strings.Compare(a.Statement, b.Statement),
		)
	})
}

// revokeStatement spells ALTER DEFAULT PRIVILEGES ... REVOKE ALL for one
// grantee. The grantor clause is FOR ALL ROLES for role 0, and the grantee is
// the keyword PUBLIC for the empty grantee; a role named "PUBLIC" stays quoted,
// so the statement cannot mistake it for the keyword.
func revokeStatement(schema, grantor, objectType, grantee string) string {
	grantorClause := "FOR ALL ROLES"
	if grantor != "" {
		grantorClause = "FOR ROLE " + quote(grantor)
	}
	granteeClause := Public
	if grantee != "" {
		granteeClause = quote(grantee)
	}
	return "ALTER DEFAULT PRIVILEGES " + grantorClause +
		" IN SCHEMA " + quote(schema) +
		" REVOKE ALL PRIVILEGES ON " + objectType +
		" FROM " + granteeClause
}

// schemaPredicate matches the schemas by name, one schema with = and several
// with IN, as the writer's cleanup query matches its managed namespaces.
func schemaPredicate(count int) string {
	if count == 1 {
		return "n.nspname = $1"
	}
	placeholders := make([]string, count)
	for i := range count {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}
	return "n.nspname IN (" + strings.Join(placeholders, ", ") + ")"
}

func quote(name string) string {
	return sqlident.Quote(platform.Postgres, name)
}
