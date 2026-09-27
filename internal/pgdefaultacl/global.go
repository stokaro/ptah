package pgdefaultacl

import (
	"cmp"
	"context"
	"database/sql"
	"fmt"
	"slices"
	"strings"

	"ptah.run/internal/aclitem"
	"ptah.run/internal/pgprivilege"
)

// A global default privilege is what ALTER DEFAULT PRIVILEGES sets without IN
// SCHEMA: pg_default_acl records it with defaclnamespace 0, and it applies in
// every schema of the database. pg_default_acl is per database, so the form is
// database-wide rather than server-wide.
//
// Its ACL is a different kind of value from a schema-scoped row's. A
// schema-scoped row holds only what was added to the global defaults. A global
// row holds the whole list, the built-in default included, and the server
// removes the row once the list equals the built-in default again. Measured on
// PostgreSQL 18.6 and YugabyteDB 2026.1.2:
//
//	FOR ROLE o GRANT SELECT ON TABLES TO r             {o=arwdDxtm/o,r=r/o}
//	FOR ROLE o REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC {o=X/o}
//	FOR ROLE o GRANT EXECUTE ON FUNCTIONS TO PUBLIC     no row
//
// So what a global row declares is its difference from the built-in default,
// [Builtin]: a privilege the row holds beyond it is a grant, and a privilege
// of it the row lacks is a revoke. [GlobalDeltas] computes that difference.
//
// CockroachDB records the same statements in a shape that cannot be read that
// way. Its pg_default_acl leaves out the owner both when the owner holds every
// privilege and when it holds none, and leaves out PUBLIC both when PUBLIC
// holds its built-in privilege and when it was revoked, measured on v26.2.7 and
// v26.3.2. Its SHOW DEFAULT PRIVILEGES names each grantee as it is, so the
// CockroachDB read takes the content from there; see [ReadGlobalFromShow].

// GlobalSQL is the predicate for a global row a declaration can name: no
// schema, a grantor, and an object class [ObjectTypeSQL] knows.
//
// The rows it leaves out with no schema are the ones [UndescribedSQL] takes, so
// the global read and the undescribed read partition them.
func GlobalSQL(alias string) string {
	return "(" + alias + ".defaclnamespace = 0 AND NOT " + UndescribedSQL(alias) + ")"
}

// BuiltinListSQL selects the built-in default of a row's object class for its
// grantor, as the JSON array [ListSQL] selects for the row itself: what the row
// would hold if nobody had changed it.
//
// acldefault takes the object-class codes of its own, which match
// pg_default_acl's except for sequences: pg_default_acl writes S and acldefault
// reads s, and reads S as a foreign server, whose default is USAGE for the
// owner alone. Measured on PostgreSQL 18.6: acldefault('S', o) answers
// {o=U/o} and acldefault('s', o) answers {o=rwU/o}. CockroachDB answers
// acldefault with privileges PostgreSQL does not have, and its global rows are
// read through [ReadGlobalFromShow], so this is not asked of it.
func BuiltinListSQL(alias string) string {
	class := "CASE " + alias + ".defaclobjtype WHEN 'S' THEN 's' ELSE " + alias + ".defaclobjtype END"
	return "COALESCE(array_to_json(acldefault(" + class + ", " + alias + ".defaclrole))::text, '[]')"
}

// Builtin reports whether the built-in default gives grantee privilege on new
// objects of objectType that grantor creates: what a database holds for them
// before anybody runs ALTER DEFAULT PRIVILEGES.
//
// The owner holds every privilege of the object class, and ALL. PUBLIC holds
// EXECUTE on functions and USAGE on types. Nobody else holds anything. The
// comparison is exact for the names and case-insensitive for the keywords;
// grantee is compared with grantor as given, so a caller folds both first,
// and a caller holding an ACL's grantees passes [Public] for the empty one.
// This is the rule acldefault applies on PostgreSQL 18.6 and YugabyteDB
// 2026.1.2, and the one SHOW DEFAULT PRIVILEGES shows on CockroachDB, where
// the owner's privileges are ALL.
func Builtin(objectType, grantor, grantee, privilege string) bool {
	class := strings.ToUpper(strings.TrimSpace(objectType))
	name := strings.ToUpper(strings.TrimSpace(privilege))
	if grantee == grantor {
		return name == "ALL" || slices.Contains(pgprivilege.All(class), name)
	}
	if !strings.EqualFold(grantee, Public) {
		return false
	}
	switch class {
	case "FUNCTIONS", "ROUTINES":
		return name == "EXECUTE"
	case "TYPES":
		return name == "USAGE"
	default:
		return false
	}
}

// Delta is one difference between a global row and the built-in default of its
// object class for its grantor.
type Delta struct {
	// Grantee is the role the difference is about, or the empty string for
	// PUBLIC, as an ACL writes it. [GranteeName] names it for a description;
	// a statement keeps the two apart, since a role may be named "PUBLIC".
	Grantee string
	// Privilege is the privilege, as aclexplode names it, or ALL.
	Privilege string
	// WithOption reports the grant option of a granted privilege. A privilege
	// the built-in default holds plainly and the row holds with the grant
	// option is a grant WITH GRANT OPTION.
	WithOption bool
	// Revoked reports a privilege of the built-in default the row does not
	// hold. It is false for a grant.
	Revoked bool
}

// GlobalDeltas returns what row adds to builtin and what it takes away: one
// [Delta] per privilege, the grants and the revokes of each grantee together,
// ordered by grantee, then grants before revokes, then privilege. A grantee
// the reserved-name rule excludes is the caller's to leave out.
func GlobalDeltas(row, builtin []aclitem.Item) []Delta {
	held := heldPrivileges(row)
	base := heldPrivileges(builtin)
	var deltas []Delta
	for grantee, privileges := range held {
		for name, grantable := range privileges {
			builtinGrantable, inBuiltin := base[grantee][name]
			if inBuiltin && (builtinGrantable || !grantable) {
				continue
			}
			deltas = append(deltas, Delta{Grantee: grantee, Privilege: name, WithOption: grantable})
		}
	}
	for grantee, privileges := range base {
		for name := range privileges {
			if _, still := held[grantee][name]; !still {
				deltas = append(deltas, Delta{Grantee: grantee, Privilege: name, Revoked: true})
			}
		}
	}
	slices.SortFunc(deltas, compareDeltas)
	return deltas
}

// heldPrivileges indexes an ACL by grantee, as the ACL writes it, and
// privilege, recording the grant option. A privilege listed twice for one
// grantee keeps the grant option if either listing carries it.
func heldPrivileges(items []aclitem.Item) map[string]map[string]bool {
	held := make(map[string]map[string]bool, len(items))
	for _, item := range items {
		grantee := item.Grantee
		if held[grantee] == nil {
			held[grantee] = make(map[string]bool, len(item.Privileges))
		}
		for _, privilege := range item.Privileges {
			held[grantee][privilege.Name] = held[grantee][privilege.Name] || privilege.Grantable
		}
	}
	return held
}

func compareDeltas(a, b Delta) int {
	return cmp.Or(
		strings.Compare(a.Grantee, b.Grantee),
		compareBools(a.Revoked, b.Revoked),
		strings.Compare(a.Privilege, b.Privilege),
		compareBools(a.WithOption, b.WithOption),
	)
}

func compareBools(a, b bool) int {
	switch {
	case a == b:
		return 0
	case !a:
		return -1
	default:
		return 1
	}
}

// GlobalClass is one global default of one grantor and one object class, read
// from CockroachDB's SHOW DEFAULT PRIVILEGES: the differences from the
// built-in default, or none when the owner's own privileges cannot be told
// apart from it.
type GlobalClass struct {
	// Grantor is the role whose new objects the default applies to.
	Grantor string
	// ObjectType is the keyword a statement writes: TABLES, SEQUENCES,
	// FUNCTIONS, TYPES or SCHEMAS.
	ObjectType string
	// Deltas are the differences from the built-in default, as
	// [GlobalDeltas] orders them.
	Deltas []Delta
	// OwnerUndescribed reports an owner that holds some of the object class's
	// privileges and not all of them. SHOW names each, among them privileges
	// PostgreSQL does not have, and which of them ALL covers depends on the
	// CockroachDB line: v26.2.7 has no REFERENCES and no TRUNCATE on tables.
	// So what the owner lost cannot be named, and the owner's entry is left
	// out of Deltas for the caller to report.
	OwnerUndescribed bool
}

// showGlobalClasses maps the object_type SHOW DEFAULT PRIVILEGES writes for a
// default without IN SCHEMA to the keyword a statement writes.
var showGlobalClasses = map[string]string{
	"tables":    "TABLES",
	"sequences": "SEQUENCES",
	"routines":  "FUNCTIONS",
	"types":     "TYPES",
	"schemas":   "SCHEMAS",
}

// ReadGlobalFromShow reads the global defaults of every role through
// CockroachDB's SHOW DEFAULT PRIVILEGES, one [GlobalClass] per grantor and
// object class that differs from the built-in default, ordered by grantor and
// object type.
//
// SHOW without IN SCHEMA names the database-wide defaults, the built-in ones
// included: the owner's ALL, with the grant option, on every class, and
// PUBLIC's EXECUTE on routines and USAGE on types, measured on v26.2.7 and
// v26.3.2. The built-in rows are subtracted here as [GlobalDeltas] subtracts
// acldefault on PostgreSQL. SHOW spells PUBLIC `public`.
//
// Every role pg_roles lists is asked, for the reason [ReadRevokesFromShow]
// asks them all: pg_default_acl could name the grantors that differ, and
// v26.2.7 refuses every read of it once a default names a role that needs
// quoting (see [Refused]).
func ReadGlobalFromShow(ctx context.Context, q Querier) ([]GlobalClass, error) {
	roles, err := readRoleNames(ctx, q)
	if err != nil {
		return nil, err
	}
	if len(roles) == 0 {
		return nil, nil
	}
	rows, err := q.QueryContext(ctx, "SHOW DEFAULT PRIVILEGES FOR ROLE "+strings.Join(roles, ", "))
	if err != nil {
		return nil, fmt.Errorf("failed to query global default privileges: %w", err)
	}
	defer rows.Close()

	type classKey struct{ grantor, objectType string }
	shown := make(map[classKey][]aclitem.Item)
	var grantors []string
	for rows.Next() {
		var role sql.NullString
		var forAllRoles, grantable bool
		var objectType, grantee, privilege string
		if err := rows.Scan(&role, &forAllRoles, &objectType, &grantee, &privilege, &grantable); err != nil {
			return nil, fmt.Errorf("failed to scan global default privileges: %w", err)
		}
		if forAllRoles || !role.Valid {
			continue
		}
		keyword, known := showGlobalClasses[objectType]
		if !known {
			return nil, fmt.Errorf(
				"failed to read the global default privileges of %s: object type %q is not one Ptah models",
				role.String, objectType,
			)
		}
		if grantee == "public" {
			grantee = ""
		}
		if !slices.Contains(grantors, role.String) {
			grantors = append(grantors, role.String)
		}
		key := classKey{grantor: role.String, objectType: keyword}
		shown[key] = append(shown[key], aclitem.Item{
			Grantee:    grantee,
			Privileges: []aclitem.Privilege{{Name: strings.ToUpper(privilege), Grantable: grantable}},
		})
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate global default privileges: %w", err)
	}

	slices.Sort(grantors)
	var classes []GlobalClass
	for _, grantor := range grantors {
		for _, objectType := range []string{"FUNCTIONS", "SCHEMAS", "SEQUENCES", "TABLES", "TYPES"} {
			class := showGlobalClass(grantor, objectType, shown[classKey{grantor: grantor, objectType: objectType}])
			if len(class.Deltas) > 0 || class.OwnerUndescribed {
				classes = append(classes, class)
			}
		}
	}
	return classes, nil
}

// showGlobalClass subtracts CockroachDB's built-in default from what SHOW
// named for one grantor and object class.
//
// The owner's built-in default is ALL with the grant option. An owner SHOW
// does not name at all lost ALL, which is a revoke a statement can say; an
// owner holding anything else is [GlobalClass.OwnerUndescribed].
func showGlobalClass(grantor, objectType string, items []aclitem.Item) GlobalClass {
	class := GlobalClass{Grantor: grantor, ObjectType: objectType}
	var owner, others []aclitem.Item
	for _, item := range items {
		if item.Grantee == grantor {
			owner = append(owner, item)
			continue
		}
		others = append(others, item)
	}
	switch {
	case len(owner) == 0:
		class.Deltas = append(class.Deltas, Delta{Grantee: grantor, Privilege: "ALL", Revoked: true})
	case len(owner) > 1 || owner[0].Privileges[0] != (aclitem.Privilege{Name: "ALL", Grantable: true}):
		class.OwnerUndescribed = true
	}
	var builtin []aclitem.Item
	for _, privilege := range []string{"EXECUTE", "USAGE"} {
		if Builtin(objectType, grantor, Public, privilege) {
			builtin = append(builtin, aclitem.Item{Privileges: []aclitem.Privilege{{Name: privilege}}})
		}
	}
	class.Deltas = append(class.Deltas, GlobalDeltas(others, builtin)...)
	slices.SortFunc(class.Deltas, compareDeltas)
	return class
}

// classCodes maps the keyword a statement writes to pg_default_acl's object
// class code, the inverse of [ObjectTypeSQL].
var classCodes = map[string]string{
	"TABLES":        "r",
	"SEQUENCES":     "S",
	"FUNCTIONS":     "f",
	"TYPES":         "T",
	"SCHEMAS":       "n",
	"LARGE OBJECTS": "L",
}

// ReadGlobalResets returns the statements that return every global default to
// the built-in one, read from pg_default_acl: for each grantee a row differs
// on, one ALTER DEFAULT PRIVILEGES without IN SCHEMA that revokes everything,
// and for a grantee the built-in default holds something, a second that grants
// it back -- ALL to the owner, EXECUTE on functions and USAGE on types to
// PUBLIC. The server removes a global row once it equals the built-in default
// again, so after them pg_default_acl holds no global row.
//
// A global default applies in every schema, so only a cleanup that owns the
// whole database runs them; one scoped to some schemas does not own it. The
// caller decides whether the server has pg_default_acl at all, as for
// [ReadRevokes]. CockroachDB is read through [ReadGlobalResetsFromShow].
func ReadGlobalResets(ctx context.Context, q Querier) ([]Revoke, error) {
	query := `
		SELECT
			pg_get_userbyid(d.defaclrole) AS grantor,
			` + ObjectTypeSQL("d") + ` AS object_type,
			` + ListSQL("d") + ` AS acl,
			` + BuiltinListSQL("d") + ` AS builtin
		FROM pg_default_acl d
		WHERE ` + GlobalSQL("d")
	rows, err := q.QueryContext(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query global default privileges: %w", err)
	}
	defer rows.Close()

	var resets []Revoke
	for rows.Next() {
		var grantor, objectType, acl, builtin string
		if err := rows.Scan(&grantor, &objectType, &acl, &builtin); err != nil {
			return nil, fmt.Errorf("failed to scan global default privileges: %w", err)
		}
		held, err := aclitem.ParseJSON(acl)
		if err != nil {
			return nil, fmt.Errorf("failed to read the global default privileges %s set on %s: %w", grantor, objectType, err)
		}
		base, err := aclitem.ParseJSON(builtin)
		if err != nil {
			return nil, fmt.Errorf("failed to read the built-in default privileges of %s on %s: %w", grantor, objectType, err)
		}
		resets = append(resets, globalResets(grantor, objectType, deltaGrantees(GlobalDeltas(held, base)))...)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("failed to iterate global default privileges: %w", err)
	}
	sortResets(resets)
	return resets, nil
}

// ReadGlobalResetsFromShow is [ReadGlobalResets] for CockroachDB, read through
// [ReadGlobalFromShow]. An owner whose own privileges could not be named is
// reset too: granting ALL back returns it to the built-in default whatever it
// lost.
func ReadGlobalResetsFromShow(ctx context.Context, q Querier) ([]Revoke, error) {
	classes, err := ReadGlobalFromShow(ctx, q)
	if err != nil {
		return nil, err
	}
	var resets []Revoke
	for _, class := range classes {
		grantees := deltaGrantees(class.Deltas)
		if class.OwnerUndescribed && !slices.Contains(grantees, class.Grantor) {
			grantees = append(grantees, class.Grantor)
		}
		resets = append(resets, globalResets(class.Grantor, class.ObjectType, grantees)...)
	}
	sortResets(resets)
	return resets, nil
}

// sortResets orders resets by grantor, object class and grantee, and keeps the
// order [globalResets] wrote for one grantee: the revoke has to run before the
// grant, or it takes back what the grant restored.
func sortResets(resets []Revoke) {
	slices.SortStableFunc(resets, func(a, b Revoke) int {
		return strings.Compare(a.Name(), b.Name())
	})
}

// deltaGrantees lists each grantee deltas name, once.
func deltaGrantees(deltas []Delta) []string {
	var grantees []string
	for _, delta := range deltas {
		if !slices.Contains(grantees, delta.Grantee) {
			grantees = append(grantees, delta.Grantee)
		}
	}
	return grantees
}

// globalResets spells the statements that return one global default to the
// built-in one: for each grantee, in byte order, a revoke of everything, then
// a grant of what the built-in default holds for that grantee, if anything.
func globalResets(grantor, objectType string, grantees []string) []Revoke {
	grantees = slices.Sorted(slices.Values(grantees))
	class := classCodes[objectType]
	var resets []Revoke
	for _, grantee := range grantees {
		reset := Revoke{Grantor: grantor, Class: class, Grantee: grantee}
		revoke := reset
		revoke.Statement = globalStatement("REVOKE ALL PRIVILEGES ON "+objectType+" FROM", grantor, grantee)
		resets = append(resets, revoke)
		if restored := builtinPrivileges(objectType, grantor, grantee); restored != "" {
			grant := reset
			grant.Statement = globalStatement("GRANT "+restored+" ON "+objectType+" TO", grantor, grantee)
			resets = append(resets, grant)
		}
	}
	return resets
}

// builtinPrivileges is what the built-in default gives grantee, as a GRANT
// writes it, or empty when it gives nothing: ALL for the owner, and EXECUTE or
// USAGE for PUBLIC, the empty grantee, on the class that has it. A role named
// "PUBLIC" is an ordinary role and gets nothing back.
func builtinPrivileges(objectType, grantor, grantee string) string {
	switch {
	case grantee == grantor:
		return "ALL PRIVILEGES"
	case grantee != "":
		return ""
	case Builtin(objectType, grantor, Public, "EXECUTE"):
		return "EXECUTE"
	case Builtin(objectType, grantor, Public, "USAGE"):
		return "USAGE"
	default:
		return ""
	}
}

// globalStatement spells an ALTER DEFAULT PRIVILEGES without IN SCHEMA: the
// grantor clause, then action, then the grantee, PUBLIC for the empty one.
func globalStatement(action, grantor, grantee string) string {
	granteeClause := Public
	if grantee != "" {
		granteeClause = quote(grantee)
	}
	return "ALTER DEFAULT PRIVILEGES FOR ROLE " + quote(grantor) + " " + action + " " + granteeClause
}
