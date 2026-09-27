// Package defaultprivnote reports the default privileges a schema read leaves
// out of its description.
//
// Two kinds of default privilege have no declaration. CockroachDB's FOR ALL
// ROLES has no grantor role, and every schema source requires one. And on
// CockroachDB, a global default whose owner holds only some of its own
// privileges cannot be told apart from the built-in default: SHOW DEFAULT
// PRIVILEGES lists privileges PostgreSQL does not have, so what the owner took
// away cannot be named. The PostgreSQL-family reader does not describe either
// kind and records it in [catalog.Database.UndescribedDefaultPrivileges]
// instead. This package turns that list into the note the read surfaces print,
// and also says so when the server refused to show pg_default_acl at all.
package defaultprivnote

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/coverage"
)

// ReportUndescribed writes a note naming the default privileges the description
// does not carry, and nothing at all when there are none.
//
// It belongs on the read surfaces, `ptah db read` and `schema inspect`, whose
// output an operator may apply to another database. A default such as
// `ALTER DEFAULT PRIVILEGES FOR ALL ROLES GRANT SELECT ON TABLES TO reader`
// changes what every new table allows there, and nothing in the statements
// shows it is missing.
//
// The note gives the reason that applies to what it names: FOR ALL ROLES for
// an entry with no grantor, and the owner's own privileges for one with a
// grantor, which the reader records only for a global default on CockroachDB.
//
// It names each one by object class, schema and grantor rather than counting
// them, which is the choice [ptah.run/internal/timescale.ReportUndescribed]
// makes: these are the database's own settings, and the note is only useful if
// the reader can tell which statement to run by hand. It does not interpret the
// ACL, whose meaning differs by engine; the three names are enough to find the
// setting in pg_default_acl.
//
// w may be nil, which is how the inspect surfaces spell "no diagnostics
// stream"; the note is then dropped. Write errors are dropped too: a diagnostic
// that fails to print must not fail a read that succeeded.
func ReportUndescribed(w io.Writer, schema *catalog.Database) {
	if w == nil || schema == nil {
		return
	}
	reportRefused(w, schema)
	if len(schema.UndescribedDefaultPrivileges) == 0 {
		return
	}
	named := make([]string, 0, len(schema.UndescribedDefaultPrivileges))
	for _, privilege := range schema.UndescribedDefaultPrivileges {
		named = append(named, describe(privilege))
	}
	sort.Strings(named)
	count := len(named)
	subject, verb, object := fmt.Sprintf("%d default privileges", count), "are", "them"
	if count == 1 {
		subject, verb, object = "1 default privilege", "is", "it"
	}
	_, _ = fmt.Fprintf(w,
		"note: %s %s not described, because %s; a description applied to another"+
			" database does not carry %s: %s.\n",
		subject, verb, reasons(schema.UndescribedDefaultPrivileges), object, strings.Join(named, ", "))
}

// reasons says why the entries are not described, naming only the reasons
// that apply to them.
func reasons(privileges []catalog.UndescribedDefaultPrivilege) string {
	var allRoles, ownerPart bool
	for _, privilege := range privileges {
		allRoles = allRoles || privilege.Grantor == ""
		ownerPart = ownerPart || privilege.Grantor != ""
	}
	var said []string
	if allRoles {
		said = append(said, "no schema source can declare one set FOR ALL ROLES")
	}
	if ownerPart {
		said = append(said, "CockroachDB does not show which of its own privileges"+
			" an owner took away from a default without IN SCHEMA")
	}
	return strings.Join(said, ", and ")
}

// reportRefused writes the note for a read that could not look at the default
// privileges at all, which the reader records as [coverage.DefaultPrivilege]
// not described.
//
// The one refusal the reader records is CockroachDB v26.2's: every read of
// pg_default_acl fails once a default privilege names a role whose name needs
// quoting (stokaro/ptah#3816). The note says so, and says what follows, because
// the description is otherwise indistinguishable from one of a database that
// has no default privileges.
func reportRefused(w io.Writer, schema *catalog.Database) {
	if _, refused := schema.NotDescribed.Limit(coverage.DefaultPrivilege); !refused {
		return
	}
	_, _ = fmt.Fprint(w,
		"note: default privileges are not described, because the server refused to read pg_default_acl,"+
			" as CockroachDB v26.2 does once a default privilege names a role whose name needs quoting;"+
			" a comparison leaves them alone, and a description applied to another database does not"+
			" carry them.\n")
}

// describe names one default privilege the way the note lists it: the object
// class, where it applies, and whose new objects it applies to.
func describe(privilege catalog.UndescribedDefaultPrivilege) string {
	where := "every schema"
	if privilege.Schema != "" {
		where = privilege.Schema
	}
	whose := "all roles"
	if privilege.Grantor != "" {
		whose = privilege.Grantor
	}
	return privilege.ObjectType + " in " + where + " for " + whose
}
