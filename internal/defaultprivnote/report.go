// Package defaultprivnote reports the default privileges a schema read leaves
// out of its description.
//
// Two kinds of pg_default_acl row have no declaration: one set by ALTER DEFAULT
// PRIVILEGES without IN SCHEMA, which applies in every schema of the database,
// and CockroachDB's FOR ALL ROLES, which has no grantor role. The SQL schema
// reader, HCL, YAML and the Go annotation all require a schema and a grantor, so
// the PostgreSQL-family reader does not describe either kind and records it in
// [catalog.Database.UndescribedDefaultPrivileges] instead. This package turns
// that list into the note the read surfaces print.
package defaultprivnote

import (
	"fmt"
	"io"
	"sort"
	"strings"

	"ptah.run/catalog"
)

// ReportUndescribed writes a note naming the default privileges the description
// does not carry, and nothing at all when there are none.
//
// It belongs on the read surfaces, `ptah db read` and `schema inspect`, whose
// output an operator may apply to another database. A default such as
// `ALTER DEFAULT PRIVILEGES REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC` changes
// what every new function allows there, and nothing in the statements shows it
// is missing: measured on PostgreSQL 18.6, a function the grantor creates in the
// source is not executable by PUBLIC, and the same function created in a
// database built from the description is.
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
	if w == nil || schema == nil || len(schema.UndescribedDefaultPrivileges) == 0 {
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
		"note: %s %s not described, because no schema source can declare one set"+
			" without IN SCHEMA or FOR ALL ROLES; a description applied to another"+
			" database does not carry %s: %s.\n",
		subject, verb, object, strings.Join(named, ", "))
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
