package schemadiff

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/catalog"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// validateUnqualifiedNames refuses a declaration that writes a table, a view,
// a routine or a sequence without a schema, on a connection whose server
// creates such a name in another schema than the one the comparison reads it
// in ([catalog.ServerInfo.UnqualifiedSchema]). The plan would create the
// object there, and the next run would read the connection's schema again,
// find no such object and plan it once more, which the server refuses because
// the name is taken. Measured on SQL Server 2025 as sa with a URL naming
// schema=app: CREATE TABLE [t1] creates dbo.t1, and the second schema apply
// fails with 2714.
func validateUnqualifiedNames(desired *schemamodel.Database, info catalog.ServerInfo) error {
	if info.UnqualifiedSchema == "" {
		return nil
	}
	var names []string
	collect := func(name string) {
		if ref, ok := tableref.Parse(name); ok && !ref.Qualified {
			names = append(names, strconv.Quote(ref.Name))
		}
	}
	for _, table := range desired.Tables {
		collect(table.QualifiedName())
	}
	for _, view := range desired.Views {
		collect(view.Name)
	}
	for _, function := range desired.Functions {
		collect(function.Name)
	}
	for _, sequence := range desired.Sequences {
		collect(sequence.QualifiedName())
	}
	if len(names) == 0 {
		return nil
	}
	slices.Sort(names)
	return fmt.Errorf(
		"the desired schema writes %s without a schema: this connection reads schema %q, and the server creates "+
			"such a name in %q, the default schema of the connection's user, so every run would plan it again; "+
			"qualify each name with its schema, or leave the schema parameter out of the URL for a user whose "+
			"default schema is %q",
		strings.Join(slices.Compact(names), ", "), info.Schema, info.UnqualifiedSchema, info.Schema)
}
