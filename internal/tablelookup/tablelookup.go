// Package tablelookup resolves structural table references without depending
// on a migration planner.
package tablelookup

import (
	"cmp"
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
	"ptah.run/internal/tableref"
)

// Declared returns the position in tables of the table reference names: the
// one of that name, in the schema a qualified reference names. It returns -1
// where no table matches, since a reference may name a table the document
// leaves to the database. A reference that is not a table reference is
// refused, and so is one that more than one table matches, naming their
// schemas.
func Declared(tables []schemamodel.Table, reference string) (int, error) {
	ref, ok := tableref.Parse(reference)
	if !ok {
		return -1, fmt.Errorf("%w: %q is not a table reference", ptaherr.ErrInvalidAttributeValue, reference)
	}
	var matches []int
	for i, declared := range tables {
		if declared.Name == ref.Name && (!ref.Qualified || declared.Schema == ref.Schema) {
			matches = append(matches, i)
		}
	}
	switch len(matches) {
	case 0:
		return -1, nil
	case 1:
		return matches[0], nil
	}
	schemas := make([]string, 0, len(matches))
	for _, i := range matches {
		schemas = append(schemas, strconv.Quote(cmp.Or(tables[i].Schema, "(default)")))
	}
	return -1, fmt.Errorf("%w: table %q is declared in schemas %s; name the schema",
		ptaherr.ErrInvalidAttributeValue, reference, strings.Join(schemas, " and "))
}

// ResolveReference returns the canonical identity of a referenced table when
// the reference can be resolved without guessing.
func ResolveReference(tables []schemamodel.Table, current schemamodel.Table, reference string) string {
	ref, ok := tableref.Parse(reference)
	if !ok {
		return reference
	}
	if ref.Qualified {
		return schemamodel.QualifyTableName(ref.Schema, ref.Name)
	}

	currentSchema := strings.TrimSpace(current.Schema)
	for _, table := range tables {
		if strings.TrimSpace(table.Schema) == currentSchema && table.Name == ref.Name {
			return table.QualifiedName()
		}
	}

	match := ""
	for _, table := range tables {
		if table.Name != ref.Name {
			continue
		}
		if match != "" {
			return reference
		}
		match = table.QualifiedName()
	}
	if match != "" {
		return match
	}
	return reference
}
