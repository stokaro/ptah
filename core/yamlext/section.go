package yamlext

import (
	"cmp"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/internal/tableref"
)

// Section is a top-level key of a YAML schema document whose value one owner
// reads, such as the ClickHouse owner's row_policies.
type Section struct {
	// Key is the document key. A key belongs to one owner, and never to the
	// frontend's own grammar.
	Key string
	// Decode reads the section's value. decode unmarshals the value into
	// target, a pointer to a type with yaml tags, and refuses a key the type
	// does not declare, naming the line, as the frontend refuses one of its
	// own. tables are the tables the document declares. Decode returns the
	// objects the section declares and the facets it gives those tables.
	Decode func(decode func(target any) error, tables Tables) ([]Contribution, error)
}

// Contribution is one thing a section adds to the schema: a standalone
// feature object, or a facet of one of the document's tables. Exactly one of
// Object and Facet is set.
type Contribution struct {
	// Object is a standalone feature object.
	Object *schemaext.Object
	// Facet is a value attached to the table at position Table of the
	// section's tables.
	Facet schemaext.Value
	// Table is the position of the table a facet belongs to, as
	// [Tables.Find] returns it.
	Table int
	// Label names what the contribution declares in a refusal, such as
	// `row policy "tenant_rows" on table "orders"`.
	Label string
	// Targets scope a facet to the targets it holds on, as an entry's
	// dialects scope it. Empty holds on every target.
	Targets []string
}

// Table is a table a YAML document declares.
type Table struct {
	// Key is the table's key in the document's tables section.
	Key string
	// Schema is the schema the table names, or empty where it names none.
	Schema string
	// Name is the table's name.
	Name string
	// Struct is the Go struct the table maps to, or empty.
	Struct string
}

// Tables are the tables a YAML document declares, ordered by key.
type Tables []Table

// Find returns the position in t of the table a declaration names: the one
// structName maps to when table is empty, and otherwise the one of that name,
// in the schema a qualified reference names. It returns -1 where no table
// matches: a declaration may name a table the document leaves to the
// database. A reference that is not a table reference is refused, and so is
// one that more than one table matches, naming their schemas.
func (t Tables) Find(structName, table string) (int, error) {
	if table == "" {
		return slices.IndexFunc(t, func(declared Table) bool { return structName != "" && declared.Struct == structName }), nil
	}
	ref, ok := tableref.Parse(table)
	if !ok {
		return -1, fmt.Errorf("%w: %q is not a table reference", ptaherr.ErrInvalidAttributeValue, table)
	}
	var matches []int
	for index, declared := range t {
		if declared.Name == ref.Name && (!ref.Qualified || declared.Schema == ref.Schema) {
			matches = append(matches, index)
		}
	}
	switch len(matches) {
	case 0:
		return -1, nil
	case 1:
		return matches[0], nil
	}
	schemas := make([]string, 0, len(matches))
	for _, index := range matches {
		schemas = append(schemas, strconv.Quote(cmp.Or(t[index].Schema, "(default)")))
	}
	return -1, fmt.Errorf("%w: table %q is declared in schemas %s; name the schema",
		ptaherr.ErrInvalidAttributeValue, table, strings.Join(schemas, " and "))
}

// Section returns the owner that reads key, or false when no extension of
// the set does.
func (s Set) Section(key string) (string, bool) {
	index, found := s.sections[key]
	if !found {
		return "", false
	}
	return s.extensions[index].Owner, true
}

// SectionKeys returns, sorted, the document keys the set's owners read.
func (s Set) SectionKeys() []string {
	keys := make([]string, 0, len(s.sections))
	for key := range s.sections {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	return keys
}

// Decode hands the section under key to the owner that reads it. A key no
// extension reads is an error, and so is a contribution that sets neither or
// both of an object and a facet, one of a model the owner does not declare,
// and a facet of a table tables does not hold.
func (s Set) Decode(key string, decode func(target any) error, tables Tables) ([]Contribution, error) {
	index, found := s.sections[key]
	if !found {
		return nil, fmt.Errorf("no selected owner reads YAML key %q", key)
	}
	extension := s.extensions[index]
	var section Section
	for _, candidate := range extension.Sections {
		if candidate.Key == key {
			section = candidate
		}
	}
	contributions, err := section.Decode(decode, slices.Clone(tables))
	if err != nil {
		return nil, err
	}
	if err := checkContributions(extension, fmt.Sprintf("YAML key %q", key), contributions, len(tables)); err != nil {
		return nil, err
	}
	return contributions, nil
}

// checkContributions refuses a contribution that sets neither or both of an
// object and a facet, a facet of no table of the document, and one of a model
// extension does not declare. source names what contributed them.
func checkContributions(extension Extension, source string, contributions []Contribution, tables int) error {
	for _, contribution := range contributions {
		if (contribution.Object == nil) == (contribution.Facet == nil) {
			return fmt.Errorf("%w: %s contributed neither or both of an object and a facet", schemaext.ErrInvalidValue, source)
		}
		value := contribution.Facet
		if contribution.Object != nil {
			value = contribution.Object.Value
		} else if contribution.Table < 0 || contribution.Table >= tables {
			return fmt.Errorf("%w: %s contributed a facet of no table the document declares", schemaext.ErrInvalidValue, source)
		}
		if value == nil || !slices.Contains(extension.Kinds, value.Kind()) {
			return fmt.Errorf("%w: %s contributed a model %s does not declare", schemaext.ErrInvalidValue, source, extension.Owner)
		}
	}
	return nil
}
