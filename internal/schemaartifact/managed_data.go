package schemaartifact

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"sort"
	"strings"

	"ptah.run/core/schemamodel"
)

// managedDataDocument is the wire form of the managed-data layer.
//
// It carries no version field. The layer media type is the version, and a
// second statement of it inside the bytes is a second thing to keep in step.
type managedDataDocument struct {
	Sets []managedDataSet `json:"sets"`
}

// managedDataSet is one declaration: the rows one annotation owns in one
// table, with the columns those rows declare.
type managedDataSet struct {
	Schema  string           `json:"schema,omitempty"`
	Table   string           `json:"table"`
	Keys    []string         `json:"keys"`
	Columns []string         `json:"columns"`
	Rows    []managedDataRow `json:"rows"`
}

// managedDataRow maps a column name to the value declared for it. A column
// absent from the object was not declared; a column whose member is JSON null
// was declared null.
type managedDataRow map[string]managedDataValue

// managedDataValue is one declared cell. A null is the JSON literal; every
// other value is an object carrying the YAML tag it resolved to and the exact
// text that declared it, because `007` and "007" are different values in the
// same column and only the tag separates them.
type managedDataValue struct {
	Tag  string `json:"tag"`
	Text string `json:"text"`
	Null bool   `json:"-"`
}

// MarshalJSON writes a null as the JSON literal and every other value as its
// tag and text.
func (v managedDataValue) MarshalJSON() ([]byte, error) {
	if v.Null {
		return []byte("null"), nil
	}
	if v.Tag == "" {
		return nil, fmt.Errorf("declared value has no YAML tag")
	}
	type wire managedDataValue
	return json.Marshal(wire{Tag: v.Tag, Text: v.Text})
}

// UnmarshalJSON reads the two forms MarshalJSON writes and refuses everything
// else, so a hand-edited layer cannot arrive as a value with no declared type.
func (v *managedDataValue) UnmarshalJSON(data []byte) error {
	if bytes.Equal(bytes.TrimSpace(data), []byte("null")) {
		*v = managedDataValue{Tag: "null", Null: true}
		return nil
	}
	type wire managedDataValue
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var decoded wire
	if err := decoder.Decode(&decoded); err != nil {
		return fmt.Errorf("read declared value: %w", err)
	}
	if decoded.Tag == "" {
		return fmt.Errorf("declared value has no YAML tag")
	}
	if decoded.Tag == "null" {
		return fmt.Errorf("declared value carries the null tag without being null")
	}
	*v = managedDataValue{Tag: decoded.Tag, Text: decoded.Text}
	return nil
}

// encodeManagedData renders the declared row sets of db as the canonical
// managed-data layer.
//
// The output is compact JSON with a trailing newline. Object members are
// sorted by encoding/json, sets by schema and table, and rows keep the order
// the author declared them in: the artifact records a declaration, and
// reordering it would make the digest depend on a sort this package invented.
func encodeManagedData(db *schemamodel.Database) ([]byte, error) {
	sets := make([]managedDataSet, 0, len(db.ManagedData))
	for _, declaration := range db.ManagedData {
		set, err := managedSetFrom(declaration)
		if err != nil {
			return nil, err
		}
		sets = append(sets, set)
	}
	sort.SliceStable(sets, func(i, j int) bool {
		if sets[i].Schema != sets[j].Schema {
			return sets[i].Schema < sets[j].Schema
		}
		return sets[i].Table < sets[j].Table
	})
	for index := 1; index < len(sets); index++ {
		if sets[index-1].Schema == sets[index].Schema && sets[index-1].Table == sets[index].Table {
			return nil, fmt.Errorf(
				"managed data declares table %s twice",
				qualifiedTable(sets[index].Schema, sets[index].Table),
			)
		}
	}
	encoded, err := json.Marshal(managedDataDocument{Sets: sets})
	if err != nil {
		return nil, fmt.Errorf("render managed data layer: %w", err)
	}
	return append(encoded, '\n'), nil
}

func managedSetFrom(declaration schemamodel.ManagedData) (managedDataSet, error) {
	table := strings.TrimSpace(declaration.Table)
	if table == "" {
		return managedDataSet{}, fmt.Errorf("managed data declaration has no table")
	}
	qualified := qualifiedTable(declaration.Schema, table)
	if declaration.Rows == nil {
		return managedDataSet{}, fmt.Errorf(
			"managed data for table %s was never read from %s",
			qualified, declaration.File,
		)
	}
	if len(declaration.Keys) == 0 {
		return managedDataSet{}, fmt.Errorf("managed data for table %s declares no key column", qualified)
	}
	keys := slices.Clone(declaration.Keys)
	if len(slices.Compact(slices.Sorted(slices.Values(keys)))) != len(keys) {
		return managedDataSet{}, fmt.Errorf("managed data for table %s repeats a key column", qualified)
	}
	columns := make(map[string]struct{})
	rows := make([]managedDataRow, 0, len(declaration.Rows))
	identities := make(map[string]int, len(declaration.Rows))
	for index, declared := range declaration.Rows {
		row := make(managedDataRow, len(declared))
		for column, value := range declared {
			if strings.TrimSpace(column) == "" {
				return managedDataSet{}, fmt.Errorf("managed data for table %s declares an empty column name", qualified)
			}
			columns[column] = struct{}{}
			row[column] = managedDataValue{Tag: value.Tag, Text: value.Text, Null: value.Null}
		}
		identity, err := rowIdentity(qualified, keys, row, index)
		if err != nil {
			return managedDataSet{}, err
		}
		if first, repeated := identities[identity]; repeated {
			return managedDataSet{}, fmt.Errorf(
				"managed data for table %s declares rows %d and %d with the same key",
				qualified, first+1, index+1,
			)
		}
		identities[identity] = index
		rows = append(rows, row)
	}
	return managedDataSet{
		Schema:  declaration.Schema,
		Table:   table,
		Keys:    keys,
		Columns: slices.Sorted(maps.Keys(columns)),
		Rows:    rows,
	}, nil
}

// rowIdentity renders the key tuple of a row, and refuses a row whose identity
// is missing or null. A key that does not identify is not a key, and finding
// that out at publication costs a refusal rather than a mutation.
func rowIdentity(qualified string, keys []string, row managedDataRow, index int) (string, error) {
	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		value, declared := row[key]
		if !declared {
			return "", fmt.Errorf(
				"managed data for table %s, row %d does not declare key column %q",
				qualified, index+1, key,
			)
		}
		if value.Null {
			return "", fmt.Errorf(
				"managed data for table %s, row %d declares key column %q as null",
				qualified, index+1, key,
			)
		}
		parts = append(parts, fmt.Sprintf("%s=%s:%s", key, value.Tag, value.Text))
	}
	return strings.Join(parts, "\x00"), nil
}

// decodeManagedData reads a managed-data layer back into declarations.
//
// Unknown members are refused rather than ignored: a layer this reader cannot
// fully account for is one whose rows it cannot claim to have published.
func decodeManagedData(data []byte) ([]schemamodel.ManagedData, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var document managedDataDocument
	if err := decoder.Decode(&document); err != nil {
		return nil, fmt.Errorf("parse managed data layer: %w", err)
	}
	if decoder.More() {
		return nil, fmt.Errorf("managed data layer carries more than one document")
	}
	declarations := make([]schemamodel.ManagedData, 0, len(document.Sets))
	for _, set := range document.Sets {
		if strings.TrimSpace(set.Table) == "" {
			return nil, fmt.Errorf("managed data layer declares a set with no table")
		}
		qualified := qualifiedTable(set.Schema, set.Table)
		if len(set.Keys) == 0 {
			return nil, fmt.Errorf("managed data layer declares no key column for table %s", qualified)
		}
		rows := make([]schemamodel.ManagedRow, 0, len(set.Rows))
		for index, decoded := range set.Rows {
			row := make(schemamodel.ManagedRow, len(decoded))
			for column, value := range decoded {
				row[column] = schemamodel.ManagedValue{Tag: value.Tag, Text: value.Text, Null: value.Null}
			}
			if _, err := rowIdentity(qualified, set.Keys, decoded, index); err != nil {
				return nil, err
			}
			rows = append(rows, row)
		}
		declarations = append(declarations, schemamodel.ManagedData{
			Table:  set.Table,
			Schema: set.Schema,
			Keys:   slices.Clone(set.Keys),
			Rows:   rows,
		})
	}
	return declarations, nil
}

func qualifiedTable(schema, table string) string {
	if schema == "" {
		return table
	}
	return schema + "." + table
}

// AttachManagedRows joins the rows of the managed-data layer to the
// declarations the canonical schema HCL already carries.
//
// The two describe one thing from two sides: the HCL says which table a
// declaration owns and by which keys, the layer carries the rows. Neither is
// allowed to exist without the other, and where they both speak they have to
// agree, because a reader that trusted one of them would publish a table whose
// rows nobody declared or whose identity nobody agreed on.
//
// It is exported because the join happens in two places: when an artifact is
// read out of a registry, and when one that was materialized to disk is read
// back from there. A second implementation of the agreement above is a second
// set of refusals to keep in step.
func AttachManagedRows(db *schemamodel.Database, layer []byte) error {
	if len(db.ManagedData) == 0 {
		if layer != nil {
			return fmt.Errorf("schema artifact carries a managed data layer no data block declares")
		}
		return nil
	}
	if layer == nil {
		return fmt.Errorf(
			"schema artifact declares managed data for table %s and carries no rows for it",
			qualifiedTable(db.ManagedData[0].Schema, db.ManagedData[0].Table),
		)
	}
	sets, err := decodeManagedData(layer)
	if err != nil {
		return err
	}
	indexed := make(map[string]schemamodel.ManagedData, len(sets))
	for _, set := range sets {
		indexed[qualifiedTable(set.Schema, set.Table)] = set
	}
	for index, declaration := range db.ManagedData {
		qualified := qualifiedTable(declaration.Schema, declaration.Table)
		set, declared := indexed[qualified]
		if !declared {
			return fmt.Errorf("schema artifact declares managed data for table %s and carries no rows for it", qualified)
		}
		if !slices.Equal(set.Keys, declaration.Keys) {
			return fmt.Errorf(
				"managed data layer keys %v for table %s do not match the declared keys %v",
				set.Keys, qualified, declaration.Keys,
			)
		}
		db.ManagedData[index].Rows = set.Rows
		delete(indexed, qualified)
	}
	for qualified := range indexed {
		return fmt.Errorf("managed data layer carries rows for table %s, which no data block declares", qualified)
	}
	return nil
}
