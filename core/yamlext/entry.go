package yamlext

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
)

// The frontend's entries an owner adds keys to.
const (
	// EntryTable is an entry of the tables key.
	EntryTable = "tables"
	// EntryIndex is an index entry, of a table's indexes or of the top-level
	// indexes key.
	EntryIndex = "indexes"
)

// EntryAttributes are scalar keys an owner adds to one of the frontend's
// entries, such as the partitioning of a YDB table. It is the YAML twin of
// annotation.DirectiveAttributes, so an owner that spells a setting alike in
// both formats reads it with one decoder.
type EntryAttributes struct {
	// Entry names the frontend's entry: [EntryTable] or [EntryIndex].
	Entry string
	// Attributes are the owner's keys. A key belongs to one owner, and never
	// to the entry itself.
	Attributes []string
	// Reads are keys of the entry itself that the owner's decoders read
	// beside its own, such as an index's type. An entry that writes one of
	// them is decoded even where it writes none of the owner's keys.
	Reads []string
	// Decode reads the owner's keys an entry wrote, and the ones of Reads it
	// wrote, keyed by name, as facets of the owner's models. It is called
	// only when the entry wrote at least one of them.
	Decode func(attributes map[string]string) (schemaext.Facets, error)
	// Parameters reads the same keys into options of the object the entry
	// declares, such as the WITH options of an index. It is optional.
	Parameters func(attributes map[string]string) (map[string]string, error)
}

// EntrySection is a key an owner adds to the frontend's table entries whose
// value is not a scalar, such as the changefeeds of a YDB table.
type EntrySection struct {
	// Key is the table entry's key. A key belongs to one owner, and never to
	// the entry itself.
	Key string
	// Decode reads the key's value of one table entry. decode unmarshals it
	// into target, refusing a key target's type does not declare and naming
	// the line. table is the table the entry declares. It returns the objects
	// the value declares and the facets it gives that table; a facet's Table
	// is ignored.
	Decode func(decode func(target any) error, table Table) ([]Contribution, error)
}

func (s *Set) claimEntryKeys(index int, extension Extension) error {
	for _, group := range extension.EntryAttributes {
		if (group.Entry != EntryTable && group.Entry != EntryIndex) || (group.Decode == nil && group.Parameters == nil) {
			return fmt.Errorf("YAML extension of %s declares entry attributes without an entry or a decoder", extension.Owner)
		}
		for _, key := range group.Attributes {
			if err := s.claimEntryKey(index, extension, group.Entry, key); err != nil {
				return err
			}
		}
	}
	for _, section := range extension.EntrySections {
		if section.Decode == nil {
			return fmt.Errorf("YAML extension of %s declares a table key without a decoder", extension.Owner)
		}
		if err := s.claimEntryKey(index, extension, EntryTable, section.Key); err != nil {
			return err
		}
	}
	return nil
}

func (s *Set) claimEntryKey(index int, extension Extension, entry, key string) error {
	if strings.TrimSpace(key) == "" {
		return fmt.Errorf("YAML extension of %s declares a key of %s without a name", extension.Owner, entry)
	}
	claimed := s.entryKeys[entry]
	if claimed == nil {
		claimed = make(map[string]int)
		s.entryKeys[entry] = claimed
	}
	if previous, taken := claimed[key]; taken {
		return fmt.Errorf("%w: key %q of %s is read by %s and %s", schemaext.ErrDuplicate, key, entry,
			s.extensions[previous].Owner, extension.Owner)
	}
	claimed[key] = index
	return nil
}

// EntryKeys returns, sorted, the keys the set's owners add to entry.
func (s Set) EntryKeys(entry string) []string {
	return slices.Sorted(maps.Keys(s.entryKeys[entry]))
}

// EntryKey reports whether an owner of the set reads key of entry, and
// whether the key holds a scalar, which [Set.DecodeEntryAttributes] reads,
// rather than a value [Set.DecodeEntrySection] reads.
func (s Set) EntryKey(entry, key string) (found, scalar bool) {
	index, found := s.entryKeys[entry][key]
	if !found {
		return false, false
	}
	for _, section := range s.extensions[index].EntrySections {
		if entry == EntryTable && section.Key == key {
			return true, false
		}
	}
	return true, true
}

// DecodeEntryAttributes hands each owner the scalar keys it adds to entry
// that attributes holds, with the ones of its Reads, and joins the facets
// they return. An owner that reads none of them is not called. A facet of a
// model the owner does not declare, and one two owners return, are errors.
func (s Set) DecodeEntryAttributes(entry string, attributes map[string]string) (schemaext.Facets, error) {
	var joined schemaext.Facets
	for _, extension := range s.extensions {
		for _, group := range extension.EntryAttributes {
			written := group.written(entry, attributes)
			if len(written) == 0 || group.Decode == nil {
				continue
			}
			facets, err := group.Decode(written)
			if err != nil {
				return schemaext.Facets{}, err
			}
			for _, kind := range facets.DeclaredKinds() {
				if !slices.Contains(extension.Kinds, kind) {
					return schemaext.Facets{}, fmt.Errorf("%w: keys of %s contributed a model %s does not declare",
						schemaext.ErrInvalidValue, entry, extension.Owner)
				}
			}
			if joined, err = joined.Merge(facets); err != nil {
				return schemaext.Facets{}, err
			}
		}
	}
	return joined, nil
}

// DecodeEntryParameters hands each owner whose keys of entry spell options
// the keys [Set.DecodeEntryAttributes] hands it, and joins the options they
// return. It returns nil where no owner returns one. An option two owners
// return is an error.
func (s Set) DecodeEntryParameters(entry string, attributes map[string]string) (map[string]string, error) {
	var joined map[string]string
	for _, extension := range s.extensions {
		for _, group := range extension.EntryAttributes {
			written := group.written(entry, attributes)
			if len(written) == 0 || group.Parameters == nil {
				continue
			}
			options, err := group.Parameters(written)
			if err != nil {
				return nil, err
			}
			for name, value := range options {
				if _, taken := joined[name]; taken {
					return nil, fmt.Errorf("%w: option %q of %s is returned by two owners", schemaext.ErrDuplicate, name, entry)
				}
				if joined == nil {
					joined = make(map[string]string, len(options))
				}
				joined[name] = value
			}
		}
	}
	return joined, nil
}

// DecodeEntrySection hands the value of a table entry's key to the owner
// that reads it. A key no extension reads is an error, and so is a
// contribution [Set.Decode] would refuse.
func (s Set) DecodeEntrySection(key string, decode func(target any) error, table Table) ([]Contribution, error) {
	index, found := s.entryKeys[EntryTable][key]
	if !found {
		return nil, fmt.Errorf("no selected owner reads key %q of a table", key)
	}
	extension := s.extensions[index]
	for _, section := range extension.EntrySections {
		if section.Key != key {
			continue
		}
		contributions, err := section.Decode(decode, table)
		if err != nil {
			return nil, err
		}
		if err := checkContributions(extension, fmt.Sprintf("key %q of a table", key), contributions, 1); err != nil {
			return nil, err
		}
		return contributions, nil
	}
	return nil, fmt.Errorf("no selected owner reads key %q of a table", key)
}

// written returns the keys of entry an entry wrote that the group reads: its
// own, and its Reads where it wrote at least one of either.
func (g EntryAttributes) written(entry string, attributes map[string]string) map[string]string {
	if g.Entry != entry {
		return nil
	}
	written := make(map[string]string)
	for _, name := range slices.Concat(g.Attributes, g.Reads) {
		if value, found := attributes[name]; found {
			written[name] = value
		}
	}
	return written
}
