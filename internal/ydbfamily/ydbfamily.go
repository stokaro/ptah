// Package ydbfamily owns the rules of a YDB row table's column families: what
// a declaration may say, when two descriptions of a table's families are the
// same, the clauses that create them, the statement that changes them, and
// the changes YDB cannot make in place.
//
// A column family is a group of a row table's columns that YDB stores
// together, with settings of its own: DATA, the kind of storage pool the
// columns are kept in; COMPRESSION; and CACHE_MODE. Every row table has the
// family `default`, which holds the key columns and every column no other
// family names. Measured on YDB 25.1.4.7 and 26.2.1.14 alike:
//
//   - CREATE TABLE declares a family with `FAMILY f (DATA = ..., COMPRESSION
//     = ...)` and puts a column in it with `c T FAMILY f`, which 25.1 takes
//     only before NOT NULL and DEFAULT. A column naming a family the
//     statement does not declare answers `Unknown family`, `default` too.
//   - COMPRESSION takes `off` and `lz4`, in any case. `zstd` answers
//     `Unsupported compression value 3`, and COMPRESSION_LEVEL `is not
//     supported for OLTP tables`: both are for column-oriented tables.
//   - DATA takes a storage pool kind the database has; local-ydb has only
//     `hdd`, and any other kind answers `database doesn't have required
//     storage pools`. Pool kinds are case-sensitive.
//   - A key column stays in `default` (`Key column 'id' must belong to the
//     default family`, and `Cannot set family for key column` on ALTER).
//   - ALTER TABLE takes `ADD FAMILY f (...)`, `ALTER FAMILY f SET DATA ...`,
//     `ALTER FAMILY f SET COMPRESSION ...` and `ALTER COLUMN c SET FAMILY f`,
//     several in one statement. Setting one setting resets no other.
//   - There is no DROP FAMILY and no RESET of a family setting, both parse
//     errors. A family and a storage pool once named stay until the table is
//     recreated.
//   - `ALTER FAMILY f SET ...`, `ALTER COLUMN c SET FAMILY f` and `ADD COLUMN
//     c T FAMILY f` each create f with YDB's settings when the table has no
//     such family, rather than refusing, and `ADD FAMILY f` of a family the
//     table has changes the settings it names.
//   - DescribeTable lists every family with its settings and gives each
//     column the name of its family, empty for `default`. A family named
//     without settings reads back as uncompressed with no pool of its own.
//
// CACHE_MODE (`regular` or `in_memory`) is taken from 25.4 on, and behind a
// flag on 25.3; see capability.ColumnFamilyCacheMode.
//
// A new table's families also take settings from the cluster's table profile.
// Measured on both lines with a dynamic configuration whose default storage
// policy names families: a codec on family 0 compresses the default family of
// every new table, `column_cache: ColumnCacheEver` turns its keep_in_memory
// on, `column_cache_mode` gives it a cache mode on 26.2, and a family with id
// 1 and a name is added to every new table, with no columns and the profile's
// codec. A setting the statement names wins over the profile, and ALTER TABLE
// keeps what the profile set. So a setting the declaration leaves out means
// "keep what the table holds", never YDB's documented default: [Satisfied]
// reads only what a declaration states, [AlterActions] writes only that, and
// a family the table holds and the declaration leaves out stays. A statement
// that has to name the whole table, the CREATE TABLE of a rebuild, takes
// [Applied], which fills each setting the declaration leaves out with the
// value the table holds. Where a column sits is the declaration's own: a
// profile places no column, and one the declaration places in no family moves
// to `default`.
//
// keep_in_memory is not a setting YQL takes (`Unknown table setting:
// KEEP_IN_MEMORY`), and the table service refuses it unless the flag
// EnablePublicApiKeepInMemory is on (`Setting keep_in_memory to ENABLED is not
// allowed`). The reader keeps it, no statement Ptah writes changes it, and a
// CREATE TABLE that would have to carry it is refused; see [CreateRefusal].
//
// The annotation parser, the YAML reader, the renderer, the reader, the
// comparison and the planner each ask this package, so a declaration one of
// them accepts is one the others read the same way.
package ydbfamily

import (
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/internal/sqlident"
)

// The attributes that declare a column family, spelled as YDB spells its
// settings, in lower case. The annotation parser, the YAML reader and the
// annotation registry read these names.
const (
	AttributeName        = "name"
	AttributeTable       = "table"
	AttributeData        = "data"
	AttributeCompression = "compression"
	AttributeCacheMode   = "cache_mode"
	AttributeFields      = "fields"
)

// Default is the name of the family every YDB row table has: it holds the key
// columns and every column no other family names.
const Default = "default"

// The values COMPRESSION and CACHE_MODE take on a row table, as Ptah writes
// them. YDB takes them in any case.
const (
	CompressionOff    = "off"
	CompressionLZ4    = "lz4"
	CacheModeRegular  = "regular"
	CacheModeInMemory = "in_memory"
)

// Compressions lists the compressions a row table's family takes.
func Compressions() []string { return []string{CompressionOff, CompressionLZ4} }

// CacheModes lists the cache modes a family takes.
func CacheModes() []string { return []string{CacheModeRegular, CacheModeInMemory} }

// DeclarationError is an attribute whose value a declaration cannot carry.
type DeclarationError struct {
	// Attribute is the attribute's name.
	Attribute string
	// Value is the value it was given.
	Value string
	// Reason says what the attribute takes.
	Reason string
}

func (e *DeclarationError) Error() string {
	return fmt.Sprintf("invalid %s %q: %s", e.Attribute, e.Value, e.Reason)
}

// ParseDeclaration reads one column family out of values, keyed by attribute
// name, and ignores every key it does not name, the table included.
//
// Each value is checked for the form YDB takes, so a typo is refused where it
// was written: a family needs a name; data names a pool kind and is kept as
// written, since pool kinds are case-sensitive; compression and cache_mode
// take the values [Compressions] and [CacheModes] list, in any case, and are
// kept in lower case; fields is a comma-separated list of column names, each
// once, and the default family takes none, since it holds every column no
// other family names.
func ParseDeclaration(values map[string]string) (ast.YDBColumnFamilySpec, error) {
	spec := ast.YDBColumnFamilySpec{Name: strings.TrimSpace(values[AttributeName])}
	if spec.Name == "" {
		return ast.YDBColumnFamilySpec{}, &DeclarationError{Attribute: AttributeName, Reason: "a column family needs a name"}
	}
	if raw, ok := values[AttributeData]; ok {
		spec.Data = strings.TrimSpace(raw)
		if spec.Data == "" {
			return ast.YDBColumnFamilySpec{}, &DeclarationError{Attribute: AttributeData, Value: raw,
				Reason: "names the kind of storage pool the family is kept in, such as ssd; leave it out to keep " +
					"the database's own pool (YDB answers an empty one with `database doesn't have required storage pools`)"}
		}
	}
	var err error
	if spec.Compression, err = compression(values); err != nil {
		return ast.YDBColumnFamilySpec{}, err
	}
	if spec.CacheMode, err = choice(values, AttributeCacheMode, CacheModes()); err != nil {
		return ast.YDBColumnFamilySpec{}, err
	}
	if raw, ok := values[AttributeFields]; ok {
		if spec.Columns, err = fields(raw); err != nil {
			return ast.YDBColumnFamilySpec{}, err
		}
	}
	if spec.Name == Default && len(spec.Columns) > 0 {
		return ast.YDBColumnFamilySpec{}, &DeclarationError{Attribute: AttributeFields, Value: values[AttributeFields],
			Reason: "the default family holds the key and every column no other family names, so it lists none"}
	}
	return spec, nil
}

// compression reads the compression attribute. YDB keeps zstd for
// column-oriented tables, and a row table refuses it.
func compression(values map[string]string) (string, error) {
	raw, ok := values[AttributeCompression]
	if !ok {
		return "", nil
	}
	if strings.EqualFold(strings.TrimSpace(raw), "zstd") {
		return "", &DeclarationError{Attribute: AttributeCompression, Value: raw,
			Reason: "takes off or lz4: YDB keeps zstd for column-oriented tables, and a row table answers " +
				"`Unsupported compression value 3`"}
	}
	return choice(values, AttributeCompression, Compressions())
}

// choice reads an attribute that takes one of allowed, in any case, and keeps
// it in lower case. An absent attribute is empty.
func choice(values map[string]string, attribute string, allowed []string) (string, error) {
	raw, ok := values[attribute]
	if !ok {
		return "", nil
	}
	value := strings.ToLower(strings.TrimSpace(raw))
	if !slices.Contains(allowed, value) {
		return "", &DeclarationError{Attribute: attribute, Value: raw, Reason: "takes " + strings.Join(allowed, " or ")}
	}
	return value, nil
}

// fields reads a comma-separated list of column names.
func fields(raw string) ([]string, error) {
	var columns []string
	for part := range strings.SplitSeq(raw, ",") {
		column := strings.TrimSpace(part)
		switch {
		case column == "":
			return nil, &DeclarationError{Attribute: AttributeFields, Value: raw,
				Reason: "takes a comma-separated list of column names, none of them empty"}
		case slices.Contains(columns, column):
			return nil, &DeclarationError{Attribute: AttributeFields, Value: raw,
				Reason: fmt.Sprintf("names column %q twice", column)}
		}
		columns = append(columns, column)
	}
	return columns, nil
}

// Refusal says why a table's declared families cannot be written for a table
// with the given columns and key, or returns "". It reads what one family
// cannot see on its own: two families of one name, a column in two families,
// a column the table does not declare, and a key column outside the default
// family, which YDB refuses (`Key column 'id' must belong to the default
// family`).
func Refusal(families []ast.YDBColumnFamilySpec, columns, key []string) string {
	seen := make(map[string]bool, len(families))
	holder := make(map[string]string)
	for _, family := range families {
		if seen[family.Name] {
			return fmt.Sprintf("it declares column family %q twice (`Family %s specified more than once`)", family.Name, family.Name)
		}
		seen[family.Name] = true
		if family.Name == Default && len(family.Columns) > 0 {
			return "its default column family lists columns, and the default family holds every column no other " +
				"family names"
		}
		for _, column := range family.Columns {
			switch {
			case holder[column] != "":
				return fmt.Sprintf("column %q is in two column families, %q and %q", column, holder[column], family.Name)
			case !slices.Contains(columns, column):
				return fmt.Sprintf("column family %q names column %q, which the table does not declare", family.Name, column)
			case slices.Contains(key, column):
				return fmt.Sprintf("column family %q names key column %q, and YDB keeps every key column in the "+
					"default family (`Key column '%s' must belong to the default family`)", family.Name, column, column)
			}
			holder[column] = family.Name
		}
	}
	return ""
}

// Requirement is a capability key a table's families need, with the settings
// it covers named for a refusal.
type Requirement struct {
	// Key is the capability.
	Key capability.Capability
	// Settings names what needs it, as a refusal reads it.
	Settings string
}

// Requirements are the keys a target must hold to write families into a
// CREATE TABLE: one for any family beyond a default family that states no
// setting, and one for a cache mode, `regular` included. A list holding only
// the default family stating nothing needs none. A change of an existing
// table needs what it writes; see [ChangeRequirements].
func Requirements(families []ast.YDBColumnFamilySpec) []Requirement {
	normalized := Normalize(families)
	if len(normalized) == 0 {
		return nil
	}
	requirements := []Requirement{{Key: capability.ColumnFamilies, Settings: "column families"}}
	if slices.ContainsFunc(normalized, func(family ast.YDBColumnFamilySpec) bool { return family.CacheMode != "" }) {
		requirements = append(requirements, Requirement{Key: capability.ColumnFamilyCacheMode, Settings: "column family cache mode"})
	}
	return requirements
}

// Normalize returns families in one form, so two lists that state the same
// families compare the same: sorted by name, each column list sorted, a
// compression or cache mode in lower case, the default family listing no
// columns, and the default family left out where it states no setting. A
// setting stays as stated: `off` is a declaration, and so is `regular`.
// families is not changed, and the result shares nothing with it.
func Normalize(families []ast.YDBColumnFamilySpec) []ast.YDBColumnFamilySpec {
	var normalized []ast.YDBColumnFamilySpec
	for _, family := range families {
		family = family.Clone()
		family.Compression = strings.ToLower(strings.TrimSpace(family.Compression))
		family.CacheMode = strings.ToLower(strings.TrimSpace(family.CacheMode))
		slices.Sort(family.Columns)
		family.Columns = slices.Compact(family.Columns)
		if family.Name == Default || len(family.Columns) == 0 {
			family.Columns = nil
		}
		if family.Name == Default && !statesSettings(family) {
			continue
		}
		normalized = append(normalized, family)
	}
	slices.SortFunc(normalized, func(a, b ast.YDBColumnFamilySpec) int { return strings.Compare(a.Name, b.Name) })
	return normalized
}

// statesSettings reports whether family states any setting.
func statesSettings(family ast.YDBColumnFamilySpec) bool {
	return family.Data != "" || family.Compression != "" || family.CacheMode != "" || family.KeepInMemory
}

// Plain reports whether family holds what YDB gives a family that states no
// setting and no table profile changes: no storage pool of its own, no
// compression, the regular cache, and keep_in_memory off. A report or an
// export reads it to leave out the default family of a table nobody gave
// families; a comparison never does, since a profile can give a new table's
// family settings of its own.
func Plain(family ast.YDBColumnFamilySpec) bool {
	compression := strings.ToLower(strings.TrimSpace(family.Compression))
	cacheMode := strings.ToLower(strings.TrimSpace(family.CacheMode))
	return family.Data == "" && (compression == "" || compression == CompressionOff) &&
		(cacheMode == "" || cacheMode == CacheModeRegular) && !family.KeepInMemory
}

// Stated returns families without a default family that is [Plain]: what an
// export or a report says a table holds. families is not changed.
func Stated(families []ast.YDBColumnFamilySpec) []ast.YDBColumnFamilySpec {
	return slices.DeleteFunc(Normalize(families), func(family ast.YDBColumnFamilySpec) bool {
		return family.Name == Default && Plain(family)
	})
}

// Satisfied reports whether a table holding current has everything desired
// states: each family desired names, with each setting it states, and each
// column either side places in a family in the family desired gives it. A
// setting desired leaves out and a family it leaves out are not compared,
// since the table keeps what it holds of them.
func Satisfied(desired, current []ast.YDBColumnFamilySpec) bool {
	actions, _ := alterations(desired, current)
	return len(actions) == 0 && ChangeRefusal(desired, current) == ""
}

// Applied returns the families a table holding current holds once desired is
// applied: every family current holds, with each setting desired states
// written over the one it holds, and each family only desired names; the
// columns sit where desired places them. A CREATE TABLE that replaces the
// table writes it, so the new table keeps each setting the declaration does
// not state. Neither argument is changed.
func Applied(desired, current []ast.YDBColumnFamilySpec) []ast.YDBColumnFamilySpec {
	applied := Normalize(current)
	for i := range applied {
		applied[i].Columns = nil
	}
	for _, family := range Normalize(desired) {
		index := slices.IndexFunc(applied, func(held ast.YDBColumnFamilySpec) bool { return held.Name == family.Name })
		if index < 0 {
			applied = append(applied, family)
			continue
		}
		held := &applied[index]
		held.Data = orHeld(family.Data, held.Data)
		held.Compression = orHeld(family.Compression, held.Compression)
		held.CacheMode = orHeld(family.CacheMode, held.CacheMode)
		held.KeepInMemory = held.KeepInMemory || family.KeepInMemory
		held.Columns = family.Columns
	}
	return Normalize(applied)
}

// FamilyOf names the family families puts column in, or [Default] when no
// family lists it.
func FamilyOf(families []ast.YDBColumnFamilySpec, column string) string {
	for _, family := range families {
		if slices.Contains(family.Columns, column) {
			return family.Name
		}
	}
	return Default
}

// ColumnClause is the clause a column definition carries to sit in family:
// ` FAMILY <name>`, or nothing for the default family, which a column joins by
// naming none. A CREATE TABLE writes it after the column's type, the only
// place 25.1 takes it.
func ColumnClause(family string) string {
	if family == "" || family == Default {
		return ""
	}
	return " FAMILY " + quote(family)
}

// CreateEntries are the `FAMILY <name> (...)` entries of a CREATE TABLE that
// declare families, in the order [Normalize] gives, each with the settings it
// states. A family stating none is written with an empty list, which YDB
// takes, and the default family stating none is not written at all.
func CreateEntries(families []ast.YDBColumnFamilySpec) []string {
	normalized := Normalize(families)
	entries := make([]string, 0, len(normalized))
	for _, family := range normalized {
		entries = append(entries, "FAMILY "+quote(family.Name)+" ("+strings.Join(settings(family), ", ")+")")
	}
	return entries
}

// CreateRefusal says why a CREATE TABLE cannot write families, or returns "":
// a family that keeps its columns in memory, which no YQL family setting says.
func CreateRefusal(families []ast.YDBColumnFamilySpec) string {
	for _, family := range Normalize(families) {
		if family.KeepInMemory {
			return fmt.Sprintf("column family %q keeps its columns in memory (keep_in_memory), and YQL has no family "+
				"setting for it (`Unknown table setting: KEEP_IN_MEMORY`), so the new table would not keep them there",
				family.Name)
		}
	}
	return ""
}

// settings are the `SETTING = value` items of a family's declaration.
func settings(family ast.YDBColumnFamilySpec) []string {
	var items []string
	if family.Data != "" {
		items = append(items, "DATA = "+quoteString(family.Data))
	}
	if family.Compression != "" {
		items = append(items, "COMPRESSION = "+quoteString(family.Compression))
	}
	if family.CacheMode != "" {
		items = append(items, "CACHE_MODE = "+quoteString(family.CacheMode))
	}
	return items
}

// AlterActions are the actions of the one ALTER TABLE that gives a table
// holding current what desired states: ADD FAMILY for a family only desired
// names, ALTER FAMILY ... SET for each setting desired states and the table
// holds otherwise, and ALTER COLUMN ... SET FAMILY for each column either
// side lists whose family differs. A family is added before the columns move
// into it, so YDB never creates one with its own settings.
//
// A setting desired leaves out is not written, and a family it leaves out is
// not touched; see the package documentation. Every column either side lists
// must exist when the statement runs, so a caller leaves a column the plan
// drops out of both.
func AlterActions(desired, current []ast.YDBColumnFamilySpec) []string {
	actions, _ := alterations(desired, current)
	return actions
}

// ChangeRequirements are the keys a target must hold to run the actions
// [AlterActions] writes: none when it writes none, and the cache mode key
// when one of them writes a cache mode.
func ChangeRequirements(desired, current []ast.YDBColumnFamilySpec) []Requirement {
	actions, cacheMode := alterations(desired, current)
	if len(actions) == 0 {
		return nil
	}
	requirements := []Requirement{{Key: capability.ColumnFamilies, Settings: "column families"}}
	if cacheMode {
		requirements = append(requirements, Requirement{Key: capability.ColumnFamilyCacheMode, Settings: "column family cache mode"})
	}
	return requirements
}

// alterations are the actions [AlterActions] lists, and whether one of them
// writes a cache mode.
func alterations(desired, current []ast.YDBColumnFamilySpec) (actions []string, cacheMode bool) {
	want, have := Normalize(desired), Normalize(current)
	var adds, changes []string
	for _, family := range want {
		index := slices.IndexFunc(have, func(held ast.YDBColumnFamilySpec) bool { return held.Name == family.Name })
		if index < 0 && family.Name != Default {
			adds = append(adds, "ADD FAMILY "+quote(family.Name)+" ("+strings.Join(settings(family), ", ")+")")
			cacheMode = cacheMode || family.CacheMode != ""
			continue
		}
		var held ast.YDBColumnFamilySpec
		if index >= 0 {
			held = have[index]
		}
		changes = append(changes, settingChanges(family, held)...)
		cacheMode = cacheMode || (family.CacheMode != "" && family.CacheMode != held.CacheMode)
	}
	return append(append(adds, changes...), columnMoves(want, have)...), cacheMode
}

// settingChanges are the ALTER FAMILY ... SET actions that give one family
// each setting family states and held does not hold.
func settingChanges(family, held ast.YDBColumnFamilySpec) []string {
	prefix := "ALTER FAMILY " + quote(family.Name) + " SET "
	var changes []string
	if family.Data != "" && family.Data != held.Data {
		changes = append(changes, prefix+"DATA "+quoteString(family.Data))
	}
	if family.Compression != "" && family.Compression != held.Compression {
		changes = append(changes, prefix+"COMPRESSION "+quoteString(family.Compression))
	}
	if family.CacheMode != "" && family.CacheMode != held.CacheMode {
		changes = append(changes, prefix+"CACHE_MODE "+quoteString(family.CacheMode))
	}
	return changes
}

// columnMoves are the ALTER COLUMN ... SET FAMILY actions for every column
// either side lists whose family differs, in column order.
func columnMoves(want, have []ast.YDBColumnFamilySpec) []string {
	var columns []string
	for _, family := range slices.Concat(want, have) {
		columns = append(columns, family.Columns...)
	}
	slices.Sort(columns)
	var moves []string
	for _, column := range slices.Compact(columns) {
		target := FamilyOf(want, column)
		if target == FamilyOf(have, column) {
			continue
		}
		moves = append(moves, "ALTER COLUMN "+quote(column)+" SET FAMILY "+quote(target))
	}
	return moves
}

// ChangeRefusal says why a table holding current cannot be given what desired
// states, in place or by recreating it, or returns "": desired keeps a
// family's columns in memory where the table does not, and no YQL statement
// says so (see [CreateRefusal]). Only a read states it, as when one database
// is compared with another.
func ChangeRefusal(desired, current []ast.YDBColumnFamilySpec) string {
	have := Normalize(current)
	for _, family := range Normalize(desired) {
		index := slices.IndexFunc(have, func(held ast.YDBColumnFamilySpec) bool { return held.Name == family.Name })
		if family.KeepInMemory && (index < 0 || !have[index].KeepInMemory) {
			return fmt.Sprintf("column family %q keeps its columns in memory (keep_in_memory) on one side only, and "+
				"YQL has no family setting for it (`Unknown table setting: KEEP_IN_MEMORY`)", family.Name)
		}
	}
	return ""
}

// WithoutColumns returns families with each of columns left out of the family
// that lists it. families is not changed.
func WithoutColumns(families []ast.YDBColumnFamilySpec, columns []string) []ast.YDBColumnFamilySpec {
	return OnlyColumns(families, func(column string) bool { return !slices.Contains(columns, column) })
}

// OnlyColumns returns families with every column keep rejects left out of the
// family that lists it, and a family left with none listing nil. families is
// not changed.
func OnlyColumns(families []ast.YDBColumnFamilySpec, keep func(column string) bool) []ast.YDBColumnFamilySpec {
	out := ast.CloneYDBColumnFamilies(families)
	for i := range out {
		out[i].Columns = slices.DeleteFunc(out[i].Columns, func(column string) bool { return !keep(column) })
		if len(out[i].Columns) == 0 {
			out[i].Columns = nil
		}
	}
	return out
}

// orHeld is the value a family states, or the one it holds when it states
// none.
func orHeld(stated, held string) string {
	if stated == "" {
		return held
	}
	return stated
}

// quote writes a family or column name as a YQL identifier.
func quote(name string) string { return sqlident.Quote(platform.YDB, name) }

// quoteString writes a YQL string literal.
func quoteString(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", `\'`) + "'"
}
