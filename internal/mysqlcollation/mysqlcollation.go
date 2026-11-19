// Package mysqlcollation resolves the collations a MySQL or MariaDB table
// declaration leaves to the server: the table's default collation, and the
// collation of each column that names none. It also names the type the server
// stores a character column of the binary character set as, since that
// character set is what decides it. Two owner services read it, so the
// two sides of a comparison resolve one way: dialect/mysql/mysqlprepare
// resolves the declaration a comparison plans toward, and the creation
// projection in dialect/mysql/mysqlconvert predicts what a read of a table
// created from a schema file reports. It consults no server; the caller hands
// it the server facts it has.
package mysqlcollation

import (
	"cmp"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/schemaprep"
)

// Server holds the facts a resolution reads from a server: the default
// collation of each character set, keyed by the stored name in lower case,
// and the character set each alias such as utf8 names. A nil Aliases selects
// the target descriptor's aliases. A nil Defaults resolves nothing that needs
// a character set's default: the resolver never guesses a server's default.
type Server struct {
	Defaults map[string]string
	Aliases  map[string]string
}

// Table is what a CREATE TABLE of a declaration leaves to the server.
// Default is the table's default collation, empty where the declaration names
// neither a collation nor a character set for the table, or names a character
// set whose default the facts do not report. Inherited maps each column that
// names no collation, and holds one, to the collation it takes; it is nil
// where no column resolves.
//
// Stored maps each column the server stores under another type than the
// declared one to that type: a character type with the binary character set
// is stored as the binary type of the same length, `varchar(10) CHARACTER SET
// binary` as varbinary(10) and `text CHARACTER SET binary` as blob. It is nil
// where no column is stored so.
type Table struct {
	Default   string
	Inherited map[string]string
	Stored    map[string]string
}

// Resolve resolves table and its fields on target, MySQL or MariaDB.
//
// A table's default collation is the one it names, or the default collation
// of the character set it names; a value in the target's platform group wins
// over both, as it does where CREATE TABLE is rendered. A column of a type
// that holds a collation, which names none, takes the default collation of
// the character set it names, the national character set for NCHAR, or else
// the table's default collation. A type with an attribute that selects a
// collation of its own, such as `varchar(20) BINARY`, is left out. Another
// target wraps ptaherr.ErrUnsupportedDialect, and an owner setting of another
// type wraps schemaext.ErrInvalidValue.
func Resolve(target string, table schemamodel.Table, fields []schemamodel.Field, server Server) (Table, error) {
	target = platform.NormalizeDialect(target)
	if target != platform.MySQL && target != platform.MariaDB {
		return Table{}, fmt.Errorf("%w: MySQL collation target %q", ptaherr.ErrUnsupportedDialect, target)
	}
	described, _ := platform.Lookup(target)
	if server.Aliases == nil {
		server.Aliases = described.Columns.CollationNames.CharsetAliases
	}
	facts := facts{
		server:   server,
		collated: described.Columns.CollationNames.Types,
		national: described.Columns.NationalCharCharset,
	}
	collation, charset, err := declaredTableDefaults(table, target)
	if err != nil {
		return Table{}, err
	}
	result := Table{Default: collation}
	if result.Default == "" && charset != "" {
		result.Default = facts.charsetDefault(charset)
	}
	for _, field := range fields {
		stored, err := facts.storedType(field)
		if err != nil {
			return Table{}, err
		}
		if stored != "" {
			if result.Stored == nil {
				result.Stored = make(map[string]string)
			}
			result.Stored[field.Name] = stored
		}
		if field.Collate != "" {
			continue
		}
		inherited, err := facts.inheritedCollation(field, result.Default)
		if err != nil {
			return Table{}, err
		}
		if inherited == "" {
			continue
		}
		if result.Inherited == nil {
			result.Inherited = make(map[string]string)
		}
		result.Inherited[field.Name] = inherited
	}
	return result, nil
}

// facts are the server facts, with the aliases settled, and the descriptor's
// types that hold a collation and the character set NCHAR implies.
type facts struct {
	server   Server
	collated []string
	national string
}

// charsetDefault is the default collation of charset as the facts report it,
// with an alias such as utf8 read as the character set the server stores it
// as, and empty where they report none.
func (f facts) charsetDefault(charset string) string {
	charset = strings.ToLower(charset)
	if stored, aliased := f.server.Aliases[charset]; aliased {
		charset = stored
	}
	return f.server.Defaults[charset]
}

// declaredTableDefaults is the collation and the character set the
// declaration gives the table on target, read as CREATE TABLE renders them: a
// value in the target's platform group wins over the table's own Collate and
// over the owner's table options.
func declaredTableDefaults(table schemamodel.Table, target string) (collation, charset string, err error) {
	options, typed, err := schemaext.FacetAs[*mysqlschema.DesiredTable](table.Facets, mysqlschema.TableKind)
	if err != nil {
		return "", "", err
	}
	collation = table.Collate
	if typed && options != nil {
		charset = options.Charset
	}
	if group, found := schemaprep.PlatformOverrideGroup(table.Overrides, target); found {
		collation = cmp.Or(group["collate"], collation)
		charset = cmp.Or(group["charset"], charset)
	}
	return strings.TrimSpace(collation), strings.TrimSpace(charset), nil
}

// inheritedCollation is the collation field takes from its character set or
// its table. It is empty for a type that holds no collation, and for a
// character set whose default the facts do not report.
func (f facts) inheritedCollation(field schemamodel.Field, tableCollation string) (string, error) {
	name, collated := collatedType(field.Type, f.collated)
	if !collated {
		return "", nil
	}
	settings, _, err := mysqlschema.Settings(field.Facets)
	if err != nil {
		return "", err
	}
	charset := settings.Charset
	if charset == "" && f.national != "" && (name == "nchar" || name == "nvarchar") {
		charset = f.national
	}
	if charset != "" {
		return f.charsetDefault(charset), nil
	}
	return tableCollation, nil
}

// binaryTypes maps a character type to the binary type the server stores it
// as under the binary character set.
var binaryTypes = map[string]string{
	"char": "binary", "character": "binary", "varchar": "varbinary",
	"tinytext": "tinyblob", "text": "blob", "mediumtext": "mediumblob", "longtext": "longblob",
}

// storedType is the binary type field is stored as where it declares a
// character type with the binary character set, keeping the declared
// arguments, and empty elsewhere.
func (f facts) storedType(field schemamodel.Field) (string, error) {
	name, collated := collatedType(field.Type, f.collated)
	binary, mapped := binaryTypes[name]
	if !collated || !mapped {
		return "", nil
	}
	settings, _, err := mysqlschema.Settings(field.Facets)
	if err != nil {
		return "", err
	}
	if !strings.EqualFold(settings.Charset, "binary") {
		return "", nil
	}
	arguments := ""
	if open := strings.IndexByte(field.Type, '('); open >= 0 {
		arguments = strings.TrimSpace(field.Type[open:])
	}
	return binary + arguments, nil
}

// collatedType returns the name of a declared type that holds a collation,
// with NATIONAL CHAR read as nchar and NATIONAL VARCHAR as nvarchar. The type
// is a name from collated followed by nothing or by one argument list. An
// attribute after it, such as BINARY or ASCII, selects a collation or a
// character set of its own, so such a type is not reported and its column
// stays uncompared.
func collatedType(declared string, collated []string) (string, bool) {
	rest := strings.ToLower(strings.TrimSpace(declared))
	if after, national := strings.CutPrefix(rest, "national "); national {
		rest = "n" + strings.TrimSpace(after)
	}
	name, arguments := rest, ""
	if open := strings.IndexByte(rest, '('); open >= 0 {
		name, arguments = strings.TrimSpace(rest[:open]), rest[open:]
	}
	if arguments != "" && !strings.HasSuffix(arguments, ")") {
		return "", false
	}
	return name, slices.Contains(collated, name)
}
