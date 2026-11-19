// Package mysqlprepare resolves, before the shared comparison, the collations
// a MySQL or MariaDB declaration leaves to the server: the collation of a
// column that names none, and the table's default collation. The character
// sets that decide them are the MySQL owner's settings, so the resolution
// belongs to the owner, and the comparison reads the resolved names only.
package mysqlprepare

import (
	"cmp"
	"context"
	"fmt"
	"slices"
	"strings"

	"ptah.run/core/platform"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
	"ptah.run/core/schemapreparation"
	"ptah.run/dialect/mysql/mysqlschema"
	"ptah.run/internal/schemaprep"
)

// Service resolves inherited and default collations. The zero value is usable
// and safe for concurrent calls; no live server is consulted. It reads the
// character set defaults and aliases the request carries from the read
// ([schemapreparation.Request.CharsetDefaultCollations] and
// [schemapreparation.Request.CharsetAliases]), and resolves nothing that needs
// a character set the request does not name.
type Service struct{}

// PrepareTables returns independent captures with
// [schemapreparation.Table.InheritedCollations] and
// [schemapreparation.Table.DefaultCollation] set where the declaration leaves
// the collation to the server and the server's defaults decide it.
//
// A table's default collation is the one it names, or the default collation of
// the character set it names. A column of a type that holds a collation, which
// names none, takes the default collation of the character set it names, the
// national character set for NCHAR, or else the table's default collation. A
// table that names neither leaves its columns and itself unresolved: the
// database's default decides them, and the comparison does not compare them.
// Only MySQL and MariaDB are accepted; cancellation returns no result.
func (Service) PrepareTables(ctx context.Context, request schemapreparation.Request) (schemapreparation.Result, error) {
	if ctx == nil {
		return schemapreparation.Result{}, fmt.Errorf("%w: preparation requires a context", schemapreparation.ErrInvalid)
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	target := platform.NormalizeDialect(request.Target)
	if target != platform.MySQL && target != platform.MariaDB {
		return schemapreparation.Result{}, fmt.Errorf("%w: MySQL preparation target %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	described, _ := platform.Lookup(target)
	request = request.Clone()
	aliases := request.CharsetAliases
	if aliases == nil {
		aliases = described.Columns.CollationNames.CharsetAliases
	}
	server := serverDefaults{
		collations: request.CharsetDefaultCollations,
		aliases:    aliases,
		collated:   described.Columns.CollationNames.Types,
		national:   described.Columns.NationalCharCharset,
	}
	for i := range request.Tables {
		if err := server.prepareTable(&request.Tables[i], target); err != nil {
			return schemapreparation.Result{}, err
		}
	}
	if err := ctx.Err(); err != nil {
		return schemapreparation.Result{}, err
	}
	return schemapreparation.Result{Complete: true, Tables: request.Tables}, nil
}

// serverDefaults are the facts a resolution reads: the read's default
// collation of each character set and its character set aliases, which fall
// back to the target descriptor's where the read reports none, and the
// descriptor's types that hold a collation and the character set NCHAR
// implies.
type serverDefaults struct {
	collations map[string]string
	aliases    map[string]string
	collated   []string
	national   string
}

// charsetDefault is the default collation of charset as the read reports it,
// with an alias such as utf8 read as the character set the server stores it
// as, and empty where the read reports none.
func (s serverDefaults) charsetDefault(charset string) string {
	charset = strings.ToLower(charset)
	if stored, aliased := s.aliases[charset]; aliased {
		charset = stored
	}
	return s.collations[charset]
}

// prepareTable resolves one table's default collation and the collations its
// columns inherit.
func (s serverDefaults) prepareTable(table *schemapreparation.Table, target string) error {
	collation, charset, err := declaredTableDefaults(table.Desired.Table, target)
	if err != nil {
		return err
	}
	tableCollation := collation
	if tableCollation == "" && charset != "" {
		tableCollation = s.charsetDefault(charset)
	}
	table.DefaultCollation = tableCollation
	for _, field := range table.Desired.Fields {
		if field.Collate != "" {
			continue
		}
		inherited, err := s.inheritedCollation(field, tableCollation)
		if err != nil {
			return err
		}
		if inherited == "" {
			continue
		}
		if table.InheritedCollations == nil {
			table.InheritedCollations = make(map[string]string)
		}
		table.InheritedCollations[field.Name] = inherited
	}
	return nil
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
// character set whose default the read does not report.
func (s serverDefaults) inheritedCollation(field schemamodel.Field, tableCollation string) (string, error) {
	name, collated := collatedType(field.Type, s.collated)
	if !collated {
		return "", nil
	}
	settings, _, err := mysqlschema.Settings(field.Facets)
	if err != nil {
		return "", err
	}
	charset := settings.Charset
	if charset == "" && s.national != "" && (name == "nchar" || name == "nvarchar") {
		charset = s.national
	}
	if charset != "" {
		return s.charsetDefault(charset), nil
	}
	return tableCollation, nil
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
