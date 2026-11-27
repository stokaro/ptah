package mysqlgrants

import (
	"slices"
	"strings"

	"ptah.run/internal/mysqlname"
)

// Lookup is the way a MySQL session finds the database-level row it applies.
// MariaDB finds it one way whatever the session holds, so on MariaDB the value
// changes nothing.
type Lookup uint8

const (
	// AccountLookup is a MySQL session with no active role: the server ranks
	// the account's own rows.
	AccountLookup Lookup = iota
	// RoleLookup is a MySQL session with an active role: the server joins the
	// account's own rows with those of every active role and the roles they
	// inherit, and looks the database up in that join.
	RoleLookup
)

// ApplyingRows returns which of rows, the database names of the
// database-level grant rows one subject holds, the server applies to database.
// A subject is what one lookup reads: on MySQL the account's own rows, or with
// RoleLookup those rows and the rows of every active role and of the roles
// they inherit; on MariaDB the account's own rows, or the rows of the active
// role and of the roles it inherits, or PUBLIC's rows. MariaDB adds what its
// three subjects hold.
//
// The server applies one row, and the privileges a subject holds at the
// database level are those of that row alone: a second row that matches adds
// nothing, even where it holds more. Rows that spell the same name are one
// row, which holds what each of them holds. The result holds each applying
// name once, sorted. It is empty where no row applies, and holds several names
// where the order the server tries them in cannot be established. The server
// applies one of those, and a caller settles the doubt toward the mistake it
// can afford: the security analysis counts what any of them holds, and the
// session privilege check only what all of them hold.
//
// Measured on MySQL 8.4.13 and 26.10.0 and on MariaDB 11.8.9 and 12.3.3, each
// with roles holding the rows and a session that activates them:
//
//   - A row whose name is the database's, a literal row, shadows every pattern,
//     even where it holds only EVENT, and whichever role of the subject holds
//     it: GRANT SELECT ON `qa%`.* beside GRANT INSERT ON qabc.* lets the
//     session insert into qabc and not read it.
//   - MySQL with RoleLookup applies the row whose name equals the database
//     byte for byte. Without one, it applies the pattern, a name holding `%`
//     or `_`, that sorts first byte for byte among those that match: `q_bc`
//     before `qa%`, and `qa%` before `qab%`. A name with neither character
//     that differs from the database applies nowhere, backslashes included:
//     `qa\bc` reaches no database qabc.
//   - MySQL with AccountLookup ranks the rows as its acl_dbs sort does: a name
//     without `%` or `_` first, then the name whose first wildcard comes later,
//     `qab%` before `qa%` and `qa%` before `q_bc`. A backslash escapes the next
//     byte, so `qa\bc` reaches qabc.
//   - MariaDB applies the row whose pattern matches the fewest names, as its
//     get_magic_sort orders them: it weighs the number of `%`, the number of
//     `_` and the literal bytes, and breaks a tie on the literal bytes before
//     the first wildcard, fewer first. `qab%` comes before `qa%`, `q_bc`
//     before `qa%`, `____` before `qab%`, `qa%` before `q_%` and `q%c` before
//     `qa%`. A backslash escapes the next byte, so `qa\bc` is literal and
//     reaches qabc.
//
// Names that rank equal are returned together: on MySQL with AccountLookup two
// patterns whose first wildcard is at the same byte, such as `qa%` and `qa_c`,
// and on MariaDB two patterns of the same weights, such as `%_bc` and `_%bc`,
// which the server orders by an unstable sort. Measured on MariaDB 11.8.9,
// `q_%c` and `q%_c` rank equal and the server applied `q_%c`. Where
// [Options.FoldCase] holds, MySQL with RoleLookup is ordered both on the names
// as written and folded, and a disagreement returns both: with
// lower_case_table_names=1 the server stores the names folded and the two
// orders agree, and no server with lower_case_table_names=2 was measured. With
// [Options.LiteralDatabaseNames], MySQL with RoleLookup keeps its rule, and a
// pattern is a name holding `%` or `_` that reaches the database as
// [Options.RoleRowReaches] says: of `X_Y` and `x_y` on the database x_y, a
// session applies `X_Y` alone, measured on MySQL 8.4.13 and 26.10.0. With
// AccountLookup every row that names the database applies together, which no
// server was measured to order.
//
// The revoke refusal does not use this: it treats every row that matches as
// one that may apply. That fails closed for a refusal, which must not plan a
// revoke a shadowed pattern would undo, and keeping the two policies apart is
// deliberate.
func (o Options) ApplyingRows(lookup Lookup, database string, rows []string) []string {
	names := distinct(rows)
	var applying []string
	switch {
	case !o.MariaDB && lookup == RoleLookup:
		applying = o.roleLookup(database, names)
	case o.LiteralDatabaseNames:
		applying = o.matching(database, names)
	case o.MariaDB:
		applying = ranked(o.matching(database, names), func(name string) (int, bool) {
			magic, ok := mariaDBMagic(name)
			return -magic, ok
		})
	default:
		applying = ranked(o.matching(database, names), func(name string) (int, bool) { return mySQLSort(name), true })
	}
	if len(applying) == 0 {
		return nil
	}
	slices.Sort(applying)
	return applying
}

// roleLookup is MySQL's lookup with an active role: a literal row equal to the
// database, else the first pattern that matches in byte order.
func (o Options) roleLookup(database string, names []string) []string {
	var exact, patterns []string
	for _, name := range names {
		switch {
		case !strings.ContainsAny(name, "%_"):
			if mysqlname.NamesOf(o.FoldCase).Same(name, database) {
				exact = append(exact, name)
			}
		case o.RoleRowReaches(name, database):
			patterns = append(patterns, name)
		}
	}
	if len(exact) > 0 {
		return exact
	}
	if len(patterns) == 0 {
		return nil
	}
	first := slices.Min(patterns)
	if !o.FoldCase {
		return []string{first}
	}
	folded := slices.MinFunc(patterns, func(a, b string) int { return strings.Compare(strings.ToLower(a), strings.ToLower(b)) })
	if folded == first {
		return []string{first}
	}
	return []string{first, folded}
}

// matching keeps the names whose row reaches database.
func (o Options) matching(database string, names []string) []string {
	return slices.DeleteFunc(names, func(name string) bool { return !o.MatchesDatabase(name, database) })
}

// ranked returns the names of the highest rank, every one of them where
// several share it. A name rank cannot place makes the order unknown, and
// then every name is returned.
func ranked(names []string, rank func(string) (int, bool)) []string {
	if len(names) < 2 {
		return names
	}
	ranks := make([]int, len(names))
	for i, name := range names {
		value, ok := rank(name)
		if !ok {
			return names
		}
		ranks[i] = value
	}
	best := slices.Max(ranks)
	var top []string
	for i, name := range names {
		if ranks[i] == best {
			top = append(top, name)
		}
	}
	return top
}

// mySQLSort is the database part of the value MySQL's get_sort gives a row,
// higher first: 128 for a name without a wildcard, 0 for an empty one, 1 for
// `%` alone, and otherwise the byte offset of the first wildcard plus two,
// capped at 127. A backslash takes the next byte literally.
func mySQLSort(name string) int {
	chars := 0
	for i := 0; i < len(name); i++ {
		switch {
		case name[i] == '\\' && i+1 < len(name):
			i++
		case name[i] == '%' || name[i] == '_':
			if name == "%" {
				return 1
			}
			return min(i+2, 127)
		}
		chars = 128
	}
	return chars
}

// mariaDBMaxNameLength is MariaDB's max_dbname_length, NAME_CHAR_LEN.
const mariaDBMaxNameLength = 64

// mariaDBMagic is the database part of the value MariaDB's get_magic_sort
// gives a row, lower first: it grows with the number of names the pattern
// matches. It counts bytes, as the server does, and reports false for a
// pattern whose literal bytes and `_` exceed the longest name, where the
// server's unsigned arithmetic wraps and the order is unknown.
func mariaDBMagic(name string) (int, bool) {
	var literal, one, many, prefix int
	for i := 0; i < len(name); i++ {
		switch {
		case name[i] == '%':
			if many == 0 && one == 0 {
				prefix = literal
			}
			many++
			for i+1 < len(name) && name[i+1] == '%' {
				i++
			}
			continue
		case name[i] == '_':
			if many == 0 && one == 0 {
				prefix = literal
			}
			one++
			continue
		case name[i] == '\\' && i+1 < len(name):
			i++
		}
		literal++
	}
	many, one = min(many, 31), min(one, 31)
	rest := 0
	if many > 0 {
		if literal+one > mariaDBMaxNameLength {
			return 0, false
		}
		rest = mariaDBMaxNameLength - literal - one
	}
	const d, d1 = mariaDBMaxNameLength + 1, 32
	if rest > one {
		return (((rest*2+1)*d+many)*d1+one)*d + prefix, true
	}
	return ((one*2*d+rest)*d1+many)*d + prefix, true
}

// distinct returns names without repeats, in the order they first appear.
func distinct(names []string) []string {
	seen := make(map[string]bool, len(names))
	var out []string
	for _, name := range names {
		if !seen[name] {
			seen[name] = true
			out = append(out, name)
		}
	}
	return out
}
