// Package mytype is the MySQL type model migration lint reads out of
// statements: a column type as MySQL and MariaDB store it (base, parameters,
// sign, character set, collation and the members of an ENUM or SET), the
// MODIFY and CHANGE clauses that spell a new one, and the character set a
// table carries at a statement.
//
// It is MySQL knowledge, so it belongs to the MySQL owner
// (ptah.run/dialect/mysql/mysqllint), whose MY rules read it. It sits here,
// shared, because two common rules of ptah.run/migration/lint read it too,
// and the host must not import an owner. DS103 keeps a MySQL arm that reads
// it to tell an unchanged type from a changed one. DD103 reads the MODIFY,
// CHANGE and ALTER COLUMN type it parses on every engine that restates the
// whole column, so it serves SQL Server and ClickHouse as well as MySQL and
// MariaDB. Both arms are on the stokaro/ptah#4140 list of engine arms that
// move behind the owner seam, and this package moves with them. Until then it
// is debt, not design.
package mytype

import (
	"strconv"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintscan"
)

// Type is one column type as InnoDB stores it, with the spellings that
// name the same storage folded together so that INT(11) UNSIGNED ZEROFILL and
// INT UNSIGNED compare equal, which is what the server does.
type Type struct {
	// Base is the canonical family name: int, varchar, decimal, enum, ...
	Base string
	// Params are the parenthesized numbers that matter to storage, after
	// the display width of an integer and the (0) of a temporal are dropped.
	Params   []int
	Unsigned bool
	// Charset is the resolved character set for a character type, empty
	// otherwise; utf8 is folded to utf8mb3. ExplicitCharset reports that the
	// spelling named it, as opposed to the table default filling it in, which
	// decides which default collation a spelling without COLLATE takes.
	Charset         string
	ExplicitCharset bool
	Collate         string
	// Members is the ENUM or SET list.
	Members MemberList
	// National records the NATIONAL prefix, which fixes the character set to
	// utf8mb3 once the spelling is complete.
	National bool
}

// baseSynonyms folds the single-word spellings of one storage type.
var baseSynonyms = map[string]string{
	"INT": "int", "INTEGER": "int", "INT4": "int",
	"TINYINT": "tinyint", "INT1": "tinyint", "BOOL": "tinyint", "BOOLEAN": "tinyint",
	"SMALLINT": "smallint", "INT2": "smallint",
	"MEDIUMINT": "mediumint", "INT3": "mediumint", "MIDDLEINT": "mediumint",
	"BIGINT": "bigint", "INT8": "bigint",
	"DECIMAL": "decimal", "DEC": "decimal", "NUMERIC": "decimal", "FIXED": "decimal",
	"FLOAT": "float", "FLOAT4": "float",
	"DOUBLE": "double", "REAL": "double", "FLOAT8": "double",
	"CHAR": "char", "CHARACTER": "char", "NCHAR": "char",
	"VARCHAR": "varchar", "NVARCHAR": "varchar",
	"BINARY": "binary", "VARBINARY": "varbinary",
	"LONG": "mediumtext",
}

// CharacterBases are the families that carry a character set.
var CharacterBases = map[string]bool{
	"char": true, "varchar": true,
	"tinytext": true, "text": true, "mediumtext": true, "longtext": true,
	"enum": true, "set": true,
}

// integerBases are the families whose parenthesized number is a display
// width, which the server folds away (INT(11) -> INT is applied INSTANT).
var integerBases = map[string]bool{
	"tinyint": true, "smallint": true, "mediumint": true, "int": true, "bigint": true,
}

// charsetMaxLen is the longest encoding of one character in each
// character set, as information_schema.CHARACTER_SETS reports MAXLEN on
// both engines. The row format stores a VARCHAR's length in one byte up to
// 255 bytes of longest encoding and in two beyond, which is the line a
// widening must not cross to stay in place.
var charsetMaxLen = map[string]int{
	"armscii8": 1, "ascii": 1, "big5": 2, "binary": 1, "cp1250": 1, "cp1251": 1,
	"cp1256": 1, "cp1257": 1, "cp850": 1, "cp852": 1, "cp866": 1, "cp932": 2,
	"dec8": 1, "eucjpms": 3, "euckr": 2, "gb18030": 4, "gb2312": 2, "gbk": 2,
	"geostd8": 1, "greek": 1, "hebrew": 1, "hp8": 1, "keybcs2": 1, "koi8r": 1,
	"koi8u": 1, "latin1": 1, "latin2": 1, "latin5": 1, "latin7": 1, "macce": 1,
	"macroman": 1, "sjis": 2, "swe7": 1, "tis620": 1, "ucs2": 2, "ujis": 3,
	"utf16": 4, "utf16le": 4, "utf32": 4, "utf8mb3": 3, "utf8mb4": 4,
}

// NormalizeCharset folds the spellings of one character set: the server
// reports utf8mb3 where a migration may still write utf8.
func NormalizeCharset(name string) string {
	name = strings.ToLower(strings.Trim(name, "'\"`"))
	if name == "utf8" {
		return "utf8mb3"
	}
	return name
}

// NormalizeCollation folds a collation name the way the server reports it.
func NormalizeCollation(name string) string {
	return strings.ToLower(strings.Trim(strings.TrimSpace(name), "'\"`"))
}

// ParseAt reads a column type from statement words starting at the
// type keyword: the base, its parameters, and the modifiers that follow
// (UNSIGNED, ZEROFILL, CHARACTER SET, COLLATE, BINARY, ASCII, UNICODE). It
// returns the index of the first word that is none of those, which is where
// the column attributes begin. defaultCharset is what a character type takes
// when the spelling names none.
func ParseAt(words []string, i int, defaultCharset string) (Type, int, bool) {
	t, j, ok := parseTypeBase(words, i)
	if !ok {
		return Type{}, i, false
	}
	params, next, ok := lintkit.ParseTypeParams(words, j)
	if !ok {
		return Type{}, i, false
	}
	t.Params = params
	j = t.parseModifiers(words, next)
	return t.finish(defaultCharset), j, true
}

// parseTypeBase reads the type keyword at words[i], the member list of an
// ENUM or SET, and the second word of a two-word type.
func parseTypeBase(words []string, i int) (t Type, next int, ok bool) {
	if i >= len(words) {
		return Type{}, i, false
	}
	j := i
	switch words[j] {
	case "NATIONAL":
		t.National = true
		t.ExplicitCharset = true
		j++
		if j < len(words) && (words[j] == "CHAR" || words[j] == "CHARACTER" || words[j] == "VARCHAR") {
			t.Base = baseSynonyms[words[j]]
			j++
		}
	case "ENUM", "SET":
		list, after, listed := MemberListAt(words, j)
		if !listed {
			return Type{}, i, false
		}
		t.Base = strings.ToLower(words[j])
		t.Members = list
		j = after
	default:
		if base, known := baseSynonyms[words[j]]; known {
			t.Base = base
		} else if lintkit.IdentLike(words[j]) && !strings.HasPrefix(words[j], "`") {
			t.Base = strings.ToLower(words[j])
		}
		j++
	}
	if t.Base == "" {
		return Type{}, i, false
	}
	j = t.parseSecondWord(words, j)
	if t.Base == "serial" {
		t.Base = "bigint"
		t.Unsigned = true
	}
	return t, j, true
}

// parseSecondWord folds the two-word type spellings and returns the index
// after the second word, or j when there is none.
func (t *Type) parseSecondWord(words []string, j int) int {
	if j >= len(words) {
		return j
	}
	switch {
	case t.Base == "double" && words[j] == "PRECISION":
	case t.Base == "char" && words[j] == "VARYING":
		t.Base = "varchar"
	case t.Base == "mediumtext" && words[j] == "VARCHAR":
	case t.Base == "mediumtext" && words[j] == "VARBINARY":
		t.Base = "mediumblob"
	case t.Base == "char" && words[j] == "BYTE":
		t.Base = "binary"
	default:
		return j
	}
	return j + 1
}

// parseModifiers reads the modifiers after a type's parameters, in any
// order, into t and returns the index of the first word that is not one.
func (t *Type) parseModifiers(words []string, j int) int {
	for j < len(words) {
		switch words[j] {
		case "UNSIGNED", "ZEROFILL":
			// ZEROFILL implies UNSIGNED, and is itself display-only.
			t.Unsigned = true
		case "SIGNED":
		case "CHARACTER", "CHAR":
			if j+2 >= len(words) || words[j+1] != "SET" {
				return j
			}
			t.Charset = NormalizeCharset(words[j+2])
			t.ExplicitCharset = true
			j += 2
		case "CHARSET":
			if j+1 < len(words) {
				t.Charset = NormalizeCharset(words[j+1])
				t.ExplicitCharset = true
				j++
			}
		case "COLLATE":
			if j+1 < len(words) {
				t.Collate = NormalizeCollation(words[j+1])
				j++
			}
		case "BINARY":
			t.Collate = "binary"
		case "ASCII":
			t.Charset = "latin1"
			t.ExplicitCharset = true
		case "UNICODE":
			t.Charset = "ucs2"
			t.ExplicitCharset = true
		default:
			return j
		}
		j++
	}
	return j
}

// finish applies the defaults the server applies: the character set a
// character type takes when none is named, and the parameters a type has
// when none are written.
func (t Type) finish(defaultCharset string) Type {
	t.finishCharset(defaultCharset)
	t.finishParams()
	return t
}

func (t *Type) finishCharset(defaultCharset string) {
	if !CharacterBases[t.Base] {
		t.Charset = ""
		t.Collate = ""
		return
	}
	switch {
	case t.Charset != "":
	case t.National:
		t.Charset = "utf8mb3"
	default:
		t.Charset = NormalizeCharset(defaultCharset)
	}
	if t.Collate == "binary" && t.Charset != "" {
		t.Collate = t.Charset + "_bin"
	}
}

func (t *Type) finishParams() {
	switch {
	case integerBases[t.Base], t.Base == "year":
		t.Params = nil
	case t.Base == "decimal":
		switch len(t.Params) {
		case 0:
			t.Params = []int{10, 0}
		case 1:
			t.Params = []int{t.Params[0], 0}
		}
	case t.Base == "float" && len(t.Params) == 1:
		if t.Params[0] > 24 {
			t.Base = "double"
		}
		t.Params = nil
	case t.Base == "char" || t.Base == "binary" || t.Base == "bit":
		if len(t.Params) == 0 {
			t.Params = []int{1}
		}
	case t.Base == "time" || t.Base == "datetime" || t.Base == "timestamp":
		if len(t.Params) == 1 && t.Params[0] == 0 {
			t.Params = nil
		}
	}
}

// ParseSpelling reads the type as information_schema.COLUMN_TYPE
// spells it -- `int(11) unsigned zerofill`, `varchar(20)`, `enum('a','b')`
// -- through the same tokenizer and parser the statement goes through, so
// the two sides cannot fold spellings differently.
func ParseSpelling(columnType, charset string) (Type, bool) {
	spelling := strings.TrimSpace(columnType)
	if spelling == "" {
		return Type{}, false
	}
	words := lintscan.Words(spelling, lintscan.ForDialect("mysql"))
	t, next, ok := ParseAt(words, 0, charset)
	if !ok || next != len(words) {
		return Type{}, false
	}
	return t, true
}

// Spell renders the canonical form for a message, the way the server would
// print it.
func (t Type) Spell() string {
	var b strings.Builder
	b.WriteString(t.Base)
	switch {
	case t.Members.Kind != 0:
		b.WriteString("(" + QuoteMembers(t.Members.Members) + ")")
	case len(t.Params) > 0:
		b.WriteString("(")
		for i, p := range t.Params {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(strconv.Itoa(p))
		}
		b.WriteString(")")
	}
	if t.Unsigned {
		b.WriteString(" unsigned")
	}
	return b.String()
}

// SameStorage reports whether two types store a value the same way: the
// same base, sign and parameters.
func (t Type) SameStorage(o Type) bool {
	if t.Base != o.Base || t.Unsigned != o.Unsigned || len(t.Params) != len(o.Params) {
		return false
	}
	for i := range t.Params {
		if t.Params[i] != o.Params[i] {
			return false
		}
	}
	return true
}

// VarcharBytes is the longest encoding of a VARCHAR(n) in its character
// set, or zero when the set is not known.
func (t Type) VarcharBytes() int {
	if len(t.Params) != 1 {
		return 0
	}
	return t.Params[0] * charsetMaxLen[t.Charset]
}

// ChangeSite is one MODIFY or CHANGE clause with its new type.
type ChangeSite struct {
	// Statement is the position of the statement in its file.
	Statement int
	// Table is the altered table; OldName and NewName the column before and
	// after, the same name for a MODIFY.
	Table   lintkit.TableReference
	OldName string
	NewName string
	// TypeStart is the word index of the new type.
	TypeStart int
}

// ChangeSites finds every MODIFY or CHANGE clause of a file's
// statements that spells a type.
func ChangeSites(statements []lintext.Statement) []ChangeSite {
	var sites []ChangeSite
	for index := range statements {
		stmt := &statements[index]
		w := stmt.Words
		if !lintkit.IsAlterTable(w) {
			continue
		}
		table := lintkit.AlterTableReference(w, stmt.SourceWords)
		for _, i := range lintkit.ClauseStarts(w) {
			if i >= len(w) || (w[i] != "MODIFY" && w[i] != "CHANGE") {
				continue
			}
			change := w[i] == "CHANGE"
			j := i + 1
			if j < len(w) && w[j] == "COLUMN" {
				j++
			}
			j = lintkit.SkipIfExists(w, j)
			if j >= len(w) || !lintkit.IdentLike(w[j]) {
				continue
			}
			oldName := lintkit.SourceWordAt(w, stmt.SourceWords, j)
			newName := oldName
			j++
			if change {
				if j >= len(w) || !lintkit.IdentLike(w[j]) {
					continue
				}
				newName = lintkit.SourceWordAt(w, stmt.SourceWords, j)
				j++
			}
			if _, _, ok := ParseAt(w, j, ""); !ok {
				continue
			}
			sites = append(sites, ChangeSite{
				Statement: index,
				Table:     table,
				OldName:   oldName,
				NewName:   newName,
				TypeStart: j,
			})
		}
	}
	return sites
}

// TableCharsetBefore is the default character set a table has when the
// statement at index runs: the baseline's answer, unless an earlier
// statement of the same file changed it.
func TableCharsetBefore(statements []lintext.Statement, index int, table lintkit.TableReference, baseline string) string {
	charset := baseline
	for i := range index {
		stmt := &statements[i]
		w := stmt.Words
		if !lintkit.IsAlterTable(w) || lintkit.AlterTableReference(w, stmt.SourceWords).Normalized != table.Normalized {
			continue
		}
		for _, j := range lintkit.ClauseStarts(w) {
			if target, ok := TableCharsetClause(w, j); ok {
				charset = target
			}
		}
	}
	return charset
}

// TableCharsetClause reads the character set a clause assigns to the table:
// CONVERT TO CHARACTER SET x, [DEFAULT] CHARACTER SET [=] x, or CHARSET [=] x.
// DEFAULT alone, which takes the database's default, resolves nothing.
func TableCharsetClause(w []string, j int) (string, bool) {
	k := j
	switch {
	case k+1 < len(w) && w[k] == "CONVERT" && w[k+1] == "TO":
		k += 2
	case k < len(w) && w[k] == "CONVERT":
		return "", false
	case k < len(w) && w[k] == "DEFAULT":
		k++
	}
	switch {
	case k+1 < len(w) && w[k] == "CHARACTER" && w[k+1] == "SET":
		k += 2
	case k < len(w) && w[k] == "CHARSET":
		k++
	default:
		return "", false
	}
	if k < len(w) && w[k] == "=" {
		k++
	}
	if k >= len(w) || w[k] == "DEFAULT" || !lintkit.IdentLike(w[k]) {
		return "", false
	}
	return NormalizeCharset(w[k]), true
}

// MemberKind is which of the two list-valued MySQL types a column has.
type MemberKind int

// The two list-valued types.
const (
	// Enum is ENUM.
	Enum MemberKind = iota + 1
	// Set is SET.
	Set
)

// String is the keyword of the type.
func (k MemberKind) String() string {
	if k == Set {
		return "SET"
	}
	return "ENUM"
}

// MemberList is the ordered members of an ENUM or SET, with the quoting and
// escapes of the DDL resolved so two spellings of one value compare equal.
type MemberList struct {
	// Kind is ENUM or SET, and Members the values in declaration order.
	Kind    MemberKind
	Members []string
}

// MemberListAt reads ENUM(...) or SET(...) from the statement words starting
// at words[i], where the type keyword is expected. It returns the list and the
// index of the word after the closing parenthesis.
func MemberListAt(words []string, i int) (MemberList, int, bool) {
	if i+1 >= len(words) || words[i+1] != "(" {
		return MemberList{}, i, false
	}
	kind, ok := MemberKindOf(words[i])
	if !ok {
		return MemberList{}, i, false
	}
	var members []string
	j := i + 2
	for j < len(words) {
		word := words[j]
		if word == ")" {
			return MemberList{Kind: kind, Members: members}, j + 1, len(members) > 0
		}
		if word == "," {
			j++
			continue
		}
		value, ok := UnquoteMember(word)
		if !ok {
			return MemberList{}, i, false
		}
		members = append(members, value)
		j++
	}
	return MemberList{}, i, false
}

// MemberKindOf is the list-valued type a keyword names, if it names one.
func MemberKindOf(keyword string) (MemberKind, bool) {
	switch strings.ToUpper(strings.TrimSpace(keyword)) {
	case "ENUM":
		return Enum, true
	case "SET":
		return Set, true
	}
	return 0, false
}

// UnquoteMember resolves one single-quoted DDL literal to the value the
// server stores. MySQL trims trailing spaces from a member when the column is
// created, so the value is compared trimmed the same way; the server's own
// spelling of the type never carries them.
func UnquoteMember(word string) (string, bool) {
	if len(word) < 2 || word[0] != '\'' || word[len(word)-1] != '\'' {
		return "", false
	}
	inner := word[1 : len(word)-1]
	var b strings.Builder
	for i := 0; i < len(inner); i++ {
		c := inner[i]
		switch {
		case c == '\\' && i+1 < len(inner):
			i++
			b.WriteByte(inner[i])
		case c == '\'' && i+1 < len(inner) && inner[i+1] == '\'':
			i++
			b.WriteByte('\'')
		default:
			b.WriteByte(c)
		}
	}
	return strings.TrimRight(b.String(), " "), true
}

// QuoteMembers spells members back the way the DDL does, for a message.
func QuoteMembers(members []string) string {
	quoted := make([]string, 0, len(members))
	for _, member := range members {
		quoted = append(quoted, "'"+strings.ReplaceAll(member, "'", "''")+"'")
	}
	return strings.Join(quoted, ", ")
}
