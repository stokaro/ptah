package lint

import (
	"fmt"
	"strconv"
	"strings"
)

// InnoDB applies some ALTER TABLE clauses to the table's metadata, rebuilds
// the table in place for others while writes continue, and for the rest
// refuses every online algorithm and copies the whole table row by row with
// writes blocked. The rules in this file name the third class for the three
// clauses Atlas reports it for: a column type change (MY130), a primary key
// dropped with no replacement (MY133), and a table converted to another
// character set (MY136). Each fires only where the copy is established, so
// a MODIFY that widens a VARCHAR by ten characters, which InnoDB applies in
// place, is not reported as a copy because it is spelled with the same
// keyword as one that is.
//
// Every claim below was measured on MySQL 8.4.11 and MariaDB 11.8.9 by
// asking for ALGORITHM=INSTANT, then INPLACE, then COPY, and for the in-place
// forms also LOCK=NONE. Both servers agreed unless a line says otherwise
// (stokaro/ptah#2942):
//
//	column type change                            INSTANT   INPLACE   LOCK=NONE
//	INT -> BIGINT, INT -> INT UNSIGNED             refused   refused
//	DECIMAL(10,2) -> DECIMAL(12,2), DATETIME(6)    refused   refused
//	TEXT -> LONGTEXT, VARCHAR -> CHAR, ENUM -> VARCHAR
//	                                               refused   refused
//	VARCHAR(20) -> VARCHAR(10)                     refused   refused
//	VARCHAR(10) -> VARCHAR(20) utf8mb4             MySQL refused, MariaDB ok
//	                                                         ok        ok
//	VARCHAR(100) -> VARCHAR(200) utf8mb4           same as the line above
//	VARCHAR(60) -> VARCHAR(70) utf8mb4             refused   refused
//	  (240 to 280 bytes: the length prefix grows from one byte to two)
//	VARCHAR(200) -> VARCHAR(300) latin1            refused   refused
//	VARCHAR(10) latin1 -> utf8mb4                  refused   refused
//	VARCHAR(10) utf8mb3 -> utf8mb4                 MySQL refused, MariaDB ok
//	                                                         ok        ok
//	collation change alone                         MySQL refused, MariaDB ok
//	                                                         ok
//	collation change on an indexed column          MySQL: refused refused
//	                                               MariaDB: refused ok
//	INT(11) -> INT, INT -> INTEGER, DECIMAL(10) -> DECIMAL(10,0),
//	DOUBLE -> DOUBLE PRECISION, BOOLEAN -> TINYINT(1), CHAR -> CHAR(1),
//	INT UNSIGNED -> INT UNSIGNED ZEROFILL           ok        ok
//	nullability, DEFAULT, or COMMENT alone          not a type change
//
//	primary key                                   INSTANT   INPLACE   LOCK=NONE
//	DROP PRIMARY KEY alone                         refused   refused
//	  MariaDB with another NOT NULL UNIQUE key:              ok        ok
//	DROP PRIMARY KEY, ADD PRIMARY KEY (...)        refused   ok        ok
//	ADD PRIMARY KEY (...)                          refused   ok        ok
//
//	table character set                           INSTANT   INPLACE   LOCK=NONE
//	CONVERT TO CHARACTER SET, latin1 -> utf8mb4    refused   refused
//	  (likewise ascii, latin2, utf8mb3 <-> latin1, utf16, ucs2: every pair
//	  measured except the one below)
//	CONVERT TO ..., utf8mb3 -> utf8mb4
//	  VARCHAR(63) or CHAR                          MySQL refused, MariaDB ok
//	                                                         ok        ok
//	  VARCHAR(64) (192 to 256 bytes) or TEXT       refused   refused
//	  ENUM or SET                                  MySQL: in place
//	                                               MariaDB: refused refused
//	  VARCHAR with an index                        MySQL: refused refused
//	                                               MariaDB: in place
//	CONVERT TO ... with no character column, or
//	  every column already in the target set       in place
//	DEFAULT CHARACTER SET = ..., COLLATE = ...     in place  ok        ok
//
// A refusal of INSTANT and INPLACE is the server saying the change needs
// ALGORITHM=COPY, and the message MySQL prints for it is "Cannot change
// column type. Try ALGORITHM=COPY". For a primary key it says "Dropping a
// primary key is not allowed without also adding a new primary key".
//
// The data consequence of a copy was measured too: a row whose value the
// new type cannot hold fails the copy with "Out of range value" or "Data
// truncated" under the strict SQL mode both servers default to, and is
// clamped or truncated when strict mode is off (300 into TINYINT reads back
// as 127, twenty characters into VARCHAR(5) as five).
//
// The two things the file does not know are indexes and the engine's
// treatment of a compatible conversion on an indexed column, so a collation
// change and a utf8mb3 to utf8mb4 conversion of a short VARCHAR are left
// unreported rather than claimed either way. The type comparison reads the
// column's current spelling and character set from the schema state the
// version starts from ([BaselineColumn.ColumnType], [BaselineColumn.Charset]
// and [BaselineColumn.TableCharset]); without that state MY130 and MY136
// stay quiet, DS103 and MY101 still report the statement, and
// [Analysis.UnmetInputs] names them.

// mysqlType is one column type as InnoDB stores it, with the spellings that
// name the same storage folded together so that INT(11) UNSIGNED ZEROFILL and
// INT UNSIGNED compare equal, which is what the server does.
type mysqlType struct {
	// base is the canonical family name: int, varchar, decimal, enum, ...
	base string
	// params are the parenthesized numbers that matter to storage, after
	// the display width of an integer and the (0) of a temporal are dropped.
	params   []int
	unsigned bool
	// charset is the resolved character set for a character type, empty
	// otherwise; utf8 is folded to utf8mb3.
	charset string
	collate string
	// members is the ENUM or SET list.
	members memberList
	// national records the NATIONAL prefix, which fixes the character set to
	// utf8mb3 once the spelling is complete.
	national bool
}

// mysqlBaseSynonyms folds the single-word spellings of one storage type.
var mysqlBaseSynonyms = map[string]string{
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

// mysqlCharacterBases are the families that carry a character set.
var mysqlCharacterBases = map[string]bool{
	"char": true, "varchar": true,
	"tinytext": true, "text": true, "mediumtext": true, "longtext": true,
	"enum": true, "set": true,
}

// mysqlIntegerBases are the families whose parenthesized number is a display
// width, which the server folds away (INT(11) -> INT is applied INSTANT).
var mysqlIntegerBases = map[string]bool{
	"tinyint": true, "smallint": true, "mediumint": true, "int": true, "bigint": true,
}

// mysqlTextBases are the families whose conversion between character sets
// is never applied in place, superset or not.
var mysqlTextBases = map[string]bool{
	"tinytext": true, "text": true, "mediumtext": true, "longtext": true,
}

// mysqlCharsetMaxLen is the longest encoding of one character in each
// character set, as information_schema.CHARACTER_SETS reports MAXLEN on
// both engines. The row format stores a VARCHAR's length in one byte up to
// 255 bytes of longest encoding and in two beyond, which is the line a
// widening must not cross to stay in place.
var mysqlCharsetMaxLen = map[string]int{
	"armscii8": 1, "ascii": 1, "big5": 2, "binary": 1, "cp1250": 1, "cp1251": 1,
	"cp1256": 1, "cp1257": 1, "cp850": 1, "cp852": 1, "cp866": 1, "cp932": 2,
	"dec8": 1, "eucjpms": 3, "euckr": 2, "gb18030": 4, "gb2312": 2, "gbk": 2,
	"geostd8": 1, "greek": 1, "hebrew": 1, "hp8": 1, "keybcs2": 1, "koi8r": 1,
	"koi8u": 1, "latin1": 1, "latin2": 1, "latin5": 1, "latin7": 1, "macce": 1,
	"macroman": 1, "sjis": 2, "swe7": 1, "tis620": 1, "ucs2": 2, "ujis": 3,
	"utf16": 4, "utf16le": 4, "utf32": 4, "utf8mb3": 3, "utf8mb4": 4,
}

// normalizeCharset folds the spellings of one character set: the server
// reports utf8mb3 where a migration may still write utf8.
func normalizeCharset(name string) string {
	name = strings.ToLower(strings.Trim(name, "'\"`"))
	if name == "utf8" {
		return "utf8mb3"
	}
	return name
}

// parseMySQLTypeAt reads a column type from statement words starting at the
// type keyword: the base, its parameters, and the modifiers that follow
// (UNSIGNED, ZEROFILL, CHARACTER SET, COLLATE, BINARY, ASCII, UNICODE). It
// returns the index of the first word that is none of those, which is where
// the column attributes begin. defaultCharset is what a character type takes
// when the spelling names none.
func parseMySQLTypeAt(words []string, i int, defaultCharset string) (mysqlType, int, bool) {
	t, j, ok := parseTypeBase(words, i)
	if !ok {
		return mysqlType{}, i, false
	}
	params, next, ok := parseTypeParams(words, j)
	if !ok {
		return mysqlType{}, i, false
	}
	t.params = params
	j = t.parseModifiers(words, next)
	return t.finish(defaultCharset), j, true
}

// parseTypeBase reads the type keyword at words[i], the member list of an
// ENUM or SET, and the second word of a two-word type.
func parseTypeBase(words []string, i int) (t mysqlType, next int, ok bool) {
	if i >= len(words) {
		return mysqlType{}, i, false
	}
	j := i
	switch words[j] {
	case "NATIONAL":
		t.national = true
		j++
		if j < len(words) && (words[j] == "CHAR" || words[j] == "CHARACTER" || words[j] == "VARCHAR") {
			t.base = mysqlBaseSynonyms[words[j]]
			j++
		}
	case "ENUM", "SET":
		list, after, listed := memberListAt(words, j)
		if !listed {
			return mysqlType{}, i, false
		}
		t.base = strings.ToLower(words[j])
		t.members = list
		j = after
	default:
		if base, known := mysqlBaseSynonyms[words[j]]; known {
			t.base = base
		} else if identLike(words[j]) && !strings.HasPrefix(words[j], "`") {
			t.base = strings.ToLower(words[j])
		}
		j++
	}
	if t.base == "" {
		return mysqlType{}, i, false
	}
	j = t.parseSecondWord(words, j)
	if t.base == "serial" {
		t.base = "bigint"
		t.unsigned = true
	}
	return t, j, true
}

// parseSecondWord folds the two-word type spellings and returns the index
// after the second word, or j when there is none.
func (t *mysqlType) parseSecondWord(words []string, j int) int {
	if j >= len(words) {
		return j
	}
	switch {
	case t.base == "double" && words[j] == "PRECISION":
	case t.base == "char" && words[j] == "VARYING":
		t.base = "varchar"
	case t.base == "mediumtext" && words[j] == "VARCHAR":
	case t.base == "mediumtext" && words[j] == "VARBINARY":
		t.base = "mediumblob"
	case t.base == "char" && words[j] == "BYTE":
		t.base = "binary"
	default:
		return j
	}
	return j + 1
}

// parseModifiers reads the modifiers after a type's parameters, in any
// order, into t and returns the index of the first word that is not one.
func (t *mysqlType) parseModifiers(words []string, j int) int {
	for j < len(words) {
		switch words[j] {
		case "UNSIGNED", "ZEROFILL":
			// ZEROFILL implies UNSIGNED, and is itself display-only.
			t.unsigned = true
		case "SIGNED":
		case "CHARACTER", "CHAR":
			if j+2 >= len(words) || words[j+1] != "SET" {
				return j
			}
			t.charset = normalizeCharset(words[j+2])
			j += 2
		case "CHARSET":
			if j+1 < len(words) {
				t.charset = normalizeCharset(words[j+1])
				j++
			}
		case "COLLATE":
			if j+1 < len(words) {
				t.collate = strings.ToLower(strings.Trim(words[j+1], "'\"`"))
				j++
			}
		case "BINARY":
			t.collate = "binary"
		case "ASCII":
			t.charset = "latin1"
		case "UNICODE":
			t.charset = "ucs2"
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
func (t mysqlType) finish(defaultCharset string) mysqlType {
	t.finishCharset(defaultCharset)
	t.finishParams()
	return t
}

func (t *mysqlType) finishCharset(defaultCharset string) {
	if !mysqlCharacterBases[t.base] {
		t.charset = ""
		t.collate = ""
		return
	}
	switch {
	case t.charset != "":
	case t.national:
		t.charset = "utf8mb3"
	default:
		t.charset = normalizeCharset(defaultCharset)
	}
	if t.collate == "binary" && t.charset != "" {
		t.collate = t.charset + "_bin"
	}
}

func (t *mysqlType) finishParams() {
	switch {
	case mysqlIntegerBases[t.base], t.base == "year":
		t.params = nil
	case t.base == "decimal":
		switch len(t.params) {
		case 0:
			t.params = []int{10, 0}
		case 1:
			t.params = []int{t.params[0], 0}
		}
	case t.base == "float" && len(t.params) == 1:
		if t.params[0] > 24 {
			t.base = "double"
		}
		t.params = nil
	case t.base == "char" || t.base == "binary" || t.base == "bit":
		if len(t.params) == 0 {
			t.params = []int{1}
		}
	case t.base == "time" || t.base == "datetime" || t.base == "timestamp":
		if len(t.params) == 1 && t.params[0] == 0 {
			t.params = nil
		}
	}
}

// parseTypeParams reads an optional ( n [, m] ) after a type keyword.
func parseTypeParams(words []string, j int) ([]int, int, bool) {
	if j >= len(words) || words[j] != "(" {
		return nil, j, true
	}
	var params []int
	k := j + 1
	for k < len(words) {
		switch words[k] {
		case ")":
			return params, k + 1, true
		case ",":
			k++
			continue
		}
		n, err := strconv.Atoi(words[k])
		if err != nil {
			return nil, j, false
		}
		params = append(params, n)
		k++
	}
	return nil, j, false
}

// parseMySQLTypeSpelling reads the type as information_schema.COLUMN_TYPE
// spells it -- `int(11) unsigned zerofill`, `varchar(20)`, `enum('a','b')`
// -- through the same tokenizer and parser the statement goes through, so
// the two sides cannot fold spellings differently.
func parseMySQLTypeSpelling(columnType, charset string) (mysqlType, bool) {
	spelling := strings.TrimSpace(columnType)
	if spelling == "" {
		return mysqlType{}, false
	}
	words := tokenizeWords(spelling, modeForDialect("mysql"))
	t, next, ok := parseMySQLTypeAt(words, 0, charset)
	if !ok || next != len(words) {
		return mysqlType{}, false
	}
	return t, true
}

// spell renders the canonical form for a message, the way the server would
// print it.
func (t mysqlType) spell() string {
	var b strings.Builder
	b.WriteString(t.base)
	switch {
	case t.members.kind != 0:
		b.WriteString("(" + quoteMembers(t.members.members) + ")")
	case len(t.params) > 0:
		b.WriteString("(")
		for i, p := range t.params {
			if i > 0 {
				b.WriteString(",")
			}
			b.WriteString(strconv.Itoa(p))
		}
		b.WriteString(")")
	}
	if t.unsigned {
		b.WriteString(" unsigned")
	}
	return b.String()
}

func (t mysqlType) sameStorage(o mysqlType) bool {
	if t.base != o.base || t.unsigned != o.unsigned || len(t.params) != len(o.params) {
		return false
	}
	for i := range t.params {
		if t.params[i] != o.params[i] {
			return false
		}
	}
	return true
}

// varcharBytes is the longest encoding of a VARCHAR(n) in its character
// set, or zero when the set is not known.
func (t mysqlType) varcharBytes() int {
	if len(t.params) != 1 {
		return 0
	}
	return t.params[0] * mysqlCharsetMaxLen[t.charset]
}

// lengthPrefixClass is 1 or 2, the bytes the row format spends on a
// VARCHAR's length, or zero when the character set is not known.
func lengthPrefixClass(n int) int {
	switch {
	case n <= 0:
		return 0
	case n <= 255:
		return 1
	default:
		return 2
	}
}

// typeOutcome is what InnoDB does with one column type change.
type typeOutcome int

const (
	// typeUnchanged: the two spellings name the same storage.
	typeUnchanged typeOutcome = iota
	// typeMemberList: ENUM to ENUM or SET to SET, which members.go judges.
	typeMemberList
	// typeInPlace: applied without a copy, as far as this file can tell.
	typeInPlace
	// typeUnknown: the outcome depends on a fact the baseline does not carry.
	typeUnknown
	// typeCopy: ALGORITHM=INSTANT and INPLACE are refused, the table is copied.
	typeCopy
)

// typeTransition is the comparison of a column's type before and after a
// MODIFY or CHANGE, with the sentence that says why the outcome is what it
// is.
type typeTransition struct {
	outcome typeOutcome
	why     string
}

// compareMySQLTypes decides the outcome for one column. dialect is the
// engine the run targets, for the one conversion the two engines treat
// differently.
func compareMySQLTypes(old, updated mysqlType, dialect string) typeTransition {
	if old.base != updated.base {
		return typeTransition{typeCopy, fmt.Sprintf("changes the type from %s to %s", old.spell(), updated.spell())}
	}
	if old.members.kind != 0 && old.charset == updated.charset {
		return typeTransition{outcome: typeMemberList}
	}
	if charset := compareCharsets(old, updated, dialect); charset.outcome == typeCopy || charset.outcome == typeUnknown {
		return charset
	}
	if old.unsigned != updated.unsigned {
		return typeTransition{typeCopy, fmt.Sprintf("changes the type from %s to %s", old.spell(), updated.spell())}
	}
	if !old.sameStorage(updated) {
		return compareLengths(old, updated)
	}
	if old.charset != updated.charset {
		return typeTransition{typeInPlace, fmt.Sprintf("converts %s from %s to %s in place", old.spell(), old.charset, updated.charset)}
	}
	if old.collate != "" && updated.collate != "" && old.collate != updated.collate {
		// In place on both engines for a column no index covers; MySQL
		// copies when one does, and the baseline does not carry indexes.
		return typeTransition{outcome: typeUnknown}
	}
	return typeTransition{outcome: typeUnchanged}
}

// compareLengths judges a change of parameters within one base type.
func compareLengths(old, updated mysqlType) typeTransition {
	if (old.base != "varchar" && old.base != "varbinary") || len(old.params) != 1 || len(updated.params) != 1 {
		return typeTransition{typeCopy, fmt.Sprintf("changes the type from %s to %s", old.spell(), updated.spell())}
	}
	if updated.params[0] < old.params[0] {
		return typeTransition{typeCopy, fmt.Sprintf("narrows %s to %s", old.spell(), updated.spell())}
	}
	if old.base == "varbinary" {
		return typeTransition{typeInPlace, "widens a VARBINARY"}
	}
	oldBytes, newBytes := old.varcharBytes(), updated.varcharBytes()
	oldClass, newClass := lengthPrefixClass(oldBytes), lengthPrefixClass(newBytes)
	switch {
	case oldClass == 0 || newClass == 0:
		return typeTransition{outcome: typeUnknown}
	case oldClass == newClass:
		return typeTransition{typeInPlace, "widens a VARCHAR within one length-prefix class"}
	default:
		return typeTransition{typeCopy, fmt.Sprintf(
			"widens %s to %s, which in %s takes the longest encoding from %d to %d bytes across the 255-byte line "+
				"where the row format's length prefix grows from one byte to two; a widening that stays on one side "+
				"of that line is applied in place, this one changes the row format",
			old.spell(), updated.spell(), updated.charset, oldBytes, newBytes)}
	}
}

// compareCharsets judges the character-set half of a change on a character
// column. The only pair either engine converts in place is utf8mb3 to
// utf8mb4, and only for a shape whose bytes do not move: a TEXT, a VARCHAR
// whose longest encoding crosses 255 bytes, and on MariaDB an ENUM or SET
// are copied like any other conversion.
func compareCharsets(old, updated mysqlType, dialect string) typeTransition {
	if !mysqlCharacterBases[old.base] || old.charset == "" || updated.charset == "" || old.charset == updated.charset {
		return typeTransition{outcome: typeUnchanged}
	}
	if old.charset != "utf8mb3" || updated.charset != "utf8mb4" {
		return typeTransition{typeCopy, fmt.Sprintf(
			"changes the character set of %s from %s to %s, which re-encodes every stored value",
			old.spell(), old.charset, updated.charset)}
	}
	compatible := "converts %s from utf8mb3 to utf8mb4, a conversion both servers apply in place for a short VARCHAR or a CHAR"
	switch {
	case mysqlTextBases[old.base]:
		return typeTransition{typeCopy, fmt.Sprintf(compatible+" but not for a TEXT column", old.spell())}
	case old.base == "varchar" && len(old.params) == 1 && len(updated.params) == 1 &&
		lengthPrefixClass(old.varcharBytes()) != lengthPrefixClass(updated.varcharBytes()):
		return typeTransition{typeCopy, fmt.Sprintf(
			compatible+" but not for one whose longest encoding crosses 255 bytes (%d to %d here)",
			old.spell(), old.varcharBytes(), updated.varcharBytes())}
	case old.members.kind != 0 && dialect == "mariadb":
		return typeTransition{typeCopy, fmt.Sprintf(compatible+", while MariaDB copies the table for an %s", old.spell(), strings.ToUpper(old.base))}
	case old.members.kind != 0 && dialect != "mysql":
		// Unknown engine: MySQL converts a list in place, MariaDB copies.
		return typeTransition{outcome: typeUnknown}
	default:
		// In place on both, unless an index covers the column on MySQL,
		// which the baseline cannot see.
		return typeTransition{typeInPlace, fmt.Sprintf(compatible, old.spell())}
	}
}

// The copy every finding in this file describes, said once.
const tableCopyConsequence = "both MySQL and MariaDB refuse ALGORITHM=INSTANT and INPLACE for it, " +
	"so the server copies the whole table row by row and blocks writes until the copy finishes"

// columnChangeSite is one MODIFY or CHANGE clause with its new type.
type columnChangeSite struct {
	statement int
	table     tableReference
	oldName   string
	newName   string
	// typeStart is the word index of the new type.
	typeStart int
}

// columnChangeSites finds every MODIFY or CHANGE clause that spells a type.
func columnChangeSites(file *File) []columnChangeSite {
	var sites []columnChangeSite
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		if !isAlterTable(w) {
			continue
		}
		table := alterTableReference(w, stmt.sourceWords)
		for _, i := range clauseStarts(w) {
			if i >= len(w) || (w[i] != "MODIFY" && w[i] != "CHANGE") {
				continue
			}
			change := w[i] == "CHANGE"
			j := i + 1
			if j < len(w) && w[j] == "COLUMN" {
				j++
			}
			j = skipIfExists(w, j)
			if j >= len(w) || !identLike(w[j]) {
				continue
			}
			oldName := sourceWordAt(w, stmt.sourceWords, j)
			newName := oldName
			j++
			if change {
				if j >= len(w) || !identLike(w[j]) {
					continue
				}
				newName = sourceWordAt(w, stmt.sourceWords, j)
				j++
			}
			if _, _, ok := parseMySQLTypeAt(w, j, ""); !ok {
				continue
			}
			sites = append(sites, columnChangeSite{
				statement: index,
				table:     table,
				oldName:   oldName,
				newName:   newName,
				typeStart: j,
			})
		}
	}
	return sites
}

func columnChangeStatements(file *File) []int {
	var indexes []int
	seen := -1
	for _, site := range columnChangeSites(file) {
		if site.statement == seen {
			continue
		}
		seen = site.statement
		indexes = append(indexes, site.statement)
	}
	return indexes
}

// tableCharsetBefore is the default character set a table has when the
// statement at index runs: the baseline's answer, unless an earlier
// statement of the same file changed it.
func tableCharsetBefore(file *File, index int, table tableReference, baseline string) string {
	charset := baseline
	for i := range index {
		stmt := &file.Statements[i]
		w := stmt.Words
		if !isAlterTable(w) || alterTableReference(w, stmt.sourceWords).normalized != table.normalized {
			continue
		}
		for _, j := range clauseStarts(w) {
			if target, ok := tableCharsetClause(w, j); ok {
				charset = target
			}
		}
	}
	return charset
}

// tableCharsetClause reads the character set a clause assigns to the table:
// CONVERT TO CHARACTER SET x, [DEFAULT] CHARACTER SET [=] x, or CHARSET [=] x.
// DEFAULT alone, which takes the database's default, resolves nothing.
func tableCharsetClause(w []string, j int) (string, bool) {
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
	if k >= len(w) || w[k] == "DEFAULT" || !identLike(w[k]) {
		return "", false
	}
	return normalizeCharset(w[k]), true
}

// resolvedColumnChange is a site joined to the column's current type.
type resolvedColumnChange struct {
	site       columnChangeSite
	old        mysqlType
	updated    mysqlType
	transition typeTransition
}

func resolveColumnChanges(file *File) []resolvedColumnChange {
	if !file.IsUp {
		return nil
	}
	var resolved []resolvedColumnChange
	for _, site := range columnChangeSites(file) {
		column, ok := file.baseline.column(site.table.normalized, normalizeIdent(site.oldName))
		if !ok {
			continue
		}
		old, ok := parseMySQLTypeSpelling(column.ColumnType, column.Charset)
		if !ok {
			continue
		}
		stmt := &file.Statements[site.statement]
		tableCharset := tableCharsetBefore(file, site.statement, site.table, column.TableCharset)
		updated, _, ok := parseMySQLTypeAt(stmt.Words, site.typeStart, tableCharset)
		if !ok {
			continue
		}
		resolved = append(resolved, resolvedColumnChange{
			site:       site,
			old:        old,
			updated:    updated,
			transition: compareMySQLTypes(old, updated, file.dialect),
		})
	}
	return resolved
}

func (c resolvedColumnChange) clause() string {
	if c.site.oldName != c.site.newName {
		return "CHANGE COLUMN"
	}
	return "MODIFY COLUMN"
}

// mysqlCostRules is the family: the three copies Atlas names, each fired
// only where it is established.
func mysqlCostRules() []Rule {
	return []Rule{columnTypeCopyRule(), primaryKeyDropCopyRule(), charsetConversionCopyRule()}
}

func columnTypeCopyRule() Rule {
	return Rule{
		Code:             "MY130",
		Title:            "column type change copies the table",
		Severity:         SeverityWarning,
		Dialects:         mysqlFamily,
		Subsumes:         []string{"DS103", "MY101"},
		Input:            InputBaselineSchema,
		BaselineSubjects: columnChangeStatements,
		CheckFile: func(file *File) []Finding {
			var findings []Finding
			for _, change := range resolveColumnChanges(file) {
				if change.transition.outcome != typeCopy {
					continue
				}
				message := fmt.Sprintf("%s %s.%s %s; %s. Every existing value is converted during the copy: "+
					"one the new type cannot hold fails the copy (Out of range value or Data truncated) under the strict SQL mode "+
					"both servers default to, and is clamped or truncated when strict mode is off. "+
					"Verify the old-to-new value mapping on production data first, and run the change through an online-DDL tool "+
					"such as gh-ost or pt-online-schema-change for a populated table",
					change.clause(), change.site.table.name, change.site.newName, change.transition.why, tableCopyConsequence)
				findings = append(findings, costFinding(file, change.site.statement, "MY130", "column type change copies the table", message,
					Subject{Kind: SubjectColumn, Name: change.site.newName, Parent: change.site.table.name, DataType: change.updated.base}))
			}
			return findings
		},
	}
}

func primaryKeyDropCopyRule() Rule {
	return Rule{
		Code:     "MY133",
		Title:    "primary key dropped without a replacement copies the table",
		Severity: SeverityWarning,
		Dialects: mysqlFamily,
		CheckStatement: func(stmt *Statement) (bool, string) {
			if !isAlterTable(stmt.Words) || !scanDropPrimaryKey(stmt.Words) || scanAddPrimaryKey(stmt.Words) {
				return false, ""
			}
			return true, "DROP PRIMARY KEY with no replacement key in the same statement: MySQL refuses ALGORITHM=INSTANT and INPLACE for it " +
				"(\"Dropping a primary key is not allowed without also adding a new primary key\") and copies the whole table row by row, " +
				"blocking writes until the copy finishes; MariaDB does the same unless another NOT NULL UNIQUE key exists for InnoDB to " +
				"promote to the clustered index. Add the replacement PRIMARY KEY in the same ALTER TABLE: both servers then rebuild the table " +
				"in place with writes allowed (ALGORITHM=INPLACE, LOCK=NONE)"
		},
	}
}

// charsetConversionSite is one CONVERT TO CHARACTER SET clause.
type charsetConversionSite struct {
	statement int
	table     tableReference
	target    string
}

func charsetConversionSites(file *File) []charsetConversionSite {
	var sites []charsetConversionSite
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		if !isAlterTable(w) {
			continue
		}
		table := alterTableReference(w, stmt.sourceWords)
		for _, i := range clauseStarts(w) {
			if i >= len(w) || w[i] != "CONVERT" {
				continue
			}
			target, ok := tableCharsetClause(w, i)
			if !ok {
				continue
			}
			sites = append(sites, charsetConversionSite{statement: index, table: table, target: target})
		}
	}
	return sites
}

func charsetConversionStatements(file *File) []int {
	var indexes []int
	seen := -1
	for _, site := range charsetConversionSites(file) {
		if site.statement == seen {
			continue
		}
		seen = site.statement
		indexes = append(indexes, site.statement)
	}
	return indexes
}

// convertedColumn is one column a conversion touches, with what happens to it.
type convertedColumn struct {
	name       string
	transition typeTransition
}

// resolveCharsetConversions joins each site to the table's columns and
// decides, column by column, which ones force the copy.
func resolveCharsetConversions(file *File) []resolvedCharsetConversion {
	if !file.IsUp {
		return nil
	}
	var resolved []resolvedCharsetConversion
	for _, site := range charsetConversionSites(file) {
		columns := file.baseline.tableColumns(site.table.normalized)
		if len(columns) == 0 {
			continue
		}
		conversion := resolvedCharsetConversion{site: site}
		for _, column := range columns {
			old, ok := parseMySQLTypeSpelling(column.ColumnType, column.Charset)
			if !ok || !mysqlCharacterBases[old.base] || old.charset == "" {
				continue
			}
			updated := old
			updated.charset = site.target
			conversion.columns = append(conversion.columns, convertedColumn{
				name:       column.Name,
				transition: compareCharsets(old, updated, file.dialect),
			})
		}
		resolved = append(resolved, conversion)
	}
	return resolved
}

type resolvedCharsetConversion struct {
	site    charsetConversionSite
	columns []convertedColumn
}

// copied and inPlace split the columns by outcome.
func (c resolvedCharsetConversion) copied() []convertedColumn {
	var out []convertedColumn
	for _, column := range c.columns {
		if column.transition.outcome == typeCopy {
			out = append(out, column)
		}
	}
	return out
}

func (c resolvedCharsetConversion) inPlace() int {
	n := 0
	for _, column := range c.columns {
		if column.transition.outcome == typeInPlace {
			n++
		}
	}
	return n
}

func charsetConversionCopyRule() Rule {
	return Rule{
		Code:             "MY136",
		Title:            "character set conversion copies the table",
		Severity:         SeverityWarning,
		Dialects:         mysqlFamily,
		Subsumes:         []string{"MY101"},
		Input:            InputBaselineSchema,
		BaselineSubjects: charsetConversionStatements,
		CheckFile: func(file *File) []Finding {
			var findings []Finding
			for _, conversion := range resolveCharsetConversions(file) {
				copied := conversion.copied()
				if len(copied) == 0 {
					continue
				}
				var reasons []string
				for _, column := range copied {
					reasons = append(reasons, column.name+" ("+column.transition.why+")")
				}
				message := fmt.Sprintf("CONVERT TO CHARACTER SET %s on %s cannot be applied in place for %d column%s: %s; %s",
					conversion.site.target, conversion.site.table.name, len(copied), plural(len(copied)),
					strings.Join(reasons, "; "), tableCopyConsequence)
				if n := conversion.inPlace(); n > 0 {
					message += fmt.Sprintf(". The other %d character column%s of the table would have converted in place", n, plural(n))
				}
				message += ". Convert the columns that need it one at a time through an online-DDL tool such as gh-ost or pt-online-schema-change, " +
					"or change only the table default (DEFAULT CHARACTER SET) if new columns are all that should change"
				findings = append(findings, costFinding(file, conversion.site.statement, "MY136", "character set conversion copies the table", message,
					Subject{Kind: SubjectTable, Name: conversion.site.table.name}))
			}
			return findings
		},
	}
}

func costFinding(file *File, statement int, code, title, message string, subject Subject) Finding {
	stmt := &file.Statements[statement]
	return Finding{
		Rule:     code,
		Title:    title,
		Severity: SeverityWarning,
		File:     file.Path,
		Line:     stmt.Line,
		Message:  message,
		Context:  statementFindingContext(statement, subject),
	}
}
