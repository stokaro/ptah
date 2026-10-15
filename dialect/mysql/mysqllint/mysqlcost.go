package mysqllint

import (
	"fmt"
	"strings"

	"ptah.run/core/lintext"
	"ptah.run/internal/lintkit"
	"ptah.run/internal/lintkit/mytype"
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
// The keyed cases were measured on the same two servers with the column's
// keys and its current collation in hand (stokaro/ptah#2957), by asking for
// ALGORITHM=INPLACE, LOCK=NONE; a refusal is the copy:
//
//	                                              MySQL       MariaDB
//	collation restated, keyed or not               in place    in place
//	collation changed, no key on the column        in place    in place
//	collation changed, a key or a prefix key       copy        in place
//	utf8mb3 -> utf8mb4, VARCHAR/CHAR, no key       in place    in place
//	  the same with a whole-column key             copy        in place
//	  the same with a prefix key                   copy        copy
//	  the same on a keyed ENUM                     in place    copy
//	CONVERT TO ... utf8mb3 -> utf8mb4, a keyed
//	  VARCHAR among the columns                    copy        as above per key shape
//	MODIFY with no COLLATE                         the column takes the table's
//	                                               default collation, latin1_bin
//	                                               to latin1_swedish_ci
//	MODIFY with CHARACTER SET and no COLLATE       the set's own default, which
//	                                               differs between engines
//
// The type comparison reads the column's current spelling, character set and
// collation, and the keys on it, from the schema state the version starts
// from ([lintext.BaselineColumn.ColumnType], [lintext.BaselineColumn.Charset],
// [lintext.BaselineColumn.TableCharset], [lintext.BaselineColumn.Collation],
// [lintext.BaselineColumn.TableCollation] and [lintext.BaselineIndex]);
// without that state MY130 and MY136 stay quiet, DS103 and MY101 still report
// the statement, and migration lint names them as inputs the run did not
// supply. A clause that names a character set
// and no collation leaves the column on that set's default collation, which
// the two engines spell differently, so such a change is judged unknown
// rather than by a guess at the name.

// mysqlTextBases are the families whose conversion between character sets
// is never applied in place, superset or not.
var mysqlTextBases = map[string]bool{
	"tinytext": true, "text": true, "mediumtext": true, "longtext": true,
}

// keyCoverage is what the keys on a column look like to InnoDB's online
// DDL: none, whole-column parts only, or at least one prefix part.
type keyCoverage int

const (
	keyNone keyCoverage = iota
	keyWhole
	keyPrefix
)

// keyCoverageOf classifies the keys on one column. columnName is the
// linter's normalized form.
func keyCoverageOf(indexes []lintext.BaselineIndex, columnName string) keyCoverage {
	coverage := keyNone
	for _, idx := range indexes {
		for _, part := range idx.Parts {
			if lintkit.NormalizeIdent(part.Column) != columnName {
				continue
			}
			if part.Prefix > 0 {
				return keyPrefix
			}
			coverage = keyWhole
		}
	}
	return coverage
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
// engine the run targets and keys what covers the column, for the
// conversions the two engines and the two key shapes treat differently.
func compareMySQLTypes(old, updated mytype.Type, dialect string, keys keyCoverage) typeTransition {
	if old.Base != updated.Base {
		return typeTransition{typeCopy, fmt.Sprintf("changes the type from %s to %s", old.Spell(), updated.Spell())}
	}
	if old.Members.Kind != 0 && old.Charset == updated.Charset {
		return typeTransition{outcome: typeMemberList}
	}
	if charset := compareCharsets(old, updated, dialect, keys); charset.outcome == typeCopy || charset.outcome == typeUnknown {
		return charset
	}
	if old.Unsigned != updated.Unsigned {
		return typeTransition{typeCopy, fmt.Sprintf("changes the type from %s to %s", old.Spell(), updated.Spell())}
	}
	if !old.SameStorage(updated) {
		return compareLengths(old, updated)
	}
	if old.Charset != updated.Charset {
		return typeTransition{typeInPlace, fmt.Sprintf("converts %s from %s to %s in place", old.Spell(), old.Charset, updated.Charset)}
	}
	return compareCollations(old, updated, dialect, keys)
}

// compareCollations judges a change of collation within one character set.
// Restating the collation changes nothing on either engine. A changed one is
// applied in place on a column no key covers and on MariaDB throughout;
// MySQL rebuilds the key and copies the table for it.
func compareCollations(old, updated mytype.Type, dialect string, keys keyCoverage) typeTransition {
	switch {
	case old.Collate == "" || old.Collate == updated.Collate:
		return typeTransition{outcome: typeUnchanged}
	case updated.Collate == "":
		// The clause named a character set and no collation, so the column
		// takes that set's default, which the two engines spell differently.
		return typeTransition{outcome: typeUnknown}
	}
	change := fmt.Sprintf("changes the collation of %s from %s to %s", old.Spell(), old.Collate, updated.Collate)
	switch {
	case keys == keyNone:
		return typeTransition{typeInPlace, change + " on a column no key covers, which both servers apply in place"}
	case dialect == "mariadb":
		return typeTransition{typeInPlace, change + ", which MariaDB applies in place"}
	case dialect == "mysql":
		return typeTransition{typeCopy, change + " on a column a key covers; MySQL rebuilds the key for it and refuses " +
			"INSTANT and INPLACE, where the same change on a column no key covers is applied in place"}
	default:
		return typeTransition{outcome: typeUnknown}
	}
}

// compareLengths judges a change of parameters within one base type.
func compareLengths(old, updated mytype.Type) typeTransition {
	if (old.Base != "varchar" && old.Base != "varbinary") || len(old.Params) != 1 || len(updated.Params) != 1 {
		return typeTransition{typeCopy, fmt.Sprintf("changes the type from %s to %s", old.Spell(), updated.Spell())}
	}
	if updated.Params[0] < old.Params[0] {
		return typeTransition{typeCopy, fmt.Sprintf("narrows %s to %s", old.Spell(), updated.Spell())}
	}
	if old.Base == "varbinary" {
		return typeTransition{typeInPlace, "widens a VARBINARY"}
	}
	oldBytes, newBytes := old.VarcharBytes(), updated.VarcharBytes()
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
			old.Spell(), updated.Spell(), updated.Charset, oldBytes, newBytes)}
	}
}

// compareCharsets judges the character-set half of a change on a character
// column. The only pair either engine converts in place is utf8mb3 to
// utf8mb4, and only for a shape whose bytes do not move: a TEXT, a VARCHAR
// whose longest encoding crosses 255 bytes, and on MariaDB an ENUM or SET
// are copied like any other conversion, and so is a VARCHAR or CHAR a key
// covers, on MySQL for any key and on MariaDB for a prefix key.
func compareCharsets(old, updated mytype.Type, dialect string, keys keyCoverage) typeTransition {
	if !mytype.CharacterBases[old.Base] || old.Charset == "" || updated.Charset == "" || old.Charset == updated.Charset {
		return typeTransition{outcome: typeUnchanged}
	}
	if old.Charset != "utf8mb3" || updated.Charset != "utf8mb4" {
		return typeTransition{typeCopy, fmt.Sprintf(
			"changes the character set of %s from %s to %s, which re-encodes every stored value",
			old.Spell(), old.Charset, updated.Charset)}
	}
	compatible := "converts %s from utf8mb3 to utf8mb4, a conversion both servers apply in place for a short VARCHAR or a CHAR"
	switch {
	case mysqlTextBases[old.Base]:
		return typeTransition{typeCopy, fmt.Sprintf(compatible+" but not for a TEXT column", old.Spell())}
	case old.Base == "varchar" && len(old.Params) == 1 && len(updated.Params) == 1 &&
		lengthPrefixClass(old.VarcharBytes()) != lengthPrefixClass(updated.VarcharBytes()):
		return typeTransition{typeCopy, fmt.Sprintf(
			compatible+" but not for one whose longest encoding crosses 255 bytes (%d to %d here)",
			old.Spell(), old.VarcharBytes(), updated.VarcharBytes())}
	case old.Members.Kind != 0 && dialect == "mariadb":
		return typeTransition{typeCopy, fmt.Sprintf(compatible+", while MariaDB copies the table for an %s", old.Spell(), strings.ToUpper(old.Base))}
	case old.Members.Kind != 0 && dialect != "mysql":
		// Unknown engine: MySQL converts a list in place, MariaDB copies.
		return typeTransition{outcome: typeUnknown}
	case old.Members.Kind != 0:
		// MySQL converts a list in place, keyed or not.
		return typeTransition{typeInPlace, fmt.Sprintf(compatible, old.Spell())}
	default:
		return compareKeyedConversion(old, dialect, keys, compatible)
	}
}

// compareKeyedConversion judges the utf8mb3 to utf8mb4 conversion of a
// short VARCHAR or CHAR by the keys on it: in place with none, copied on
// MySQL with any and on both engines with a prefix key.
func compareKeyedConversion(old mytype.Type, dialect string, keys keyCoverage, compatible string) typeTransition {
	inPlace := typeTransition{typeInPlace, fmt.Sprintf(compatible, old.Spell())}
	switch {
	case keys == keyNone:
		return inPlace
	case keys == keyPrefix:
		return typeTransition{typeCopy, fmt.Sprintf(compatible+" but not for one a prefix key covers, which both servers copy for", old.Spell())}
	case dialect == "mysql":
		return typeTransition{typeCopy, fmt.Sprintf(compatible+" but not, on MySQL, for one a key covers", old.Spell())}
	case dialect == "mariadb":
		return inPlace
	default:
		// Unknown engine: MySQL copies for the keyed column, MariaDB does not.
		return typeTransition{outcome: typeUnknown}
	}
}

// The copy every finding in this file describes, said once.
const tableCopyConsequence = "both MySQL and MariaDB refuse ALGORITHM=INSTANT and INPLACE for it, " +
	"so the server copies the whole table row by row and blocks writes until the copy finishes"

func columnChangeStatements(file *lintkit.File) []int {
	var indexes []int
	seen := -1
	for _, site := range mytype.ChangeSites(file.Statements) {
		if site.Statement == seen {
			continue
		}
		seen = site.Statement
		indexes = append(indexes, site.Statement)
	}
	return indexes
}

// resolvedColumnChange is a site joined to the column's current type.
type resolvedColumnChange struct {
	site       mytype.ChangeSite
	old        mytype.Type
	updated    mytype.Type
	transition typeTransition
}

func resolveColumnChanges(file *lintkit.File) []resolvedColumnChange {
	if !file.IsUp {
		return nil
	}
	var resolved []resolvedColumnChange
	for _, site := range mytype.ChangeSites(file.Statements) {
		column, ok := file.Baseline.Column(site.Table.Normalized, lintkit.NormalizeIdent(site.OldName))
		if !ok {
			continue
		}
		old, ok := mytype.ParseSpelling(column.ColumnType, column.Charset)
		if !ok {
			continue
		}
		stmt := &file.Statements[site.Statement]
		tableCharset := mytype.TableCharsetBefore(file.Statements, site.Statement, site.Table, column.TableCharset)
		updated, _, ok := mytype.ParseAt(stmt.Words, site.TypeStart, tableCharset)
		if !ok {
			continue
		}
		old.Collate = mytype.NormalizeCollation(column.Collation)
		updated.Collate = resolvedCollation(updated, column, tableCharset)
		keys := keyCoverageOf(file.Baseline.ColumnIndexes(site.Table.Normalized, lintkit.NormalizeIdent(site.OldName)), lintkit.NormalizeIdent(site.OldName))
		resolved = append(resolved, resolvedColumnChange{
			site:       site,
			old:        old,
			updated:    updated,
			transition: compareMySQLTypes(old, updated, file.Target.Name, keys),
		})
	}
	return resolved
}

// resolvedCollation is the collation a MODIFY or CHANGE leaves the column
// with: the one it names; the table's default when it names neither a
// collation nor a character set and the table's default set is the one the
// baseline recorded; and unknown otherwise, since a character set named
// without a collation takes that set's own default, which the two engines
// spell differently, and a table whose default set the file changed earlier
// has a default collation the baseline did not see.
func resolvedCollation(updated mytype.Type, column lintext.BaselineColumn, tableCharset string) string {
	switch {
	case updated.Collate != "":
		return updated.Collate
	case !mytype.CharacterBases[updated.Base] || updated.ExplicitCharset:
		return ""
	case mytype.NormalizeCharset(tableCharset) != mytype.NormalizeCharset(column.TableCharset):
		return ""
	default:
		return mytype.NormalizeCollation(column.TableCollation)
	}
}

func (c resolvedColumnChange) clause() string {
	if c.site.OldName != c.site.NewName {
		return "CHANGE COLUMN"
	}
	return "MODIFY COLUMN"
}

// mysqlCostRules is the family: the three copies Atlas names, each fired
// only where it is established.
func mysqlCostRules() []lintkit.Rule {
	return []lintkit.Rule{columnTypeCopyRule(), primaryKeyDropCopyRule(), charsetConversionCopyRule()}
}

func columnTypeCopyRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "MY130",
			Title:    "column type change copies the table",
			Severity: lintext.Warning,
			Targets:  family(),
			Subsumes: []string{"DS103", "MY101"},
			Input:    lintext.InputBaselineSchema,
		},
		BaselineSubjects: columnChangeStatements,
		File: func(file *lintkit.File) []lintext.Violation {
			var findings []lintext.Violation
			for _, change := range resolveColumnChanges(file) {
				if change.transition.outcome != typeCopy {
					continue
				}
				message := fmt.Sprintf("%s %s.%s %s; %s. Every existing value is converted during the copy: "+
					"one the new type cannot hold fails the copy (Out of range value or Data truncated) under the strict SQL mode "+
					"both servers default to, and is clamped or truncated when strict mode is off. "+
					"Verify the old-to-new value mapping on production data first, and run the change through an online-DDL tool "+
					"such as gh-ost or pt-online-schema-change for a populated table",
					change.clause(), change.site.Table.Name, change.site.NewName, change.transition.why, tableCopyConsequence)
				findings = append(findings, lintext.Violation{Statement: change.site.Statement, Message: message,
					Subjects: []lintext.Subject{{Kind: lintext.SubjectColumn, Name: change.site.NewName, Parent: change.site.Table.Name,
						DataType: change.updated.Base}}})
			}
			return findings
		},
	}
}

func primaryKeyDropCopyRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "MY133",
			Title:    "primary key dropped without a replacement copies the table",
			Severity: lintext.Warning,
			Targets:  family(),
		},
		Statement: func(_ *lintkit.File, stmt *lintext.Statement) (bool, string) {
			if !lintkit.IsAlterTable(stmt.Words) || !lintkit.ScanDropPrimaryKey(stmt.Words) || lintkit.ScanAddPrimaryKey(stmt.Words) {
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
	table     lintkit.TableReference
	target    string
}

func charsetConversionSites(file *lintkit.File) []charsetConversionSite {
	var sites []charsetConversionSite
	for index := range file.Statements {
		stmt := &file.Statements[index]
		w := stmt.Words
		if !lintkit.IsAlterTable(w) {
			continue
		}
		table := lintkit.AlterTableReference(w, stmt.SourceWords)
		for _, i := range lintkit.ClauseStarts(w) {
			if i >= len(w) || w[i] != "CONVERT" {
				continue
			}
			target, ok := mytype.TableCharsetClause(w, i)
			if !ok {
				continue
			}
			sites = append(sites, charsetConversionSite{statement: index, table: table, target: target})
		}
	}
	return sites
}

func charsetConversionStatements(file *lintkit.File) []int {
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
func resolveCharsetConversions(file *lintkit.File) []resolvedCharsetConversion {
	if !file.IsUp {
		return nil
	}
	var resolved []resolvedCharsetConversion
	for _, site := range charsetConversionSites(file) {
		columns := file.Baseline.TableColumns(site.table.Normalized)
		if len(columns) == 0 {
			continue
		}
		conversion := resolvedCharsetConversion{site: site}
		for _, column := range columns {
			old, ok := mytype.ParseSpelling(column.ColumnType, column.Charset)
			if !ok || !mytype.CharacterBases[old.Base] || old.Charset == "" {
				continue
			}
			updated := old
			updated.Charset = site.target
			keys := keyCoverageOf(file.Baseline.ColumnIndexes(site.table.Normalized, lintkit.NormalizeIdent(column.Name)), lintkit.NormalizeIdent(column.Name))
			conversion.columns = append(conversion.columns, convertedColumn{
				name:       column.Name,
				transition: compareCharsets(old, updated, file.Target.Name, keys),
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

func charsetConversionCopyRule() lintkit.Rule {
	return lintkit.Rule{
		Rule: lintext.Rule{
			Code:     "MY136",
			Title:    "character set conversion copies the table",
			Severity: lintext.Warning,
			Targets:  family(),
			Subsumes: []string{"MY101"},
			Input:    lintext.InputBaselineSchema,
		},
		BaselineSubjects: charsetConversionStatements,
		File: func(file *lintkit.File) []lintext.Violation {
			var findings []lintext.Violation
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
					conversion.site.target, conversion.site.table.Name, len(copied), lintkit.Plural(len(copied)),
					strings.Join(reasons, "; "), tableCopyConsequence)
				if n := conversion.inPlace(); n > 0 {
					message += fmt.Sprintf(". The other %d character column%s of the table would have converted in place", n, lintkit.Plural(n))
				}
				message += ". Convert the columns that need it one at a time through an online-DDL tool such as gh-ost or pt-online-schema-change, " +
					"or change only the table default (DEFAULT CHARACTER SET) if new columns are all that should change"
				findings = append(findings, lintext.Violation{Statement: conversion.site.statement, Message: message,
					Subjects: []lintext.Subject{{Kind: lintext.SubjectTable, Name: conversion.site.table.Name}}})
			}
			return findings
		},
	}
}
