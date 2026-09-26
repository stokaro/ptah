package mysqlname

import (
	"math"
	"strconv"
	"strings"
)

// checkLabel sits between a table's name and the number of a CHECK the server
// named.
const checkLabel = "_chk_"

// MaxIdentifierChars is the longest name MySQL and MariaDB keep, counted in
// characters. MySQL does not shorten a name it derives past it: measured on
// 8.4.11 and 26.7.0, an unnamed CHECK on a 59-character table is `ERROR 1059
// (42000): Identifier name ... is too long`, and on a 58-character table it is
// accepted.
const MaxIdentifierChars = 64

// Check answers the name MySQL gives the CHECK numbered n of table:
// `<table>_chk_<n>`, with the table spelled as it was declared.
//
// Which number the server picks depends on the statement. CREATE TABLE numbers
// its unnamed CHECKs from 1 in the order it writes them, on a column or on the
// table alike, and a named CHECK does not move the count: `CONSTRAINT c3_chk_5
// CHECK (a > 0), CHECK (a < 9)` names the second `c3_chk_1`. ALTER TABLE takes
// the number [NextCheckNumber] answers.
//
// The server does not move a derived name out of the way of another CHECK. A
// CHECK anywhere in the database already called the derived name, compared
// without case and written before or after, is `ERROR 3822 (HY000): Duplicate
// check constraint name`. Measured on MySQL 8.4.11 and 26.7.0.
func Check(table string, n uint32) string {
	return table + checkLabel + strconv.FormatUint(uint64(n), 10)
}

// NextCheckNumber answers the number ALTER TABLE gives an unnamed CHECK it adds
// to table, which holds the CHECKs named held: one more than the highest
// number among them, counted as the server counts it, and 1 when none counts.
//
// A name counts when it is `<table>_chk_` with the table spelled exactly as
// declared, followed by what the server reads as a number: optional spaces or
// tabs, an optional sign and at least one digit, with anything after the digits
// ignored. The server keeps the number in 32 unsigned bits, so a negative or
// larger number wraps, and one more than the largest wraps to 0. Measured on
// MySQL 8.4.11 and 26.7.0, the CHECK an unnamed ADD CHECK named beside:
//
//	held                         added
//	al_chk_1, al_chk_5, al_chk_7 al_chk_8
//	al3_chk_07                   al3_chk_8
//	al6_chk_3x                   al6_chk_4
//	b6_chk_ 3                    b6_chk_4
//	b2_chk_+3                    b2_chk_4
//	b1_chk_-3                    b1_chk_4294967294
//	h1_chk_4294967296            h1_chk_1
//	h6_chk_4294967295            h6_chk_0
//	AL4_chk_3                    al4_chk_1
//	al5_CHK_3                    al5_chk_1
//	mixedalt_chk_3 on MixedAlt   MixedAlt_chk_1
//	b3_chk_x3                    b3_chk_1
//	h5_chk_-                     h5_chk_1
func NextCheckNumber(table string, held []string) uint32 {
	var highest uint32
	for _, name := range held {
		if n, ok := checkNumber(table, name); ok && n > highest {
			highest = n
		}
	}
	return highest + 1
}

// checkNumber reads the number the server counts in a CHECK name of table,
// and false for a name it does not count. See [NextCheckNumber].
func checkNumber(table, name string) (uint32, bool) {
	rest, found := strings.CutPrefix(name, table+checkLabel)
	if !found {
		return 0, false
	}
	rest = strings.TrimLeft(rest, " \t")
	negative := false
	switch {
	case strings.HasPrefix(rest, "-"):
		negative, rest = true, rest[1:]
	case strings.HasPrefix(rest, "+"):
		rest = rest[1:]
	}
	digits := len(rest) - len(strings.TrimLeft(rest, "0123456789"))
	if digits == 0 {
		return 0, false
	}
	value, err := strconv.ParseInt(rest[:digits], 10, 64)
	switch {
	case err != nil && negative:
		value = math.MinInt64
	case err != nil:
		value = math.MaxInt64
	case negative:
		value = -value
	}
	// #nosec G115 -- the server keeps the number in 32 bits; the wrap is the measured behavior.
	return uint32(value), true
}

// mariaDBCheckPrefix is what MariaDB names an unnamed table-level CHECK with.
const mariaDBCheckPrefix = "CONSTRAINT_"

// MariaDBCheck answers the name MariaDB gives an unnamed CHECK written at table
// level: `CONSTRAINT_<n>`, with the smallest n from 1 whose name taken answers
// false for.
//
// taken answers for the names of the table's CHECKs, compared without case:
// every name the statement writes, before the unnamed CHECK or after it, the
// names already derived, and in ALTER TABLE the CHECKs the table holds. A
// CHECK written on a column is named after its column, and that name counts as
// taken too. Measured on MariaDB 11.8.9:
//
//	CREATE TABLE e2 (a int, CHECK (a < 9), CONSTRAINT CONSTRAINT_1 CHECK (a > 0))   CONSTRAINT_2
//	CREATE TABLE e3 (a int, CONSTRAINT constraint_1 CHECK (a > 0), CHECK (a < 9))   CONSTRAINT_2
//	CREATE TABLE g1 (CONSTRAINT_1 int CHECK (CONSTRAINT_1 > 0), CHECK (...))       CONSTRAINT_2
//	ALTER TABLE e6 ADD CHECK, after CONSTRAINT_1 was dropped                       CONSTRAINT_1
func MariaDBCheck(taken func(name string) bool) string {
	for n := 1; ; n++ {
		name := mariaDBCheckPrefix + strconv.Itoa(n)
		if !taken(name) {
			return name
		}
	}
}
