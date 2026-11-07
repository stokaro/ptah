package typechange

import (
	"regexp"
	"slices"
	"strings"
)

// A type's capacity decides whether a server can keep every value through a change. The
// families here lose a value in one of two ways, and neither is a reason to rate them
// apart. Some servers refuse the statement while a row does not fit. Others convert
// without a word: measured on PostgreSQL 18, 1.23456789012 reads 1.2345679 under `real`
// and 9007199254740993 reads 9.007199254740992e+15 under `double precision`, and on
// ClickHouse 26.10 1.5 reads 1 under Int64.

// lobTierBytes is what MySQL's text and blob names hold, in bytes.
const (
	tinyLobBytes   = 255
	lobBytes       = 65535
	mediumLobBytes = 16777215
	longLobBytes   = 4294967295
)

// bytesPerCharacter is utf8mb4's worst case. A column's character set is not part of its
// type, and this is the one MySQL 8 and MariaDB 12 give a column by default.
const bytesPerCharacter = 4

// namedLobTier is the size a text or blob name states by itself on any engine, since only
// MySQL and MariaDB spell these names.
func namedLobTier(name string) (int, bool) {
	switch name {
	case "tinytext", "tinyblob":
		return tinyLobBytes, true
	case "mediumtext", "mediumblob":
		return mediumLobBytes, true
	case "longtext", "longblob":
		return longLobBytes, true
	default:
		return 0, false
	}
}

// lobTierFor is the tier MySQL builds for a size a column asks for: `blob(100)` is a
// tinyblob, and `text(100)` is a text, since 100 characters may take 400 bytes.
func lobTierFor(bytes int) int {
	for _, tier := range []int{tinyLobBytes, lobBytes, mediumLobBytes} {
		if bytes <= tier {
			return tier
		}
	}
	return longLobBytes
}

// lobTier is what a text or blob type holds in bytes where it comes in tiers, which is
// where the engine says so, or where the other side of the change is one of the names
// only such an engine has.
func lobTier(s, other spec, engine *Engine) (int, bool) {
	if tier, named := namedLobTier(s.name); named {
		return tier, true
	}
	if s.name != "text" && s.name != "blob" {
		return 0, false
	}
	_, otherNamed := namedLobTier(other.name)
	if !otherNamed && (engine == nil || !engine.Bare.LobTiers) {
		return 0, false
	}
	switch {
	case s.arg == 0:
		return lobBytes, true
	case s.name == "text":
		return lobTierFor(s.arg * bytesPerCharacter), true
	default:
		return lobTierFor(s.arg), true
	}
}

// stringNarrows reports that a value of old does not fit updated, where both hold text.
// A tier counts bytes and a length counts characters, so each is read at the worst case
// for the direction: a tier full of one-byte characters, a length full of four-byte ones.
func stringNarrows(old, updated spec, engine *Engine) bool {
	oldTier, oldTiered := lobTier(old, updated, engine)
	newTier, newTiered := lobTier(updated, old, engine)
	switch {
	case oldTiered && newTiered:
		return newTier < oldTier
	case oldTiered:
		newLen, ok := stringLength(updated, engine)
		return ok && newLen < oldTier
	case newTiered:
		oldLen, ok := stringLength(old, engine)
		return ok && (oldLen == unboundedStringLength || oldLen*bytesPerCharacter > newTier)
	case isTextType(old.name) && isSizedString(updated.name):
		// A bare VARCHAR has no limit, so a text value keeps every character there.
		newLen, bounded := stringLength(updated, engine)
		return !bounded || newLen != unboundedStringLength
	}
	oldLen, oldOK := stringLength(old, engine)
	newLen, newOK := stringLength(updated, engine)
	return oldOK && newOK && newLen < oldLen
}

// isBinaryType reports a type that holds bytes.
func isBinaryType(name string) bool {
	switch name {
	case "binary", "varbinary", "raw", "bytea", "image", "blob", "tinyblob", "mediumblob", "longblob":
		return true
	default:
		return false
	}
}

// binaryLength is how many bytes a binary type holds, and false where the engine's answer
// is needed and nobody named one.
func binaryLength(s, other spec, engine *Engine) (int, bool) {
	if tier, tiered := lobTier(s, other, engine); tiered {
		return tier, true
	}
	switch {
	case s.unlimited || s.name == "bytea" || s.name == "image" || s.name == "blob":
		return unboundedStringLength, true
	case s.arg > 0:
		return s.arg, true
	case engine == nil || !engine.Bare.Stated:
		return 0, false
	case s.name == "binary":
		return 1, true
	case s.name == "varbinary" && engine.Bare.VariableLength > 0:
		return engine.Bare.VariableLength, true
	default:
		return 0, false
	}
}

// bitLength is how many bits a bit type holds. A bare BIT holds one and a bare BIT VARYING
// has no limit. Without an engine a bare BIT has no answer, since SQL Server's is a
// boolean.
func bitLength(s spec, engine *Engine) (int, bool) {
	switch {
	case s.arg > 0:
		return s.arg, true
	case s.name != "bit":
		return unboundedStringLength, true
	case engine != nil && engine.Bare.Stated:
		return 1, true
	default:
		return 0, false
	}
}

// isBitType reports a type that holds bits.
func isBitType(name string) bool {
	return name == "bit" || name == "varbit" || name == "bit varying"
}

// memberRe reads a member of an enum or a set out of its declaration.
var memberRe = regexp.MustCompile(`'((?:[^']|'')*)'`)

// membersOf is what an enum or a set names, and false for any other type. MySQL's enum and
// set and ClickHouse's Enum8 and Enum16 spell theirs this way. PostgreSQL's enum is a type
// of its own and has no list here.
func membersOf(raw string) ([]string, bool) {
	name, list, found := strings.Cut(strings.ToLower(strings.TrimSpace(raw)), "(")
	if !found {
		return nil, false
	}
	switch strings.TrimSpace(name) {
	case "enum", "set", "enum8", "enum16":
	default:
		return nil, false
	}
	var members []string
	for _, match := range memberRe.FindAllStringSubmatch(list, -1) {
		members = append(members, match[1])
	}
	return members, true
}

// hasMembers reports an enum or a set.
func hasMembers(raw string) bool {
	_, ok := membersOf(raw)
	return ok
}

// membersNarrow reports a member of old that updated does not name.
func membersNarrow(old, updated []string) bool {
	return slices.ContainsFunc(old, func(member string) bool { return !slices.Contains(updated, member) })
}

// numericKind says how a number type holds a value.
type numericKind int

const (
	numericInteger numericKind = iota + 1
	numericDecimal
	numericFloat
)

// numericType is what a number type holds: its range and sign, its digits, or its bits of
// mantissa.
type numericType struct {
	kind     numericKind
	integer  fixedWidthInteger
	decimal  decimalSize
	mantissa int
}

// numericOf reads a number type, and false for any other type or one whose size needs an
// engine nobody named. MySQL's FLOAT(10,2) and DOUBLE(10,2) declare digits rather than
// bits, and read as the decimal their digits describe.
func numericOf(raw string, s spec, engine *Engine) (numericType, bool) {
	if fixed, ok := parseFixedWidthInteger(raw); ok {
		return numericType{kind: numericInteger, integer: fixed}, true
	}
	switch {
	case s.kind == "integer":
		return numericType{kind: numericInteger, integer: fixedWidthInteger{unsigned: isUnsigned(raw), bits: integerBits(s.name)}}, true
	case s.kind == "decimal":
		size, ok := decimalSizeOf(s, engine)
		return numericType{kind: numericDecimal, decimal: size}, ok
	case (s.name == "float" || s.name == "double") && len(s.args) == 2:
		return numericType{kind: numericDecimal, decimal: decimalSize{precision: s.args[0], scale: s.args[1]}}, true
	}
	e := Engine{}
	if engine != nil {
		e = *engine
	}
	bits, ok := e.floatBits(s)
	return numericType{kind: numericFloat, mantissa: bits}, ok
}

// isUnsigned reports MySQL's unsigned integer, which normalizeName takes off the name.
func isUnsigned(raw string) bool {
	lower := strings.ToLower(raw)
	return strings.Contains(lower, "unsigned") || strings.Contains(lower, "zerofill")
}

func integerBits(name string) int {
	switch integerRank(name) {
	case 1:
		return 8
	case 2:
		return 16
	case 3:
		return 24
	case 4:
		return 32
	default:
		return 64
	}
}

// decimalDigits is how many decimal digits every value of the integer may need.
func (i fixedWidthInteger) decimalDigits() int {
	switch i.bits {
	case 8:
		return 3
	case 16:
		return 5
	case 24:
		return 8
	case 32:
		return 10
	case 64:
		if i.unsigned {
			return 20
		}
		return 19
	case 128:
		return 39
	default:
		return 78
	}
}

// floatDigits is how many decimal digits a mantissa keeps exactly: 6 for four bytes and 15
// for eight.
func floatDigits(mantissa int) int {
	const digitsPerBit = 0.30103
	return int(float64(mantissa-1) * digitsPerBit)
}

// numericNarrows reports that some value of old is not a value of updated. A decimal that
// holds no more digits than a float keeps exactly still reads back as it was written, so
// only more digits than that count.
func numericNarrows(old, updated numericType) bool {
	switch {
	case old.kind == numericInteger && updated.kind == numericInteger:
		return fixedWidthNarrows(old.integer, updated.integer)
	case old.kind == numericInteger && updated.kind == numericDecimal:
		return !updated.decimal.unlimited && updated.decimal.precision-updated.decimal.scale < old.integer.decimalDigits()
	case old.kind == numericInteger:
		// The widths that exist answer the same with a sign or without one: 24 bits fit
		// a four-byte mantissa either way, and the next width does not.
		return old.integer.bits-1 > updated.mantissa
	case old.kind == numericDecimal && updated.kind == numericInteger:
		return old.decimal.unlimited || old.decimal.scale > 0 || updated.integer.unsigned ||
			old.decimal.precision >= updated.integer.decimalDigits()
	case old.kind == numericDecimal && updated.kind == numericDecimal:
		return decimalSizeNarrows(old.decimal, updated.decimal)
	case old.kind == numericDecimal:
		return old.decimal.unlimited || old.decimal.precision > floatDigits(updated.mantissa)
	case updated.kind == numericInteger:
		return true
	case updated.kind == numericDecimal:
		return !updated.decimal.unlimited
	default:
		return updated.mantissa < old.mantissa
	}
}

// textLength is how many characters every value of a number type may need when a column
// holds it as text: its digits and a sign, and a point where it has a fraction.
func (n numericType) textLength() int {
	const floatTextLength = 24
	switch {
	case n.kind == numericFloat:
		return floatTextLength
	case n.kind == numericInteger:
		return n.integer.decimalDigits() + 1
	case n.decimal.unlimited:
		return unboundedStringLength
	default:
		return n.decimal.precision + 2
	}
}

// familiesNarrow answers a change between two families. A column of text may hold a value
// no number, date or enum can stand for, and a number needs room for its digits as text.
// Text becoming bytes and any other pair of families are not read here.
func familiesNarrow(pair elementPair, engine *Engine) bool {
	newNumber, newIsNumber := numericOf(pair.newRaw, pair.updated, engine)
	if pair.old.kind == "string" {
		if newIsNumber {
			return true
		}
		if _, temporal := parseTemporal(pair.newRaw); temporal {
			return true
		}
		_, enum := membersOf(pair.newRaw)
		return enum
	}
	oldNumber, oldIsNumber := numericOf(pair.oldRaw, pair.old, engine)
	if oldIsNumber && newIsNumber {
		return numericNarrows(oldNumber, newNumber)
	}
	if oldIsNumber && pair.updated.kind == "string" {
		newLen, ok := stringLength(pair.updated, engine)
		return ok && newLen < oldNumber.textLength()
	}
	return false
}
