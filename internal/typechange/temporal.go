package typechange

import (
	"regexp"
	"strconv"
	"strings"
)

// A temporal type is compared on what its values hold, because a server converts one to
// another by dropping what the new type has no room for. Measured on PostgreSQL 17.11,
// MySQL 8.4, MariaDB 12.3, SQL Server 2025 and ClickHouse 26.10: `.123456` read back as
// nothing under a lower precision, and a timestamp read back as its date under `date`.
//
// What a type that declares no precision holds is the engine's answer and differs by
// engine: six digits on PostgreSQL, none on MySQL and seven on SQL Server. So
// `timestamp(3)` to `timestamp` loses digits on MySQL and gains them on PostgreSQL, and
// only [Engine.Narrows] can say which.

// temporalFamily is what a value of a temporal type is made of.
type temporalFamily int

const (
	familyDateTime temporalFamily = iota + 1
	familyDate
	familyTime
	familyInterval
)

// undeclared stands for a precision a spelling leaves out.
const undeclared = -1

// temporalType is a temporal type as its spelling reads.
type temporalType struct {
	family temporalFamily
	// name is the type's name, with a zone qualifier folded into it.
	name string
	// digits is the declared count of fractional-second digits, or undeclared.
	digits int
	// leading is an Oracle interval's declared leading-field digits, or undeclared.
	leading int
	// smallest ranks the smallest field an interval keeps; see [intervalFieldRank].
	smallest int
	// array reports an array of the type.
	array bool
}

// temporalNameRe reads every temporal type but an interval: the name, what its
// parentheses hold, and a zone qualifier behind them.
var temporalNameRe = regexp.MustCompile(
	`^(timestamptz|timestamp|timetz|time64|time|smalldatetime|datetimeoffset|datetime64|datetime32|datetime2|datetime|date32|date)` +
		`(?: ?\(([^)]*)\))?(?: (with|without)(?: local)? time zone)?$`)

// intervalRe reads an interval: `interval`, `interval(3)`, `interval year to month`,
// `interval day to second(3)`, and Oracle's `interval day(2) to second(6)`.
var intervalRe = regexp.MustCompile(
	`^interval(?: (year|month|day|hour|minute|second)(?: ?\((\d+)\))?(?: to (month|hour|minute|second)(?: ?\((\d+)\))?)?)?(?: ?\((\d+)\))?$`)

// intervalFieldRank orders the fields an interval can end at, from the finest. A server
// clears what lies below the last field a column declares: measured on PostgreSQL 17.11,
// `2 years 3 mons 4 days 05:06:07` under `interval year to month` reads `2 years 3 mons`.
var intervalFieldRank = map[string]int{"second": 0, "minute": 1, "hour": 2, "day": 3, "month": 4, "year": 5}

// temporalRangeRank orders the names of one family by the range their values span, so a
// lower rank cannot hold every value of a higher one. MySQL's timestamp ends in 2038
// where its datetime runs to 9999, SQL Server's smalldatetime ends in 2079 where its
// datetime runs to 9999 from 1753, its datetime2 starts at the year 1, and ClickHouse's
// DateTime and Date end before its DateTime64 and Date32. No engine holds two of these
// in the other order. A name left out shares the lowest rank.
var temporalRangeRank = map[string]int{
	"smalldatetime": 1, "timestamp": 2, "timestamptz": 2, "datetime": 3, "datetime32": 3,
	"datetime2": 4, "datetimeoffset": 4, "datetime64": 4,
	"date": 1, "date32": 2,
}

// parseTemporal reads raw as a temporal type. Nullable, LowCardinality and Array are
// looked inside, since they change what a column admits and not the values it holds.
func parseTemporal(raw string) (temporalType, bool) {
	clean := normalizeName(raw)
	array := false
	for {
		switch {
		case strings.HasSuffix(clean, "[]"):
			clean, array = strings.TrimSpace(strings.TrimSuffix(clean, "[]")), true
		case strings.HasPrefix(clean, "array(") && strings.HasSuffix(clean, ")"):
			clean, array = clean[len("array("):len(clean)-1], true
		case strings.HasPrefix(clean, "nullable(") && strings.HasSuffix(clean, ")"):
			clean = clean[len("nullable(") : len(clean)-1]
		case strings.HasPrefix(clean, "lowcardinality(") && strings.HasSuffix(clean, ")"):
			clean = clean[len("lowcardinality(") : len(clean)-1]
		default:
			parsed, ok := parseUnwrappedTemporal(clean)
			parsed.array = array
			return parsed, ok
		}
	}
}

func parseUnwrappedTemporal(clean string) (temporalType, bool) {
	if match := intervalRe.FindStringSubmatch(clean); match != nil {
		last := match[1]
		if match[3] != "" {
			last = match[3]
		}
		if last == "" {
			last = "second"
		}
		parsed := temporalType{
			family: familyInterval, name: "interval", digits: undeclared, leading: undeclared,
			smallest: intervalFieldRank[last],
		}
		for _, digits := range []string{match[5], match[4]} {
			if digits != "" {
				parsed.digits = atoiOr(digits, undeclared)
			}
		}
		if match[3] == "" && match[1] == "second" && match[2] != "" {
			parsed.digits = atoiOr(match[2], undeclared)
		} else if match[2] != "" {
			parsed.leading = atoiOr(match[2], undeclared)
		}
		return parsed, true
	}
	match := temporalNameRe.FindStringSubmatch(clean)
	if match == nil {
		return temporalType{}, false
	}
	parsed := temporalType{name: match[1], digits: undeclared, leading: undeclared}
	if digits, _, _ := strings.Cut(match[2], ","); strings.TrimSpace(digits) != "" {
		parsed.digits = atoiOr(strings.TrimSpace(digits), undeclared)
	}
	switch {
	case match[3] == "with" && parsed.name == "timestamp":
		parsed.name = "timestamptz"
	case match[3] == "with" && parsed.name == "time":
		parsed.name = "timetz"
	}
	switch parsed.name {
	case "date", "date32":
		parsed.family = familyDate
	case "time", "timetz", "time64":
		parsed.family = familyTime
	default:
		parsed.family = familyDateTime
	}
	return parsed, true
}

func atoiOr(text string, fallback int) int {
	value, err := strconv.Atoi(text)
	if err != nil {
		return fallback
	}
	return value
}

// holdsOffset reports a type whose values keep the zone offset they were written with.
// PostgreSQL's timestamptz is not one: it stores the instant, and a change to timestamp
// reinterprets it without dropping anything stored. Oracle's TIMESTAMP WITH TIME ZONE
// keeps an offset, and the server refuses to change it while a row exists (ORA-01439).
func (t temporalType) holdsOffset() bool {
	return t.name == "timetz" || t.name == "datetimeoffset"
}

// resolvedDigits is how many fractional-second digits a value of the type holds, and
// false when that depends on an engine nobody named. A negative answer is coarser than
// a whole second.
func (t temporalType) resolvedDigits(engine *Engine) (int, bool) {
	switch {
	case t.family == familyDate:
		return 0, true
	case t.family == familyInterval && t.smallest != intervalFieldRank["second"]:
		return 0, true
	case t.digits != undeclared:
		return t.digits, true
	case t.name == "smalldatetime":
		return -1, true
	case t.name == "datetime64":
		// A bare DateTime64 is DateTime64(3); chtype.Canonical says so too.
		return 3, true
	case engine == nil:
		return 0, false
	default:
		return engine.TimeDigits, true
	}
}

// resolvedLeading is how many digits an interval's leading field holds, and false where
// neither the spelling nor the engine says.
func (t temporalType) resolvedLeading(engine *Engine) (int, bool) {
	switch {
	case t.leading != undeclared:
		return t.leading, true
	case engine != nil && engine.Bare.IntervalLeading > 0:
		return engine.Bare.IntervalLeading, true
	default:
		return 0, false
	}
}

// temporalNarrows reports whether some value of old is not a value of updated. A family
// the two do not share loses the date, or the time of day, that updated has no room
// for; a date becoming a datetime and a time becoming one lose nothing.
func temporalNarrows(old, updated temporalType, engine *Engine) bool {
	if old.array != updated.array {
		return false
	}
	if old.family != updated.family {
		switch {
		case old.family == familyInterval || updated.family == familyInterval:
			return false
		case updated.family == familyDateTime:
			return false
		default:
			return true
		}
	}
	if old.holdsOffset() && !updated.holdsOffset() {
		return true
	}
	if temporalRangeRank[updated.name] < temporalRangeRank[old.name] {
		return true
	}
	if old.family == familyInterval {
		if updated.smallest > old.smallest {
			return true
		}
		oldLeading, oldOK := old.resolvedLeading(engine)
		newLeading, newOK := updated.resolvedLeading(engine)
		if oldOK && newOK && newLeading < oldLeading {
			return true
		}
	}
	oldDigits, oldOK := old.resolvedDigits(engine)
	newDigits, newOK := updated.resolvedDigits(engine)
	// SQL Server's datetime takes no precision, and holds three digits rounded to
	// 1/300 of a second: measured on SQL Server 2025, .123 reads .1233333 under
	// datetime2(7) and is gone under datetime2(0). MySQL's and ClickHouse's datetime
	// never meet these two names.
	if old.name == "datetime" && (updated.name == "datetime2" || updated.name == "datetimeoffset") {
		oldDigits, oldOK = 3, true
	}
	return oldOK && newOK && newDigits < oldDigits
}

// temporalWidens reports a type of the same family that keeps more digits or spans a
// wider range, and loses nothing.
func temporalWidens(old, updated temporalType, engine *Engine) bool {
	if old.array != updated.array || old.family != updated.family || temporalNarrows(old, updated, engine) {
		return false
	}
	if temporalRangeRank[updated.name] > temporalRangeRank[old.name] {
		return true
	}
	oldDigits, oldOK := old.resolvedDigits(engine)
	newDigits, newOK := updated.resolvedDigits(engine)
	return oldOK && newOK && newDigits > oldDigits
}

// IsTemporal reports whether a type is a date, a time, a timestamp or an interval, on
// any engine.
func IsTemporal(typeName string) bool {
	_, ok := parseTemporal(typeName)
	return ok
}
