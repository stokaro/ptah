package typechange

// Rounding is a type change that keeps fewer digits after the point than the type the
// column holds: a decimal with a lower scale, a decimal under an integer, or a temporal
// type with a lower fractional-second precision or, for an interval, a coarser last
// field.
//
// A server that converts the column stores each value at the new type's digits, and
// most do it without an error. Measured with a stored 1234.567 under DECIMAL(8,1) and
// 10:00:00.987654 under a precision of 0: MySQL 8.4 reads 1234.6 and 10:00:01 back,
// MariaDB 11.8 1234.6 and 10:00:00, PostgreSQL 18 and SQL Server 2022 1234.6 and
// 10:00:01, ClickHouse 26.10 1234.5 and 10:00:00, each with the statement reporting
// success, and on MySQL and MariaDB under a strict sql_mode as well. CockroachDB 26.4
// rounds between two sized decimals and refuses a value an unsized decimal holds beyond
// the new scale. Oracle 23.26 refuses the change while the column holds a row
// (ORA-01440, ORA-30082), whether or not a value has the digits.
type Rounding struct {
	// Temporal reports a temporal type. False reports a decimal, under a decimal or an
	// integer.
	Temporal bool
}

// Rounds reports whether changing from oldType to newType keeps fewer digits after the
// point, as the engine reads a type that declares no size, and which kind of type it is.
//
// It answers within one kind of type: two decimals, a decimal and an integer, or two
// temporal types whose values are made of the same fields. A date under a timestamp loses
// more than digits, and a float under a decimal is not a scale, so neither is a rounding
// here; [Engine.Narrows] calls both a narrowing. A type whose digits neither its spelling
// nor the engine states is not a rounding either, and neither is an array: what the
// question is asked of is a value, and an array holds several.
func (e Engine) Rounds(oldType, newType string) (Rounding, bool) {
	if old, ok := parseTemporal(oldType); ok {
		updated, ok := parseTemporal(newType)
		if !ok || old.array || updated.array || old.family != updated.family {
			return Rounding{}, false
		}
		_, fewer := fewerFractionDigits(old, updated, &e)
		// An interval that ends at a coarser field clears what lies below it, which
		// is the same loss as fewer digits: '1 day 02:03:04' under `interval hour`
		// reads '1 day 02:00:00' on PostgreSQL 18.
		coarser := old.family == familyInterval && updated.smallest > old.smallest
		if !fewer && !coarser {
			return Rounding{}, false
		}
		return Rounding{Temporal: true}, true
	}
	if _, isArray := unwrappedType(oldType); isArray {
		return Rounding{}, false
	}
	pair, ok := elementSpecs(oldType, newType)
	if !ok || pair.old.kind != "decimal" {
		return Rounding{}, false
	}
	old, oldOK := numericOf(pair.oldRaw, pair.old, &e)
	updated, newOK := numericOf(pair.newRaw, pair.updated, &e)
	if !oldOK || !newOK {
		return Rounding{}, false
	}
	switch {
	case updated.kind == numericDecimal && pair.updated.kind == "decimal":
		return Rounding{}, fewerScaleDigits(old.decimal, updated.decimal)
	case updated.kind == numericInteger:
		return Rounding{}, fewerScaleDigits(old.decimal, integerScale)
	default:
		return Rounding{}, false
	}
}
