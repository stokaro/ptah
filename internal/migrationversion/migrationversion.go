// Package migrationversion answers the one question every writer that picks a
// migration version has to answer before it writes: can the reader read this
// version back.
//
// A version is not free-form. The ptah layout renders it with %010d and parses
// it with a ten-digit regex, so 9999999999 is the largest value that survives a
// round trip. The Atlas layout renders it with %d and its parser refuses a
// version that is not strictly positive, so the bound there is math.MaxInt64.
//
// Both bounds matter because discovery drops a name it cannot parse SILENTLY:
// [migrator.DiscoverMigrationFiles] only reports "no migration files matched"
// when the matched set is empty, so an unreadable eleventh file among ten
// readable ones is written, hashed into the integrity file, reported as
// created, and then never executed. Any rule of the form "one above the newest
// migration" therefore has to ask [Next] instead of computing latest+1 itself
// (stokaro/ptah#938).
//
// There is a third bound that is not about parsing at all. Every Atlas-format
// version this binary stamps is a UTC yyyyMMddHHmmss instant, so a fourteen-
// digit version is READ as a date by every human and every tool that renders
// one. Integer arithmetic does not respect that: 20991231235959 + 1 is
// 20991231235960, sixty seconds past the minute, an instant that does not
// exist and that time.Parse refuses. [Writable] and [Advance] step over those
// values so no writer can mint one.
package migrationversion

import (
	"fmt"
	"math"
	"strconv"
	"time"

	"go.5x5.cz/ptah/migration/migrator"
)

const (
	// PtahMax is the largest version a ptah NNNNNNNNNN_description.up.sql name
	// can carry. GenerateMigrationFileName renders the version with %010d and
	// ParseMigrationFileName matches exactly ten digits, so 10000000000 writes
	// an eleven-digit name no reader accepts.
	PtahMax int64 = 9999999999

	// AtlasMax is the largest version an Atlas <version>_description.sql name
	// can carry. The name renders the version with %d and the parser requires a
	// digit-leading stem parsed as a positive int64, so the ceiling is the int64
	// ceiling itself -- one more wraps to a negative version whose leading `-`
	// is not a digit.
	AtlasMax int64 = math.MaxInt64
)

// Max returns the largest version format's file names can carry.
//
// Only the Atlas layout gets the int64 ceiling. Every other value -- ptah,
// auto, or the empty format -- is bounded by the ptah width, because a writer
// that is not writing Atlas names is writing the fixed-width paired ones.
func Max(format migrator.MigrationDirFormat) int64 {
	if format == migrator.MigrationDirFormatAtlas {
		return AtlasMax
	}
	return PtahMax
}

// Next returns the first version above latest that format can still write, or
// an error when there is none.
//
// The check runs BEFORE the addition on purpose: on the Atlas layout
// latest+1 for latest == [AtlasMax] wraps to math.MinInt64, which renders as a
// perfectly plausible file name and is what stokaro/ptah#938 measured being
// written and then dropped. A directory reaches that value through ordinary
// use -- `migrate import --dir-format flyway` stamps a Flyway `R__` repeatable
// with [AtlasMax] so it sorts last.
func Next(latest int64, format migrator.MigrationDirFormat) (int64, error) {
	limit := Max(format)
	if latest >= limit {
		return 0, fmt.Errorf(
			"cannot allocate a migration version above %d: %s migration file names carry at most %d, "+
				"and a larger version would be written but never read back",
			latest, formatLabel(format), limit,
		)
	}
	return latest + 1, nil
}

// Check reports whether version can be written as a format file name and read
// back. It is the counterpart of [Next] for callers that advance a version
// themselves, such as a scan stepping past names that are already taken.
func Check(version int64, format migrator.MigrationDirFormat) error {
	limit := Max(format)
	if version <= 0 || version > limit {
		return fmt.Errorf(
			"migration version %d cannot be written: %s migration file names carry a version between 1 and %d, "+
				"and a version outside that range would be written but never read back",
			version, formatLabel(format), limit,
		)
	}
	return nil
}

// StampLayout is the Go reference layout every Atlas-format version this binary
// stamps is rendered in.
const StampLayout = "20060102150405"

const (
	// stampLow and stampHigh bracket the values that HAVE the fourteen digits of
	// a [StampLayout] version. A value inside them is read as a date whether or
	// not it is one; a value outside them makes no claim about a time. Paired
	// versions top out at [PtahMax], ten digits, so they are always outside.
	stampLow  int64 = 10000000000000 // 1000-01-01 00:00:00
	stampHigh int64 = 99999999999999 // 9999-99-99 99:99:99, the largest 14-digit value

	// aboveStamps is the first version that no longer has fourteen digits, and
	// so the first one above [stampHigh] that claims to be no instant at all.
	aboveStamps int64 = 100000000000000
)

// IsStamp reports whether version reads back as the UTC instant its fourteen
// digits claim. Values outside the fourteen-digit range are not stamps and
// claim nothing, so they are false.
func IsStamp(version int64) bool {
	if version < stampLow || version > stampHigh {
		return false
	}
	digits := strconv.FormatInt(version, 10)
	at, err := time.Parse(StampLayout, digits)
	if err != nil {
		return false
	}
	return at.UTC().Format(StampLayout) == digits
}

// Writable returns the first version at or above candidate that format can
// write into a file name AND that no reader can mistake for an instant that
// does not exist.
//
// It is [Check] with a step instead of a refusal for the one failure a step can
// repair. A fourteen-digit non-instant -- 20991231235960, 20260231000000 --
// parses fine as a version, so refusing would strand a caller that only wanted
// the next free slot; raising it to the next real second gives the caller a
// version that means what it looks like. The bounds in [Check] are different:
// nothing above them is writable at all, so those still return an error.
func Writable(candidate int64, format migrator.MigrationDirFormat) (int64, error) {
	version := candidate
	if version >= stampLow && version <= stampHigh && !IsStamp(version) {
		raised, ok := ceilStamp(version)
		if !ok {
			// Past 9999-12-31 23:59:59 there is no next instant, so the first
			// version that stops claiming to be one is the first fifteen-digit
			// value.
			raised = aboveStamps
		}
		version = raised
	}
	if err := Check(version, format); err != nil {
		return 0, err
	}
	return version, nil
}

// Advance returns the first version STRICTLY above latest that format can write
// and that reads back as the instant it looks like.
//
// It is what every "one past the newest migration" rule wants: [Next] bounds
// the arithmetic, and [Writable] keeps the answer a real second. Beside a
// migration dated 29991231235959 the raw increment produced 29991231235960 --
// the value stokaro/ptah#938 names, sixty seconds past the minute -- and this
// returns 30000101000000 instead.
func Advance(latest int64, format migrator.MigrationDirFormat) (int64, error) {
	next, err := Next(latest, format)
	if err != nil {
		return 0, err
	}
	return Writable(next, format)
}

// WritableRun returns the first version at or above candidate such that it and
// the count-1 versions after it are ALL writable under [Writable].
//
// A batch that stages its files at version+0, version+1, ... needs the whole
// run, not just its first slot: a two-file plan based at 20991231235959 would
// put its second file at 20991231235960. count below 1 is treated as 1.
func WritableRun(candidate int64, count int, format migrator.MigrationDirFormat) (int64, error) {
	if count < 1 {
		count = 1
	}
	for {
		base, err := Writable(candidate, format)
		if err != nil {
			return 0, err
		}
		if base > Max(format)-int64(count)+1 {
			return 0, fmt.Errorf(
				"cannot allocate %d consecutive migration versions from %d: %s migration file names carry "+
					"at most %d, and the run would pass it",
				count, base, formatLabel(format), Max(format),
			)
		}
		last, err := Writable(base+int64(count)-1, format)
		if err != nil {
			return 0, err
		}
		if last == base+int64(count)-1 {
			return base, nil
		}
		// The run is broken somewhere inside it. Restart above the base rather
		// than guessing where: candidate strictly increases, so this ends.
		candidate = base + 1
	}
}

// ceilStamp returns the smallest [StampLayout] instant at or above version, for
// a version that already has fourteen digits. The second return is false when
// there is none, which happens only past 9999-12-31 23:59:59.
//
// The carries run from the least significant field up, and every one of them
// raises the value it is applied to: an out-of-range field is replaced by its
// minimum and the fields below it are zeroed, which always costs less than the
// carry into the field above adds.
func ceilStamp(version int64) (int64, bool) {
	year := int(version / 10000000000)
	month := int(version / 100000000 % 100)
	day := int(version / 1000000 % 100)
	hour := int(version / 10000 % 100)
	minute := int(version / 100 % 100)
	second := int(version % 100)

	if second > 59 {
		second, minute = 0, minute+1
	}
	if minute > 59 {
		minute, hour = 0, hour+1
	}
	if hour > 23 {
		hour, day = 0, day+1
	}
	if month < 1 {
		month, day, hour, minute, second = 1, 1, 0, 0, 0
	}
	if month > 12 {
		year, month, day, hour, minute, second = year+1, 1, 1, 0, 0, 0
	}
	if day < 1 {
		day, hour, minute, second = 1, 0, 0, 0
	}
	if day > daysInMonth(year, month) {
		month, day, hour, minute, second = month+1, 1, 0, 0, 0
		if month > 12 {
			year, month = year+1, 1
		}
	}
	if year > 9999 {
		return 0, false
	}
	return int64(year)*10000000000 +
		int64(month)*100000000 +
		int64(day)*1000000 +
		int64(hour)*10000 +
		int64(minute)*100 +
		int64(second), true
}

// daysInMonth returns the last day of month in year. Day zero of the following
// month is the last day of this one, and time.Date normalizes month 13 into
// January of the next year, so leap years need no special case.
func daysInMonth(year, month int) int {
	return time.Date(year, time.Month(month)+1, 0, 0, 0, 0, 0, time.UTC).Day()
}

func formatLabel(format migrator.MigrationDirFormat) string {
	if format == migrator.MigrationDirFormatAtlas {
		return string(migrator.MigrationDirFormatAtlas)
	}
	return string(migrator.MigrationDirFormatPtah)
}
