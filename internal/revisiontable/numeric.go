package revisiontable

import (
	"strconv"
	"strings"
)

// NumericTableRefuses reports whether Ptah's revision table, which records
// versions as numbers, cannot keep an Atlas migration whose revision version
// is version.
//
// repeatable marks R__<name>.sql, which the Atlas format runs once as the
// version R, under the key above every numbered file. positioned says the
// directory's migrations run under their positions in execution order, as
// migrationfile.AtlasKeyedByPosition reports. Where either holds, every
// version that is not a decimal number above zero is refused. A position moves
// when a file is added before it, so the row the table keeps would name
// another file the next run; and no file spells the key the table would keep
// for R, so the next status reads R as pending and the key as a migration with
// no file. Elsewhere the table keeps the number the name spells.
//
// The migrator refuses a run over a directory holding such a migration, and
// migration lint reports the file. Both ask this one predicate, because two
// lists of the refused files would agree when the second was written and stop
// agreeing when the first was extended.
func NumericTableRefuses(version string, repeatable, positioned bool) bool {
	return (repeatable || positioned) && !decimalAboveZero(version)
}

// decimalAboveZero reports whether version is a decimal number above zero,
// spelled with digits alone, which a numeric revision column keeps.
func decimalAboveZero(version string) bool {
	_, err := strconv.ParseInt(version, 10, 64)
	return err == nil && strings.Trim(version, "0123456789") == "" && strings.Trim(version, "0") != ""
}
