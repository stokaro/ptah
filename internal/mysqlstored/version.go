package mysqlstored

import (
	"strconv"
	"strings"
)

// VersionID spells a version a server reports, such as "8.0.46" or
// "11.4.2-MariaDB-log", as the server compares it with the version an
// executable comment is gated on: major*10000 + minor*100 + patch. A version
// it cannot read is 0.
func VersionID(version string) int {
	parts := strings.SplitN(version, ".", 3)
	if len(parts) < 2 {
		return 0
	}
	id := 0
	for i, scale := range []int{10000, 100, 1} {
		number := 0
		if i < len(parts) {
			digits := strings.IndexFunc(parts[i], func(r rune) bool { return r < '0' || r > '9' })
			if digits < 0 {
				digits = len(parts[i])
			}
			value, err := strconv.Atoi(parts[i][:digits])
			if err != nil || i < 2 && digits != len(parts[i]) {
				return 0
			}
			number = value
		}
		id += number * scale
	}
	return id
}
