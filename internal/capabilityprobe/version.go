package capabilityprobe

import (
	"fmt"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/platform"
)

// Version is a server's product version, corrected per dialect.
//
// It exists because one parser cannot read nine banners. The shared
// capability.parseVersion takes the first dotted run of digits it finds, which
// is right for six dialects and wrong for two:
//
//   - SQL Server's @@VERSION opens with the marketing year, so the shared parse
//     reads "Microsoft SQL Server 2025 (RTM-CU7) ... 17.0.4065.4" as major
//     2025. That is latent today only because the sqlserver dialect never
//     reaches capability.go's version switch; it becomes a live mis-selection
//     the day a `case platform.SQLServer:` is added. It is not latent HERE: a
//     matrix cell labeled by the marketing year would be measuring a version
//     that does not exist.
//   - YugabyteDB's banner opens with the PostgreSQL compatibility version, so
//     the shared parse reads "PostgreSQL 15.12-YB-2026.1.0.0-b0" as 15.12 when
//     the product is 2026.1.0.0. That one is masked in capability.go because
//     the "-yb-" banner match fires before the parse runs.
//
// MariaDB is a third instance of the same class, already special-cased in
// capability.go by trimming the fake "5.5.5-" replication prefix. Correcting
// the class rather than the one instance the issue named is why this is a
// per-dialect extractor and not another single parser.
type Version struct {
	// Raw is the banner the extractor was given.
	Raw string
	// Numbers are the numeric components in order, most significant first.
	Numbers []int
	// Source names where the numbers came from, for the report.
	Source string
}

// Components renders the numeric components as strings for cell matching.
func (v Version) Components() []string {
	out := make([]string, 0, len(v.Numbers))
	for _, n := range v.Numbers {
		out = append(out, strconv.Itoa(n))
	}
	return out
}

// String renders the dotted version.
func (v Version) String() string {
	return strings.Join(v.Components(), ".")
}

// mariaDBReplicationPrefix is the fake version MariaDB prepends when speaking
// the MySQL protocol ("5.5.5-10.11.6-MariaDB").
const mariaDBReplicationPrefix = "5.5.5-"

// ParseVersion extracts the product version from a live server banner.
//
// productVersion, when non-empty, is a version the caller read from a
// dedicated catalog surface rather than from the banner — today that is
// SERVERPROPERTY('ProductVersion') on SQL Server, which reports the clean
// four-part build with no marketing year in front of it. It wins over any
// banner heuristic, because a heuristic is a guess about text and this is the
// server's own answer.
func ParseVersion(dialect, banner, productVersion string) (Version, error) {
	if trimmed := strings.TrimSpace(productVersion); trimmed != "" {
		numbers, ok := leadingNumbers(trimmed)
		if !ok {
			return Version{}, fmt.Errorf("no numeric version in product version %q", productVersion)
		}
		return Version{Raw: banner, Numbers: numbers, Source: "server product version"}, nil
	}

	text, source := versionText(platform.NormalizeDialect(dialect), banner)
	numbers, ok := leadingNumbers(text)
	if !ok {
		return Version{}, fmt.Errorf("no numeric version in %q", banner)
	}
	return Version{Raw: banner, Numbers: numbers, Source: source}, nil
}

// versionText narrows a banner to the substring that holds the PRODUCT
// version, and names which rule did the narrowing.
func versionText(dialect, banner string) (text, source string) {
	switch dialect {
	case platform.SQLServer:
		// "Microsoft SQL Server 2025 (RTM-CU7) (KB...) - 17.0.4065.4 (X64) ..."
		// The product version is the token after the first " - "; everything
		// before it is the marketing year and the servicing label.
		if _, after, found := strings.Cut(banner, " - "); found {
			return after, "banner text after the first \" - \" (the marketing year precedes it)"
		}
		return banner, "banner (no \" - \" separator found)"
	case platform.YugabyteDB:
		// "PostgreSQL 15.12-YB-2026.1.0.0-b0 on aarch64 ..." — the leading
		// number is the PostgreSQL compatibility version, not the product.
		if _, after, found := strings.Cut(strings.ToUpper(banner), "-YB-"); found {
			return after, "banner text after \"-YB-\" (the PostgreSQL compatibility version precedes it)"
		}
		return banner, "banner (no \"-YB-\" marker found)"
	case platform.MariaDB:
		return strings.TrimPrefix(banner, mariaDBReplicationPrefix), "banner with the 5.5.5- replication prefix trimmed"
	default:
		return banner, "banner"
	}
}

// leadingNumbers returns the first dotted run of digits in s.
func leadingNumbers(s string) ([]int, bool) {
	i := 0
	for i < len(s) && !isDigit(s[i]) {
		i++
	}
	if i == len(s) {
		return nil, false
	}
	var numbers []int
	for i < len(s) && isDigit(s[i]) {
		start := i
		value := 0
		for i < len(s) && isDigit(s[i]) {
			value = value*10 + int(s[i]-'0')
			i++
		}
		if start == i {
			break
		}
		numbers = append(numbers, value)
		if i == len(s) || s[i] != '.' {
			break
		}
		i++
	}
	return numbers, len(numbers) > 0
}

func isDigit(b byte) bool {
	return b >= '0' && b <= '9'
}
