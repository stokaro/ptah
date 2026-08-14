package capabilityprobe_test

import (
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/capabilityprobe"
)

// sqlServer2025Banner is what @@VERSION returned from a live
// mcr.microsoft.com/mssql/server:2025-latest container, verbatim including the
// embedded newlines.
const sqlServer2025Banner = "Microsoft SQL Server 2025 (RTM-CU7) (KB5096981) - 17.0.4065.4 (X64) \n" +
	"\tJul  8 2026 23:26:08 \n" +
	"\tCopyright (C) 2025 Microsoft Corporation\n" +
	"\tEnterprise Developer Edition (64-bit) on Linux (Ubuntu 24.04.4 LTS) <X64>"

// yugabyteBanner is the shape a YugabyteDB YSQL server reports: the leading
// number is the PostgreSQL compatibility version, not the product version.
const yugabyteBanner = "PostgreSQL 15.12-YB-2026.1.0.0-b0 on aarch64-unknown-linux-gnu, compiled by clang, 64-bit"

// TestParseVersion_CorrectsTheBannersOneParserCannotRead pins each dialect's
// extraction rule against the banner that defeats the shared one.
//
// The postgres rows are the control: they use the same "first dotted run of
// digits" rule capability.parseVersion uses, so seeing them read the SQL Server
// banner as 2025 and the YugabyteDB banner as 15.12 is what makes the corrected
// rows mean something. Without the control this test would only assert that a
// function returns what it was written to return.
//
// "The same rule" is an equivalence, and an equivalence nobody executes is a
// comment. Two tests execute it instead of asserting it:
// TestParseVersion_TheSharedParserReallyMisreadsTheSQLServerBanner below runs
// the shared resolver on these bytes, and
// capability.TestParseVersion_ReadsTheWrongNumberOutOfTwoRealBanners calls the
// shared parser itself, where the numbers are readable.
func TestParseVersion_CorrectsTheBannersOneParserCannotRead(t *testing.T) {

	for _, tc := range []struct {
		name    string
		dialect string
		banner  string
		product string
		want    string
	}{{
		name:    "the shared rule reads the SQL Server marketing year",
		dialect: platform.Postgres,
		banner:  sqlServer2025Banner,
		want:    "2025",
	}, {
		name:    "the sqlserver rule reads the product version out of the banner",
		dialect: platform.SQLServer,
		banner:  sqlServer2025Banner,
		want:    "17.0.4065.4",
	}, {
		name:    "SERVERPROPERTY('ProductVersion') wins over every banner heuristic",
		dialect: platform.SQLServer,
		banner:  sqlServer2025Banner,
		product: "17.0.4065.4",
		want:    "17.0.4065.4",
	}, {
		name:    "the shared rule reads YugabyteDB's PostgreSQL compatibility version",
		dialect: platform.Postgres,
		banner:  yugabyteBanner,
		want:    "15.12",
	}, {
		name:    "the yugabytedb rule reads the product version",
		dialect: platform.YugabyteDB,
		banner:  yugabyteBanner,
		want:    "2026.1.0.0",
	}, {
		name:    "MariaDB's fake replication prefix is trimmed",
		dialect: platform.MariaDB,
		banner:  "5.5.5-10.11.6-MariaDB-1:10.11.6+maria~ubu2204",
		want:    "10.11.6",
	}, {
		name:    "a MariaDB banner without the prefix is unchanged",
		dialect: platform.MariaDB,
		banner:  "11.4.12-MariaDB-ubu2404",
		want:    "11.4.12",
	}, {
		name:    "PostgreSQL",
		dialect: platform.Postgres,
		banner:  "PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on aarch64-unknown-linux-gnu",
		want:    "17.10",
	}, {
		name:    "MySQL",
		dialect: platform.MySQL,
		banner:  "9.7.1",
		want:    "9.7.1",
	}, {
		name:    "CockroachDB",
		dialect: platform.CockroachDB,
		banner:  "CockroachDB CCL v26.2.5 (aarch64-unknown-linux-gnu, built 2026/07/28 18:55:27, go1.25.5)",
		want:    "26.2.5",
	}} {
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)
			got, err := capabilityprobe.ParseVersion(tc.dialect, tc.banner, tc.product)
			c.Assert(err, qt.IsNil)
			c.Assert(got.String(), qt.Equals, tc.want)
		})
	}
}

// TestParseVersion_TheSharedParserReallyMisreadsTheSQLServerBanner runs the
// shared code on the SQL Server banner instead of describing what it would do.
//
// capability.parseVersion is unexported, so the executable surface is
// capability.ResolveServerVersion on the postgres dialect: it saturates exactly
// when the parsed major is above the newest measured PostgreSQL line. A banner
// the shared parser reads as 2025 therefore saturates and the product version
// it should have read, 17.0.4065.4, does not — one call each, opposite answers,
// no equivalence taken on trust.
//
// YugabyteDB cannot be exercised the same way and that is the finding rather
// than a gap: ResolveServerVersion matches "-yb-" before it parses anything, so
// no exported surface routes that banner through the shared parser at all.
// capability.TestResolveServerVersion_MasksTheYugabyteMisread executes the
// masking, and capability.TestParseVersion_ReadsTheWrongNumberOutOfTwoRealBanners
// executes the misread underneath it.
func TestParseVersion_TheSharedParserReallyMisreadsTheSQLServerBanner(t *testing.T) {
	c := qt.New(t)

	shared := capability.ResolveServerVersion(platform.Postgres, sqlServer2025Banner)
	c.Assert(shared.Saturated, qt.IsTrue,
		qt.Commentf("the shared parser must have read a major above the newest measured PostgreSQL line "+
			"(%s) out of this banner, which is only possible if it took the marketing year", shared.NewestMeasured))

	corrected, err := capabilityprobe.ParseVersion(platform.SQLServer, sqlServer2025Banner, "")
	c.Assert(err, qt.IsNil)
	c.Assert(corrected.String(), qt.Equals, "17.0.4065.4")
	c.Assert(capability.ResolveServerVersion(platform.Postgres, corrected.String()).Saturated, qt.IsFalse,
		qt.Commentf("the version the sqlserver rule recovers is below that line, so the assertion above is "+
			"about which number the shared parser picked and not about the banner being long"))
}

func TestParseVersion_RefusesABannerWithNoDigits(t *testing.T) {
	c := qt.New(t)

	_, err := capabilityprobe.ParseVersion(platform.Postgres, "no digits at all", "")
	c.Assert(err, qt.ErrorMatches, `no numeric version in "no digits at all"`)
}
