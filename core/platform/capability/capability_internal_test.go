package capability

// White-box testing required: parseVersion is unexported and every exported
// caller collapses its result into a preset or a boolean, so no external test
// can read the numbers it produced. Those numbers are the subject here — two
// banners this package reads WRONGLY, and a claim written in prose in
// internal/capabilityprobe/version.go that until now nothing executed.

import (
	"testing"

	qt "github.com/frankban/quicktest"
)

// sqlServer2025Banner is what @@VERSION returned from a live
// mcr.microsoft.com/mssql/server:2025-latest container, verbatim including the
// embedded newlines. internal/capabilityprobe/version_test.go pins the same
// bytes; each package keeps its own copy because this one cannot import that
// one without a cycle.
const sqlServer2025Banner = "Microsoft SQL Server 2025 (RTM-CU7) (KB5096981) - 17.0.4065.4 (X64) \n" +
	"\tJul  8 2026 23:26:08 \n" +
	"\tCopyright (C) 2025 Microsoft Corporation\n" +
	"\tEnterprise Developer Edition (64-bit) on Linux (Ubuntu 24.04.4 LTS) <X64>"

// yugabyteBanner is what a live yugabytedb/yugabyte:2026.1.0.0-b118 reported
// through YSQL. The leading number is the PostgreSQL compatibility version.
const yugabyteBanner = "PostgreSQL 15.12-YB-2026.1.0.0-b0 on aarch64-unknown-linux-gnu, compiled by clang, 64-bit"

// TestParseVersion_ReadsTheWrongNumberOutOfTwoRealBanners executes the misread
// that internal/capabilityprobe exists to correct.
//
// It is a characterization test, not a wish: both rows below record what this
// parser does today, and both are wrong about the product. The SQL Server row
// is the one with teeth — the marketing year 2025 selected a PostgreSQL preset
// for a version that does not exist, for as long as a SQL Server banner named
// no product and fell through to this parser on another dialect. BannerPlatform
// claims it now and ResolveServerVersion answers from the product, so nothing
// exported routes the banner here any more; this test is the only thing left
// that executes the misread, which is why it stays. The PostgreSQL and MySQL
// rows are the control that makes the other two mean something: the same rule
// reads those banners correctly, so the defect is in what these two banners put
// first, not in the parser being broken for everything.
func TestParseVersion_ReadsTheWrongNumberOutOfTwoRealBanners(t *testing.T) {
	c := qt.New(t)

	for _, tc := range []struct {
		name   string
		banner string
		want   serverVersion
	}{{
		name:   "the SQL Server banner reads as the marketing year, not the product version 17.0.4065.4",
		banner: sqlServer2025Banner,
		want:   serverVersion{major: 2025},
	}, {
		name:   "the YugabyteDB banner reads as the PostgreSQL compatibility version, not the product 2026.1.0.0",
		banner: yugabyteBanner,
		want:   serverVersion{major: 15, minor: 12},
	}, {
		name:   "control: a PostgreSQL banner reads correctly under the same rule",
		banner: "PostgreSQL 17.10 (Debian 17.10-1.pgdg13+1) on aarch64-unknown-linux-gnu",
		want:   serverVersion{major: 17, minor: 10},
	}, {
		name:   "control: a MySQL banner reads correctly under the same rule",
		banner: "9.7.1",
		want:   serverVersion{major: 9, minor: 7, patch: 1},
	}} {
		c.Run(tc.name, func(c *qt.C) {
			got, ok := parseVersion(tc.banner)
			c.Assert(ok, qt.IsTrue)
			c.Assert(got, qt.Equals, tc.want)
		})
	}
}

// TestResolveServerVersion_MasksTheYugabyteMisread executes the other half of
// the claim: the YugabyteDB misread above never reaches a preset, because the
// banner match fires before any version is parsed. That is why the SQL Server
// case is the dangerous one and this one is not.
func TestResolveServerVersion_MasksTheYugabyteMisread(t *testing.T) {
	c := qt.New(t)

	resolved := ResolveServerVersion("postgres", yugabyteBanner)
	c.Assert(resolved.Capabilities, qt.DeepEquals, YugabyteDB25(),
		qt.Commentf("the -yb- banner match must answer before parseVersion sees 15.12"))
	c.Assert(resolved.Saturated, qt.IsFalse)
	c.Assert(resolved.VersionSpecific, qt.IsTrue)
}
