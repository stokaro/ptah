package capabilityprobe

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/capabilityline"
)

// Refinement records whether a probe observation can be attributed to one
// matrix cell, and why. It is declared here as data rather than read back from
// capability.VersionResolution, because that struct cannot tell the mechanisms
// apart:
//
//   - a banner-matched dialect reports VersionSpecific=true without a version
//     ever being parsed, so it is indistinguishable from a successful ladder
//     match;
//   - a dialect with no ladder reports VersionSpecific=false, which is exactly
//     what an unparseable banner on a laddered dialect reports.
//
// Deriving undecidability from the returned struct would therefore record a
// banner-only CockroachDB line as an agreeing version-specific match and a
// cleanly-parsed SQL Server as an unreadable banner.
type Refinement string

const (
	// RefinedByVersion: the dialect has a version ladder and the parsed
	// version selects which arm of it answers. Only these lines can be
	// measured per line, because only here does the version change the answer.
	RefinedByVersion Refinement = "version-ladder"

	// RefinedByMeasuredLine: the resolver still reaches the preset through an
	// engine banner, but this specific matrix line is backed by a live
	// measurement of that release line. Sibling lines that share the banner
	// preset stay RefinedByBanner until they are measured themselves.
	RefinedByMeasuredLine Refinement = "measured-release-line"

	// RefinedByBanner: the preset is selected by a substring of the banner
	// before any version is parsed, so every release of the engine receives
	// the same set and no observation can be attributed to one line rather
	// than another.
	RefinedByBanner Refinement = "banner-substring"

	// NotRefined: the version is parsed and then discarded; the dialect
	// default is returned for every release.
	NotRefined Refinement = "dialect-default"
)

// Cell is one release line the capability matrix covers.
//
// Cells is the ONLY place a line is declared. Adding a release line is one
// literal below and no code change anywhere else: the probe reads the line to
// match a live server, the preset to compare against, and the refinement to
// decide whether an observation can be attributed to this line at all.
type Cell struct {
	// Dialect is the normalized platform dialect this line belongs to.
	Dialect string

	// Line is the release line in the numbering the CORRECTED version parse
	// produces (see version.go), not the vendor's marketing name. A server
	// matches when its parsed version begins with these components, so "17"
	// matches every PostgreSQL 17.x and "10.11" matches only MariaDB 10.11.x.
	Line string

	// Label is the line's human name where it differs from Line, for example
	// "SQL Server 2025" for line "17.0". Empty means Line reads well enough.
	Label string

	// Preset is the capability preset this line claims, or nil when Ptah has
	// no measured preset for the line yet.
	//
	// A nil Preset is the declared gap that issue #916 is about, and it is
	// what makes "adding a cell and adding a preset entry are the same
	// change" enforceable: the probe refuses to report success against a
	// server on a nil-preset line, naming the line instead. Filling this in
	// is what promotes a line to measured.
	Preset func() capability.Capabilities

	// PresetName names Preset for the report. It is written out rather than
	// recovered by comparing sets, because MySQLLegacy and MariaDBLegacy are
	// byte-identical and a set comparison would label MariaDB rows MySQL.
	PresetName string

	// Refinement says how the preset is reached. RefinedByBanner and
	// NotRefined make every capability row on this line UNDECIDABLE: the
	// observation is still taken and printed, but it cannot be credited to
	// this line rather than to the line's siblings, which receive the
	// identical set. RefinedByMeasuredLine records the direct live evidence
	// that makes one otherwise banner-selected line attributable.
	Refinement Refinement

	// Image reproduces the line locally, empty when there is no container for
	// it. When ResolveNewestPatch is true, its tag is a release-line selector
	// rather than a registry tag: the CI driver resolves the newest concrete
	// patch before invoking Docker.
	Image string

	// ResolveNewestPatch selects registries that do not publish a floating tag
	// for a release line. The current implementation supports Docker Hub and
	// requires Image's tag to equal Line, so the declaration cannot quietly
	// freeze one patch while claiming that it follows the line.
	ResolveNewestPatch bool

	// Note records why a line is shaped the way it is: a missing preset, a
	// missing server, a version axis that does not exist.
	Note string
}

// Match reports whether a parsed server version falls on this cell's line.
// The comparison is component-wise on the dotted line, so "17" accepts every
// 17.x and "10.11" accepts only 10.11.x.
func (c Cell) Match(v Version) bool {
	want := strings.Split(c.Line, ".")
	got := v.Components()
	if len(want) > len(got) {
		return false
	}
	for i, component := range want {
		if got[i] != component {
			return false
		}
	}
	return true
}

// Measured reports whether the line claims a capability preset at all.
func (c Cell) Measured() bool {
	return c.Preset != nil
}

// String renders the cell as "dialect line" plus its label.
func (c Cell) String() string {
	if c.Label == "" {
		return c.Dialect + " " + c.Line
	}
	return fmt.Sprintf("%s %s (%s)", c.Dialect, c.Line, c.Label)
}

// Cells is the capability matrix: every release line Ptah covers, in one
// place. Release strings shared with the version resolver are named once in
// internal/capabilityline and referenced here; this slice remains the only
// declaration of which lines are matrix cells.
//
// The lines are the vendor-supported ones, checked against the vendors on
// 2026-08-09, plus any line whose preset Ptah still ships — a preset with no
// cell is a claim nothing in this repository can measure — plus any line this
// repository's own docker-compose.yaml or integration workflow starts. That
// last source is not decoration: a container CI runs and the matrix does not
// declare is a server whose preset nothing here can describe, and cells_test.go
// derives the check from those two files rather than from a list somebody has
// to remember to edit.
//
// Five presets still have no cell, and each absence is deliberate:
//
//   - MySQLLegacy, MySQL8016 and MySQL8019 describe the MySQL 8.0 line, which
//     left vendor support on 2026-04-30 and is not one line in any case: the
//     three presets split it at 8.0.16 and 8.0.19, so a single "8.0" cell
//     could not name one preset. Giving them cells means three cells keyed on
//     the patch component, which the Line matcher already supports.
//   - MariaDBLegacy describes MariaDB before 10.2, whose newest release left
//     support in 2022.
//
// MariaDB 10.6 left community support on 2026-07-06 and is absent for that
// reason alone.
//
// A nil Preset is a line the matrix wants and Ptah has not measured. Those are
// the cells PRs bumping a container tag are waiting on, and probing a server
// on one of them fails rather than reporting a green row against a preset
// chosen by saturation.
var Cells = []Cell{
	// PostgreSQL: five supported majors (postgresql.org/support/versioning,
	// read 2026-08-09 — 18.4, 17.10, 16.14, 15.18, 14.23; the project does not
	// use the term LTS). Postgres16's own doc covers 14 through 16.
	{
		Dialect: platform.Postgres, Line: "18",
		Preset: capability.Postgres17, PresetName: "Postgres17",
		Refinement: RefinedByVersion, Image: "postgres:18",
		Note: "measured live on PostgreSQL 18.4 in capability-matrix run 31615442780: all 25 observed " +
			"capability rows agree with Postgres17",
	},
	{
		Dialect: platform.Postgres, Line: "17",
		Preset: capability.Postgres17, PresetName: "Postgres17",
		Refinement: RefinedByVersion, Image: "postgres:17",
	},
	{
		Dialect: platform.Postgres, Line: "16",
		Preset: capability.Postgres16, PresetName: "Postgres16",
		Refinement: RefinedByVersion, Image: "postgres:16",
	},
	{
		Dialect: platform.Postgres, Line: "15",
		Preset: capability.Postgres16, PresetName: "Postgres16",
		Refinement: RefinedByVersion, Image: "postgres:15",
	},
	{
		Dialect: platform.Postgres, Line: "14",
		Preset: capability.Postgres16, PresetName: "Postgres16",
		Refinement: RefinedByVersion, Image: "postgres:14",
		Note: "final PostgreSQL 14 release is November 2026",
	},
	{
		Dialect: platform.Postgres, Line: "13",
		Preset: capability.Postgres13, PresetName: "Postgres13",
		Refinement: RefinedByVersion, Image: "postgres:13",
		Note: "past its final vendor release; the cell exists because Ptah still ships a Postgres13 preset " +
			"and a preset with no cell is a claim nothing can measure. The preset's doc covers PostgreSQL " +
			"12 as well and there is no 12 cell: 12 was never probed, so a 12 server correctly falls off " +
			"the matrix rather than borrowing this line's result",
	},

	// MySQL: the two LTS lines (endoflife.date/api/mysql.json, read
	// 2026-08-09 — 9.7 latest 9.7.2 EOL 2034-04-21, 8.4 latest 8.4.11 EOL
	// 2032-04-30; both flagged LTS), plus 26.7, which is the line this
	// repository actually starts.
	{
		Dialect: platform.MySQL, Line: capabilityline.MySQL26,
		Preset: capability.MySQL84, PresetName: "MySQL84",
		Refinement: RefinedByVersion, Image: "mysql:26.7",
		Note: "measured live on MySQL 26.7.0 in capability-matrix run 31615442780: all 24 observable " +
			"rows agree with MySQL84; role_management remains undecidable because the probe deliberately " +
			"does not create a privileged account",
	},
	{
		Dialect: platform.MySQL, Line: capabilityline.MySQL9,
		Preset: capability.MySQL84, PresetName: "MySQL84",
		Refinement: RefinedByVersion, Image: "mysql:9.7",
	},
	{
		Dialect: platform.MySQL, Line: capabilityline.MySQL8,
		Preset: capability.MySQL84, PresetName: "MySQL84",
		Refinement: RefinedByVersion, Image: "mysql:8.4",
	},

	// MariaDB: the maintained LTS lines (mariadb.org/about/maintenance-policy,
	// read 2026-08-09 — 11.8 EOL 2028-06-04, 11.4 EOL 2029-05-29, 10.11 EOL
	// 2028-02-16) plus 12.3, which the vendor KB names as the latest
	// long-term stable series.
	{
		Dialect: platform.MariaDB, Line: capabilityline.MariaDB12,
		Preset: capability.MariaDB1011, PresetName: "MariaDB1011",
		Refinement: RefinedByVersion, Image: "mariadb:12.3",
		Note: "measured live on MariaDB 12.3.2 in capability-matrix run 31615442780: all 23 observable " +
			"rows agree with MariaDB1011; role_management and sequences remain deliberately undecidable",
	},
	{
		Dialect: platform.MariaDB, Line: capabilityline.MariaDB11LTS,
		Preset: capability.MariaDB1011, PresetName: "MariaDB1011",
		Refinement: RefinedByVersion, Image: "mariadb:11.8",
	},
	{
		Dialect: platform.MariaDB, Line: capabilityline.MariaDB114,
		Preset: capability.MariaDB1011, PresetName: "MariaDB1011",
		Refinement: RefinedByVersion, Image: "mariadb:11.4",
	},
	{
		Dialect: platform.MariaDB, Line: capabilityline.MariaDB10,
		Preset: capability.MariaDB1011, PresetName: "MariaDB1011",
		Refinement: RefinedByVersion, Image: "mariadb:10.11",
	},

	// ClickHouse has no version ladder: ResolveServerVersion parses the
	// version and then discards it, so all four lines receive ClickHouse24.
	{
		Dialect: platform.ClickHouse, Line: "26.7",
		Preset: capability.ClickHouse24, PresetName: "ClickHouse24",
		Refinement: NotRefined, Image: "clickhouse/clickhouse-server:26.7",
	},
	{
		Dialect: platform.ClickHouse, Line: "26.3",
		Preset: capability.ClickHouse24, PresetName: "ClickHouse24",
		Refinement: NotRefined, Image: "clickhouse/clickhouse-server:26.3",
	},
	{
		Dialect: platform.ClickHouse, Line: "25.8",
		Preset: capability.ClickHouse24, PresetName: "ClickHouse24",
		Refinement: NotRefined, Image: "clickhouse/clickhouse-server:25.8",
	},
	{
		Dialect: platform.ClickHouse, Line: "24.10",
		Preset: capability.ClickHouse24, PresetName: "ClickHouse24",
		Refinement: NotRefined, Image: "clickhouse/clickhouse-server:24.10",
		Note: "the second ClickHouse service .github/workflows/go-integration-tests.yml starts, and " +
			"the line the ClickHouse24 preset is named after; a live clickhouse/clickhouse-server:24.10 " +
			"reports 24.10.4.191",
	},

	// SQL Server lines are numbered by product version here, not by the
	// marketing year: the year is what the shared parseVersion reads out of
	// @@VERSION, and reading "2025" as a major version is the defect
	// version.go corrects.
	{
		Dialect: platform.SQLServer, Line: "17.0", Label: "SQL Server 2025",
		Preset: capability.SQLServer2022, PresetName: "SQLServer2022",
		Refinement: NotRefined, Image: "mcr.microsoft.com/mssql/server:2025-latest",
	},
	{
		Dialect: platform.SQLServer, Line: "16.0", Label: "SQL Server 2022",
		Preset: capability.SQLServer2022, PresetName: "SQLServer2022",
		Refinement: NotRefined, Image: "mcr.microsoft.com/mssql/server:2022-latest",
	},
	{
		Dialect: platform.SQLServer, Line: "15.0", Label: "SQL Server 2019",
		Preset: capability.SQLServer2022, PresetName: "SQLServer2022",
		Refinement: NotRefined, Image: "mcr.microsoft.com/mssql/server:2019-latest",
	},

	// CockroachDB resolves through a version ladder and both YugabyteDB lines
	// still reach their preset through a banner substring. Every line below is
	// backed by a direct live measurement, which makes the YugabyteDB
	// observations attributable despite that resolver mechanism.
	{
		Dialect: platform.CockroachDB, Line: capabilityline.CockroachDB26,
		Preset: capability.CockroachDB26, PresetName: "CockroachDB26",
		Refinement: RefinedByVersion, Image: "cockroachdb/cockroach:latest-v26.2",
		Note: "measured live on CockroachDB CCL v26.2.5: role_management, row_level_security, " +
			"and sequences agree with CockroachDB26 after issue #1376; create_index_concurrently " +
			"remains false because the keyword is accepted inside a transaction",
	},
	{
		Dialect: platform.CockroachDB, Line: capabilityline.CockroachDB25,
		Preset: capability.CockroachDB25, PresetName: "CockroachDB25",
		Refinement: RefinedByVersion, Image: "cockroachdb/cockroach:latest-v25.4",
		Note: "measured live on CockroachDB CCL v25.4.5 in capability-matrix run 31615442780: " +
			"create_or_replace_trigger and drop_constraint_generic are unsupported on this line; " +
			"guarded DROP CONSTRAINT is therefore undecidable and the other 22 rows agree with CockroachDB25",
	},
	{
		Dialect: platform.YugabyteDB, Line: "2026.1",
		Preset: capability.YugabyteDB25, PresetName: "YugabyteDB25",
		Refinement: RefinedByMeasuredLine, Image: "yugabytedb/yugabyte:2026.1", ResolveNewestPatch: true,
		Note: "measured live on YugabyteDB 2026.1.0.0-b118: advisory_locks, " +
			"create_index_concurrently, and row_level_security agree with YugabyteDB25 after issue #1376; " +
			"drop_index_concurrently remains false because the server refuses that spelling. Docker Hub " +
			"publishes no floating 2026.1 tag, so the CI driver resolves the newest numeric patch tag",
	},
	{
		Dialect: platform.YugabyteDB, Line: "2025.2",
		Preset: capability.YugabyteDB25, PresetName: "YugabyteDB25",
		Refinement: RefinedByMeasuredLine, Image: "yugabytedb/yugabyte:2025.2", ResolveNewestPatch: true,
		Note: "measured live on YugabyteDB v2025.2.5.2-b0 in capability-matrix run 31627407769, " +
			"job 94218797895: all 25 rows match YugabyteDB25 with zero mismatches. Docker Hub has no " +
			"floating line tag, so the CI driver resolves the newest numeric patch",
	},

	// SQLite is not a server line. Its version is whatever the driver pinned
	// in go.mod compiles in, so the cell matches any 3.x and the "line" is a
	// dependency bump rather than a release a vendor supports.
	{
		Dialect: platform.SQLite, Line: "3",
		Preset: capability.SQLite3, PresetName: "SQLite3",
		Refinement: NotRefined,
		Note:       "no container: the version is the modernc.org/sqlite amalgamation pinned in go.mod",
	},

	// Spanner is a managed service with no version axis and no local server.
	// SpannerPostgres' own doc concedes that nothing in it was executed
	// against a server; issue #942 is the missing container.
	{
		Dialect: platform.Spanner, Line: "0",
		Preset: capability.SpannerPostgres, PresetName: "SpannerPostgres",
		Refinement: RefinedByBanner,
		Note:       "no Spanner server exists in this repository (stokaro/ptah#942), so no row on this line has ever been executed",
	},
}

// PresetsWithoutCell names every capability preset Ptah ships that no cell
// claims, and why that absence is deliberate.
//
// It exists because the five absences above are otherwise indistinguishable
// from an oversight, and stokaro/ptah#1341 asks that a preset with no cell fail
// the build. Written as data rather than prose, the claim is checkable in both
// directions: a preset that loses its cell has to be added here with a reason,
// and an entry here that a cell does claim, or that names a preset the
// capability package no longer ships, fails too.
var PresetsWithoutCell = map[string]string{
	"CockroachDB23": "is the conservative historical preset below the maintained 25.x and 26.x lines; " +
		"the matrix tests the vendor-supported lines through CockroachDB25 and CockroachDB26",
	"MySQLLegacy": "describes MySQL before 8.0.16, which left vendor support on 2026-04-30; the 8.0 line " +
		"is three presets split at 8.0.16 and 8.0.19, so no single 8.0 cell could name one of them",
	"MySQL8016": "describes MySQL 8.0.16 to 8.0.18, inside the out-of-support 8.0 line",
	"MySQL8019": "describes MySQL 8.0.19 to 8.3, inside the out-of-support 8.0 line",
	"MariaDBLegacy": "describes MariaDB before 10.2, whose newest release left support in 2022; it is the " +
		"conservative floor ForServerVersion assigns to such a server rather than a line anybody runs",
}

// CellFor returns the matrix cell a live server falls on.
//
// A server with no cell is a failure and not a default: it is a release line
// the matrix does not cover, which is precisely the missing-cell signal issue
// #916 asks for. Returning some neighboring cell would report a green matrix
// for a line nobody declared.
func CellFor(dialect string, v Version) (Cell, bool) {
	normalized := platform.NormalizeDialect(dialect)
	for _, cell := range Cells {
		if cell.Dialect == normalized && cell.Match(v) {
			return cell, true
		}
	}
	return Cell{}, false
}
