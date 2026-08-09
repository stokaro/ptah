package capabilityprobe

import (
	"fmt"
	"strings"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
)

// Refinement records HOW capability.ResolveServerVersion reaches a cell's
// preset. It is declared here as data rather than read back from
// capability.VersionResolution, because that struct cannot tell the three
// mechanisms apart:
//
//   - a banner-matched dialect reports VersionSpecific=true without a version
//     ever being parsed, so it is indistinguishable from a successful ladder
//     match;
//   - a dialect with no ladder reports VersionSpecific=false, which is exactly
//     what an unparseable banner on a laddered dialect reports.
//
// Deriving undecidability from the returned struct would therefore record a
// CockroachDB v26.2.5 as an agreeing version-specific match and a
// cleanly-parsed SQL Server as an unreadable banner.
type Refinement string

const (
	// RefinedByVersion: the dialect has a version ladder and the parsed
	// version selects which arm of it answers. Only these lines can be
	// measured per line, because only here does the version change the answer.
	RefinedByVersion Refinement = "version-ladder"

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

	// Refinement says how the preset is reached. Anything other than
	// RefinedByVersion makes every capability row on this line UNDECIDABLE:
	// the observation is still taken and printed, but it cannot be credited
	// to this line rather than to the line's siblings, which receive the
	// identical set.
	Refinement Refinement

	// Image reproduces the line locally, empty when there is no container for
	// it.
	Image string

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
// place.
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
// Four presets still have no cell, and each absence is deliberate:
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
		Preset: nil, PresetName: "",
		Refinement: RefinedByVersion, Image: "postgres:18",
		Note: "no measured PostgreSQL 18 preset: newestMeasuredPostgresMajor is 17, so an 18 server resolves saturated onto Postgres17",
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
		Dialect: platform.MySQL, Line: "26.7",
		Preset: nil, PresetName: "",
		Refinement: RefinedByVersion, Image: "mysql:26.7",
		Note: "no measured MySQL 26 preset: newestMeasuredMySQLMajor is 9, so a 26 server resolves " +
			"saturated onto MySQL84. This is the line docker-compose.yaml and " +
			".github/workflows/go-integration-tests.yml pin, and a live mysql:26.7 reports VERSION() " +
			"26.7.0 — so until a preset is measured for it, the servers this repository runs its own " +
			"MySQL suite against are described by a stand-in",
	},
	{
		Dialect: platform.MySQL, Line: "9.7",
		Preset: capability.MySQL84, PresetName: "MySQL84",
		Refinement: RefinedByVersion, Image: "mysql:9.7",
	},
	{
		Dialect: platform.MySQL, Line: "8.4",
		Preset: capability.MySQL84, PresetName: "MySQL84",
		Refinement: RefinedByVersion, Image: "mysql:8.4",
	},

	// MariaDB: the maintained LTS lines (mariadb.org/about/maintenance-policy,
	// read 2026-08-09 — 11.8 EOL 2028-06-04, 11.4 EOL 2029-05-29, 10.11 EOL
	// 2028-02-16) plus 12.3, which the vendor KB names as the latest
	// long-term stable series.
	{
		Dialect: platform.MariaDB, Line: "12.3",
		Preset: nil, PresetName: "",
		Refinement: RefinedByVersion, Image: "mariadb:12.3",
		Note: "no measured MariaDB 12 preset: newestMeasuredMariaDBMajor is 11, so a 12 server resolves saturated onto MariaDB1011",
	},
	{
		Dialect: platform.MariaDB, Line: "11.8",
		Preset: capability.MariaDB1011, PresetName: "MariaDB1011",
		Refinement: RefinedByVersion, Image: "mariadb:11.8",
	},
	{
		Dialect: platform.MariaDB, Line: "11.4",
		Preset: capability.MariaDB1011, PresetName: "MariaDB1011",
		Refinement: RefinedByVersion, Image: "mariadb:11.4",
	},
	{
		Dialect: platform.MariaDB, Line: "10.11",
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

	// CockroachDB and YugabyteDB are matched on a banner substring before any
	// version is parsed, so their lines all receive one preset.
	{
		Dialect: platform.CockroachDB, Line: "26.2",
		Preset: capability.CockroachDB23, PresetName: "CockroachDB23",
		Refinement: RefinedByBanner, Image: "cockroachdb/cockroach:v26.2.5",
	},
	{
		Dialect: platform.CockroachDB, Line: "25.4",
		Preset: capability.CockroachDB23, PresetName: "CockroachDB23",
		Refinement: RefinedByBanner, Image: "cockroachdb/cockroach:v25.4.5",
	},
	{
		Dialect: platform.YugabyteDB, Line: "2026.1",
		Preset: capability.YugabyteDB25, PresetName: "YugabyteDB25",
		Refinement: RefinedByBanner, Image: "yugabytedb/yugabyte:2026.1.0.0-b118",
	},
	{
		Dialect: platform.YugabyteDB, Line: "2025.2",
		Preset: capability.YugabyteDB25, PresetName: "YugabyteDB25",
		Refinement: RefinedByBanner, Image: "yugabytedb/yugabyte:2025.2.0.0-b0",
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
