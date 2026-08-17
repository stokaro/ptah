package servertarget_test

import (
	"errors"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/servertarget"
)

const (
	// sqlServerBanner is the @@VERSION shape this repository already records
	// for SQL Server. The marketing year sits in front of the product version,
	// so a resolver that reads the first number out of it reads 2022.
	sqlServerBanner = "Microsoft SQL Server 2022 (RTM-CU12) - 16.0.4115.5"

	// clickHouseBanner is system.build_options VERSION_FULL, measured on
	// clickhouse/clickhouse-server:26.7. It is the ClickHouse surface that
	// names the product at all: SELECT version() answers a bare "26.7.3.19".
	clickHouseBanner = "ClickHouse 26.7.3.19"
)

// TestResolve_ResolvesSilently covers the outcomes that owe the operator
// nothing: the version named a measured release line, or there was no version
// to resolve at all. Silence is the claim here, not the absence of one — a note
// on these rows would describe a plan nobody departed from.
func TestResolve_ResolvesSilently(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		want    capability.Capabilities
	}{
		{
			name:    "a measured release line",
			dialect: platform.Postgres,
			version: "PostgreSQL 16.3 (Debian)",
			want:    capability.Postgres16(),
		},
		{
			name:    "no version at all resolves to the dialect default",
			dialect: platform.Postgres,
			version: "",
			want:    capability.ForDialect(platform.Postgres),
		},
		{
			// The no-ladder note in TestResolve_SaysWhatItPlannedInstead is the
			// one CockroachDB used to receive for a dotted version, and it was
			// false: the ladder existed and the string simply never reached it.
			name:    "a dotted CockroachDB version selects a measured line",
			dialect: platform.CockroachDB,
			version: "25.4.5",
			want:    capability.CockroachDB25(),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			target, err := servertarget.Resolve(tt.dialect, tt.version)

			c.Assert(err, qt.IsNil)
			c.Assert(target.Note, qt.Equals, "")
			c.Assert(target.Capabilities, qt.DeepEquals, tt.want)
		})
	}
}

// TestResolve_SaysWhatItPlannedInstead covers the three ways a recognized
// version can succeed without naming a measured release line. Each one plans
// with capabilities the operator did not ask for, so each one says so, and the
// whole note is pinned rather than a fragment of it: the sentence is what the
// person typing the version reads, and a wording that drifts is a wording that
// stops matching the remedy it names.
func TestResolve_SaysWhatItPlannedInstead(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		version  string
		want     capability.Capabilities
		wantNote string
	}{
		{
			name:    "a version above the ladder names the line it planned as",
			dialect: platform.Postgres,
			version: "99.0",
			want:    capability.Postgres17(),
			wantNote: "postgres 99.0 is newer than the newest measured release line 18.x; " +
				"capabilities were planned as 18.x",
		},
		{
			name:    "a version between measured lines",
			dialect: platform.MySQL,
			version: "8.0.42-log",
			// 8.0.42 is above the 8.0.20 step, so it takes MySQL8020 -- the
			// arm the global SHOW_ROUTINE privilege is on (stokaro/ptah#916).
			want: capability.MySQL8020(),
			wantNote: "mysql 8.0.42-log is not a measured release line; " +
				"capabilities fall back to the preset its ladder assigns (newest measured line: 26.7)",
		},
		{
			// SQLite gained a ladder in stokaro/ptah#916; SQL Server is the
			// dialect with none, and the number in its version is read and
			// then spent on nothing.
			name:     "a good version for a dialect with no ladder changed nothing",
			dialect:  platform.SQLServer,
			version:  "16.0.4115.5",
			want:     capability.SQLServer2022(),
			wantNote: "the sqlserver dialect has no measured version ladder; the version did not refine capabilities",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			target, err := servertarget.Resolve(tt.dialect, tt.version)

			c.Assert(err, qt.IsNil)
			c.Assert(target.Capabilities, qt.DeepEquals, tt.want)
			c.Assert(target.Note, qt.Equals, tt.wantNote)
		})
	}
}

// TestResolve_RefusesAVersionThatNamesNoServer is the defect this package
// exists for: before capability.VersionResolution published Recognized,
// "not-a-version" resolved to the dialect default and the caller reported the
// result as the version it had applied.
//
// The refusal carries the shapes it would have accepted, which is the only part
// of it a person can act on, so every row asserts that they are there.
func TestResolve_RefusesAVersionThatNamesNoServer(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
	}{
		{
			name:    "a string that names no server",
			dialect: platform.Postgres,
			version: "not-a-version",
		},
		{
			name:    "a MariaDB banner carrying no version",
			dialect: platform.MariaDB,
			version: "MariaDB something",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			target, err := servertarget.Resolve(tt.dialect, tt.version)

			c.Assert(target.Capabilities, qt.IsNil)
			var unrecognized *servertarget.UnrecognizedVersionError
			c.Assert(err, qt.ErrorAs, &unrecognized)
			c.Assert(unrecognized.Version, qt.Equals, tt.version)
			c.Assert(unrecognized.Dialect, qt.Equals, tt.dialect)
			c.Assert(err.Error(), qt.Contains, tt.version)
			c.Assert(err.Error(), qt.Contains, "10.11.6-MariaDB")
		})
	}
}

// TestResolve_RefusesABannerFromAnotherServer covers the contradiction the
// resolver cannot see on its own.
//
// capability.ResolveServerVersion answers a product banner before it consults
// the declared dialect, and that precedence is correct: MariaDB announces
// itself over the MySQL protocol and CockroachDB over the PostgreSQL one, so on
// a live connection the banner is the better evidence. Between two values a
// person typed there is no better evidence, only a contradiction — and the
// returned Capabilities carry no record of which product produced them, which
// is why capability.VersionResolution.ResolvedDialect had to be published
// before this could be refused at all.
func TestResolve_RefusesABannerFromAnotherServer(t *testing.T) {
	tests := []struct {
		name     string
		dialect  string
		version  string
		resolved string
	}{
		{
			name:     "MariaDB banner on mysql",
			dialect:  platform.MySQL,
			version:  "10.11.6-MariaDB",
			resolved: platform.MariaDB,
		},
		{
			name:     "the replication-protocol MariaDB prefix on mysql",
			dialect:  platform.MySQL,
			version:  "5.5.5-10.11.6-MariaDB",
			resolved: platform.MariaDB,
		},
		{
			name:     "CockroachDB banner on sqlite",
			dialect:  platform.SQLite,
			version:  "CockroachDB CCL v25.4.5",
			resolved: platform.CockroachDB,
		},
		{
			name:     "YugabyteDB banner on postgres",
			dialect:  platform.Postgres,
			version:  "PostgreSQL 15.2-YB-2026.1.0.0-b0",
			resolved: platform.YugabyteDB,
		},
		{
			name:     "Spanner banner on postgres",
			dialect:  platform.Postgres,
			version:  "Cloud Spanner PostgreSQL",
			resolved: platform.Spanner,
		},
		{
			name:     "PostgreSQL banner on mysql",
			dialect:  platform.MySQL,
			version:  "PostgreSQL 16.3 (Debian)",
			resolved: platform.Postgres,
		},
		{
			name:     "PostgreSQL banner on sqlite",
			dialect:  platform.SQLite,
			version:  "PostgreSQL 16.3 (Debian)",
			resolved: platform.Postgres,
		},
		{
			name:     "PostgreSQL banner on cockroachdb",
			dialect:  platform.CockroachDB,
			version:  "PostgreSQL 16.3 (Debian)",
			resolved: platform.Postgres,
		},
		{
			name:     "PostgreSQL banner on yugabytedb",
			dialect:  platform.YugabyteDB,
			version:  "PostgreSQL 16.3 (Debian)",
			resolved: platform.Postgres,
		},
		{
			name:     "PostgreSQL banner on spanner",
			dialect:  platform.Spanner,
			version:  "PostgreSQL 14.1",
			resolved: platform.Postgres,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			target, err := servertarget.Resolve(tt.dialect, tt.version)

			c.Assert(target.Capabilities, qt.IsNil)
			var mismatch *servertarget.DialectMismatchError
			c.Assert(err, qt.ErrorAs, &mismatch)
			c.Assert(mismatch.Dialect, qt.Equals, tt.dialect)
			c.Assert(mismatch.ResolvedDialect, qt.Equals, tt.resolved)
			c.Assert(mismatch.Version, qt.Equals, tt.version)
			c.Assert(err.Error(), qt.Contains, "names a "+tt.resolved+" server")
		})
	}
}

// TestResolve_AcceptsAMatchingBanner is the control: the refusal above is about
// a contradiction, not about banners. Every documented banner shape has to keep
// resolving against the dialect it names, or the refusal would have made the
// shapes RecognizedVersionShapes advertises unusable.
func TestResolve_AcceptsAMatchingBanner(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		want    capability.Capabilities
	}{
		{
			name:    "MariaDB banner on mariadb",
			dialect: platform.MariaDB,
			version: "10.11.6-MariaDB",
			want:    capability.MariaDB1011(),
		},
		{
			name:    "the replication-protocol MariaDB prefix on mariadb",
			dialect: platform.MariaDB,
			version: "5.5.5-10.11.6-MariaDB",
			want:    capability.MariaDB1011(),
		},
		{
			name:    "CockroachDB banner on cockroachdb",
			dialect: platform.CockroachDB,
			version: "CockroachDB CCL v25.4.5",
			want:    capability.CockroachDB25(),
		},
		{
			name:    "YugabyteDB banner on yugabytedb",
			dialect: platform.YugabyteDB,
			version: "PostgreSQL 15.2-YB-2026.1.0.0-b0",
			want:    capability.YugabyteDB25(),
		},
		{
			name:    "Spanner banner on spanner",
			dialect: platform.Spanner,
			version: "Cloud Spanner PostgreSQL",
			want:    capability.SpannerPostgres(),
		},
		{
			name:    "PostgreSQL banner on postgres",
			dialect: platform.Postgres,
			version: "PostgreSQL 16.3 (Debian)",
			want:    capability.Postgres16(),
		},
		{
			name:    "a plain dotted version on mysql",
			dialect: platform.MySQL,
			version: "8.4.0",
			want:    capability.MySQL84(),
		},
		{
			name:    "a plain dotted version on cockroachdb reaches its ladder",
			dialect: platform.CockroachDB,
			version: "25.4.5",
			want:    capability.CockroachDB25(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			target, err := servertarget.Resolve(tt.dialect, tt.version)

			c.Assert(err, qt.IsNil)
			c.Assert(target.Capabilities, qt.DeepEquals, tt.want)
		})
	}
}

// TestResolve_RefusesWhatTheLiveResolverDeliberatelyKeeps executes the split
// between the two paths as one claim, because the whole design rests on them
// answering the same pair differently.
//
// A PostgreSQL banner on a PostgreSQL-family dialect is two different events. On
// a live connection it is one server describing itself less specifically than it
// could: CockroachDB, YugabyteDB and Spanner all speak this protocol, so the
// dialect the operator connected with is the only product evidence there is and
// the resolver keeps its preset. Typed on a command line it is two values naming
// two servers, with nothing to prefer between them, so this package refuses.
//
// Reading capability.VersionResolution.ResolvedDialect here instead of
// capability.BannerPlatform is what collapses the two: ResolvedDialect reports
// the CockroachDB ladder for these inputs, correctly, and a refusal keyed on it
// cannot fire.
func TestResolve_RefusesWhatTheLiveResolverDeliberatelyKeeps(t *testing.T) {
	tests := []struct {
		name    string
		dialect string
		version string
		live    capability.Capabilities
	}{
		{
			name:    "cockroachdb",
			dialect: platform.CockroachDB,
			version: "PostgreSQL-compatible server 25.4",
			live:    capability.CockroachDB25(),
		},
		{
			name:    "yugabytedb",
			dialect: platform.YugabyteDB,
			version: "PostgreSQL 11.2 on x86_64-pc-linux-gnu",
			live:    capability.YugabyteDB25(),
		},
		{
			name:    "spanner",
			dialect: platform.Spanner,
			version: "PostgreSQL 14.1",
			live:    capability.SpannerPostgres(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			resolution := capability.ResolveServerVersion(tt.dialect, tt.version)
			c.Assert(resolution.ResolvedDialect, qt.Equals, tt.dialect)
			c.Assert(resolution.Capabilities, qt.DeepEquals, tt.live)

			target, err := servertarget.Resolve(tt.dialect, tt.version)

			c.Assert(target.Capabilities, qt.IsNil)
			var mismatch *servertarget.DialectMismatchError
			c.Assert(err, qt.ErrorAs, &mismatch)
			c.Assert(mismatch.Dialect, qt.Equals, tt.dialect)
			c.Assert(mismatch.ResolvedDialect, qt.Equals, platform.Postgres)
		})
	}
}

// TestResolve_MatchesTheDocumentedBannerPlatformRecipe executes the recipe
// docs/capabilities.md gives an API caller, against the package the commands
// actually call, so the two cannot drift apart in prose alone.
//
// They already had. The document told callers to refuse a ResolvedDialect other
// than the dialect they asked for, and that rule is not the one implemented: it
// accepts all three PostgreSQL-family rows below, which every native command
// exits 2 on. A library caller following the published instruction would have
// let through exactly the contradiction the flag was added to refuse.
//
// Both halves of the recipe are asserted, because both have a failure mode a
// row here would otherwise not reach. An empty BannerPlatform is not a
// mismatch — a bare dotted version names no product — and the comparison is
// against the normalized dialect, since no alias is ever the answer, so a check
// written against a raw "crdb" refuses a CockroachDB banner on a CockroachDB
// target.
//
// resolvedDialectAgrees is the column that makes the divergence observable
// rather than asserted: it is true wherever the withdrawn rule would have
// accepted, and the first three rows pair it with a refusal.
func TestResolve_MatchesTheDocumentedBannerPlatformRecipe(t *testing.T) {
	tests := []struct {
		name string
		// dialect is passed exactly as a caller would type it, alias included.
		dialect string
		version string
		// banner is what capability.BannerPlatform answers for version alone.
		banner string
		// normalized is platform.NormalizeDialect(dialect).
		normalized string
		// refused is what the recipe, and Resolve, must both decide.
		refused bool
		// resolvedDialectAgrees reports whether the withdrawn
		// ResolvedDialect rule would have accepted this pair.
		resolvedDialectAgrees bool
	}{
		{
			name:                  "PostgreSQL banner on cockroachdb",
			dialect:               platform.CockroachDB,
			version:               "PostgreSQL 16.3",
			banner:                platform.Postgres,
			normalized:            platform.CockroachDB,
			refused:               true,
			resolvedDialectAgrees: true,
		},
		{
			name:                  "PostgreSQL banner on yugabytedb",
			dialect:               platform.YugabyteDB,
			version:               "PostgreSQL 11.2 on x86_64-pc-linux-gnu",
			banner:                platform.Postgres,
			normalized:            platform.YugabyteDB,
			refused:               true,
			resolvedDialectAgrees: true,
		},
		{
			name:                  "PostgreSQL banner on spanner",
			dialect:               platform.Spanner,
			version:               "PostgreSQL 14.1",
			banner:                platform.Postgres,
			normalized:            platform.Spanner,
			refused:               true,
			resolvedDialectAgrees: true,
		},
		{
			name:                  "PostgreSQL banner on mysql, where the two rules agree",
			dialect:               platform.MySQL,
			version:               "PostgreSQL 16.3 (Debian)",
			banner:                platform.Postgres,
			normalized:            platform.MySQL,
			refused:               true,
			resolvedDialectAgrees: false,
		},
		{
			name:                  "MariaDB banner on mysql, where the two rules agree",
			dialect:               platform.MySQL,
			version:               "10.11.6-MariaDB",
			banner:                platform.MariaDB,
			normalized:            platform.MySQL,
			refused:               true,
			resolvedDialectAgrees: false,
		},
		{
			name:                  "a bare dotted version names no product",
			dialect:               platform.MySQL,
			version:               "8.0.42",
			banner:                "",
			normalized:            platform.MySQL,
			refused:               false,
			resolvedDialectAgrees: true,
		},
		{
			name:                  "a CockroachDB banner on the crdb alias",
			dialect:               "crdb",
			version:               "CockroachDB CCL v25.4.0",
			banner:                platform.CockroachDB,
			normalized:            platform.CockroachDB,
			refused:               false,
			resolvedDialectAgrees: true,
		},
		{
			name:                  "a PostgreSQL banner on the pgx alias",
			dialect:               "pgx",
			version:               "PostgreSQL 16.3 (Debian)",
			banner:                platform.Postgres,
			normalized:            platform.Postgres,
			refused:               false,
			resolvedDialectAgrees: true,
		},
		{
			name:                  "a SQL Server banner on postgres",
			dialect:               platform.Postgres,
			version:               sqlServerBanner,
			banner:                platform.SQLServer,
			normalized:            platform.Postgres,
			refused:               true,
			resolvedDialectAgrees: false,
		},
		{
			name:                  "a SQL Server banner on the mssql alias",
			dialect:               "mssql",
			version:               sqlServerBanner,
			banner:                platform.SQLServer,
			normalized:            platform.SQLServer,
			refused:               false,
			resolvedDialectAgrees: true,
		},
		{
			name:                  "a ClickHouse banner on mysql",
			dialect:               platform.MySQL,
			version:               clickHouseBanner,
			banner:                platform.ClickHouse,
			normalized:            platform.MySQL,
			refused:               true,
			resolvedDialectAgrees: false,
		},
		{
			name:                  "a ClickHouse banner on the ch alias",
			dialect:               "ch",
			version:               clickHouseBanner,
			banner:                platform.ClickHouse,
			normalized:            platform.ClickHouse,
			refused:               false,
			resolvedDialectAgrees: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			banner := capability.BannerPlatform(tt.version)
			normalized := platform.NormalizeDialect(tt.dialect)
			c.Assert(banner, qt.Equals, tt.banner)
			c.Assert(normalized, qt.Equals, tt.normalized)

			recipe := banner != "" && banner != normalized
			c.Assert(recipe, qt.Equals, tt.refused)

			resolution := capability.ResolveServerVersion(tt.dialect, tt.version)
			c.Assert(resolution.ResolvedDialect == normalized, qt.Equals, tt.resolvedDialectAgrees)

			_, err := servertarget.Resolve(tt.dialect, tt.version)
			var mismatch *servertarget.DialectMismatchError
			c.Assert(errors.As(err, &mismatch), qt.Equals, tt.refused)
		})
	}
}
