package serverprofile_test

import (
	"slices"
	"testing"

	qt "github.com/frankban/quicktest"

	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/serverprofile"
)

// The strings below are what real servers answer about themselves. They are
// literals for the same reason core/platform/capability's own test pins its
// banners: reading these exact bytes is the behavior under test, so a helper
// that produced them would be a second spelling of the resolver.
const (
	// mariaDBOverMySQLBanner is SELECT VERSION() from a mariadb:10.11 server
	// reached over a mysql:// URL, which is how MariaDB is normally deployed.
	// Two corrections have to fire on it at once: the 5.5.5- replication
	// prefix MariaDB reports over the MySQL protocol is trimmed, and the
	// MariaDB ladder answers instead of MySQL's. Asking for "mariadb" directly
	// exercises neither, which is why the row below asks for "mysql".
	mariaDBOverMySQLBanner = "5.5.5-10.11.6-MariaDB-1:10.11.6+maria~ubu2204"

	// postgres16Banner is the Debian-packaged shape of SELECT version(), with
	// the distribution's own version repeated in parentheses. The second
	// number is what a naive parse picks up.
	postgres16Banner = "PostgreSQL 16.3 (Debian 16.3-1.pgdg120+1)"

	// postgres13Banner reaches the only legacy-tested line a PostgreSQL banner
	// can reach: cells.go declares 13 past its final vendor release and keeps
	// it as a regression sentinel. A server on it still resolves, still gets
	// the Postgres13 preset, and is still operated on — an upstream
	// end-of-life date lowers what Ptah promises and changes nothing about
	// what Ptah does.
	postgres13Banner = "PostgreSQL 13.14 (Debian 13.14-1.pgdg120+1)"

	// sqlServer2022Banner is @@VERSION's short shape. The marketing year in
	// front of the product version is the whole reason SQL Server needs a
	// corrected parse: read literally, "2022" is a major version that falls on
	// no declared line at all.
	sqlServer2022Banner = "Microsoft SQL Server 2022 (RTM-CU12) - 16.0.4115.5"

	// sqlServer2022ProductVersion is SERVERPROPERTY('ProductVersion'), the
	// cleaner surface capabilityprobe.ProductVersion reads. Its patch
	// component deliberately differs from the banner's, so the two rows below
	// cannot agree by accident about which string was read.
	sqlServer2022ProductVersion = "16.0.4025.1"

	// mySQL80Banner is what MySQL's SELECT VERSION() answers: a bare version
	// naming no product. 8.0 left vendor support and cells.go declares no line
	// for it — one cell could not name one preset, since the 8.0 presets split
	// at 8.0.16 and 8.0.19 — so this is the undeclared-line path.
	mySQL80Banner = "8.0.42"

	// unreadableBanner is what no server sends. It reaches a profile from a
	// typed --server-version, and it must produce a report rather than a
	// refusal.
	unreadableBanner = "not-a-version"
)

// TestFor_HappyPath resolves five real banners against the declared release
// lines.
//
// Every row pins the three answers Profile deliberately keeps apart — what the
// server said it is, whose capability ladder answered, and what this repository
// promises about the line — because collapsing any two of them is invisible on
// a server where they happen to agree. The first row is the one where they do
// not: a MariaDB behind a mysql:// URL reports dialect "mysql" and is planned
// as MariaDB, and if the banner ever stopped outranking the URL scheme this row
// is the only one that would notice.
func TestFor_HappyPath(t *testing.T) {
	tests := []struct {
		name              string
		dialect           string
		banner            string
		productVersion    string
		wantDialect       string
		wantProduct       string
		wantVersion       string
		wantPresetName    string
		wantPresetDialect string
		wantSource        serverprofile.Source
		wantNote          string
		wantLine          string
		wantLabel         string
		wantLevel         capability.SupportLevel
		wantIdentifiers   capability.IdentifierLimit
	}{
		{
			name:              "mariadb behind a mysql url is planned as mariadb",
			dialect:           "mysql",
			banner:            mariaDBOverMySQLBanner,
			wantDialect:       "mysql",
			wantProduct:       "mariadb",
			wantVersion:       "10.11.6",
			wantPresetName:    "MariaDB1011",
			wantPresetDialect: "mariadb",
			wantSource:        serverprofile.SourceVersionLadder,
			// MySQL's ladder declares no 10.11 line, so this line number can
			// only have come from MariaDB's.
			wantLine:        "10.11",
			wantLevel:       capability.Certified,
			wantIdentifiers: capability.IdentifierLimit{Max: 64, Unit: capability.IdentifierCharacters},
		},
		{
			name:              "postgres 16 is a certified line",
			dialect:           "postgres",
			banner:            postgres16Banner,
			wantDialect:       "postgres",
			wantProduct:       "postgres",
			wantVersion:       "16.3",
			wantPresetName:    "Postgres16",
			wantPresetDialect: "postgres",
			wantSource:        serverprofile.SourceVersionLadder,
			wantLine:          "16",
			wantLevel:         capability.Certified,
			wantIdentifiers:   capability.IdentifierLimit{Max: 63, Unit: capability.IdentifierBytes},
		},
		{
			name:              "postgres 13 is retained as a legacy-tested sentinel",
			dialect:           "postgres",
			banner:            postgres13Banner,
			wantDialect:       "postgres",
			wantProduct:       "postgres",
			wantVersion:       "13.14",
			wantPresetName:    "Postgres13",
			wantPresetDialect: "postgres",
			wantSource:        serverprofile.SourceVersionLadder,
			wantLine:          "13",
			wantLevel:         capability.LegacyTested,
			wantIdentifiers:   capability.IdentifierLimit{Max: 63, Unit: capability.IdentifierBytes},
		},
		{
			// The product version wins where a dialect offers one, and it is
			// the field the version is reported from.
			name:              "sql server reads the product version when it is supplied",
			dialect:           "sqlserver",
			banner:            sqlServer2022Banner,
			productVersion:    sqlServer2022ProductVersion,
			wantDialect:       "sqlserver",
			wantProduct:       "sqlserver",
			wantVersion:       "16.0.4025.1",
			wantPresetName:    "SQLServer2022",
			wantPresetDialect: "sqlserver",
			// SQL Server has no version ladder, so the version identifies the
			// line without selecting the preset. Those are separate answers
			// and the profile reports both.
			wantSource: serverprofile.SourceDialectDefault,
			wantNote: "the sqlserver dialect has no measured version ladder; " +
				"the version did not refine capabilities",
			wantLine:  "16.0",
			wantLabel: "SQL Server 2022",
			// A DECLARED best-effort level: cells.go covers 16.0 and says
			// plainly that nothing here runs against it. It is the same level
			// TestFor_BestEffort reaches by matching no line at all, from the
			// opposite direction.
			wantLevel:       capability.BestEffort,
			wantIdentifiers: capability.IdentifierLimit{Max: 128, Unit: capability.IdentifierCharacters},
		},
		{
			// Same server, product version withheld. The banner alone still
			// lands on 16.0: the parse reads past the marketing year to the
			// token after the first " - ". Were the year read as the major
			// version, this row would fall on no declared line and the pair
			// would disagree.
			name:              "sql server reads past the marketing year without a product version",
			dialect:           "sqlserver",
			banner:            sqlServer2022Banner,
			wantDialect:       "sqlserver",
			wantProduct:       "sqlserver",
			wantVersion:       "16.0.4115.5",
			wantPresetName:    "SQLServer2022",
			wantPresetDialect: "sqlserver",
			wantSource:        serverprofile.SourceDialectDefault,
			wantNote: "the sqlserver dialect has no measured version ladder; " +
				"the version did not refine capabilities",
			wantLine:        "16.0",
			wantLabel:       "SQL Server 2022",
			wantLevel:       capability.BestEffort,
			wantIdentifiers: capability.IdentifierLimit{Max: 128, Unit: capability.IdentifierCharacters},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			profile := serverprofile.For(test.dialect, test.banner, test.productVersion)

			c.Assert(profile.Dialect, qt.Equals, test.wantDialect)
			c.Assert(profile.Server.Banner, qt.Equals, test.banner,
				qt.Commentf("the banner is reported verbatim, never normalized"))
			c.Assert(profile.Server.Product, qt.Equals, test.wantProduct)
			c.Assert(profile.Server.Version, qt.Equals, test.wantVersion)
			c.Assert(profile.Preset.Name, qt.Equals, test.wantPresetName)
			c.Assert(profile.Preset.Dialect, qt.Equals, test.wantPresetDialect)
			c.Assert(profile.Preset.Source, qt.Equals, test.wantSource)
			c.Assert(profile.Preset.Note, qt.Equals, test.wantNote)
			c.Assert(profile.Certification.Line, qt.Equals, test.wantLine)
			c.Assert(profile.Certification.Label, qt.Equals, test.wantLabel)
			c.Assert(profile.Certification.Level, qt.Equals, test.wantLevel)
			c.Assert(profile.Certification.Reason, qt.Not(qt.Equals), "",
				qt.Commentf("a level with no stated reason is a verdict nobody can check"))
			c.Assert(profile.Traits.Identifiers, qt.Equals, test.wantIdentifiers)
		})
	}
}

// TestFor_BestEffort covers the two ways a live server ends up on no declared
// release line: a version the matrix does not cover, and a string no version
// can be read from at all.
//
// Neither is an error, and that is the contract worth defending. The matrix is
// a record of what this repository tests, not a gate on the user's database, so
// both rows must still carry a resolved capability set, resolved traits, and a
// sentence saying what happened. A change that refused an undeclared line, or
// that returned an empty set for one, would take a working connection away over
// a missing CI cell.
func TestFor_BestEffort(t *testing.T) {
	tests := []struct {
		name              string
		dialect           string
		banner            string
		wantVersion       string
		wantPresetDialect string
		wantSource        serverprofile.Source
		wantIdentifierMax int
		wantEnumModeling  capability.EnumMode
		wantNoteContains  string
	}{
		{
			name:              "mysql 8.0 falls on a line the matrix does not declare",
			dialect:           "mysql",
			banner:            mySQL80Banner,
			wantVersion:       "8.0.42",
			wantPresetDialect: "mysql",
			wantSource:        serverprofile.SourceDialectDefault,
			wantIdentifierMax: 64,
			wantEnumModeling:  capability.EnumInline,
			wantNoteContains:  "8.0.42",
		},
		{
			name:              "an unreadable banner names no server",
			dialect:           "postgres",
			banner:            unreadableBanner,
			wantVersion:       "",
			wantPresetDialect: "postgres",
			wantSource:        serverprofile.SourceUnrecognized,
			wantIdentifierMax: 63,
			wantEnumModeling:  capability.EnumNamedType,
			// The note has to name the string it could not read. Without it a
			// reader has the verdict and no way to see the typo.
			wantNoteContains: `"not-a-version"`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			profile := serverprofile.For(test.dialect, test.banner, "")

			c.Assert(profile.Certification.Level, qt.Equals, capability.BestEffort)
			c.Assert(profile.Certification.Reason, qt.Not(qt.Equals), "")
			c.Assert(profile.Certification.Line, qt.Equals, "")
			c.Assert(profile.Certification.Label, qt.Equals, "")
			c.Assert(profile.Preset.Name, qt.Equals, "",
				qt.Commentf("no cell matched, so no preset name can be attributed"))
			c.Assert(profile.Preset.Dialect, qt.Equals, test.wantPresetDialect)
			c.Assert(profile.Preset.Source, qt.Equals, test.wantSource)
			c.Assert(profile.Server.Version, qt.Equals, test.wantVersion)

			// The undeclared line is reported, not refused: the capability set
			// is complete and the traits are the dialect's real ones.
			c.Assert(profile.Capabilities, qt.HasLen, len(capability.All()))
			c.Assert(profile.Traits.Identifiers.Max, qt.Equals, test.wantIdentifierMax)
			c.Assert(profile.Traits.EnumModeling, qt.Equals, test.wantEnumModeling,
				qt.Commentf("a nil capability set would report enums unsupported here"))

			c.Assert(profile.Preset.Note, qt.Contains, test.wantNoteContains)

			// An unreadable string on a laddered dialect reports the same empty
			// NewestMeasured as a dialect that has no ladder, and the note
			// generator once told a PostgreSQL operator that postgres has no
			// version ladder — false about the dialect and silent about the
			// banner. Neither row may say it.
			c.Assert(profile.Preset.Note, qt.Not(qt.Contains), "no measured version ladder")
		})
	}
}

// TestFor_EveryDefaultDialect is the census: every dialect capability ships a
// preset for must produce a complete profile from a server that said nothing
// about itself.
//
// The empty banner is the honest worst case, and it is reachable — an offline
// caller has no banner to pass. What the census forbids is the shape a new
// dialect arrives in: a profile with an unset support level, which would render
// as an empty string and read as a promise nobody made, or a reason field left
// blank, or a capability list shorter than the registry because some path
// returned early. A dialect added to DefaultDialects without a thought about
// certification fails here rather than in front of a user.
func TestFor_EveryDefaultDialect(t *testing.T) {
	dialects := capability.DefaultDialects()

	c := qt.New(t)
	c.Assert(dialects, qt.Not(qt.HasLen), 0,
		qt.Commentf("a census over an empty list asserts nothing"))

	for _, dialect := range dialects {
		t.Run(dialect, func(t *testing.T) {
			c := qt.New(t)

			profile := serverprofile.For(dialect, "", "")

			c.Assert(profile.Dialect, qt.Equals, dialect)
			c.Assert(profile.Capabilities, qt.HasLen, len(capability.All()))
			c.Assert(profile.Certification.Level.Valid(), qt.IsTrue,
				qt.Commentf("level %q is not one capability defines", profile.Certification.Level))
			c.Assert(profile.Certification.Reason, qt.Not(qt.Equals), "")
			c.Assert(profile.Preset.Source, qt.Equals, serverprofile.SourceUnrecognized,
				qt.Commentf("an empty banner named no server, and the profile says so"))
			c.Assert(profile.Preset.Note, qt.Not(qt.Equals), "")
		})
	}
}

// TestFor_ReportsAbsentCapabilities pins that the capability list carries the
// false rows, in an order that does not move between runs.
//
// The report exists to answer "why did Ptah refuse this", and that question is
// only answerable from the keys a server does NOT have. Filtered to the
// supported ones, the output for a server missing a capability would be
// byte-identical to the output of a build that never knew the key existed, and
// the two call for opposite responses.
//
// Order is the second half. capability.All ranges over the registry map and
// documents that its order is unspecified, so a list rendered in the order it
// arrives changes on every invocation and anything diffing this output reports
// a change that did not happen.
func TestFor_ReportsAbsentCapabilities(t *testing.T) {
	c := qt.New(t)

	profile := serverprofile.For("postgres", postgres16Banner, "")

	keys := make([]string, 0, len(profile.Capabilities))
	docs := make([]string, 0, len(profile.Capabilities))
	byKey := make(map[string]serverprofile.Capability, len(profile.Capabilities))
	for _, entry := range profile.Capabilities {
		keys = append(keys, entry.Key)
		docs = append(docs, entry.Doc)
		byKey[entry.Key] = entry
	}

	// Comparing against the SORTED registry is the assertion that cannot pass
	// by luck: it fixes both the membership and the order, whatever order
	// capability.All happened to return this time.
	registry := make([]string, 0, len(capability.All()))
	for _, key := range capability.All() {
		registry = append(registry, string(key))
	}
	slices.Sort(registry)
	c.Assert(keys, qt.DeepEquals, registry)

	// The promise the field's documentation makes, stated directly. It is the
	// weaker of the two — two randomized orders can coincide — so it stands
	// beside the comparison above rather than in place of it.
	second := serverprofile.For("postgres", postgres16Banner, "")
	c.Assert(second.Capabilities, qt.DeepEquals, profile.Capabilities,
		qt.Commentf("two runs against one server must render identically"))

	// PostgreSQL 16 models enums as a named type and not inline. Both keys are
	// listed; exactly one is true. Drop the false row and a reader cannot tell
	// "this server does not do inline enums" from "this build has no such key".
	c.Assert(byKey[string(capability.EnumCustomType)].Supported, qt.IsTrue)
	c.Assert(byKey[string(capability.EnumInlineColumn)].Supported, qt.IsFalse)

	// ALTER COLUMN SET EXPRESSION arrived in PostgreSQL 17, so its false row on
	// a 16 server is the version ladder's answer showing up in the report
	// rather than a dialect-wide constant.
	c.Assert(byKey[string(capability.AlterGeneratedColumnExpression)].Supported, qt.IsFalse)

	// Every row carries its meaning, absent rows included: a false row with no
	// documentation states a refusal without saying what was refused.
	c.Assert(docs, qt.Not(qt.Contains), "")
}
