package dbcapabilities_test

import (
	"bytes"
	"encoding/json"
	"errors"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	qt "github.com/frankban/quicktest"
	_ "modernc.org/sqlite"

	"go.5x5.cz/ptah/cmd/dbcapabilities"
	"go.5x5.cz/ptah/core/platform/capability"
	"go.5x5.cz/ptah/internal/serverprofile"
)

// sqliteURL is a database URL for a file this test owns. SQLite is the one
// engine the command can be driven against without a server, and it is a real
// connection rather than a stub: the version string asserted below is what the
// compiled-in engine reports about itself.
func sqliteURL(c *qt.C) string {
	c.Helper()
	return "sqlite://" + filepath.Join(c.TB.(*testing.T).TempDir(), "capabilities.db")
}

func runCapabilities(c *qt.C, args ...string) (stdout, stderr string, err error) {
	c.Helper()

	cmd := dbcapabilities.NewCapabilitiesCommand()
	var out, errOut bytes.Buffer
	cmd.SetOut(&out)
	cmd.SetErr(&errOut)
	cmd.SetArgs(args)

	err = cmd.Execute()
	return out.String(), errOut.String(), err
}

// headerFields reads the aligned block at the top of the text form back as
// label to value pairs.
//
// The block is tabwriter output, so the gap between the two columns is whatever
// the widest label made it, and an assertion written against those spaces would
// change meaning when a label is added. Splitting on the colon asserts about the
// fields instead. The block ends at the first blank line, which is where the
// renderer moves from the two-column table to the sentences that follow it.
func headerFields(text string) map[string]string {
	block, _, _ := strings.Cut(text, "\n\n")
	fields := make(map[string]string)
	for line := range strings.SplitSeq(block, "\n") {
		label, value, _ := strings.Cut(line, ":")
		fields[strings.TrimSpace(label)] = strings.TrimSpace(value)
	}
	return fields
}

// headerLabels is the same block read for its order rather than its values,
// which a map cannot carry.
func headerLabels(text string) []string {
	block, _, _ := strings.Cut(text, "\n\n")
	lines := strings.Split(block, "\n")
	labels := make([]string, 0, len(lines))
	for _, line := range lines {
		label, _, _ := strings.Cut(line, ":")
		labels = append(labels, strings.TrimSpace(label))
	}
	return labels
}

// squeezedLines collapses each line's whitespace runs to one space, so a row of
// the behavior or capability blocks can be named by what it says rather than by
// the column width tabwriter chose for the block it landed in.
func squeezedLines(text string) []string {
	lines := strings.Split(text, "\n")
	out := make([]string, 0, len(lines))
	for _, line := range lines {
		out = append(out, strings.Join(strings.Fields(line), " "))
	}
	return out
}

// jsonKeys is how a key name gets asserted BY NAME. Unmarshaling into
// serverprofile.Profile would round-trip through the very struct tags under
// test, so a rename would travel through both directions of the round trip and
// every assertion would still pass while every consumer of the document broke.
// internal/capabilityprobe/matrix_test.go pins `support_level` the same way and
// for the same reason.
func jsonKeys(c *qt.C, document json.RawMessage) []string {
	c.Helper()

	var object map[string]json.RawMessage
	c.Assert(json.Unmarshal(document, &object), qt.IsNil)
	return slices.Sorted(maps.Keys(object))
}

// jsonField returns one member verbatim. A member the document does not carry
// returns nil, which compares unequal to every expected value rather than
// decoding as a zero value that looks like an answer.
func jsonField(c *qt.C, document json.RawMessage, name string) json.RawMessage {
	c.Helper()

	var object map[string]json.RawMessage
	c.Assert(json.Unmarshal(document, &object), qt.IsNil)
	return object[name]
}

// jsonLeaves flattens a document to the values a reader would go looking for:
// every string and number in it, minus the two kinds the text form is
// documented not to carry verbatim.
//
// Booleans are left out because the text form spells them "supported" and
// "unsupported", which is a difference in wording rather than in content, and
// `doc` because omitting each capability's registry documentation is the one
// deliberate asymmetry between the two forms.
func jsonLeaves(c *qt.C, document json.RawMessage) []string {
	c.Helper()

	var value any
	c.Assert(json.Unmarshal(document, &value), qt.IsNil)
	return appendLeaves(nil, value)
}

func appendLeaves(out []string, value any) []string {
	switch typed := value.(type) {
	case map[string]any:
		for name, member := range typed {
			if name == "doc" {
				continue
			}
			out = appendLeaves(out, member)
		}
	case []any:
		for _, member := range typed {
			out = appendLeaves(out, member)
		}
	case string:
		out = append(out, typed)
	case float64:
		out = append(out, strconv.FormatFloat(typed, 'f', -1, 64))
	}
	return out
}

// mariaDBProfile is a profile with every field populated, including the three
// the SQLite path leaves empty: a product name, a release-line label and a
// preset note.
//
// It is a MariaDB reached over a mysql:// URL because that is the target where
// Profile.Dialect, Server.Product and Preset.Dialect are three different
// answers — mysql, mariadb and mariadb — which is the case serverprofile keeps
// them in separate fields for. Building it by hand rather than connecting is
// what serverprofile.For's purity is for: no MariaDB server is needed to assert
// how a MariaDB renders.
func mariaDBProfile() serverprofile.Profile {
	return serverprofile.Profile{
		Dialect: "mysql",
		Server: serverprofile.Server{
			Banner:  "5.5.5-11.4.4-MariaDB-ubu2404",
			Version: "11.4.4",
			Product: "mariadb",
		},
		Preset: serverprofile.Preset{
			Name:    "MariaDB11",
			Dialect: "mariadb",
			Source:  serverprofile.SourceVersionLadder,
			Note:    "planned against the MariaDB 11.4 preset",
		},
		Certification: serverprofile.Certification{
			Level:  capability.Certified,
			Line:   "11.4",
			Label:  "MariaDB 11.4 LTS",
			Reason: "exercised by this repository's integration suite",
		},
		Traits: capability.Traits{
			Identifiers:         capability.Identifiers("mariadb"),
			EnumModeling:        capability.EnumInline,
			ForeignKeyReference: capability.ReferenceIndexed,
		},
		Capabilities: []serverprofile.Capability{
			{Key: string(capability.ForeignKeys), Supported: true, Doc: "declarative foreign keys"},
			{Key: string(capability.Sequences), Supported: false, Doc: "standalone sequence objects"},
		},
	}
}

func TestCapabilitiesCommand_TextOutput_HappyPath(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c))

	c.Assert(err, qt.IsNil)
	c.Assert(stdout, qt.Contains, "Dialect:")
	c.Assert(stdout, qt.Contains, "sqlite")
	c.Assert(stdout, qt.Contains, "Capability preset:")
	c.Assert(stdout, qt.Contains, "SQLite3")
	c.Assert(stdout, qt.Contains, "Support level:")
	c.Assert(stdout, qt.Contains, string(capability.Certified))
	c.Assert(stdout, qt.Contains, "Behavior:")
	c.Assert(stdout, qt.Contains, "identifier_limit")
	c.Assert(stdout, qt.Contains, "foreign_key_reference")
	c.Assert(stdout, qt.Contains, "Capabilities:")
}

// TestCapabilitiesCommand_TextOutputNamesEveryCapability is the non-vacuity
// guard on the block above. "Capabilities:" appearing proves a heading was
// printed, not that anything was printed under it, and a renderer that emitted
// the heading and no rows would satisfy every assertion in the happy path.
func TestCapabilitiesCommand_TextOutputNamesEveryCapability(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c))

	c.Assert(err, qt.IsNil)
	for _, key := range capability.All() {
		c.Assert(stdout, qt.Contains, string(key))
	}
	// Absent keys are the ones an operator is diagnosing with, so the words
	// for both answers have to reach the output.
	c.Assert(stdout, qt.Contains, "supported")
	c.Assert(stdout, qt.Contains, "unsupported")
}

func TestCapabilitiesCommand_JSONOutput_HappyPath(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c), "--format", "json")

	c.Assert(err, qt.IsNil)

	var profile serverprofile.Profile
	c.Assert(json.Unmarshal([]byte(stdout), &profile), qt.IsNil)
	c.Assert(profile.Dialect, qt.Equals, "sqlite")
	c.Assert(profile.Preset.Name, qt.Equals, "SQLite3")
	c.Assert(profile.Certification.Level, qt.Equals, capability.Certified)
	c.Assert(profile.Certification.Line, qt.Equals, "3")
	c.Assert(profile.Certification.Reason, qt.Not(qt.Equals), "")
	c.Assert(profile.Server.Version, qt.Not(qt.Equals), "")
	c.Assert(profile.Capabilities, qt.HasLen, len(capability.All()))
	c.Assert(profile.Traits.ForeignKeyReference, qt.Equals, capability.ReferenceUnique)
	c.Assert(profile.Traits.EnumModeling, qt.Equals, capability.EnumUnsupported)
	// SQLite is the dialect Ptah models no identifier limit for, so this
	// round-trips the zero value rather than a number somebody chose.
	c.Assert(profile.Traits.Identifiers.Unlimited(), qt.IsTrue)
}

// TestCapabilitiesCommand_JSONOutputIsStable pins the property that makes the
// machine-readable form usable at all. capability.All ranges over a map and
// documents that its order is unspecified, so an unsorted rendering produced a
// different document on every invocation and any tool diffing it reported a
// change every time.
func TestCapabilitiesCommand_JSONOutputIsStable(t *testing.T) {
	c := qt.New(t)

	url := sqliteURL(c)
	first, _, err := runCapabilities(c, "--db-url", url, "--format", "json")
	c.Assert(err, qt.IsNil)
	second, _, err := runCapabilities(c, "--db-url", url, "--format", "json")
	c.Assert(err, qt.IsNil)

	c.Assert(second, qt.Equals, first)
}

// TestCapabilitiesCommand_PayloadAndNarrationAreSeparated keeps the JSON form
// pipeable. A progress line on stdout would be inside the document.
func TestCapabilitiesCommand_PayloadAndNarrationAreSeparated(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c), "--format", "json")

	c.Assert(err, qt.IsNil)
	c.Assert(json.Valid([]byte(stdout)), qt.IsTrue)
}

func TestCapabilitiesCommand_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{
			name:    "missing database url",
			args:    make([]string, 0),
			wantErr: "database URL is required",
		},
		{
			name:    "unknown format",
			args:    []string{"--db-url", "sqlite:///tmp/unused.db", "--format", "yaml"},
			wantErr: `invalid --format value "yaml": expected text or json`,
		},
		{
			name:    "empty format",
			args:    []string{"--db-url", "sqlite:///tmp/unused.db", "--format", ""},
			wantErr: `invalid --format value "": expected text or json`,
		},
		{
			name:    "unparsable connect timeout",
			args:    []string{"--db-url", "sqlite:///tmp/unused.db", "--connect-timeout", "soon"},
			wantErr: ".*soon.*",
		},
		{
			name:    "unknown flag",
			args:    []string{"--db-url", "sqlite:///tmp/unused.db", "--bogus-flag"},
			wantErr: "unknown flag: --bogus-flag",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			_, _, err := runCapabilities(c, test.args...)

			c.Assert(err, qt.ErrorMatches, test.wantErr)
		})
	}
}

// TestCapabilitiesCommand_ValidatesBeforeConnecting matters because the
// alternative reads as a database problem. A bad --format resolved after the
// dial would report a connection failure for a target the operator never
// meant to reach, and the address below is unreachable on purpose.
func TestCapabilitiesCommand_ValidatesBeforeConnecting(t *testing.T) {
	c := qt.New(t)

	_, _, err := runCapabilities(c,
		"--db-url", "postgres://ptah:ptah@127.0.0.1:1/ptah?sslmode=disable",
		"--format", "yaml")

	c.Assert(err, qt.ErrorMatches, `invalid --format value "yaml": expected text or json`)
}

// TestCapabilitiesCommand_TextHeaderLabelsInReadingOrder pins the header on the
// path the operator actually runs, so the seam below cannot pass while the
// command renders through something else.
//
// The labels are asserted as a whole sequence rather than one Contains at a
// time, because all three ways of breaking them are the same break to a reader
// following the documentation: renaming a label the docs quote, dropping a
// field — Server.Product went unrendered for exactly as long as nothing asked
// what the complete set was — and reordering the block the docs reproduce. The
// order is reading order: what Ptah parsed stands above the banner it was parsed
// out of. The values are left to the seam, being this machine's SQLite build.
func TestCapabilitiesCommand_TextHeaderLabelsInReadingOrder(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c))

	c.Assert(err, qt.IsNil)
	c.Assert(headerLabels(stdout), qt.DeepEquals, []string{
		"Dialect",
		"Server version",
		"Server product",
		"Banner",
		"Capability preset",
		"Preset source",
		"Support level",
		"Release line",
	})
}

// TestWriteProfile_TextHeader walks the header through profiles no SQLite can
// produce.
//
// Every field of the header is a three-way choice — a preset with a name and a
// dialect, with a name only, or with neither; a release line with a label,
// without one, or absent; a value or the word standing in for its absence — and
// SQLite takes one arm of each. The rows below take the others: a MariaDB
// answering behind a mysql:// URL, a server whose banner named nothing at all,
// a preset attributed to no dialect, and the SQL Server line whose label is
// half of what "16.0 (SQL Server 2022)" means.
//
// The whole field map is compared rather than a handful of substrings, so a
// renamed label, a dropped field and an added one are all one failure.
func TestWriteProfile_TextHeader(t *testing.T) {
	tests := []struct {
		name    string
		profile serverprofile.Profile
		want    map[string]string
	}{
		{
			name:    "product and preset dialect differ from the connected dialect",
			profile: mariaDBProfile(),
			want: map[string]string{
				"Dialect":           "mysql",
				"Server version":    "11.4.4",
				"Server product":    "mariadb",
				"Banner":            "5.5.5-11.4.4-MariaDB-ubu2404",
				"Capability preset": "MariaDB11 (mariadb)",
				"Preset source":     string(serverprofile.SourceVersionLadder),
				"Support level":     string(capability.Certified),
				"Release line":      "11.4 (MariaDB 11.4 LTS)",
			},
		},
		{
			name: "server that said nothing about itself",
			profile: serverprofile.Profile{
				Dialect: "mysql",
				Preset: serverprofile.Preset{
					Dialect: "mysql",
					Source:  serverprofile.SourceUnrecognized,
				},
				Certification: serverprofile.Certification{
					Level:  capability.BestEffort,
					Reason: "no product version could be read from the banner",
				},
			},
			want: map[string]string{
				"Dialect":           "mysql",
				"Server version":    "none",
				"Server product":    "none",
				"Banner":            "none",
				"Capability preset": "(unnamed, resolved for mysql)",
				"Preset source":     string(serverprofile.SourceUnrecognized),
				"Support level":     string(capability.BestEffort),
				"Release line":      "none",
			},
		},
		{
			name: "preset attributed to no dialect and a line with no label",
			profile: serverprofile.Profile{
				Dialect: "sqlite",
				Server: serverprofile.Server{
					Banner:  "3.53.3",
					Version: "3.53.3",
				},
				Preset: serverprofile.Preset{
					Name:   "SQLite3",
					Source: serverprofile.SourceDialectDefault,
				},
				Certification: serverprofile.Certification{
					Level:  capability.Certified,
					Line:   "3",
					Reason: "the amalgamation pinned in go.mod",
				},
			},
			want: map[string]string{
				"Dialect":           "sqlite",
				"Server version":    "3.53.3",
				"Server product":    "none",
				"Banner":            "3.53.3",
				"Capability preset": "SQLite3",
				"Preset source":     string(serverprofile.SourceDialectDefault),
				"Support level":     string(capability.Certified),
				"Release line":      "3",
			},
		},
		{
			name: "release line whose label is the name people use for it",
			profile: serverprofile.Profile{
				Dialect: "sqlserver",
				Server: serverprofile.Server{
					Banner:  "Microsoft SQL Server 2022 (RTM-CU21) - 16.0.4215.2 (X64)",
					Version: "16.0.4215",
					Product: "sqlserver",
				},
				Preset: serverprofile.Preset{
					Name:    "SQLServer2022",
					Dialect: "sqlserver",
					Source:  serverprofile.SourceDialectDefault,
				},
				Certification: serverprofile.Certification{
					Level:  capability.BestEffort,
					Line:   "16.0",
					Label:  "SQL Server 2022",
					Reason: "nothing in this repository is run against this line",
				},
			},
			want: map[string]string{
				"Dialect":           "sqlserver",
				"Server version":    "16.0.4215",
				"Server product":    "sqlserver",
				"Banner":            "Microsoft SQL Server 2022 (RTM-CU21) - 16.0.4215.2 (X64)",
				"Capability preset": "SQLServer2022 (sqlserver)",
				"Preset source":     string(serverprofile.SourceDialectDefault),
				"Support level":     string(capability.BestEffort),
				"Release line":      "16.0 (SQL Server 2022)",
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			var out bytes.Buffer
			err := dbcapabilities.WriteProfile(&out, "text", test.profile)

			c.Assert(err, qt.IsNil)
			c.Assert(headerFields(out.String()), qt.DeepEquals, test.want)
		})
	}
}

// TestWriteProfile_TextCarriesEveryValueTheJSONCarries is the census the other
// text tests cannot be.
//
// Each of them names the fields somebody remembered to name, which is how
// Server.Product came to be in the JSON and nowhere in the text form for as long
// as this package existed: no assertion was wrong, one was simply never written.
// This test names no field. It reads the document back and requires every value
// in it to reach the text form, so a field added to serverprofile.Profile that
// the renderer forgets is a failure here rather than an absence nobody sees.
//
// Its blind spot is measured rather than assumed, because a census that is
// trusted for more than it does is worse than none. A value the document
// carries TWICE is still found when only one of its two renderings survives:
// deleting the header's product row leaves "mariadb" in the output, since the
// preset was resolved by the MariaDB ladder and says so. That is why
// TestWriteProfile_TextHeader compares the header's fields as a whole map and
// TestCapabilitiesCommand_TextHeaderLabelsInReadingOrder compares its labels as
// a whole sequence — those two kill the deletion this one cannot.
func TestWriteProfile_TextCarriesEveryValueTheJSONCarries(t *testing.T) {
	c := qt.New(t)

	var document, text bytes.Buffer
	c.Assert(dbcapabilities.WriteProfile(&document, "json", mariaDBProfile()), qt.IsNil)
	c.Assert(dbcapabilities.WriteProfile(&text, "text", mariaDBProfile()), qt.IsNil)

	// The count is the non-vacuity guard: without it a flattener that returned
	// nothing would satisfy the loop below, and the census would pass by
	// examining no value at all. Eighteen is what this profile carries — twelve
	// strings across the four blocks, the identifier limit's number and unit,
	// and one key per capability entry.
	leaves := jsonLeaves(c, json.RawMessage(document.Bytes()))
	c.Assert(leaves, qt.HasLen, 18)
	for _, leaf := range leaves {
		c.Assert(text.String(), qt.Contains, leaf)
	}
}

// TestWriteProfile_TextBehaviorValues covers the block SQLite cannot exercise.
// SQLite is the dialect Ptah models no identifier limit for, so the one text
// test that existed rendered "unlimited" — the arm that never touches the unit,
// which is the part deciding whether a name of 64 two-byte characters fits.
func TestWriteProfile_TextBehaviorValues(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	err := dbcapabilities.WriteProfile(&out, "text", mariaDBProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(squeezedLines(out.String()), qt.Contains, "identifier_limit 64 characters")
	c.Assert(squeezedLines(out.String()), qt.Contains, "enum_modeling inline")
	c.Assert(squeezedLines(out.String()), qt.Contains, "foreign_key_reference indexed")
}

// TestWriteProfile_TextPairsEachKeyWithItsVerdict asserts the pairing rather
// than the presence of both words anywhere in the document. A renderer that
// printed every key as supported still contains "unsupported" — it is in the
// behavior block above — so the key and its verdict have to be asserted on one
// line.
//
// The absent doc string is the one documented difference between the two forms,
// and it is asserted here so that "the text form omits it" stays a decision
// instead of becoming a regression nobody notices in the other direction.
func TestWriteProfile_TextPairsEachKeyWithItsVerdict(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	err := dbcapabilities.WriteProfile(&out, "text", mariaDBProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(squeezedLines(out.String()), qt.Contains, "foreign_keys supported")
	c.Assert(squeezedLines(out.String()), qt.Contains, "sequences unsupported")
	c.Assert(out.String(), qt.Not(qt.Contains), "declarative foreign keys")
}

// TestWriteProfile_TextCarriesTheNoteAndTheReason pins the two sentences that
// sit below the aligned block. Both are the answer to "why did Ptah plan it
// that way", and both are printed outside the table because a sentence wrapped
// into a tabwriter column stops being readable.
func TestWriteProfile_TextCarriesTheNoteAndTheReason(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	err := dbcapabilities.WriteProfile(&out, "text", mariaDBProfile())

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Contains, "\nNote: planned against the MariaDB 11.4 preset\n")
	c.Assert(out.String(), qt.Contains, "\ncertified: exercised by this repository's integration suite\n")
}

// TestWriteProfile_TextOmitsAnEmptyNote is the other arm. An empty note means
// the version selected an exact measured release line, and a bare "Note:" with
// nothing after it would read as a note that failed to render.
func TestWriteProfile_TextOmitsAnEmptyNote(t *testing.T) {
	c := qt.New(t)

	profile := mariaDBProfile()
	profile.Preset.Note = ""

	var out bytes.Buffer
	err := dbcapabilities.WriteProfile(&out, "text", profile)

	c.Assert(err, qt.IsNil)
	c.Assert(out.String(), qt.Not(qt.Contains), "Note:")
}

// TestCapabilitiesCommand_JSONNamesItsTopLevelKeys pins the published document's
// key names on the command's own output.
//
// Decoding into serverprofile.Profile — which every other JSON test here does —
// cannot see a rename: the document is produced by the struct tags and read back
// through the same ones, so renaming `certification` to `cert` round-trips
// perfectly while every consumer asking for `certification` silently receives
// nothing.
func TestCapabilitiesCommand_JSONNamesItsTopLevelKeys(t *testing.T) {
	c := qt.New(t)

	stdout, _, err := runCapabilities(c, "--db-url", sqliteURL(c), "--format", "json")

	c.Assert(err, qt.IsNil)
	c.Assert(jsonKeys(c, json.RawMessage(stdout)), qt.DeepEquals, []string{
		"capabilities",
		"capability_preset",
		"certification",
		"dialect",
		"server",
		"traits",
	})
}

// TestWriteProfile_JSONNamesEveryKey pins the nested names too, including the
// four that carry `omitempty` and therefore disappear from a SQLite document:
// server.product, capability_preset.note, certification.label, and the
// identifier limit's max and unit. A key that is absent because the value was
// empty and a key that is absent because it was renamed are the same bytes, so
// the profile below populates all of them.
func TestWriteProfile_JSONNamesEveryKey(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	err := dbcapabilities.WriteProfile(&out, "json", mariaDBProfile())
	c.Assert(err, qt.IsNil)

	document := json.RawMessage(out.Bytes())
	c.Assert(jsonKeys(c, document), qt.DeepEquals, []string{
		"capabilities",
		"capability_preset",
		"certification",
		"dialect",
		"server",
		"traits",
	})
	c.Assert(jsonKeys(c, jsonField(c, document, "server")), qt.DeepEquals,
		[]string{"banner", "product", "version"})
	c.Assert(jsonKeys(c, jsonField(c, document, "capability_preset")), qt.DeepEquals,
		[]string{"dialect", "name", "note", "source"})
	c.Assert(jsonKeys(c, jsonField(c, document, "certification")), qt.DeepEquals,
		[]string{"label", "level", "line", "reason"})
	c.Assert(jsonKeys(c, jsonField(c, document, "traits")), qt.DeepEquals,
		[]string{"enum_modeling", "foreign_key_reference", "identifiers"})
	c.Assert(jsonKeys(c, jsonField(c, jsonField(c, document, "traits"), "identifiers")), qt.DeepEquals,
		[]string{"max", "unit"})
}

// TestWriteProfile_JSONCarriesEachCapabilityAsKeySupportedDoc pins the shape a
// consumer iterates. `supported` is the field a tool branches on, and a consumer
// asking a renamed document for it reads false — a valid-looking answer meaning
// "this server cannot do it", which is the worst way for a rename to fail.
func TestWriteProfile_JSONCarriesEachCapabilityAsKeySupportedDoc(t *testing.T) {
	c := qt.New(t)

	var out bytes.Buffer
	err := dbcapabilities.WriteProfile(&out, "json", mariaDBProfile())
	c.Assert(err, qt.IsNil)

	var entries []json.RawMessage
	document := json.RawMessage(out.Bytes())
	c.Assert(json.Unmarshal(jsonField(c, document, "capabilities"), &entries), qt.IsNil)
	c.Assert(entries, qt.HasLen, 2)

	c.Assert(jsonKeys(c, entries[0]), qt.DeepEquals, []string{"doc", "key", "supported"})
	c.Assert(string(jsonField(c, entries[0], "key")), qt.Equals, `"foreign_keys"`)
	c.Assert(string(jsonField(c, entries[0], "supported")), qt.Equals, "true")
	c.Assert(string(jsonField(c, entries[0], "doc")), qt.Equals, `"declarative foreign keys"`)
	c.Assert(string(jsonField(c, entries[1], "key")), qt.Equals, `"sequences"`)
	c.Assert(string(jsonField(c, entries[1], "supported")), qt.Equals, "false")
}

// TestWriteProfile_FailurePath keeps the exported seam as strict as the command
// that calls it. A caller handing it "yaml" must be told so rather than served
// the text form under a name it did not ask for.
func TestWriteProfile_FailurePath(t *testing.T) {
	tests := []struct {
		name    string
		format  string
		wantErr string
	}{
		{
			name:    "unknown format",
			format:  "yaml",
			wantErr: `invalid --format value "yaml": expected text or json`,
		},
		{
			name:    "empty format",
			format:  "",
			wantErr: `invalid --format value "": expected text or json`,
		},
		{
			name:    "format that differs only in case",
			format:  "JSON",
			wantErr: `invalid --format value "JSON": expected text or json`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			var out bytes.Buffer
			err := dbcapabilities.WriteProfile(&out, test.format, mariaDBProfile())

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(out.String(), qt.Equals, "")
		})
	}
}

// errRefusedWrite is what a closed pipe reports. `ptah db capabilities | head`
// is the ordinary way to read this command, and head exiting first is the
// ordinary way for the writer to start failing.
var errRefusedWrite = errors.New("refused write")

// refuseAt fails the single write carrying its marker and lets every other
// write through.
//
// A writer that stayed broken would prove much less than it appears to. The
// renderer returns an error from eight places, and once the writer is broken
// for good, a place that dropped its error is covered by the next write failing
// too — so a swallowed check still produces an error and the test stays green.
// Failing exactly one write leaves the rest of the render succeeding, so the
// error reaches the caller only through the one check under test.
type refuseAt struct {
	marker string
	spent  bool
}

func (r *refuseAt) Write(p []byte) (int, error) {
	if !r.spent && bytes.Contains(p, []byte(r.marker)) {
		r.spent = true
		return 0, errRefusedWrite
	}
	return len(p), nil
}

// TestWriteProfile_FailurePath_WriterRefusesOneWrite pins that a failed write
// reaches the caller from every stage of both renderers.
//
// The command's exit status is the only thing a script reads, and a renderer
// that dropped a write error would exit 0 having printed part of a profile —
// the failure mode where a truncated capability report is indistinguishable
// from a complete one. Each row names the text the failing write carries, which
// is how a row names a stage of the renderer without encoding the number of
// writes tabwriter happens to make.
func TestWriteProfile_FailurePath_WriterRefusesOneWrite(t *testing.T) {
	tests := []struct {
		name   string
		format string
		marker string
	}{
		{name: "header block", format: "text", marker: "Dialect:"},
		{name: "preset note", format: "text", marker: "Note:"},
		{name: "certification reason", format: "text", marker: "exercised by"},
		{name: "behavior heading", format: "text", marker: "Behavior:"},
		{name: "behavior rows", format: "text", marker: "identifier_limit"},
		{name: "capabilities heading", format: "text", marker: "Capabilities:"},
		{name: "capability rows", format: "text", marker: "foreign_keys"},
		{name: "json document", format: "json", marker: "dialect"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			err := dbcapabilities.WriteProfile(&refuseAt{marker: test.marker}, test.format, mariaDBProfile())

			c.Assert(err, qt.ErrorIs, errRefusedWrite)
		})
	}
}
