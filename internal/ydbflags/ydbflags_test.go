package ydbflags_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"

	qt "github.com/frankban/quicktest"

	"ptah.run/core/platform/capability"
	"ptah.run/internal/ydbflags"
)

// page reads one feature-flags page the monitoring endpoint of a local-ydb
// container answered, unedited.
func page(c *qt.C, name string) []byte {
	c.Helper()
	body, err := os.ReadFile(filepath.Join("testdata", name))
	c.Assert(err, qt.IsNil)
	return body
}

// TestRefine_EveryLineDefaultIsItsPreset is the measurement the mapping rests
// on: each line's flags at their defaults refine the line's preset into
// itself. A gate whose flag defaulted differently from what the preset claims
// would turn a cluster nobody configured into one Ptah plans for differently.
func TestRefine_EveryLineDefaultIsItsPreset(t *testing.T) {
	for _, test := range []struct {
		page   string
		preset func() capability.Capabilities
	}{
		{page: "local-ydb-25.1.4.7.json", preset: capability.YDB251},
		{page: "local-ydb-25.2.1.24.json", preset: capability.YDB252},
		{page: "local-ydb-25.3.1.25.json", preset: capability.YDB253},
		{page: "local-ydb-25.4.1.15.json", preset: capability.YDB253},
		{page: "local-ydb-26.1.1.22.json", preset: capability.YDB261},
		{page: "local-ydb-26.2.1.14.json", preset: capability.YDB262},
	} {
		t.Run(test.page, func(t *testing.T) {
			c := qt.New(t)
			flags, err := ydbflags.Decode(page(c, test.page), "/local")
			c.Assert(err, qt.IsNil)

			c.Assert(flags.Refine(test.preset()), qt.DeepEquals, test.preset())
		})
	}
}

// TestRefine_HappyPath pins what each flag does to its capability.
func TestRefine_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name  string
		flags ydbflags.Flags
		key   capability.Capability
		want  bool
	}{
		{
			name:  "a flag the cluster turned on turns the capability on",
			flags: ydbflags.Flags{"EnableAddUniqueIndex": true},
			key:   capability.UniqueIndexOnExistingTable,
			want:  true,
		},
		{
			name:  "a flag the cluster turned off turns the capability off",
			flags: ydbflags.Flags{"EnableAddColumsWithDefaults": false},
			key:   capability.AddColumnWithDefault,
			want:  false,
		},
		{
			name:  "a flag the line does not list is a feature the line lacks",
			flags: ydbflags.Flags{},
			key:   capability.AlterColumnDefault,
			want:  false,
		},
		{
			name:  "the wide date and time types follow their flag",
			flags: ydbflags.Flags{"EnableTableDatetime64": true},
			key:   capability.WideDateTimeTypes,
			want:  true,
		},
		{
			name:  "a decimal precision follows its flag",
			flags: ydbflags.Flags{"EnableParameterizedDecimal": true},
			key:   capability.ParameterizedDecimal,
			want:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			// The preset of the oldest line, where every one of these flags
			// is off, and of the newest, where most are on: each row has to
			// move one of them to its answer.
			for _, preset := range []capability.Capabilities{capability.YDB251(), capability.YDB262()} {
				c.Check(test.flags.Refine(preset).Has(test.key), qt.Equals, test.want)
			}
		})
	}
}

// TestRefine_LeavesUngatedKeysAndItsInputAlone pins the rest of the set: a
// flag decides only its own capability, and the caller's set is not written.
func TestRefine_LeavesUngatedKeysAndItsInputAlone(t *testing.T) {
	c := qt.New(t)
	preset := capability.YDB262()
	flags := ydbflags.Flags{"EnableAddUniqueIndex": true, "EnableAsyncIndexes": false, "EnableViews": true}

	refined := flags.Refine(preset)

	c.Assert(refined.Has(capability.UniqueIndexOnExistingTable), qt.IsTrue)
	c.Assert(refined.Has(capability.AsyncIndexes), qt.IsTrue)
	c.Assert(refined.Has(capability.Views), qt.IsFalse)
	c.Assert(preset, qt.DeepEquals, capability.YDB262())
}

func TestDecode_HappyPath(t *testing.T) {
	c := qt.New(t)

	// The page a 26.2.1.14 container answered when it was started with
	// YDB_FEATURE_FLAGS=enable_add_unique_index,enable_set_column_constraint.
	flags, err := ydbflags.Decode(page(c, "local-ydb-26.2.1.14-add-unique-index.json"), "local/")

	c.Assert(err, qt.IsNil)
	c.Assert(flags["EnableAddUniqueIndex"], qt.IsTrue)
	c.Assert(flags["EnableOnlineAddUniqueIndex"], qt.IsFalse)
	// Current wins over Default in both directions.
	c.Assert(flags["EnableDrainOnShutdown"], qt.IsFalse)
	c.Assert(flags["EnableTableDatetime64"], qt.IsTrue)
	c.Assert(flags.Refine(capability.YDB262()).Has(capability.UniqueIndexOnExistingTable), qt.IsTrue)
}

func TestDecode_FailurePath(t *testing.T) {
	for _, test := range []struct {
		name    string
		body    string
		wantErr string
	}{
		{
			name:    "not JSON",
			body:    "Failed to resolve database",
			wantErr: `the feature-flags page is not the JSON Ptah reads: .*`,
		},
		{
			name:    "another page layout",
			body:    `{"Version":3,"Databases":[]}`,
			wantErr: `the feature-flags page has version 3, and Ptah reads version 2`,
		},
		{
			name:    "the database is not listed",
			body:    `{"Version":2,"Databases":[{"Name":"/other","FeatureFlags":[]}]}`,
			wantErr: `the feature-flags page does not list database /local`,
		},
		{
			name:    "a flag with no value",
			body:    `{"Version":2,"Databases":[{"Name":"/local","FeatureFlags":[{"Name":"EnableAddUniqueIndex"}]}]}`,
			wantErr: `the feature-flags page lists EnableAddUniqueIndex with no value`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			flags, err := ydbflags.Decode([]byte(test.body), "/local")

			c.Assert(err, qt.ErrorMatches, test.wantErr)
			c.Assert(flags, qt.IsNil)
		})
	}
}

func TestRead_HappyPath(t *testing.T) {
	c := qt.New(t)
	body := page(c, "local-ydb-26.2.1.14.json")
	var asked *url.URL
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		asked = r.URL
		_, _ = w.Write(body)
	}))
	c.Cleanup(server.Close)
	endpoint, err := url.Parse(server.URL)
	c.Assert(err, qt.IsNil)

	flags, err := ydbflags.Read(t.Context(), endpoint, "/local")

	c.Assert(err, qt.IsNil)
	c.Assert(flags["EnableSetDropDefaultValue"], qt.IsTrue)
	c.Assert(asked.Path, qt.Equals, "/viewer/json/feature_flags")
	c.Assert(asked.Query().Get("database"), qt.Equals, "/local")
}

func TestRead_FailurePath(t *testing.T) {
	c := qt.New(t)
	// The answer a 26.2.1.14 monitoring endpoint gives for a database it does
	// not serve.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "Failed to resolve database", http.StatusBadRequest)
	}))
	c.Cleanup(server.Close)
	endpoint, err := url.Parse(server.URL)
	c.Assert(err, qt.IsNil)

	t.Run("a refusing endpoint", func(t *testing.T) {
		c := qt.New(t)
		flags, err := ydbflags.Read(t.Context(), endpoint, "/nope")
		c.Assert(err, qt.ErrorMatches,
			`read YDB feature flags from http://.*/viewer/json/feature_flags\?database=%2Fnope: `+
				`400 Bad Request: Failed to resolve database`)
		c.Assert(flags, qt.IsNil)
	})
	t.Run("no endpoint", func(t *testing.T) {
		c := qt.New(t)
		flags, err := ydbflags.Read(t.Context(), nil, "/local")
		c.Assert(err, qt.ErrorMatches, `no monitoring endpoint to read feature flags from`)
		c.Assert(flags, qt.IsNil)
	})
}

// TestRefused_HappyPath pins each refusal text, quoted from the line that
// answered it, to the capability it says is off.
func TestRefused_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name     string
		refusal  string
		wantKey  capability.Capability
		wantFlag string
	}{
		{
			name:     "26.2.1.14 adding a unique index",
			refusal:  "Status: BAD_REQUEST Issues: <main>: Error: Failed item check: Adding a unique index to an existing table is disabled",
			wantKey:  capability.UniqueIndexOnExistingTable,
			wantFlag: "EnableAddUniqueIndex",
		},
		{
			name:     "25.3.1.25 adding a column with a default",
			refusal:  "Status: PRECONDITION_FAILED Issues: <main>: Error: Adding columns with defaults is disabled",
			wantKey:  capability.AddColumnWithDefault,
			wantFlag: "EnableAddColumsWithDefaults",
		},
		{
			name: "25.1.4.7 adding a column with a default",
			refusal: "Status: BAD_REQUEST Issues: <main>: Error: At AlterMainTable state got unsuccess propose result, " +
				"status: StatusInvalidParameter, reason: Column addition with default value is not supported now.",
			wantKey:  capability.AddColumnWithDefault,
			wantFlag: "EnableAddColumsWithDefaults",
		},
		{
			name: "26.1.1.22 setting a default",
			refusal: `<main>:1:86: Error: AlterTable : db.[/local/mall/sc_def]". ` +
				`Set/drop default value is not enabled.`,
			wantKey:  capability.AlterColumnDefault,
			wantFlag: "EnableSetDropDefaultValue",
		},
		{
			name: "25.1.4.7 a Timestamp64 column",
			refusal: "Type 'Timestamp64' specified for column 'c', but support for new date/time 64 types is disabled " +
				"(EnableTableDatetime64 feature flag is off), code: 2003",
			wantKey:  capability.WideDateTimeTypes,
			wantFlag: "EnableTableDatetime64",
		},
		{
			name: "25.1.4.7 a Decimal(10,2) column",
			refusal: "Type 'Decimal(10,2)' specified for column 'c', but support for parametrized decimal is disabled " +
				"(EnableParameterizedDecimal feature flag is off), code: 2003",
			wantKey:  capability.ParameterizedDecimal,
			wantFlag: "EnableParameterizedDecimal",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)

			gate, ok := ydbflags.Refused(test.refusal)

			c.Assert(ok, qt.IsTrue)
			c.Assert(gate.Key, qt.Equals, test.wantKey)
			c.Assert(gate.Flag, qt.Equals, test.wantFlag)
		})
	}
}

// TestRefused_FailurePath is the control: a refusal about something else, and
// one about a flag no capability Ptah reads depends on, name no gate.
func TestRefused_FailurePath(t *testing.T) {
	for _, refusal := range []string{
		"Status: GENERIC_ERROR Issues: <main>:1:70: Error: SET NOT NULL is currently not supported.",
		"Error: Resource pools are disabled. Please contact your system administrator to enable it",
		"Error: Conflict with existing key., code: 2012",
	} {
		t.Run(refusal, func(t *testing.T) {
			c := qt.New(t)
			gate, ok := ydbflags.Refused(refusal)
			c.Assert(ok, qt.IsFalse)
			c.Assert(gate.Key, qt.Equals, capability.Capability(""))
			c.Assert(gate.Flag, qt.Equals, "")
		})
	}
}

// TestGates_NameEveryFlagOnce keeps the mapping a function: a key decided by
// two flags, or a flag deciding two keys, would make Refine's answer depend on
// the order of the list.
func TestGates_NameEveryFlagOnce(t *testing.T) {
	c := qt.New(t)
	keys := make(map[capability.Capability]int)
	flags := make(map[string]int)
	for _, gate := range ydbflags.Gates() {
		keys[gate.Key]++
		flags[gate.Flag]++
	}
	c.Assert(len(keys) > 0, qt.IsTrue)
	for key, count := range keys {
		c.Check(count, qt.Equals, 1, qt.Commentf("%s", key))
	}
	for flag, count := range flags {
		c.Check(count, qt.Equals, 1, qt.Commentf("%s", flag))
	}
}
