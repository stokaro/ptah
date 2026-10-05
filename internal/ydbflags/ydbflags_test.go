package ydbflags_test

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
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
		{page: "local-ydb-25.4.1.15.json", preset: capability.YDB254},
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
		{
			name:  "renaming an index follows its flag",
			flags: ydbflags.Flags{"EnableMoveIndex": false},
			key:   capability.IndexRename,
			want:  false,
		},
		{
			name:  "a changefeed's auto-partitioned topic follows its flag",
			flags: ydbflags.Flags{"EnableTopicAutopartitioningForCDC": true},
			key:   capability.ChangefeedTopicAutoPartitioning,
			want:  true,
		},
		{
			name:  "resource pools and their classifiers follow their flag",
			flags: ydbflags.Flags{"EnableResourcePools": true},
			key:   capability.ResourcePools,
			want:  true,
		},
		{
			name:  "a vector index follows its flag on",
			flags: ydbflags.Flags{"EnableVectorIndex": true},
			key:   capability.VectorIndexes,
			want:  true,
		},
		{
			name:  "a vector index follows its flag off",
			flags: ydbflags.Flags{"EnableVectorIndex": false},
			key:   capability.VectorIndexes,
			want:  false,
		},
		{
			name:  "a transfer follows its flag",
			flags: ydbflags.Flags{"EnableTopicTransfer": true},
			key:   capability.Transfers,
			want:  true,
		},
		{
			name:  "an async replication follows its flag",
			flags: ydbflags.Flags{"EnableReplication": false},
			key:   capability.AsyncReplication,
			want:  false,
		},
		{
			name:  "a column family's cache mode follows its flag",
			flags: ydbflags.Flags{"EnableTableCacheModes": true},
			key:   capability.ColumnFamilyCacheMode,
			want:  true,
		},
		{
			name:  "a secret follows its flag",
			flags: ydbflags.Flags{"EnableSchemaSecrets": false},
			key:   capability.Secrets,
			want:  false,
		},
		{
			name:  "an external data source follows its flag",
			flags: ydbflags.Flags{"EnableExternalDataSources": true},
			key:   capability.ExternalDataSources,
			want:  true,
		},
		{
			name:  "replacing an external object follows its flag",
			flags: ydbflags.Flags{"EnableReplaceIfExistsForExternalEntities": true},
			key:   capability.ExternalObjectReplace,
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

// A flag the database does not list leaves its key as the preset has it. A
// line without a flag either predates the feature, where its preset says false
// already, or graduated it and removed the flag, where turning the key off
// would narrow what Ptah writes -- without wide_date_time_types a declared
// TIMESTAMP is written as the narrow Timestamp.
func TestRefine_AnAbsentFlagKeepsThePreset(t *testing.T) {
	c := qt.New(t)
	for _, preset := range []func() capability.Capabilities{capability.YDB251, capability.YDB262} {
		c.Assert(ydbflags.Flags{}.Refine(preset()), qt.DeepEquals, preset())
	}
	graduated := ydbflags.Flags{
		"EnableAddUniqueIndex": false, "EnableAddColumsWithDefaults": true,
		"EnableSetDropDefaultValue": true, "EnableParameterizedDecimal": true,
	}
	c.Assert(graduated.Refine(capability.YDB262()).Has(capability.WideDateTimeTypes), qt.IsTrue)
}

// A cluster that turned EnableAsyncIndexes off still builds async indexes:
// measured on both certified lines with the flag off in the startup
// configuration, which is what these pages were recorded from. So the flag
// leaves async_indexes, and every other key, as the line's preset has it.
func TestRefine_AsyncIndexesFlagOffKeepsThePreset(t *testing.T) {
	for _, test := range []struct {
		page   string
		preset func() capability.Capabilities
	}{
		{page: "local-ydb-26.2.1.14-async-indexes-off.json", preset: capability.YDB262},
		{page: "local-ydb-25.1.4.7-async-indexes-off.json", preset: capability.YDB251},
	} {
		t.Run(test.page, func(t *testing.T) {
			c := qt.New(t)
			flags, err := ydbflags.Decode(page(c, test.page), "/local")
			c.Assert(err, qt.IsNil)
			c.Assert(flags["EnableAsyncIndexes"], qt.IsFalse)

			refined := flags.Refine(test.preset())

			c.Assert(refined.Has(capability.AsyncIndexes), qt.IsTrue)
			c.Assert(refined, qt.DeepEquals, test.preset())
		})
	}
}

// A cluster that turned EnableMoveIndex off refuses ALTER TABLE ... RENAME
// INDEX, measured on both certified lines with the flag off in the startup
// configuration, which is what these pages were recorded from. The flag turns
// index_rename off, so Ptah plans a drop and a create the server takes, and
// leaves every other key as the line's preset has it.
func TestRefine_MoveIndexFlagOffTurnsIndexRenameOff(t *testing.T) {
	for _, test := range []struct {
		page   string
		preset func() capability.Capabilities
	}{
		{page: "local-ydb-26.2.1.14-move-index-off.json", preset: capability.YDB262},
		{page: "local-ydb-25.1.4.7-move-index-off.json", preset: capability.YDB251},
	} {
		t.Run(test.page, func(t *testing.T) {
			c := qt.New(t)
			flags, err := ydbflags.Decode(page(c, test.page), "/local")
			c.Assert(err, qt.IsNil)
			c.Assert(test.preset().Has(capability.IndexRename), qt.IsTrue)

			refined := flags.Refine(test.preset())

			c.Assert(refined.Has(capability.IndexRename), qt.IsFalse)
			c.Assert(refined, qt.DeepEquals, test.preset().With(capability.IndexRename, false))
		})
	}
}

// A 25.1 cluster started with EnableVectorIndex on, as the integration
// workflow starts its 25.1 server, builds a vector index: the flag turns
// vector_indexes on and leaves every other key as the line's preset has it.
// The page was recorded from local-ydb 25.1.4.7 started with
// YDB_FEATURE_FLAGS=enable_vector_index, and differs from the default page in
// that one flag's current value.
func TestRefine_VectorIndexFlagOnTurnsVectorIndexesOn(t *testing.T) {
	c := qt.New(t)
	flags, err := ydbflags.Decode(page(c, "local-ydb-25.1.4.7-vector-index.json"), "/local")
	c.Assert(err, qt.IsNil)
	c.Assert(capability.YDB251().Has(capability.VectorIndexes), qt.IsFalse)

	refined := flags.Refine(capability.YDB251())

	c.Assert(refined, qt.DeepEquals, capability.YDB251().With(capability.VectorIndexes, true))
}

// TestRefine_LeavesUngatedKeysAndItsInputAlone pins the rest of the set: a
// flag decides only its own capability, and the caller's set is not written.
//
// EnableViews is not a gate. Turned off through the dynamic config, it
// refuses CREATE VIEW, DROP VIEW and a read of a view on 25.1.4.7 (`Views are
// disabled. Please contact your system administrator to enable the feature`)
// and changes nothing on 26.2.1.14, which creates and reads views with the
// flag off. Read as the key, it would take views away from a 26.2 cluster
// that has them.
func TestRefine_LeavesUngatedKeysAndItsInputAlone(t *testing.T) {
	c := qt.New(t)
	preset := capability.YDB262()
	flags := ydbflags.Flags{"EnableAddUniqueIndex": true, "EnableAsyncIndexes": false, "EnableViews": false}

	refined := flags.Refine(preset)

	c.Assert(refined.Has(capability.UniqueIndexOnExistingTable), qt.IsTrue)
	c.Assert(refined.Has(capability.AsyncIndexes), qt.IsTrue)
	c.Assert(refined.Has(capability.Views), qt.IsTrue)
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

// The page is read with the connection's credential, sent bare in the
// Authorization header, which is the one spelling a YDB monitoring endpoint
// that enforces authentication accepts; an anonymous connection sends none.
func TestRead_HappyPath(t *testing.T) {
	for _, test := range []struct {
		name              string
		ticket            string
		wantAuthorization []string
	}{
		{name: "an anonymous connection", ticket: "", wantAuthorization: nil},
		{name: "a connection with a credential", ticket: "eyJhbGciOi.t0k3n", wantAuthorization: []string{"eyJhbGciOi.t0k3n"}},
	} {
		t.Run(test.name, func(t *testing.T) {
			c := qt.New(t)
			body := page(c, "local-ydb-26.2.1.14.json")
			var asked *http.Request
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				asked = r
				_, _ = w.Write(body)
			}))
			c.Cleanup(server.Close)
			endpoint, err := url.Parse(server.URL)
			c.Assert(err, qt.IsNil)

			flags, err := ydbflags.Read(t.Context(), endpoint, "/local", test.ticket)

			c.Assert(err, qt.IsNil)
			c.Assert(flags["EnableSetDropDefaultValue"], qt.IsTrue)
			c.Assert(asked.URL.Path, qt.Equals, "/viewer/json/feature_flags")
			c.Assert(asked.URL.Query().Get("database"), qt.Equals, "/local")
			c.Assert(asked.Header.Values("Authorization"), qt.DeepEquals, test.wantAuthorization)
		})
	}
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
		flags, err := ydbflags.Read(t.Context(), endpoint, "/nope", "")
		c.Assert(err, qt.ErrorMatches,
			`read YDB feature flags from http://.*/viewer/json/feature_flags\?database=%2Fnope: `+
				`400 Bad Request: Failed to resolve database`)
		c.Assert(flags, qt.IsNil)
	})
	// 26.2.1.14 answers the same to a user without DESCRIBE SCHEMA on the
	// database, and the error says so when the page was read as a user.
	t.Run("a refusal of the connection's user", func(t *testing.T) {
		c := qt.New(t)
		flags, err := ydbflags.Read(t.Context(), endpoint, "/local", "t0k3n")
		c.Assert(err, qt.ErrorMatches,
			`read YDB feature flags from http://.*/viewer/json/feature_flags\?database=%2Flocal: `+
				`400 Bad Request: Failed to resolve database; the page is read as the connection's user, `+
				`who needs DESCRIBE SCHEMA on /local`)
		c.Assert(flags, qt.IsNil)
	})
	// An endpoint that repeats the request's headers in its refusal repeats
	// the credential, and the error must not.
	t.Run("a refusal that repeats the credential", func(t *testing.T) {
		c := qt.New(t)
		mirror := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Error(w, "Could not find correct token validator for "+r.Header.Get("Authorization"), http.StatusForbidden)
		}))
		c.Cleanup(mirror.Close)
		mirroring, err := url.Parse(mirror.URL)
		c.Assert(err, qt.IsNil)

		flags, err := ydbflags.Read(t.Context(), mirroring, "/local", "eyJhbGciOi.t0k3n")

		c.Assert(err, qt.ErrorMatches, `read YDB feature flags from http://.*: 403 Forbidden: `+
			`Could not find correct token validator for <redacted>`)
		c.Assert(flags, qt.IsNil)
	})
	t.Run("no endpoint", func(t *testing.T) {
		c := qt.New(t)
		flags, err := ydbflags.Read(t.Context(), nil, "/local", "")
		c.Assert(err, qt.ErrorMatches, `no monitoring endpoint to read feature flags from`)
		c.Assert(flags, qt.IsNil)
	})
	// The page is read from the endpoint the operator named. A redirect is
	// answered as the status it is, and the host it points at is never asked.
	t.Run("a redirect", func(t *testing.T) {
		c := qt.New(t)
		var elsewhere atomic.Int32
		other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			elsewhere.Add(1)
			_, _ = w.Write([]byte(`{"Version":2,"Databases":[{"Name":"/local","FeatureFlags":[]}]}`))
		}))
		c.Cleanup(other.Close)
		redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, other.URL+"/latest/meta-data/", http.StatusFound)
		}))
		c.Cleanup(redirector.Close)
		redirecting, err := url.Parse(redirector.URL)
		c.Assert(err, qt.IsNil)

		flags, err := ydbflags.Read(t.Context(), redirecting, "/local", "t0k3n")

		c.Assert(err, qt.ErrorMatches, `read YDB feature flags from http://.*: 302 Found: .*`)
		c.Assert(flags, qt.IsNil)
		c.Assert(elsewhere.Load(), qt.Equals, int32(0))
	})
	// An error repeats the start of a refusing page, not the page.
	t.Run("a long refusal", func(t *testing.T) {
		c := qt.New(t)
		long := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = w.Write([]byte(strings.Repeat("y", 1<<20)))
		}))
		c.Cleanup(long.Close)
		refusing, err := url.Parse(long.URL)
		c.Assert(err, qt.IsNil)

		flags, err := ydbflags.Read(t.Context(), refusing, "/local", "")

		c.Assert(err, qt.ErrorMatches, `read YDB feature flags from http://.*: 500 Internal Server Error: y{256}\.\.\.`)
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
		{
			name: "26.2.1.14 renaming an index",
			refusal: "Status: PRECONDITION_FAILED Issues: <main>: Error: Executing ESchemeOpMoveIndex, code: 2029 " +
				"<main>: Error: Move index is not supported yet, code: 2029",
			wantKey:  capability.IndexRename,
			wantFlag: "EnableMoveIndex",
		},
		{
			name:     "25.1.4.7 renaming an index",
			refusal:  "Status: PRECONDITION_FAILED Issues: <main>: Error: Move index is not supported yet, code: 2029",
			wantKey:  capability.IndexRename,
			wantFlag: "EnableMoveIndex",
		},
		{
			name: "25.1.4.7 a changefeed with an auto-partitioned topic",
			refusal: "operation/BAD_REQUEST (code = 400010, issues = [{#2017 'Topic autopartitioning for CDC " +
				"is disabled'}])",
			wantKey:  capability.ChangefeedTopicAutoPartitioning,
			wantFlag: "EnableTopicAutopartitioningForCDC",
		},
		{
			name: "26.2.1.14 creating a resource pool",
			refusal: "Status: UNSUPPORTED Issues: <main>: Error: Executing operation with object \"RESOURCE_POOL\", " +
				"code: 2030 <main>: Error: <main>: Error: Resource pools are disabled. Please contact your system " +
				"administrator to enable it, code: 2030",
			wantKey:  capability.ResourcePools,
			wantFlag: "EnableResourcePools",
		},
		{
			name: "25.1.4.7 creating a resource pool classifier",
			refusal: "Status: GENERIC_ERROR Issues: <main>: Error: preparation problem: Resource pool classifiers " +
				"are disabled. Please contact your system administrator to enable it",
			wantKey:  capability.ResourcePools,
			wantFlag: "EnableResourcePools",
		},
		{
			name: "25.1.4.7 a vector index in CREATE TABLE",
			refusal: "Status: PRECONDITION_FAILED Issues: <main>: Error: Execution, code: 1060 <main>:1:113: Error: " +
				"Executing CREATE TABLE <main>: Error: Vector index support is disabled, code: 2029",
			wantKey:  capability.VectorIndexes,
			wantFlag: "EnableVectorIndex",
		},
		{
			name: "25.1.4.7 a vector index added to a table",
			refusal: "Status: GENERIC_ERROR Issues: <main>: Error: Execution, code: 1060 <main>:1:65: Error: " +
				"Vector index support is disabled",
			wantKey:  capability.VectorIndexes,
			wantFlag: "EnableVectorIndex",
		},
		{
			name:     "25.1.4.7 a transfer",
			refusal:  "Status: BAD_REQUEST Issues: <main>: Error: Topic transfer creation is disabled, code: 2017",
			wantKey:  capability.Transfers,
			wantFlag: "EnableTopicTransfer",
		},
		{
			name: "26.2.1.14 an async replication with the flag off",
			refusal: "Status: PRECONDITION_FAILED Issues: <main>: Error: Executing ESchemeOpCreateReplication, " +
				"code: 2029 <main>: Error: Asynchronous replication is disabled, code: 2029",
			wantKey:  capability.AsyncReplication,
			wantFlag: "EnableReplication",
		},
		{
			name: "25.3.1.25 a column family's cache mode",
			refusal: "operation/GENERIC_ERROR (code = 400080, address = localhost:2136, issues = [{#1060 'Execution' " +
				"[{1:101 => 'Executing CREATE TABLE' [{'Setting cache_mode is not allowed'}]}]}])",
			wantKey:  capability.ColumnFamilyCacheMode,
			wantFlag: "EnableTableCacheModes",
		},
		{
			name: "25.3.1.25 a secret",
			refusal: "Status: INTERNAL_ERROR Issues: <main>: Fatal: Secrets are disabled. Please contact your " +
				"system administrator to enable it, code: 1",
			wantKey:  capability.Secrets,
			wantFlag: "EnableSchemaSecrets",
		},
		{
			name: "26.2.1.14 an external data source",
			refusal: "Status: GENERIC_ERROR Issues: <main>: Error: External data sources are disabled. Please " +
				"contact your system administrator to enable it",
			wantKey:  capability.ExternalDataSources,
			wantFlag: "EnableExternalDataSources",
		},
		{
			name: "26.2.1.14 replacing an external data source",
			refusal: "Status: PRECONDITION_FAILED Issues: <main>: Error: Unsupported: feature flag " +
				"EnableReplaceIfExistsForExternalEntities is off, code: 2029",
			wantKey:  capability.ExternalObjectReplace,
			wantFlag: "EnableReplaceIfExistsForExternalEntities",
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
		"Error: Backup collections are disabled. Please contact your system administrator to enable it, code: 2029",
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
