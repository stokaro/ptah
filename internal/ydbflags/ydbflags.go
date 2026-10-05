// Package ydbflags reads the feature flags a YDB cluster runs with from its
// monitoring endpoint, and turns the flags that gate a capability Ptah reads
// into that capability's value on the cluster.
//
// A YDB capability preset describes a release line running with its default
// flags. An operator can turn a flag on or off, and YDB ships features behind
// flags on odd releases and turns them on in even ones, so one release can do
// more or less than its line's preset says. The cluster's own list is the
// answer, and it is read-only: `GET /viewer/json/feature_flags?database=<db>`
// on the monitoring port lists every flag the database knows with its default
// and, where it was set, its current value (stokaro/ptah#4015, decision 12).
//
// The page is read with the database connection's own credential, which the
// connection layer hands in, so a cluster that enforces authentication serves
// it to the user that connects. The package links no YDB SDK, so the connection
// layer reaches it without pulling a driver into any other path.
package ydbflags

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"slices"
	"strings"
	"time"

	"ptah.run/core/platform/capability"
)

// Gate is one capability a YDB feature flag decides.
type Gate struct {
	// Key is the capability.
	Key capability.Capability
	// Flag is the flag's name as the monitoring endpoint spells it.
	Flag string
	// Requires is an additional capability needed even when this flag is on.
	Requires capability.Capability
	// refusals are texts the server's refusal contains when the flag is off,
	// each measured; a gate whose refusal was never seen has none.
	refusals []string
}

// gates are the measured flags. Each one's default on every YDB line agrees
// with the line's preset, measured on local-ydb 25.1.4.7, 25.2.1.24,
// 25.3.1.25, 25.4.1.15, 26.1.1.22 and 26.2.1.14. Each one was then turned
// on with YDB_FEATURE_FLAGS on every line where it is off by default, and the
// statement it gates was accepted and did what it says on each of them, so
// Refine claims no capability a line refuses with the flag on.
//
// EnableAsyncIndexes is not a gate, because turning it off refuses nothing.
// YDB_FEATURE_FLAGS can only turn a flag on, so it was turned off in the
// startup configuration of 26.2.1.14 and 25.1.4.7, where the page then reads
// Current: false. Both servers still created a GLOBAL ASYNC index inline in
// CREATE TABLE and through ALTER TABLE ... ADD INDEX on a table holding a row,
// described each as GlobalAsync, and answered a read through it. Mapping the
// flag would turn async_indexes off on a cluster that builds async indexes.
var gates = []Gate{
	{Key: capability.StreamingQueries, Flag: "EnableStreamingQueries", Requires: capability.ExternalDataSources, refusals: []string{"Streaming queries are disabled"}},
	{
		// Off on every line that lists it (25.3 and later); 25.1 and 25.2 do
		// not list it, and refuse a unique index on an existing table outright.
		Key:      capability.UniqueIndexOnExistingTable,
		Flag:     "EnableAddUniqueIndex",
		refusals: []string{"Adding a unique index to an existing table is disabled"},
	},
	{
		// The upstream spelling drops the n. Off up to 25.4 and on from 26.1;
		// 25.1 answers with the second text, and takes the column once the
		// flag is on.
		Key:  capability.AddColumnWithDefault,
		Flag: "EnableAddColumsWithDefaults",
		refusals: []string{
			"Adding columns with defaults is disabled",
			"Column addition with default value is not supported now",
		},
	},
	{
		// Off on 26.1 and on from 26.2; the lines before 26.1 do not list it,
		// and their parser has no SET DEFAULT in ALTER COLUMN at all.
		Key:      capability.AlterColumnDefault,
		Flag:     "EnableSetDropDefaultValue",
		refusals: []string{"Set/drop default value is not enabled"},
	},
	{
		// Off on 25.1 and on from 25.2. The refusal names the flag itself.
		Key:      capability.WideDateTimeTypes,
		Flag:     "EnableTableDatetime64",
		refusals: []string{"EnableTableDatetime64 feature flag is off"},
	},
	{
		// Off on 25.1 and on from 25.2, like EnableTableDatetime64.
		Key:      capability.ParameterizedDecimal,
		Flag:     "EnableParameterizedDecimal",
		refusals: []string{"EnableParameterizedDecimal feature flag is off"},
	},
	{
		// On by default on every line, so it was measured the other way:
		// turned off in the startup configuration of 26.2.1.14 and 25.1.4.7,
		// and again at runtime through the dynamic configuration, ALTER TABLE
		// ... RENAME INDEX answers PRECONDITION_FAILED with this text and the
		// index keeps its name.
		Key:      capability.IndexRename,
		Flag:     "EnableMoveIndex",
		refusals: []string{"Move index is not supported yet"},
	},
	{
		// Off on 25.1 and on from 25.2. With the flag on, 25.1 takes a
		// changefeed with TOPIC_AUTO_PARTITIONING = 'ENABLED' and reads it
		// back as an auto-partitioned topic.
		Key:      capability.ChangefeedTopicAutoPartitioning,
		Flag:     "EnableTopicAutopartitioningForCDC",
		refusals: []string{"Topic autopartitioning for CDC is disabled"},
	},
	{
		// Off on every line from 25.1 to 26.2. Measured with the flag on
		// at startup on each of them: a pool and a classifier are created,
		// altered with SET and RESET, read back from .sys and dropped.
		// EnableBackupService, which gates a backup collection, is not a
		// gate: Ptah models no collection, so no key follows it.
		Key:  capability.ResourcePools,
		Flag: "EnableResourcePools",
		refusals: []string{
			"Resource pools are disabled",
			"Resource pool classifiers are disabled",
		},
	},
	{
		// Off on 25.3 through 26.1 and on from 26.2, as the captured
		// monitoring pages record. Measured on 26.2: both full-text methods
		// create indexes whose analyzer settings DescribeTable reports.
		Key:      capability.FullTextIndexes,
		Flag:     "EnableFulltextIndex",
		refusals: []string{"Fulltext index support is disabled"},
	},
	{
		// Off on 25.1 and on from 25.2. 25.1 answers a vector index in
		// CREATE TABLE and in ADD INDEX with this text, and with the flag on
		// builds it over a table holding rows and answers a search through
		// it.
		Key:      capability.VectorIndexes,
		Flag:     "EnableVectorIndex",
		refusals: []string{"Vector index support is disabled"},
	},
	{
		// Off on 25.1 and on from 25.2. With the flag on, 25.1 creates a
		// transfer, moves a topic's message into its table, describes it and
		// changes its lambda and batch settings in place.
		Key:      capability.Transfers,
		Flag:     "EnableTopicTransfer",
		refusals: []string{"Topic transfer creation is disabled"},
	},
	{
		// Listed on 26.2 only, on by default; the lines before it create a
		// replication with no flag at all, so an absent flag keeps their
		// preset. Turned off through the dynamic configuration of 26.2.1.14,
		// CREATE ASYNC REPLICATION answers PRECONDITION_FAILED with this text.
		Key:      capability.AsyncReplication,
		Flag:     "EnableReplication",
		refusals: []string{"Asynchronous replication is disabled"},
	},
	{
		// Off on 25.3 and on from 25.4; 25.1 and 25.2 do not list it, and
		// their parser has no CACHE_MODE at all (`Unknown table setting:
		// CACHE_MODE`). With the flag on, 25.3 takes a family's CACHE_MODE
		// in CREATE TABLE and through ALTER FAMILY, and reads it back.
		Key:      capability.ColumnFamilyCacheMode,
		Flag:     "EnableTableCacheModes",
		refusals: []string{"Setting cache_mode is not allowed"},
	},
	{
		// Off on 25.3 and on from 25.4; 25.1 and 25.2 do not list it, and
		// their parser has no CREATE SECRET at all. With the flag on, 25.3
		// creates, alters and drops a secret, and lists it as a SECRET entry.
		Key:      capability.Secrets,
		Flag:     "EnableSchemaSecrets",
		refusals: []string{"Secrets are disabled"},
	},
	{
		// Off on every line. With the flag on, every line creates an external
		// data source and an external table over it, and describes both.
		Key:      capability.ExternalDataSources,
		Flag:     "EnableExternalDataSources",
		refusals: []string{"External data sources are disabled"},
	},
	{
		// Off on every line. With the flag on, every line replaces an external
		// data source and an external table with CREATE OR REPLACE.
		Key:      capability.ExternalObjectReplace,
		Flag:     "EnableReplaceIfExistsForExternalEntities",
		refusals: []string{"feature flag EnableReplaceIfExistsForExternalEntities is off"},
	},
}

// Gates returns every capability a flag decides, in a fixed order.
func Gates() []Gate {
	return slices.Clone(gates)
}

// Flags is the value of every flag one database lists: the current value
// where the cluster set one, and the default otherwise.
type Flags map[string]bool

// Refine returns caps with every gated capability the flags list set from its
// flag. caps is not changed.
//
// A flag the database does not list leaves its capability as caps has it. A
// line can lack a flag because it predates the feature, and then its preset
// already says false (25.1 and 25.2 list no EnableAddUniqueIndex), or because
// the feature graduated and the flag was removed, and then turning the key off
// would narrow what Ptah writes: without wide_date_time_types a declared
// TIMESTAMP becomes the narrow Timestamp. On the seven measured pages both
// readings give the same set.
func (f Flags) Refine(caps capability.Capabilities) capability.Capabilities {
	refined := caps.Clone()
	for _, gate := range gates {
		if value, listed := f[gate.Flag]; listed {
			refined = refined.With(gate.Key, value)
		}
	}
	for _, gate := range gates {
		if gate.Requires != "" && !refined.Has(gate.Requires) {
			refined = refined.With(gate.Key, false)
		}
	}
	return refined
}

// Path is the monitoring page the flags are read from.
const Path = "/viewer/json/feature_flags"

// timeout bounds one read. The client applies it whatever the caller's
// context says, and the earlier of the two ends the read.
const timeout = 30 * time.Second

// maxBody bounds the page Ptah reads. A database lists a few hundred flags,
// about 15 KB measured.
const maxBody = 4 << 20

// maxEcho bounds how much of a refusing page an error repeats: enough for the
// endpoint's own message, and not the whole of whatever answered.
const maxEcho = 256

// client follows no redirect. The page is read from the endpoint the operator
// named, and a redirect to another host is answered as the status it is
// rather than followed somewhere Ptah was not pointed at.
var client = &http.Client{
	Timeout: timeout,
	CheckRedirect: func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	},
}

// Read asks the monitoring endpoint for the flags of database, an absolute
// path such as /local.
//
// ticket is the credential the database connection presents, and "" for an
// anonymous one. It is sent as the Authorization header, bare: measured on
// 26.2.1.14 and 25.1.4.7 with authentication enforced, the endpoint answers
// `401 Unauthorized` with no credential, reads the page for the token a
// static user's login returns, and answers `403 Forbidden` with `Token is not
// supported` for the same token behind `Bearer `. The ticket appears in no
// error Read returns, the endpoint's own answer included.
func Read(ctx context.Context, monitoring *url.URL, database, ticket string) (Flags, error) {
	if monitoring == nil {
		return nil, errors.New("no monitoring endpoint to read feature flags from")
	}
	page := *monitoring
	page.Path = Path
	page.RawQuery = url.Values{"database": {database}}.Encode()
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, page.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("read YDB feature flags: %w", err)
	}
	request.Header.Set("Accept", "application/json")
	if ticket != "" {
		request.Header.Set("Authorization", ticket)
	}
	response, err := client.Do(request)
	if err != nil {
		return nil, fmt.Errorf("read YDB feature flags from %s: %w", page.Redacted(), err)
	}
	defer response.Body.Close()
	body, err := io.ReadAll(io.LimitReader(response.Body, maxBody))
	if err != nil {
		return nil, fmt.Errorf("read YDB feature flags from %s: %w", page.Redacted(), err)
	}
	if response.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read YDB feature flags from %s: %s: %s%s",
			page.Redacted(), response.Status, echo(body, ticket), rightsHint(response.StatusCode, ticket, database))
	}
	flags, err := Decode(body, database)
	if err != nil {
		return nil, fmt.Errorf("read YDB feature flags from %s: %w", page.Redacted(), err)
	}
	return flags, nil
}

// echo is the start of a refusing page, as an error repeats it. An endpoint
// that repeats the request's headers would repeat the ticket, so the ticket is
// taken out before the page is cut.
func echo(body []byte, ticket string) string {
	text := strings.TrimSpace(string(body))
	if ticket != "" {
		text = strings.ReplaceAll(text, ticket, "<redacted>")
	}
	if len(text) <= maxEcho {
		return text
	}
	return strings.ToValidUTF8(text[:maxEcho], "") + "..."
}

// rightsHint says what a 400 means when the page was read with a credential:
// measured on 26.2.1.14, the endpoint answers `Failed to resolve database` to
// a user without DESCRIBE SCHEMA on the database, the same user reads the
// page once granted it, and anonymous reads on a cluster that does not
// enforce authentication are not checked at all.
func rightsHint(status int, ticket, database string) string {
	if status != http.StatusBadRequest || ticket == "" {
		return ""
	}
	return fmt.Sprintf("; the page is read as the connection's user, who needs DESCRIBE SCHEMA on %s", database)
}

// pageVersion is the only page layout Ptah reads; measured on 25.1.4.7
// through 26.2.1.14.
const pageVersion = 2

// page is the monitoring endpoint's answer.
type page struct {
	Version   int `json:"Version"`
	Databases []struct {
		Name         string `json:"Name"`
		FeatureFlags []struct {
			Name    string `json:"Name"`
			Default *bool  `json:"Default"`
			Current *bool  `json:"Current"`
		} `json:"FeatureFlags"`
	} `json:"Databases"`
}

// Decode reads the flags of database out of a feature-flags page. A page in
// another layout, one that does not list the database, and a flag with no
// value are refused rather than read as flags that are off.
func Decode(body []byte, database string) (Flags, error) {
	var decoded page
	if err := json.Unmarshal(body, &decoded); err != nil {
		return nil, fmt.Errorf("the feature-flags page is not the JSON Ptah reads: %w", err)
	}
	if decoded.Version != pageVersion {
		return nil, fmt.Errorf("the feature-flags page has version %d, and Ptah reads version %d",
			decoded.Version, pageVersion)
	}
	want := "/" + strings.Trim(database, "/")
	for _, listed := range decoded.Databases {
		if "/"+strings.Trim(listed.Name, "/") != want {
			continue
		}
		flags := make(Flags, len(listed.FeatureFlags))
		for _, flag := range listed.FeatureFlags {
			switch {
			case flag.Current != nil:
				flags[flag.Name] = *flag.Current
			case flag.Default != nil:
				flags[flag.Name] = *flag.Default
			default:
				return nil, fmt.Errorf("the feature-flags page lists %s with no value", flag.Name)
			}
		}
		return flags, nil
	}
	return nil, fmt.Errorf("the feature-flags page does not list database %s", want)
}

// Refused returns the gate whose flag a server refusal says is off, and false
// when the refusal is about something else.
func Refused(serverText string) (Gate, bool) {
	for _, gate := range gates {
		for _, refusal := range gate.refusals {
			if strings.Contains(serverText, refusal) {
				return gate, true
			}
		}
	}
	return Gate{}, false
}
