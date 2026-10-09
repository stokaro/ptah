// Package annotationmeta defines Ptah Go annotation directive metadata.
package annotationmeta

import (
	"slices"
	"sort"
	"strings"

	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/dialectscope"
	"ptah.run/internal/matviewrefresh"
	"ptah.run/internal/rowdeletion"
	"ptah.run/internal/ydbchangefeed"
	"ptah.run/internal/ydbcolumn"
	"ptah.run/internal/ydbexternal"
	"ptah.run/internal/ydbfamily"
	"ptah.run/internal/ydbindex"
	"ptah.run/internal/ydbpartition"
	"ptah.run/internal/ydbreplication"
	"ptah.run/internal/ydbsecret"
	"ptah.run/internal/ydbtopic"
)

// Scope describes where a directive is valid in Go source.
type Scope string

const (
	ScopeFile   Scope = "file"
	ScopeStruct Scope = "struct"
	ScopeField  Scope = "field"
)

// Attribute describes a single directive attribute.
type Attribute struct {
	Name        string `json:"name"`
	Description string `json:"description"`
	Value       string `json:"value"`
	Required    bool   `json:"required,omitempty"`
	Boolean     bool   `json:"boolean,omitempty"`
	AliasFor    string `json:"alias_for,omitempty"`
	// Sensitive marks an attribute whose VALUE is a credential. Display
	// surfaces must redact it; planning and destructive writes keep using the
	// original bytes.
	Sensitive bool `json:"sensitive,omitempty"`
	// Retired carries the reason an attribute Ptah still RECOGNIZES is refused.
	// Empty for every ordinary attribute.
	//
	// Recognizing it is the point. Deleting the entry instead would make the
	// parser answer "unknown annotation attribute", which reads as a typo and
	// says nothing about why a correctly spelled attribute is refused -- and
	// worse, it would make a bareword spelling vanish: a directive carrying
	// `refresh_strategy` with no `=value` is promoted into the key/value map
	// only while the attribute is UNKNOWN, so a deleted entry is silently
	// dropped rather than refused (stokaro/ptah#1625).
	Retired string `json:"retired,omitempty"`
}

// Directive describes one //ptah annotation directive.
type Directive struct {
	Name          string      `json:"name"`
	Description   string      `json:"description"`
	Scopes        []Scope     `json:"scopes"`
	Attributes    []Attribute `json:"attributes"`
	AllowPlatform bool        `json:"allow_platform,omitempty"`
}

// Directives returns every supported annotation directive in stable order.
func Directives() []Directive {
	out := make([]Directive, len(directives))
	copy(out, directives)
	return out
}

// Lookup returns metadata for name, without a leading // comment marker.
func Lookup(name string) (Directive, bool) {
	name = strings.TrimPrefix(strings.TrimSpace(name), "//")
	i := slices.IndexFunc(directives, func(d Directive) bool {
		return d.Name == name
	})
	if i < 0 {
		return Directive{}, false
	}
	return directives[i], true
}

// AllowsScope reports whether directive can be attached at scope.
func AllowsScope(directive Directive, scope Scope) bool {
	return slices.Contains(directive.Scopes, scope)
}

// MatchCommentDirective returns the directive matching a comment line.
func MatchCommentDirective(comment string) (Directive, bool) {
	body := strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(comment), "//"))
	for _, directive := range directivesByDescendingLength() {
		if !strings.HasPrefix(body, directive.Name) {
			continue
		}
		rest := body[len(directive.Name):]
		if rest == "" || rest[0] == ' ' || rest[0] == '\t' {
			return directive, true
		}
	}
	return Directive{}, false
}

// KnownAttributes returns a set containing every declared attribute name.
func KnownAttributes(directive string) map[string]bool {
	spec, ok := Lookup(directive)
	if !ok {
		return nil
	}
	out := make(map[string]bool, len(spec.Attributes))
	for _, attr := range spec.Attributes {
		out[attr.Name] = true
	}
	return out
}

// RequiredAttributes returns every required attribute name for directive.
func RequiredAttributes(directive string) []string {
	spec, ok := Lookup(directive)
	if !ok {
		return nil
	}
	var out []string
	for _, attr := range spec.Attributes {
		if attr.Required {
			out = append(out, attr.Name)
		}
	}
	return out
}

// AllowsAttribute reports whether key is valid for directive.
func AllowsAttribute(directive, key string) bool {
	spec, ok := Lookup(directive)
	if !ok {
		return false
	}
	if spec.AllowPlatform && IsPlatformAttribute(key) {
		return true
	}
	return slices.ContainsFunc(spec.Attributes, func(attr Attribute) bool {
		return attr.Name == key
	})
}

// PlatformAttributePattern is the JSON Schema pattern for dialect-specific
// override attributes accepted by the Go annotation parser.
const PlatformAttributePattern = `^platform\.[A-Za-z0-9_]+\.[A-Za-z0-9_]+(?:\.[A-Za-z0-9_]+)*$`

// IsPlatformAttribute reports whether key has the platform.<dialect>.<name>
// shape consumed by parseutils.ParsePlatformSpecific.
func IsPlatformAttribute(key string) bool {
	parts := strings.Split(key, ".")
	if len(parts) < 3 || parts[0] != "platform" {
		return false
	}
	for _, part := range parts[1:] {
		if !isIdentifierPart(part) {
			return false
		}
	}
	return true
}

func isIdentifierPart(part string) bool {
	if part == "" {
		return false
	}
	for _, r := range part {
		if r == '_' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			continue
		}
		return false
	}
	return true
}

// AttributeNames returns sorted attribute names for directive.
func AttributeNames(directive string) []string {
	spec, ok := Lookup(directive)
	if !ok {
		return nil
	}
	names := make([]string, 0, len(spec.Attributes))
	for _, attr := range spec.Attributes {
		names = append(names, attr.Name)
	}
	sort.Strings(names)
	return names
}

// SensitiveAttributes returns the attribute names of directive whose values are
// credentials, including alias spellings. Display surfaces use this to redact a
// value without re-deriving which attributes are secret.
func SensitiveAttributes(directive string) map[string]bool {
	found, ok := Lookup(directive)
	if !ok {
		return nil
	}
	sensitive := make(map[string]bool)
	for _, attribute := range found.Attributes {
		if attribute.Sensitive {
			sensitive[attribute.Name] = true
		}
	}
	// An alias of a sensitive attribute is just as sensitive.
	for _, attribute := range found.Attributes {
		if attribute.AliasFor != "" && sensitive[attribute.AliasFor] {
			sensitive[attribute.Name] = true
		}
	}
	if len(sensitive) == 0 {
		return nil
	}
	return sensitive
}

// AllSensitiveAttributes returns every attribute name marked Sensitive on any
// directive, including alias spellings.
//
// Redaction uses this rather than SensitiveAttributes because a redactor must be
// the widest matcher in the system, not the narrowest. It must still protect a
// sensitive attribute when the surrounding directive is malformed.
func AllSensitiveAttributes() map[string]bool {
	sensitive := make(map[string]bool)
	for _, directive := range directives {
		for _, attribute := range directive.Attributes {
			if attribute.Sensitive {
				sensitive[strings.ToLower(attribute.Name)] = true
			}
		}
	}
	for _, directive := range directives {
		for _, attribute := range directive.Attributes {
			if attribute.AliasFor != "" && sensitive[strings.ToLower(attribute.AliasFor)] {
				sensitive[strings.ToLower(attribute.Name)] = true
			}
		}
	}
	return sensitive
}

// BooleanAttributes returns the attributes accepted as bare booleans.
func BooleanAttributes() map[string]bool {
	out := make(map[string]bool)
	for _, directive := range directives {
		for _, attr := range directive.Attributes {
			if attr.Boolean {
				out[attr.Name] = true
			}
		}
	}
	return out
}

// DirectiveTokens returns every directive path segment that must not be
// auto-promoted to a bare boolean attribute by annotation parsers.
func DirectiveTokens() map[string]bool {
	out := map[string]bool{
		"ptah":   true,
		"schema": true,
	}
	for _, directive := range directives {
		for part := range strings.SplitSeq(directive.Name, ":") {
			out[part] = true
		}
	}
	return out
}

// Markdown returns concise documentation for hover cards.
func Markdown(directive Directive) string {
	var b strings.Builder
	b.WriteString("`//")
	b.WriteString(directive.Name)
	b.WriteString("`\n\n")
	b.WriteString(directive.Description)
	if len(directive.Attributes) == 0 {
		return b.String()
	}
	b.WriteString("\n\nAttributes:\n")
	for _, attr := range directive.Attributes {
		b.WriteString("- `")
		b.WriteString(attr.Name)
		b.WriteString("`")
		if attr.Required {
			b.WriteString(" required")
		}
		if attr.AliasFor != "" {
			b.WriteString(" alias for `")
			b.WriteString(attr.AliasFor)
			b.WriteString("`")
		}
		if attr.Description != "" {
			b.WriteString(": ")
			b.WriteString(attr.Description)
		}
		b.WriteString("\n")
	}
	if directive.AllowPlatform {
		b.WriteString("- `platform.<dialect>.<key>`: dialect-specific override attributes.\n")
	}
	return strings.TrimSpace(b.String())
}

func directivesByDescendingLength() []Directive {
	out := Directives()
	sort.SliceStable(out, func(i, j int) bool {
		return len(out[i].Name) > len(out[j].Name)
	})
	return out
}

const (
	valueString  = "string"
	valueBoolean = "boolean"
	valueList    = "comma-list"
	valueSQL     = "sql"
)

var directives = []Directive{
	{
		Name:          "ptah:schema:field",
		Description:   "Maps a Go struct field to a database column.",
		Scopes:        []Scope{ScopeField},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Column name.", valueString, false, false),
			attr("api_name", "Name this column carries in an exported API schema, when that differs from the column name.", valueString, false, false),
			attr("openapi_name", "Name this column carries in the OpenAPI export only, overriding api_name there.", valueString, false, false),
			attr("graphql_name", "Name this column carries in the GraphQL export only, overriding api_name there.", valueString, false, false),
			attr("proto_name", "Name this column carries in the Protobuf export only, overriding api_name there. It is the wire identity, so changing it retires a field number.", valueString, false, false),
			attr("api_type", "Type the API export projects this column as, named in Ptah's own type vocabulary.", valueString, false, false),
			attr("api_expose", "Whether this column reaches an exported API contract: none, read, write or read-write. It declares a contract shape and is not access control.", valueString, false, false),
			attr("type", "Database column type.", valueString, false, false),
			attr("not_null", "Marks the column NOT NULL.", valueBoolean, false, true),
			attr("primary", "Marks the column as part of the primary key.", valueBoolean, false, true),
			attr("auto_increment", "Marks the column as auto-incrementing.", valueBoolean, false, true),
			attr("identity_generation", "SQL identity generation mode.", valueString, false, false),
			attr("identity_start", "SQL identity start value.", valueString, false, false),
			attr("identity_increment", "SQL identity increment value.", valueString, false, false),
			attr("identity_options", "Raw SQL identity options.", valueSQL, false, false),
			attr("unique", "Adds a single-column unique constraint.", valueBoolean, false, true),
			attr("unique_expr", "Uniqueness over an expression. Not implemented: rendering refuses a column that declares it.", valueSQL, false, false),
			attr("generated", "Generated column expression.", valueSQL, false, false),
			attr("generated_kind", "Generated column kind, such as STORED or VIRTUAL.", valueString, false, false),
			attr("stored", "Shortcut controlling generated column storage.", valueBoolean, false, false),
			attr("default", "Literal column default.", valueString, false, false),
			attr("default_expr", "SQL default expression.", valueSQL, false, false),
			attr("foreign", "Foreign key reference in table(column) form.", valueString, false, false),
			attr("foreign_key_name", "Explicit foreign key constraint name.", valueString, false, false),
			attr("on_delete", "Foreign key ON DELETE action.", valueString, false, false),
			attr("on_update", "Foreign key ON UPDATE action.", valueString, false, false),
			attr("enum", "Comma-separated enum values.", valueList, false, false),
			attr("check", "Column CHECK expression.", valueSQL, false, false),
			attr("check_name", "Explicit CHECK constraint name.", valueString, false, false),
			attr("comment", "Column comment.", valueString, false, false),
		},
	},
	{
		Name:          "ptah:embedded",
		Description:   "Controls how an embedded Go field contributes schema objects.",
		Scopes:        []Scope{ScopeField},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("mode", "Embedding mode: inline, json, or relation.", valueString, false, false),
			attr("prefix", "Column prefix for inline embedded fields.", valueString, false, false),
			attr("name", "Column name for json embedding.", valueString, false, false),
			attr("type", "Column type for json embedding.", valueString, false, false),
			attr("nullable", "Marks generated embedded columns nullable.", valueBoolean, false, true),
			attr("field", "Generated relation field name.", valueString, false, false),
			attr("ref", "Relation target in table(column) form.", valueString, false, false),
			attr("on_delete", "Generated foreign key ON DELETE action.", valueString, false, false),
			attr("on_update", "Generated foreign key ON UPDATE action.", valueString, false, false),
			attr("comment", "Generated column comment.", valueString, false, false),
		},
	},
	{
		Name:          "ptah:schema:index",
		Description:   "Declares an index for a table.",
		Scopes:        []Scope{ScopeStruct, ScopeField},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Index name.", valueString, false, false),
			attr("fields", "Comma-separated Go field or column names.", valueList, false, false),
			alias("columns", "fields", "Legacy synonym for fields.", valueList, false),
			attr(
				"include",
				"Comma-separated INCLUDE columns for covering indexes (PostgreSQL: default/BTREE/GIST, plus SPGIST on 14+; "+
					"YugabyteDB: default/LSM, with BTREE as the default-LSM alias; Spanner PostgreSQL dialect: default only; "+
					"SQL Server: default/BTREE, rendered nonclustered; YDB: COVER on a global index).",
				valueList,
				false,
				false,
			),
			attr("unique", "Creates a unique index.", valueBoolean, false, true),
			attr("comment", "Index comment.", valueString, false, false),
			attr("type", "Index type or method.", valueString, false, false),
			attr("condition", "Partial index condition.", valueSQL, false, false),
			alias("where", "condition", "Atlas-style partial index condition alias.", valueSQL, false),
			attr("ops", "PostgreSQL operator class.", valueString, false, false),
			attr("table", "Explicit target table.", valueString, false, false),
			attr("key_block_size", "MySQL and MariaDB index block-size hint; zero uses the engine default.", valueString, false, false),
			attr("nulls_distinct", "Controls NULLS DISTINCT behavior where supported.", valueBoolean, false, false),
			attr("invisible", "Hides the index from the optimizer: INVISIBLE on MySQL, IGNORED on MariaDB, NOT VISIBLE on CockroachDB.",
				valueBoolean, false, true),
			attr(ydbpartition.AttributeBySize, "YDB: whether the index's table splits a partition that grows past its size, ENABLED or DISABLED.",
				valueString, false, false),
			attr(ydbpartition.AttributePartitionSizeMB, "YDB: the size in MB at which the index's table splits a partition.",
				valueString, false, false),
			attr(ydbpartition.AttributeByLoad, "YDB: whether the index's table splits a busy partition, ENABLED or DISABLED.",
				valueString, false, false),
			attr(ydbpartition.AttributeMinPartitions, "YDB: the fewest partitions the index's table keeps.", valueString, false, false),
			attr(ydbpartition.AttributeMaxPartitions, "YDB: the most partitions the index's table splits into.", valueString, false, false),
			attr(ydbpartition.AttributeReadReplicas, "YDB: the index's read replicas, PER_AZ:<n> or ANY_AZ:<n>.", valueString, false, false),
			attr("false_positive_probability", "YDB local Bloom index: false-positive probability between zero and one.", valueString, false, false),
			attr("ngram_size", "YDB local n-gram index: token length.", valueString, false, false),
			attr("case_sensitive", "YDB local n-gram index: case-sensitive matching.", valueString, false, false),
			attr("tokenizer", "YDB full-text index: tokenizer.", valueString, false, false),
			attr("language", "YDB full-text index: language.", valueString, false, false),
			attr("use_filter_lowercase", "YDB full-text index: use filter lowercase.", valueString, false, false),
			attr("use_filter_stopwords", "YDB full-text index: use filter stopwords.", valueString, false, false),
			attr("use_filter_ngram", "YDB full-text index: use filter ngram.", valueString, false, false),
			attr("use_filter_edge_ngram", "YDB full-text index: use filter edge ngram.", valueString, false, false),
			attr("filter_ngram_min_length", "YDB full-text index: filter ngram min length.", valueString, false, false),
			attr("filter_ngram_max_length", "YDB full-text index: filter ngram max length.", valueString, false, false),
			attr("use_filter_length", "YDB full-text index: use filter length.", valueString, false, false),
			attr("filter_length_min", "YDB full-text index: filter length min.", valueString, false, false),
			attr("filter_length_max", "YDB full-text index: filter length max.", valueString, false, false),
			attr("use_filter_snowball", "YDB full-text index: use filter snowball.", valueString, false, false),
			attr(ydbindex.AttributeDistance, "YDB vector index: the distance it orders by, cosine, euclidean or manhattan.",
				valueString, false, false),
			attr(ydbindex.AttributeSimilarity, "YDB vector index: the similarity it orders by, inner_product or cosine.",
				valueString, false, false),
			attr(ydbindex.AttributeVectorType, "YDB vector index: the element type, float, uint8, int8 or bit.",
				valueString, false, false),
			attr(ydbindex.AttributeVectorDimension, "YDB vector index: the number of elements in a vector, 1 to 16384.",
				valueString, false, false),
			attr(ydbindex.AttributeLevels, "YDB vector index: the depth of its k-means tree, 1 to 16.", valueString, false, false),
			attr(ydbindex.AttributeClusters, "YDB vector index: the clusters each level splits into, 2 to 2048.",
				valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:changefeed",
		Description: "Declares a YDB changefeed: a stream of a table's changes, kept in a topic at " +
			"<table>/<name>. It belongs to the struct's table, or to the table it names.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbchangefeed.AttributeName, "Changefeed name, unique among the table's changefeeds and indexes.",
				valueString, true, false),
			attr(ydbchangefeed.AttributeTable, "Table the changefeed belongs to, when not the struct's own.",
				valueString, false, false),
			attr(ydbchangefeed.AttributeMode, "What a record carries: KEYS_ONLY, UPDATES, NEW_IMAGE, OLD_IMAGE or "+
				"NEW_AND_OLD_IMAGES.", valueString, true, false),
			attr(ydbchangefeed.AttributeFormat, "How a record is written: JSON or DEBEZIUM_JSON.", valueString, true, false),
			attr(ydbchangefeed.AttributeVirtualTimestamps, "Each record carries the virtual timestamp of its change.",
				valueBoolean, false, true),
			attr(ydbchangefeed.AttributeResolvedTimestamps, "Interval of the barrier records, an ISO 8601 duration "+
				"such as PT10S.", valueString, false, false),
			attr(ydbchangefeed.AttributeInitialScan, "The stream opens with a record for every row the table holds.",
				valueBoolean, false, true),
			attr(ydbchangefeed.AttributeUserSIDs, "Each record names the user who made the change.",
				valueBoolean, false, true),
			attr(ydbchangefeed.AttributeSchemaChanges, "The stream carries a record for each schema change.",
				valueBoolean, false, true),
			attr(ydbchangefeed.AttributeTopicMinActivePartitions, "Partitions the topic starts with.",
				valueString, false, false),
			attr(ydbchangefeed.AttributeTopicAutoPartitioning, "The topic gains partitions as writes grow.",
				valueBoolean, false, true),
			attr(ydbchangefeed.AttributeRetentionPeriod, "How long the topic keeps a record, an ISO 8601 duration; "+
				"24 hours when omitted.", valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:changefeed:consumer",
		Description: "Declares a consumer of a YDB changefeed's topic: a named reader that keeps its own " +
			"position in the stream.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbchangefeed.AttributeName, "Consumer name, unique within the topic.", valueString, true, false),
			attr(ydbchangefeed.AttributeChangefeed, "Changefeed whose topic the consumer reads.", valueString, true, false),
			attr(ydbchangefeed.AttributeTable, "Table of the changefeed, when not the struct's own.",
				valueString, false, false),
			attr(ydbchangefeed.AttributeImportant, "The topic keeps a record this consumer has not read past "+
				"the retention period.", valueBoolean, false, true),
			attr(ydbchangefeed.AttributeReadFrom, "RFC 3339 time a partition this consumer has not read is read from.",
				valueString, false, false),
			attr(ydbchangefeed.AttributeSupportedCodecs, "Codecs the consumer reads: raw, gzip, lzop, zstd, custom.",
				valueList, false, false),
			attr(ydbchangefeed.AttributeAvailabilityPeriod, "How long the topic keeps a record this consumer has "+
				"not read past the retention period, an ISO 8601 duration.", valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:async_replication",
		Description: "Declares a YDB async replication: tables of another database copied into read-only replica " +
			"tables YDB creates and keeps current. Its tables are declared with ptah:schema:async_replication:item " +
			"in the same file, and the replica tables are not declared as tables.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: append(replicationNaming("Replication"), append(replicationConnection(true),
			attr(ydbreplication.AttributeConsistencyLevel, "Consistency of the replica: row or global; row when "+
				"omitted.", valueString, false, false),
			attr(ydbreplication.AttributeCommitInterval, "How often a global replication commits, an ISO 8601 "+
				"duration; ten seconds when omitted.", valueString, false, false),
		)...),
	},
	{
		Name: "ptah:schema:async_replication:item",
		Description: "Declares one table, or directory of tables, an async replication copies, and where its " +
			"replica is created. The replication is declared in the same file.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbreplication.AttributeReplication, "Replication the item belongs to.", valueString, true, false),
			attr(ydbreplication.AttributeSchema, "Directory of the replication, when it has one.",
				valueString, false, false),
			attr(ydbreplication.AttributeSource, "Path in the source database, relative to its root or absolute.",
				valueString, true, false),
			attr(ydbreplication.AttributeTarget, "Path of the replica in this database, relative to its root.",
				valueString, true, false),
		},
	},
	{
		Name: "ptah:schema:transfer",
		Description: "Declares a YDB transfer: messages of a topic turned into rows of a table through a YQL " +
			"lambda.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: append(replicationNaming("Transfer"), append(replicationConnection(false),
			attr(ydbreplication.AttributeSource, "Topic the transfer reads, relative to the database root; for a "+
				"topic of another database, relative to its root or absolute.", valueString, true, false),
			attr(ydbreplication.AttributeTarget, "Table the transfer writes, relative to the database root.",
				valueString, true, false),
			attr(ydbreplication.AttributeUsing, "The YQL lambda, written inline: ($msg) -> { ... }.",
				valueString, true, false),
			attr(ydbreplication.AttributeConsumer, "Existing topic consumer the transfer reads through; YDB "+
				"creates one when omitted.", valueString, false, false),
			attr(ydbreplication.AttributeBatchSizeBytes, "Bytes the transfer gathers before a write; 8 MiB "+
				"when omitted.", valueString, false, false),
			attr(ydbreplication.AttributeFlushInterval, "Longest wait before a write, an ISO 8601 duration of "+
				"whole seconds; a minute when omitted.", valueString, false, false),
		)...),
	},
	{
		Name: "ptah:schema:columnfamily",
		Description: "Declares a YDB column family: columns stored together, with a storage pool, compression " +
			"and cache mode of their own. It belongs to the struct's table, or to the table it names.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbfamily.AttributeName, "Family name; default is the family holding the key and every column "+
				"no other family names.", valueString, true, false),
			attr(ydbfamily.AttributeTable, "Table the family belongs to, when not the struct's own.",
				valueString, false, false),
			attr(ydbfamily.AttributeData, "Kind of storage pool the family's columns are kept in, such as ssd.",
				valueString, false, false),
			attr(ydbfamily.AttributeCompression, "Compression: off or lz4.", valueString, false, false),
			attr(ydbfamily.AttributeCacheMode, "Cache mode: regular or in_memory.", valueString, false, false),
			attr(ydbfamily.AttributeFields, "Columns the family holds. The default family lists none.",
				valueList, false, false),
		},
	},
	{
		Name:          "ptah:schema:table",
		Description:   "Maps a Go struct to a database table.",
		Scopes:        []Scope{ScopeStruct},
		AllowPlatform: true,
		Attributes: []Attribute{
			attr("name", "Table name.", valueString, false, false),
			attr("api_name", "Name this table carries in an exported API schema, when that differs from the table name.", valueString, false, false),
			attr("openapi_name", "Name this table carries in the OpenAPI export only, overriding api_name there.", valueString, false, false),
			attr("graphql_name", "Name this table carries in the GraphQL export only, overriding api_name there.", valueString, false, false),
			attr("proto_name", "Name this table carries in the Protobuf export only, overriding api_name there. It is the message identity, so changing it retires the old message.", valueString, false, false),
			attr("schema", "Database schema name.", valueString, false, false),
			attr("engine", "MySQL/MariaDB table engine shortcut.", valueString, false, false),
			attr("comment", "Table comment.", valueString, false, false),
			attr("primary_key_comment", "MySQL-family primary index comment.", valueString, false, false),
			attr("primary_key_block_size", "MySQL-family primary index block-size hint.", valueString, false, false),
			attr("primary_key", "Comma-separated primary key columns.", valueList, false, false),
			attr("checks", "Comma-separated table-level check expressions.", valueList, false, false),
			attr("depends_on", "Comma-separated tables this table must be created after.", valueList, false, false),
			attr("custom", "Raw custom CREATE TABLE SQL.", valueSQL, false, false),
			// The row deletion policy: Spanner's TTL clause and YDB's TTL
			// setting. A policy needs both the column and the interval.
			attr(rowdeletion.AttributeColumn, "Row deletion policy (Spanner and YDB TTL): the column a row's age is measured from. Needs row_deletion_interval.", valueString, false, false),
			attr(rowdeletion.AttributeInterval, "Row deletion policy: how long after the column's time a row is deleted, such as `P30D` on YDB or `30 days` on Spanner.", valueString, false, false),
			attr(rowdeletion.AttributeUnit, "YDB TTL on an integer column: what the column counts since the Unix epoch, SECONDS, MILLISECONDS, MICROSECONDS or NANOSECONDS.", valueString, false, false),
			// A YDB row table's settings, named for the settings they become,
			// as an index's partitioning is.
			attr(ydbcolumn.AttributeStore, "YDB table storage: ROW or COLUMN.", valueString, false, false),
			attr(ydbcolumn.AttributeHash, "YDB column table: hash-partitioning columns, separated by commas.", valueString, false, false),
			attr(ydbcolumn.AttributeShards, "YDB column table: initial shard count.", valueString, false, false),
			attr(ydbcolumn.AttributeTTL, "YDB column table: JSON retention policy with column, optional unit and tiers.", valueString, false, false),
			attr(ydbpartition.AttributeBySize, "YDB: whether the table splits a partition that grows past its size, ENABLED or DISABLED.",
				valueString, false, false),
			attr(ydbpartition.AttributePartitionSizeMB, "YDB: the size in MB at which the table splits a partition.",
				valueString, false, false),
			attr(ydbpartition.AttributeByLoad, "YDB: whether the table splits a busy partition, ENABLED or DISABLED.",
				valueString, false, false),
			attr(ydbpartition.AttributeMinPartitions, "YDB: the fewest partitions the table keeps.", valueString, false, false),
			attr(ydbpartition.AttributeMaxPartitions, "YDB: the most partitions the table splits into.", valueString, false, false),
			attr(ydbpartition.AttributeReadReplicas, "YDB: the table's read replicas, PER_AZ:<n> or ANY_AZ:<n>.", valueString, false, false),
			attr(ydbpartition.AttributeKeyBloomFilter, "YDB: whether the table keeps a bloom filter of its keys, ENABLED or DISABLED.",
				valueString, false, false),
			attr(ydbpartition.AttributeUniformPartitions, "YDB: the partitions a new table starts with, splitting a Uint32 or Uint64 first key evenly.",
				valueString, false, false),
			attr(ydbpartition.AttributePartitionAtKeys, "YDB: the keys a new table starts split before, such as `10, 20` or `(10, 'a'), (20)`.",
				valueString, false, false),
		},
	},
	{
		Name:        "ptah:schema:schema",
		Description: "Declares a database schema or namespace.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Schema name.", valueString, true, false),
			attr("comment", "Schema comment.", valueString, false, false),
		},
	},
	{
		Name:        "ptah:schema:constraint",
		Description: "Declares a table constraint.",
		Scopes:      []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr("name", "Constraint name.", valueString, false, false),
			attr("type", "Constraint type: CHECK, UNIQUE, PRIMARY KEY, FOREIGN KEY, or EXCLUDE.", valueString, false, false),
			attr("table", "Explicit target table.", valueString, false, false),
			attr("key_block_size", "MySQL-family primary index block-size hint.", valueString, false, false),
			attr("using", "EXCLUDE index method.", valueString, false, false),
			attr("elements", "EXCLUDE constraint elements.", valueSQL, false, false),
			attr("condition", "Constraint WHERE condition.", valueSQL, false, false),
			attr("check", "CHECK expression.", valueSQL, false, false),
			attr("columns", "Comma-separated local columns.", valueList, false, false),
			attr("include", "Comma-separated PostgreSQL INCLUDE columns for covering UNIQUE constraints.", valueList, false, false),
			attr("nulls_distinct", "Controls NULLS DISTINCT behavior where supported.", valueBoolean, false, false),
			attr("foreign_table", "Referenced table for FOREIGN KEY constraints.", valueString, false, false),
			attr("foreign_column", "Single referenced column for FOREIGN KEY constraints.", valueString, false, false),
			attr("foreign_columns", "Comma-separated referenced columns for composite FOREIGN KEY constraints.", valueList, false, false),
			attr("on_delete", "Foreign key ON DELETE action.", valueString, false, false),
			attr("on_update", "Foreign key ON UPDATE action.", valueString, false, false),
			attr("comment", "Constraint comment.", valueString, false, false),
		},
	},
	{
		Name:        "ptah:schema:enum",
		Description: "Declares a reusable enum type.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Enum type name.", valueString, true, false),
			attr("values", "Comma-separated enum values.", valueList, true, false),
			attr("comment", "Enum type comment.", valueString, false, false),
		},
	},
	{
		Name:        "ptah:schema:extension",
		Description: "Declares a PostgreSQL extension.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Extension name.", valueString, false, false),
			attr("schema", "PostgreSQL installation schema.", valueString, false, false),
			attr("if_not_exists", "Adds IF NOT EXISTS where supported.", valueBoolean, false, false),
			attr("version", "Extension version.", valueString, false, false),
			attr("comment", "Extension comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name: "ptah:schema:notdescribed",
		Description: "Declares that this description does not describe an object family, " +
			"or one named object in it, so its absence is not a removal.",
		Scopes: []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("kind", "Object family, such as extension, schema, role or sequence.", valueString, true, false),
			attr("name", "One object of that family. Omitted, the whole family is declined.", valueString, false, false),
		},
	},
	{
		Name:        "ptah:schema:function",
		Description: "Declares a database function.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Function name.", valueString, false, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("params", "Function parameter list.", valueString, false, false),
			attr("returns", "Return type.", valueString, false, false),
			attr("language", "Function language.", valueString, false, false),
			attr("security", "Security mode, such as DEFINER.", valueString, false, false),
			attr("volatility", "Volatility class.", valueString, false, false),
			attr("settings", "Routine configuration settings, `name=value`, separated by `;`; pin `search_path` on a DEFINER routine.", valueString, false, false),
			attr("leakproof", "Marks the function LEAKPROOF, letting a filter using it be pushed past a security barrier.", valueBoolean, false, false),
			attr("parallel", "Parallel level: SAFE, RESTRICTED or UNSAFE.", valueString, false, false),
			attr("strict", "Marks the function STRICT: it returns NULL without running when any argument is NULL.", valueBoolean, false, false),
			attr("body", "Function body SQL.", valueSQL, false, false),
			attr("comment", "Function comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:procedure",
		Description: "Declares a stored procedure: a routine that returns nothing and is invoked with CALL.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Procedure name.", valueString, false, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("params", "Procedure parameter list.", valueString, false, false),
			attr("language", "Procedure language.", valueString, false, false),
			attr("security", "Security mode, such as DEFINER.", valueString, false, false),
			attr("volatility", "Volatility class.", valueString, false, false),
			attr("body", "Procedure body SQL.", valueSQL, false, false),
			attr("comment", "Procedure comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:sequence",
		Description: "Declares a standalone PostgreSQL sequence.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Sequence name.", valueString, true, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("as", "Underlying integer type, such as bigint.", valueString, false, false),
			attr("start", "START WITH value.", valueString, false, false),
			attr("increment", "INCREMENT BY value; must be non-zero.", valueString, false, false),
			attr("minvalue", "MINVALUE bound.", valueString, false, false),
			attr("maxvalue", "MAXVALUE bound.", valueString, false, false),
			attr("cache", "CACHE size.", valueString, false, false),
			attr("cycle", "Enables CYCLE wrap-around.", valueBoolean, false, false),
			attr("owned_by", "Owning table.column association (OWNED BY).", valueString, false, false),
			attr("if_not_exists", "Adds IF NOT EXISTS where supported.", valueBoolean, false, false),
			attr("comment", "Sequence comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:domain",
		Description: "Declares a PostgreSQL domain type.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Domain name.", valueString, true, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("type", "Underlying base data type.", valueString, true, false),
			attr("not_null", "Marks the domain NOT NULL.", valueBoolean, false, false),
			attr("default", "Literal DEFAULT value.", valueString, false, false),
			attr("default_expr", "DEFAULT expression.", valueSQL, false, false),
			attr("check", "CHECK constraint expression (uses VALUE).", valueSQL, false, false),
			attr("comment", "Domain comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:composite",
		Description: "Declares a PostgreSQL composite type.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Composite type name.", valueString, true, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("fields", "Comma-separated name:type field list.", valueString, true, false),
			attr("comment", "Composite type comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:range",
		Description: "Declares a PostgreSQL range type.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Range type name.", valueString, true, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("subtype", "Element subtype the range is built over.", valueString, true, false),
			attr("subtype_opclass", "Operator class for the subtype.", valueString, false, false),
			attr("collation", "Collation for the subtype.", valueString, false, false),
			attr("canonical", "Canonicalization function.", valueString, false, false),
			attr("subtype_diff", "Subtype difference function.", valueString, false, false),
			attr("comment", "Range type comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:view",
		Description: "Declares a database view.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "View name.", valueString, true, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("body", "View SELECT body.", valueSQL, true, false),
			attr("with_check", "Controls WITH CHECK OPTION where supported.", valueBoolean, false, false),
			attr("depends_on", "Comma-separated objects this view must be created after.", valueList, false, false),
			attr("comment", "View comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:matview",
		Description: "Declares a materialized view.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Materialized view name.", valueString, true, false),
			attr("schema", "Target schema/namespace.", valueString, false, false),
			attr("body", "Materialized view SELECT body.", valueSQL, true, false),
			attr("refresh", "ClickHouse refresh schedule, as ClickHouse spells it: "+
				"`every 1 hour`, `after 30 minute`, `every 1 day offset 2 hour`. "+
				"Omitted leaves the view maintained by inserts into its source.",
				valueString, false, false),
			attr("depends_on", "Comma-separated objects this view must be created after.", valueList, false, false),
			retiredAttr("refresh_strategy",
				"Retired: refused when the annotation is parsed, on every dialect.",
				matviewrefresh.Reason),
			attr("comment", "Materialized view comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name: "ptah:schema:continuousaggregate",
		Description: "Declares a TimescaleDB continuous aggregate: a materialized view over a " +
			"hypertable the extension keeps up to date.",
		Scopes: []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Aggregate name, which is also the view name.", valueString, true, false),
			attr("schema", "Schema holding the aggregate.", valueString, false, false),
			attr("body", "The SELECT the aggregate materializes.", valueString, true, false),
			attr("materialized_only", "Read only materialized data, rather than combining it with "+
				"the rows since the last refresh.", valueBoolean, false, false),
			attr("comment", "Continuous aggregate comment.", valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:hypertable",
		Description: "Declares a TimescaleDB hypertable: a table partitioned on a range " +
			"dimension.",
		Scopes: []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("table", "Table to partition, optionally schema-qualified.", valueString, true, false),
			attr("column", "Range dimension: the column chunks are cut on.", valueString, true, false),
			attr("chunk_interval", "Width of one chunk, spelled the way PostgreSQL spells an "+
				"interval. Omit to take TimescaleDB's default.", valueString, false, false),
			attr("if_not_exists", "Skip a table that is already a hypertable instead of failing.",
				valueBoolean, false, false),
			attr("comment", "Hypertable comment.", valueString, false, false),
		},
	},
	{
		Name:        "ptah:schema:synonym",
		Description: "Declares a SQL Server synonym, an alias for another object.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Synonym name, the alias being declared.", valueString, true, false),
			attr("schema", "Schema the alias lives in.", valueString, false, false),
			attr("target", "Object the alias stands for, as one to four dot-separated parts.", valueString, true, false),
			attr("comment", "Synonym comment.", valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:coordinationnode",
		Description: "Declares a YDB coordination node, which holds an application's semaphores " +
			"and rate limiter resources. A setting left out takes YDB's default.",
		Scopes: []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Node name.", valueString, true, false),
			attr("schema", "Directory holding the node, relative to the database root.", valueString, false, false),
			attr(ydbcoordination.SettingSelfCheckPeriod, "How often the node checks it is alive, as an ISO 8601 "+
				"duration from `PT0.5S` to `PT10S`. YDB's default is `PT1S`.", valueString, false, false),
			attr(ydbcoordination.SettingSessionGracePeriod, "How long a session keeps its semaphores while the "+
				"node changes its leader, as an ISO 8601 duration from the self-check period plus one second "+
				"to `PT30S`. YDB's default is `PT10S`.", valueString, false, false),
			attr(ydbcoordination.SettingReadConsistencyMode, "`strict` or `relaxed`. YDB's default is `relaxed`.",
				valueString, false, false),
			attr(ydbcoordination.SettingAttachConsistencyMode, "`strict` or `relaxed`. YDB's default is `strict`.",
				valueString, false, false),
			attr(ydbcoordination.SettingRateLimiterCountersMode, "`aggregated` or `detailed`. YDB's default is "+
				"`aggregated`.", valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:topic",
		Description: "Declares a YDB topic: a persistent message queue at a path of the scheme tree. " +
			"Its consumers are declared with ptah:schema:topic:consumer in the same file.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbtopic.AttributeName, "Topic name, the last segment of its path.", valueString, true, false),
			attr(ydbtopic.AttributeSchema, "Directory that holds the topic, relative to the database root.",
				valueString, false, false),
			attr(ydbtopic.AttributeMinActivePartitions, "Partitions writers write to; the count only grows.",
				valueString, false, false),
			attr(ydbtopic.AttributeMaxActivePartitions, "Most partitions auto-partitioning splits the topic into; "+
				"needs a strategy other than disabled.", valueString, false, false),
			attr(ydbtopic.AttributeStrategy, "Auto-partitioning: disabled, scale_up, scale_up_and_down or paused.",
				valueString, false, false),
			attr(ydbtopic.AttributeUpUtilizationPercent, "Share of a partition's write speed above which it "+
				"splits, 1 to 100.", valueString, false, false),
			attr(ydbtopic.AttributeDownUtilizationPercent, "Share of a partition's write speed below which "+
				"partitions merge, 1 to 100.", valueString, false, false),
			attr(ydbtopic.AttributeStabilizationWindow, "How long a load lasts before the partition count follows "+
				"it, an ISO 8601 duration.", valueString, false, false),
			attr(ydbtopic.AttributeRetentionPeriod, "How long the topic keeps a message, an ISO 8601 duration; "+
				"24 hours when omitted.", valueString, false, false),
			attr(ydbtopic.AttributeWriteSpeed, "Write quota of one partition in bytes per second.",
				valueString, false, false),
			attr(ydbtopic.AttributeWriteBurst, "Burst a partition takes above its quota, in bytes; the write "+
				"speed when omitted.", valueString, false, false),
			attr(ydbtopic.AttributeSupportedCodecs, "Codecs a writer may use: raw, gzip, lzop, zstd, custom.",
				valueList, false, false),
		},
	},
	{
		Name: "ptah:schema:topic:consumer",
		Description: "Declares a consumer of a YDB topic: a named reader that keeps its own position in " +
			"the topic. The topic is declared in the same file.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbtopic.AttributeName, "Consumer name, unique within the topic.", valueString, true, false),
			attr(ydbtopic.AttributeTopic, "Topic the consumer reads.", valueString, true, false),
			attr(ydbtopic.AttributeSchema, "Directory of the topic, when it has one.", valueString, false, false),
			attr(ydbtopic.AttributeImportant, "The topic keeps a message this consumer has not read past "+
				"the retention period.", valueBoolean, false, true),
			attr(ydbtopic.AttributeReadFrom, "RFC 3339 time a partition this consumer has not read is read from.",
				valueString, false, false),
			attr(ydbtopic.AttributeSupportedCodecs, "Codecs the consumer reads: raw, gzip, lzop, zstd, custom.",
				valueList, false, false),
			attr(ydbtopic.AttributeAvailabilityPeriod, "How long the topic keeps a message this consumer has "+
				"not read past the retention period, an ISO 8601 duration.", valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:resourcepool",
		Description: "Declares a YDB resource pool, which limits the queries that run in it. It belongs to " +
			"the whole database; a setting left out has no limit.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbworkload.AttributeName, "Pool name; `default` changes the pool YDB creates.", valueString, true, false),
			attr(ydbworkload.AttributeConcurrentQueryLimit, "Most queries that run at once.", valueString, false, false),
			attr(ydbworkload.AttributeQueueSize, "Most queries that wait; needs concurrent_query_limit or "+
				"database_load_cpu_threshold.", valueString, false, false),
			attr(ydbworkload.AttributeDatabaseLoadCPUThreshold, "Database CPU load in percent above which new "+
				"queries wait.", valueString, false, false),
			attr(ydbworkload.AttributeQueryMemoryLimitPercentPerNode, "Share of a node's memory one query may take.",
				valueString, false, false),
			attr(ydbworkload.AttributeQueryCPULimitPercentPerNode, "Share of a node's CPU one query may take.",
				valueString, false, false),
			attr(ydbworkload.AttributeTotalCPULimitPercentPerNode, "Share of a node's CPU the pool's queries may "+
				"take together.", valueString, false, false),
			attr(ydbworkload.AttributeResourceWeight, "The pool's share of the CPU when pools compete for it.",
				valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:resourcepool:classifier",
		Description: "Declares a YDB resource pool classifier, which sends a user's or a group's queries " +
			"to a resource pool. It belongs to the whole database.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbworkload.AttributeName, "Classifier name.", valueString, true, false),
			attr(ydbworkload.AttributeResourcePool, "Pool the classifier sends queries to: a declared pool or "+
				"`default`.", valueString, true, false),
			attr(ydbworkload.AttributeMemberName, "User or group whose queries the classifier matches; every "+
				"query when omitted.", valueString, false, false),
			attr(ydbworkload.AttributeRank, "Order among the classifiers, lowest first; unique, from 0.",
				valueString, true, false),
		},
	},
	{
		Name: "ptah:schema:secret",
		Description: "Declares a YDB secret: a scheme object whose value the server keeps and never returns. " +
			"The value comes from an environment variable when the statement that creates the secret runs, " +
			"and a declaration never writes it.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbsecret.AttributeName, "Secret name, the last segment of its path.", valueString, true, false),
			attr(ydbsecret.AttributeSchema, "Directory that holds the secret, relative to the database root.",
				valueString, false, false),
			attr(ydbsecret.AttributeValueEnv, "Environment variable that holds the value; its name starts with "+
				ydbsecret.ValuePrefix+".", valueString, true, false),
		},
	},
	{
		Name:        "ptah:schema:streamingquery",
		Description: "Declares a YDB query that continuously processes topic messages.",
		Scopes:      []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr("name", "Query name, the final path segment.", valueString, true, false),
			attr("schema", "Database-relative directory.", valueString, false, false),
			attr("text", "YQL body inside DO BEGIN ... END DO.", valueSQL, true, false),
			attr("run", "Start the query; omitted means true.", valueBoolean, false, false),
			attr("resource_pool", "Execution pool; omitted means default.", valueString, false, false),
			attr("allow_state_reset", "Permit a changed body to reset aggregation state while retaining topic offsets.", valueBoolean, false, false),
		},
	},
	{
		Name: "ptah:schema:externaldatasource",
		Description: "Declares a YDB external data source: another system YDB reads from, such as an object " +
			"storage bucket or a PostgreSQL database, and how YDB authenticates to it. A credential is named " +
			"by the secret that holds it, never written.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbexternal.AttributeName, "Data source name, the last segment of its path.", valueString, true, false),
			attr(ydbexternal.AttributeSchema, "Directory that holds the data source, relative to the database root.",
				valueString, false, false),
			attr(ydbexternal.AttributeSourceType, "SOURCE_TYPE, such as ObjectStorage, PostgreSQL or ClickHouse.",
				valueString, true, false),
			attr(ydbexternal.AttributeLocation, "LOCATION: the bucket's address or the server's host and port.",
				valueString, false, false),
			attr(ydbexternal.AttributeAuthMethod, "AUTH_METHOD, such as NONE, BASIC or SERVICE_ACCOUNT.",
				valueString, true, false),
			attr(ydbexternal.AttributeOptions, "Every other option, `NAME=value` separated by `;`, such as "+
				"`DATABASE_NAME=app;LOGIN=reader;PASSWORD_SECRET_PATH=ext/pg_password`.", valueString, false, false),
		},
	},
	{
		Name: "ptah:schema:externaltable",
		Description: "Declares a YDB external table: columns over files that an external data source of " +
			"type ObjectStorage holds. YDB stores no row of it.",
		Scopes: []Scope{ScopeStruct, ScopeField},
		Attributes: []Attribute{
			attr(ydbexternal.AttributeName, "External table name, the last segment of its path.", valueString, true, false),
			attr(ydbexternal.AttributeSchema, "Directory that holds the external table, relative to the database root.",
				valueString, false, false),
			attr(ydbexternal.AttributeDataSource, "Path of the data source the table reads, relative to the "+
				"database root.", valueString, true, false),
			attr(ydbexternal.AttributeLocation, "LOCATION: the files' path under the data source.", valueString, true, false),
			attr(ydbexternal.AttributeColumns, "Columns, `name Type [NOT NULL]` separated by commas, such as "+
				"`id Int64 NOT NULL, amount Decimal(22,9)`.", valueString, true, false),
			attr(ydbexternal.AttributeOptions, "Every other option, `NAME=value` separated by `;`, such as "+
				"`FORMAT=json_each_row;COMPRESSION=gzip`.", valueString, false, false),
		},
	},
	{
		Name:        "ptah:schema:extendedproperty",
		Description: "Declares a SQL Server extended property on a schema, a table, or a column.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Property name. MS_Description is refused: it is the object comment, "+
				"which Ptah already manages.", valueString, true, false),
			attr("schema", "Schema the property is on, or that owns the addressed table. "+
				"Omit for a database-scoped property.", valueString, false, false),
			attr("table", "Table the property is on. Requires schema; omit for a schema-scoped "+
				"property.", valueString, false, false),
			attr("column", "Column the property is on. Requires table.", valueString, false, false),
			attr("value", "The value, written back as an N'' literal.", valueString, true, false),
			attr("comment", "Extended property comment.", valueString, false, false),
		},
	},
	{
		Name:        "ptah:schema:trigger",
		Description: "Declares a database trigger.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Trigger name.", valueString, true, false),
			attr("table", "Target table.", valueString, true, false),
			attr("timing", "Trigger timing, such as BEFORE or AFTER.", valueString, true, false),
			attr("event", "Trigger event, such as INSERT or UPDATE.", valueString, true, false),
			attr("for", "Trigger granularity; defaults to ROW.", valueString, false, false),
			attr("when", "WHEN condition; the trigger fires only where it is true. PostgreSQL family.", valueSQL, false, false),
			attr("old_table", "Transition table holding the rows before the change "+
				"(REFERENCING OLD TABLE). PostgreSQL family.", valueString, false, false),
			attr("new_table", "Transition table holding the rows after the change "+
				"(REFERENCING NEW TABLE). PostgreSQL family.", valueString, false, false),
			attr("body", "Trigger body SQL.", valueSQL, true, false),
			attr("comment", "Trigger comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:rls:policy",
		Description: "Declares a row-level security policy.",
		Scopes:      []Scope{ScopeFile, ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Policy name.", valueString, false, false),
			attr("table", "Target table.", valueString, false, false),
			attr("for", "Policy command, such as ALL or SELECT.", valueString, false, false),
			attr("to", "Comma-separated roles; omitted means PUBLIC on PostgreSQL.", valueList, false, false),
			attr("using", "USING expression.", valueSQL, false, false),
			attr("with_check", "WITH CHECK expression.", valueSQL, false, false),
			attr("as", "PERMISSIVE (the default) or RESTRICTIVE.", valueString, false, false),
			attr("comment", "Policy comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:rls:enable",
		Description: "Enables row-level security on a table.",
		Scopes:      []Scope{ScopeFile, ScopeStruct},
		Attributes: []Attribute{
			attr("table", "Target table.", valueString, false, false),
			attr("force", "Apply the table's policies to its owner too.", valueBoolean, false, false),
			attr("comment", "RLS enablement comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:role",
		Description: "Declares a database role.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("name", "Role name.", valueString, false, false),
			attr("login", "Creates the role with LOGIN.", valueBoolean, false, false),
			sensitiveAttr("password", "Role password.", valueString, false, false),
			attr("superuser", "Creates the role as SUPERUSER.", valueBoolean, false, false),
			attr("createdb", "Allows database creation.", valueBoolean, false, false),
			alias("create_db", "createdb", "Alias for createdb.", valueBoolean, false),
			attr("createrole", "Allows role creation.", valueBoolean, false, false),
			alias("create_role", "createrole", "Alias for createrole.", valueBoolean, false),
			attr("inherit", "Controls role inheritance; defaults to true.", valueBoolean, false, false),
			attr("replication", "Allows replication.", valueBoolean, false, false),
			attr("comment", "Role comment.", valueString, false, false),
			attr("group", "Declares a group, which never logs in and has members. YDB only.",
				valueBoolean, false, false),
			attr("member_of", "Comma-separated groups the role is a member of. YDB only.", valueList, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:grant",
		Description: "Declares database grants.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("role", "Target role.", valueString, false, false),
			attr("privilege", "Privilege or comma-separated privileges.", valueList, false, false),
			alias("privileges", "privilege", "Alias for privilege.", valueList, false),
			attr("on_table", "Target table.", valueString, false, false),
			attr("on_schema", "Target schema.", valueString, false, false),
			attr("on_sequence", "Target sequence.", valueString, false, false),
			attr("on_database", "Targets the database itself. YDB only.", valueBoolean, false, false),
			attr("on_function", "Target function with its argument types, such as purge(uuid). PostgreSQL only.",
				valueString, false, false),
			attr("on_procedure", "Target procedure with its argument types, such as archive(uuid). PostgreSQL only.",
				valueString, false, false),
			attr("columns", "Comma-separated columns of on_table the privileges are limited to, such as "+
				"state,decided_at. PostgreSQL only.", valueList, false, false),
			attr("with_option", "Adds WITH GRANT OPTION where supported.", valueBoolean, false, false),
			alias("grant_option", "with_option", "Alias for with_option.", valueBoolean, false),
			attr("comment", "Grant comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name: "ptah:schema:revoke",
		Description: "Declares privileges a role does not hold, including ones it holds without a grant: " +
			"PUBLIC's EXECUTE on a new function, or what ALTER DEFAULT PRIVILEGES gave a role on a new table.",
		Scopes: []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("role", "Role the privileges are taken from; PUBLIC names every role.", valueString, true, false),
			attr("privilege", "Privilege or comma-separated privileges.", valueList, false, false),
			alias("privileges", "privilege", "Alias for privilege.", valueList, false),
			attr("on_table", "Target table.", valueString, false, false),
			attr("on_schema", "Target schema.", valueString, false, false),
			attr("on_sequence", "Target sequence.", valueString, false, false),
			attr("on_database", "Targets the database itself. YDB only.", valueBoolean, false, false),
			attr("on_function", "Target function with its argument types, such as purge(uuid). PostgreSQL only.",
				valueString, false, false),
			attr("on_procedure", "Target procedure with its argument types, such as archive(uuid). PostgreSQL only.",
				valueString, false, false),
			attr("columns", "Comma-separated columns of on_table the privileges are limited to, such as "+
				"state,decided_at. PostgreSQL only.", valueList, false, false),
			attr("comment", "Comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name: "ptah:schema:defaultprivilege",
		Description: "Declares a PostgreSQL default privilege: what a grantee receives on objects " +
			"a named role creates, in a named schema or, without one, in every schema of the database.",
		Scopes: []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("for_role", "Role whose newly created objects the privileges apply to. "+
				"PostgreSQL refuses the statement from a non-member of this role, so it is "+
				"part of the object's identity rather than a decoration.", valueString, true, false),
			attr("schema", "Schema the default applies in. Left out, the declaration is the global "+
				"default, ALTER DEFAULT PRIVILEGES without IN SCHEMA, which starts from the built-in "+
				"default: revoked can take a built-in privilege away, such as EXECUTE from PUBLIC.",
				valueString, false, false),
			attr("object_type", "TABLES, SEQUENCES, FUNCTIONS or TYPES; without a schema also SCHEMAS "+
				"or LARGE OBJECTS.", valueString, true, false),
			attr("grantee", "Role receiving the privileges; PUBLIC names every role.", valueString, true, false),
			attr("privileges", "Comma-separated privileges, such as SELECT,INSERT. Required unless revoked is set.",
				valueList, false, false),
			attr("grantable", "The subset of privileges carrying WITH GRANT OPTION. A name "+
				"outside privileges is refused.", valueList, false, false),
			attr("revoked", "Comma-separated privileges the grantee must not hold by default, such as "+
				"INSERT,UPDATE; ALL names every privilege of the object type. A name also in privileges is refused.",
				valueList, false, false),
			attr("comment", "Default privilege comment.", valueString, false, false),
			dialectsAttr(),
		},
	},
	{
		Name:        "ptah:schema:data",
		Description: "Declares external reference/seed row data for a table.",
		Scopes:      []Scope{ScopeStruct},
		Attributes: []Attribute{
			attr("table", "Target table the rows belong to.", valueString, true, false),
			attr("schema", "Database schema the table belongs to.", valueString, false, false),
			attr("key", "Comma-separated key column(s) forming each row's identity.", valueList, true, false),
			attr("file", "Path to the YAML row-data file, relative to the Go source file.", valueString, true, false),
		},
	},
}

// dialectsAttr is the `dialects=` scope every directive declaring a standalone
// schema object accepts.
//
// It is one constructor rather than thirteen literals because the spelling, the
// value kind and the description are the contract: a directive that described
// the same attribute differently would document a second, imaginary feature,
// and the JSON Schema generated from this table would disagree with itself.
//
// Directives that describe TABLE STRUCTURE -- table, field, index, constraint,
// embedded, enum, schema -- deliberately do not take it. Omitting a column or
// the table that holds it raises a question a scope cannot answer on its own:
// what happens to the objects that reference what left. Those are their own
// design decision, not a line in this table.
func dialectsAttr() Attribute {
	return attr(
		dialectscope.Attribute,
		"Comma-separated target dialects this object belongs to. Omitted means "+
			"every dialect; a target the scope excludes leaves the object out of "+
			"its desired state entirely, so nothing compares or plans it there.",
		valueList,
		false,
		false,
	)
}

func attr(name, description, value string, required, boolean bool) Attribute {
	return Attribute{
		Name:        name,
		Description: description,
		Value:       value,
		Required:    required,
		Boolean:     boolean,
	}
}

// retiredAttr is attr for an attribute Ptah recognizes and refuses.
//
// It keeps AllowsAttribute answering true, which is what routes a bareword
// spelling into the key/value map and therefore into the refusal, and it
// carries the reason so every surface can quote one wording.
func retiredAttr(name, description, reason string) Attribute {
	a := attr(name, description, valueString, false, false)
	a.Retired = reason
	return a
}

// RetiredAttribute reports the reason a recognized attribute is refused, and
// whether it is retired at all.
func RetiredAttribute(directive, key string) (string, bool) {
	spec, ok := Lookup(directive)
	if !ok {
		return "", false
	}
	for _, attribute := range spec.Attributes {
		if attribute.Name == key && attribute.Retired != "" {
			return attribute.Retired, true
		}
	}
	return "", false
}

// sensitiveAttr is attr for an attribute carrying a credential.
func sensitiveAttr(name, description, value string, required, boolean bool) Attribute {
	a := attr(name, description, value, required, boolean)
	a.Sensitive = true
	return a
}

func alias(name, aliasFor, description, value string, boolean bool) Attribute {
	a := attr(name, description, value, false, boolean)
	a.AliasFor = aliasFor
	return a
}

// replicationNaming is the name and the directory of an async replication or
// a transfer.
func replicationNaming(kind string) []Attribute {
	return []Attribute{
		attr(ydbreplication.AttributeName, kind+" name, the last segment of its path.", valueString, true, false),
		attr(ydbreplication.AttributeSchema, "Directory that holds it, relative to the database root.",
			valueString, false, false),
	}
}

// replicationConnection is how an async replication or a transfer reaches
// another database: the connection string and a credential, named by the
// secret that holds it. A replication always reads another database, so its
// connection string is required; a transfer reads its own without one.
func replicationConnection(required bool) []Attribute {
	return []Attribute{
		attr(ydbreplication.AttributeConnectionString, "The other database: grpc://host:port/?database=/path or "+
			"grpcs://...", valueString, required, false),
		attr(ydbreplication.AttributeTokenSecretName, "Object secret holding an access token.",
			valueString, false, false),
		attr(ydbreplication.AttributeTokenSecretPath, "Path of a secret holding an access token, relative to "+
			"the database root (YDB 25.4 and later).", valueString, false, false),
		attr(ydbreplication.AttributeUser, "User a password secret signs in as.", valueString, false, false),
		attr(ydbreplication.AttributePasswordSecretName, "Object secret holding the user's password.",
			valueString, false, false),
		attr(ydbreplication.AttributePasswordSecretPath, "Path of a secret holding the user's password, "+
			"relative to the database root (YDB 25.4 and later).", valueString, false, false),
	}
}
