package ydbsource

import (
	"slices"

	"ptah.run/core/annotation"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/ydbchangefeed"
	"ptah.run/internal/ydbfamily"
)

// The value shapes an attribute names, for documentation and the editor.
const (
	valueString  = "string"
	valueBoolean = "boolean"
	valueList    = "comma-list"
	valueSQL     = "sql"
)

// directives describes the YDB directives of the Go annotation frontend: a
// standalone object each, or a table's changefeed or column family, and the
// consumers and items those declare in the same file. None takes a dialect
// attribute: each declares a YDB object and nothing else, and every other
// target refuses one.
func directives() []annotation.Directive {
	return slices.Concat(tableDirectives(), replicationDirectives(), topicDirectives(), workloadDirectives(),
		externalDirectives())
}

// tableDirectives declare a table's changefeeds and column families, and
// the consumers of a changefeed's topic.
func tableDirectives() []annotation.Directive {
	return []annotation.Directive{
		{
			Name: "ptah:schema:changefeed",
			Description: "Declares a YDB changefeed: a stream of a table's changes, kept in a topic at " +
				"<table>/<name>. It belongs to the struct's table, or to the table it names.",
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
			Name: "ptah:schema:columnfamily",
			Description: "Declares a YDB column family: columns stored together, with a storage pool, compression " +
				"and cache mode of their own. It belongs to the struct's table, or to the table it names.",
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
	}
}

// replicationDirectives declare async replications, their items and
// transfers.
func replicationDirectives() []annotation.Directive {
	return []annotation.Directive{
		{
			Name: "ptah:schema:async_replication",
			Description: "Declares a YDB async replication: tables of another database copied into read-only replica " +
				"tables YDB creates and keeps current. Its tables are declared with ptah:schema:async_replication:item " +
				"in the same file, and the replica tables are not declared as tables.",
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
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
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
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
	}
}

// topicDirectives declare topics and their consumers.
func topicDirectives() []annotation.Directive {
	return []annotation.Directive{
		{
			Name: "ptah:schema:topic",
			Description: "Declares a YDB topic: a persistent message queue at a path of the scheme tree. " +
				"Its consumers are declared with ptah:schema:topic:consumer in the same file.",
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
	}
}

// workloadDirectives declare coordination nodes, resource pools, their
// classifiers and streaming queries.
func workloadDirectives() []annotation.Directive {
	return []annotation.Directive{
		{
			Name: "ptah:schema:coordinationnode",
			Description: "Declares a YDB coordination node, which holds an application's semaphores " +
				"and rate limiter resources. A setting left out takes YDB's default.",
			Scopes: []annotation.Scope{annotation.ScopeStruct},
			Attributes: []annotation.Attribute{
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
			Name: "ptah:schema:resourcepool",
			Description: "Declares a YDB resource pool, which limits the queries that run in it. It belongs to " +
				"the whole database; a setting left out has no limit.",
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
			Name:        "ptah:schema:streamingquery",
			Description: "Declares a YDB query that continuously processes topic messages.",
			Scopes:      []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
				attr("name", "Query name, the final path segment.", valueString, true, false),
				attr("schema", "Database-relative directory.", valueString, false, false),
				attr("text", "YQL body inside DO BEGIN ... END DO.", valueSQL, true, false),
				attr("run", "Start the query; omitted means true.", valueBoolean, false, false),
				attr("resource_pool", "Execution pool; omitted means default.", valueString, false, false),
				attr("allow_state_reset", "Permit a changed body to reset aggregation state while retaining topic offsets.", valueBoolean, false, false),
			},
		},
	}
}

// externalDirectives declare secrets and the external objects that read
// credentials from them.
func externalDirectives() []annotation.Directive {
	return []annotation.Directive{
		{
			Name: "ptah:schema:secret",
			Description: "Declares a YDB secret: a scheme object whose value the server keeps and never returns. " +
				"The value comes from an environment variable when the statement that creates the secret runs, " +
				"and a declaration never writes it.",
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
				attr(ydbsecret.AttributeName, "Secret name, the last segment of its path.", valueString, true, false),
				attr(ydbsecret.AttributeSchema, "Directory that holds the secret, relative to the database root.",
					valueString, false, false),
				attr(ydbsecret.AttributeValueEnv, "Environment variable that holds the value; its name starts with "+
					ydbsecret.ValuePrefix+".", valueString, true, false),
				// The value itself is recognized so that writing it is refused
				// with the reason, which names the attribute and never the value.
				retired(ydbsecret.AttributeValue, "Refused when the annotation is parsed:", ydbsecret.LiteralValueRefusal),
			},
		},

		{
			Name: "ptah:schema:externaldatasource",
			Description: "Declares a YDB external data source: another system YDB reads from, such as an object " +
				"storage bucket or a PostgreSQL database, and how YDB authenticates to it. A credential is named " +
				"by the secret that holds it, never written.",
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
			Scopes: []annotation.Scope{annotation.ScopeStruct, annotation.ScopeField},
			Attributes: []annotation.Attribute{
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
	}
}

func attr(name, description, value string, required, boolean bool) annotation.Attribute {
	return annotation.Attribute{Name: name, Description: description, Value: value, Required: required, Boolean: boolean}
}

// retired is an attribute the frontend recognizes and refuses with reason.
func retired(name, description, reason string) annotation.Attribute {
	attribute := attr(name, description, valueString, false, false)
	attribute.Retired = reason
	return attribute
}

// replicationNaming is the name and the directory of an async replication or
// a transfer.
func replicationNaming(kind string) []annotation.Attribute {
	return []annotation.Attribute{
		attr(ydbreplication.AttributeName, kind+" name, the last segment of its path.", valueString, true, false),
		attr(ydbreplication.AttributeSchema, "Directory that holds it, relative to the database root.",
			valueString, false, false),
	}
}

// replicationConnection is how an async replication or a transfer reaches
// another database: the connection string and a credential, named by the
// secret that holds it. A replication always reads another database, so its
// connection string is required; a transfer reads its own without one.
func replicationConnection(required bool) []annotation.Attribute {
	return []annotation.Attribute{
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
