package ydbsource

import (
	"cmp"
	"fmt"
	"maps"
	"slices"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/core/yamlext"
	"ptah.run/dialect/ydb/ydbcoordination"
	"ptah.run/dialect/ydb/ydbexternal"
	"ptah.run/dialect/ydb/ydbreplication"
	"ptah.run/dialect/ydb/ydbschema"
	"ptah.run/dialect/ydb/ydbsecret"
	"ptah.run/dialect/ydb/ydbstreaming"
	"ptah.run/dialect/ydb/ydbtopic"
	"ptah.run/dialect/ydb/ydbworkload"
	"ptah.run/internal/yamlvalue"
	"ptah.run/internal/ydbchangefeed"
	"ptah.run/internal/ydbfamily"
	"ptah.run/internal/ydbpartition"
	"ptah.run/internal/ydbttl"
)

// YAML is the YDB owner's contribution to the YAML frontend: the top-level
// keys of its standalone objects; the keys a table declares its changefeeds,
// column families, column storage and partitioning with, and the ones an
// index declares its partitioning, vector settings and full-text or local
// options with, read by the same decoders as the Go annotation attributes;
// and the claim that a YAML document describes each of them, and a table's
// TTL, completely.
func YAML() yamlext.Extension {
	return yamlext.Extension{
		Owner: ydbschema.Owner,
		Kinds: Annotations().Kinds,
		Sections: []yamlext.Section{
			{Key: "topics", Decode: decodeTopics},
			{Key: "resource_pools", Decode: decodePools},
			{Key: "resource_pool_classifiers", Decode: decodeClassifiers},
			{Key: "async_replications", Decode: decodeReplications},
			{Key: "transfers", Decode: decodeTransfers},
			{Key: "coordination_nodes", Decode: decodeCoordinationNodes},
			{Key: "secrets", Decode: decodeSecrets},
			{Key: "streaming_queries", Decode: decodeStreamingQueries},
			{Key: "external_data_sources", Decode: decodeExternalDataSources},
			{Key: "external_tables", Decode: decodeExternalTables},
		},
		EntryAttributes: []yamlext.EntryAttributes{
			{Entry: yamlext.EntryTable, Attributes: ydbpartition.TableAttributes(), Decode: decodeTablePartitioning},
			{Entry: yamlext.EntryIndex, Attributes: indexAttributeNames(), Reads: []string{"type", "ops"},
				Decode: decodeIndex, Parameters: indexOptions},
		},
		EntrySections: []yamlext.EntrySection{
			{Key: "changefeeds", Decode: decodeChangefeeds},
			{Key: "column_families", Decode: decodeColumnFamilies},
			{Key: "column_store", Decode: decodeColumnStore},
		},
		Coverage: func() (schemaext.Coverage, error) { return Coverage(Limits{}) },
	}
}

// indexAttributeNames are the names of the index attributes the owner adds
// to the Go annotation's index directive, which a YAML index entry spells
// alike.
func indexAttributeNames() []string {
	attributes := indexAttributes()
	names := make([]string, 0, len(attributes))
	for _, attribute := range attributes {
		names = append(names, attribute.Name)
	}
	return names
}

// decodeTablePartitioning reads a row table's partitioning, which a YAML
// table entry spells as the annotation does. Column storage has a block of
// its own in YAML, so the table directive's store keys are not read here.
func decodeTablePartitioning(values map[string]string) (schemaext.Facets, error) {
	partitioning, err := ydbpartition.ParseTableDeclaration(values)
	if err != nil || partitioning == nil {
		return schemaext.Facets{}, err
	}
	return schemaext.NewFacets(&ydbschema.DesiredTablePartitioning{TablePartitioning: *partitioning})
}

// sortedKeys returns the keys of entries in order, which a Go map does not
// keep.
func sortedKeys[V any](entries map[string]V) []string {
	return slices.Sorted(maps.Keys(entries))
}

// present keeps the scalars a document wrote, keyed by attribute name. One it
// wrote empty is present, so an empty value is refused rather than read as no
// setting.
func present(scalars map[string]*yamlvalue.Scalar) map[string]string {
	values := make(map[string]string)
	for attribute, value := range scalars {
		if value != nil {
			values[attribute] = string(*value)
		}
	}
	return values
}

// entryName is the name an entry gives, or its key.
func entryName(written yamlvalue.Scalar, key string) string {
	return cmp.Or(string(written), key)
}

// objectsOf contributes every object of objects.
func objectsOf(objects schemaext.Objects) ([]yamlext.Contribution, error) {
	all, err := objects.All()
	if err != nil {
		return nil, err
	}
	contributions := make([]yamlext.Contribution, 0, len(all))
	for _, object := range all {
		contributions = append(contributions, yamlext.Contribution{Object: &object, Label: fmt.Sprintf("YDB object %s", object.Ref)})
	}
	return contributions, nil
}

// changefeedSpec is a YDB changefeed of the table, keyed by its name, with
// each option keyed as the annotation keys it.
type changefeedSpec struct {
	Mode                     *yamlvalue.Scalar                        `yaml:"mode"`
	Format                   *yamlvalue.Scalar                        `yaml:"format"`
	VirtualTimestamps        *yamlvalue.Scalar                        `yaml:"virtual_timestamps"`
	ResolvedTimestamps       *yamlvalue.Scalar                        `yaml:"resolved_timestamps"`
	InitialScan              *yamlvalue.Scalar                        `yaml:"initial_scan"`
	UserSIDs                 *yamlvalue.Scalar                        `yaml:"user_sids"`
	SchemaChanges            *yamlvalue.Scalar                        `yaml:"schema_changes"`
	TopicMinActivePartitions *yamlvalue.Scalar                        `yaml:"topic_min_active_partitions"`
	TopicAutoPartitioning    *yamlvalue.Scalar                        `yaml:"topic_auto_partitioning"`
	RetentionPeriod          *yamlvalue.Scalar                        `yaml:"retention_period"`
	Consumers                yamlvalue.OrderedMap[changefeedConsumer] `yaml:"consumers"`
}

// changefeedConsumer is a consumer of a changefeed's topic, keyed by its
// name.
type changefeedConsumer struct {
	Important          *yamlvalue.Scalar `yaml:"important"`
	ReadFrom           *yamlvalue.Scalar `yaml:"read_from"`
	SupportedCodecs    yamlvalue.List    `yaml:"supported_codecs"`
	AvailabilityPeriod *yamlvalue.Scalar `yaml:"availability_period"`
}

func (spec changefeedSpec) values(name string) map[string]string {
	values := present(map[string]*yamlvalue.Scalar{
		ydbchangefeed.AttributeMode:                     spec.Mode,
		ydbchangefeed.AttributeFormat:                   spec.Format,
		ydbchangefeed.AttributeVirtualTimestamps:        spec.VirtualTimestamps,
		ydbchangefeed.AttributeResolvedTimestamps:       spec.ResolvedTimestamps,
		ydbchangefeed.AttributeInitialScan:              spec.InitialScan,
		ydbchangefeed.AttributeUserSIDs:                 spec.UserSIDs,
		ydbchangefeed.AttributeSchemaChanges:            spec.SchemaChanges,
		ydbchangefeed.AttributeTopicMinActivePartitions: spec.TopicMinActivePartitions,
		ydbchangefeed.AttributeTopicAutoPartitioning:    spec.TopicAutoPartitioning,
		ydbchangefeed.AttributeRetentionPeriod:          spec.RetentionPeriod,
	})
	values[ydbchangefeed.AttributeName] = name
	return values
}

func (spec changefeedConsumer) values(name string) map[string]string {
	values := present(map[string]*yamlvalue.Scalar{
		ydbchangefeed.AttributeImportant:          spec.Important,
		ydbchangefeed.AttributeReadFrom:           spec.ReadFrom,
		ydbchangefeed.AttributeAvailabilityPeriod: spec.AvailabilityPeriod,
	})
	values[ydbchangefeed.AttributeName] = name
	if spec.SupportedCodecs != nil {
		values[ydbchangefeed.AttributeSupportedCodecs] = strings.Join(spec.SupportedCodecs, ",")
	}
	return values
}

// decodeChangefeeds reads a table's changefeeds and their consumers as
// objects on the table.
func decodeChangefeeds(decode func(any) error, table yamlext.Table) ([]yamlext.Contribution, error) {
	var specs yamlvalue.OrderedMap[changefeedSpec]
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, entry := range specs {
		changefeed, err := ydbchangefeed.ParseDeclaration(entry.Value.values(entry.Name))
		if err != nil {
			return nil, fmt.Errorf("table %q: changefeed %q: %w", table.Name, entry.Name, err)
		}
		for _, consumerEntry := range entry.Value.Consumers {
			consumer, err := ydbtopic.ParseConsumer(consumerEntry.Value.values(consumerEntry.Name))
			if err != nil {
				return nil, fmt.Errorf("table %q: changefeed %q: consumer %q: %w", table.Name, entry.Name, consumerEntry.Name, err)
			}
			changefeed.Consumers = append(changefeed.Consumers, consumer)
		}
		if objects, err = objects.With(ydbschema.DesiredObject(table.Schema, table.Name, changefeed)); err != nil {
			return nil, err
		}
	}
	return objectsOf(objects)
}

// columnFamilySpec is a YDB column family of the table, keyed by its name,
// with each setting keyed as the annotation keys it.
type columnFamilySpec struct {
	Data        *yamlvalue.Scalar `yaml:"data"`
	Compression *yamlvalue.Scalar `yaml:"compression"`
	CacheMode   *yamlvalue.Scalar `yaml:"cache_mode"`
	Fields      yamlvalue.List    `yaml:"fields"`
}

func (spec columnFamilySpec) values(name string) map[string]string {
	values := present(map[string]*yamlvalue.Scalar{
		ydbfamily.AttributeData:        spec.Data,
		ydbfamily.AttributeCompression: spec.Compression,
		ydbfamily.AttributeCacheMode:   spec.CacheMode,
	})
	values[ydbfamily.AttributeName] = name
	if spec.Fields != nil {
		values[ydbfamily.AttributeFields] = strings.Join(spec.Fields, ",")
	}
	return values
}

// decodeColumnFamilies reads a table's column families as the owner's facet.
func decodeColumnFamilies(decode func(any) error, table yamlext.Table) ([]yamlext.Contribution, error) {
	var specs yamlvalue.OrderedMap[columnFamilySpec]
	if err := decode(&specs); err != nil {
		return nil, err
	}
	if len(specs) == 0 {
		return nil, nil
	}
	families := make([]ydbschema.ColumnFamily, 0, len(specs))
	for _, entry := range specs {
		family, err := ydbfamily.ParseDeclaration(entry.Value.values(entry.Name))
		if err != nil {
			return nil, fmt.Errorf("table %q: column family %q: %w", table.Name, entry.Name, err)
		}
		families = append(families, family)
	}
	declared := &ydbschema.DesiredColumnFamilies{Families: families}
	if err := ydbschema.ValidateDesiredColumnFamilies(declared); err != nil {
		return nil, fmt.Errorf("table %q: column families: %w", table.Name, err)
	}
	return []yamlext.Contribution{{Facet: declared, Label: "column families"}}, nil
}

// columnStoreSpec is a table's column_store block: YDB column storage, its
// hash key, its shard count and its tiered TTL.
type columnStoreSpec struct {
	HashColumns []string       `yaml:"hash_columns,omitempty"`
	Partitions  uint64         `yaml:"partitions,omitempty"`
	TTL         *tieredTTLSpec `yaml:"ttl,omitempty"`
}

// tieredTTLSpec is a column table's ttl block.
type tieredTTLSpec struct {
	Column string        `yaml:"column"`
	Unit   string        `yaml:"unit,omitempty"`
	Tiers  []ttlTierSpec `yaml:"tiers"`
}

// ttlTierSpec is one tier of a column table's ttl block.
type ttlTierSpec struct {
	Interval       string `yaml:"interval"`
	ExternalSource string `yaml:"external_source,omitempty"`
}

// decodeColumnStore reads a table's column_store block as the owner's facet.
// A table without one is a row table. The unit is kept as YQL writes it.
func decodeColumnStore(decode func(any) error, table yamlext.Table) ([]yamlext.Contribution, error) {
	var spec *columnStoreSpec
	if err := decode(&spec); err != nil {
		return nil, err
	}
	if spec == nil {
		return nil, nil
	}
	store := &ydbschema.DesiredColumnStore{ColumnStore: ydbschema.ColumnStore{HashColumns: spec.HashColumns, Partitions: spec.Partitions}}
	if spec.TTL != nil {
		unit, err := ydbttl.Unit(spec.TTL.Unit)
		if err != nil {
			return nil, fmt.Errorf("table %q: %w", table.Key, err)
		}
		store.TTL = &ydbschema.TieredTTL{Column: spec.TTL.Column, Unit: unit}
		for _, tier := range spec.TTL.Tiers {
			store.TTL.Tiers = append(store.TTL.Tiers, ydbschema.TTLTier{Interval: tier.Interval, ExternalSource: tier.ExternalSource})
		}
	}
	if err := ydbschema.CheckColumnStore(store.ColumnStore); err != nil {
		return nil, fmt.Errorf("table %q: %w", table.Key, err)
	}
	return []yamlext.Contribution{{Facet: store, Label: "column storage"}}, nil
}

// topicSpec is one YDB topic: its directory, its settings spelled as YDB
// spells them, and its consumers in the order they are written.
type topicSpec struct {
	Name                   yamlvalue.Scalar                        `yaml:"name"`
	Schema                 yamlvalue.Scalar                        `yaml:"schema"`
	MinActivePartitions    *yamlvalue.Scalar                       `yaml:"min_active_partitions"`
	MaxActivePartitions    *yamlvalue.Scalar                       `yaml:"max_active_partitions"`
	Strategy               *yamlvalue.Scalar                       `yaml:"auto_partitioning_strategy"`
	UpUtilizationPercent   *yamlvalue.Scalar                       `yaml:"auto_partitioning_up_utilization_percent"`
	DownUtilizationPercent *yamlvalue.Scalar                       `yaml:"auto_partitioning_down_utilization_percent"`
	StabilizationWindow    *yamlvalue.Scalar                       `yaml:"auto_partitioning_stabilization_window"`
	RetentionPeriod        *yamlvalue.Scalar                       `yaml:"retention_period"`
	WriteSpeed             *yamlvalue.Scalar                       `yaml:"partition_write_speed_bytes_per_second"`
	WriteBurst             *yamlvalue.Scalar                       `yaml:"partition_write_burst_bytes"`
	SupportedCodecs        *yamlvalue.List                         `yaml:"supported_codecs"`
	Consumers              yamlvalue.OrderedMap[topicConsumerSpec] `yaml:"consumers"`
}

// topicConsumerSpec is one consumer of a topic, keyed by its name.
type topicConsumerSpec struct {
	Important          *yamlvalue.Scalar `yaml:"important"`
	ReadFrom           *yamlvalue.Scalar `yaml:"read_from"`
	SupportedCodecs    *yamlvalue.List   `yaml:"supported_codecs"`
	AvailabilityPeriod *yamlvalue.Scalar `yaml:"availability_period"`
}

func (spec topicSpec) values() map[string]string {
	values := present(map[string]*yamlvalue.Scalar{
		ydbtopic.AttributeMinActivePartitions:    spec.MinActivePartitions,
		ydbtopic.AttributeMaxActivePartitions:    spec.MaxActivePartitions,
		ydbtopic.AttributeStrategy:               spec.Strategy,
		ydbtopic.AttributeUpUtilizationPercent:   spec.UpUtilizationPercent,
		ydbtopic.AttributeDownUtilizationPercent: spec.DownUtilizationPercent,
		ydbtopic.AttributeStabilizationWindow:    spec.StabilizationWindow,
		ydbtopic.AttributeRetentionPeriod:        spec.RetentionPeriod,
		ydbtopic.AttributeWriteSpeed:             spec.WriteSpeed,
		ydbtopic.AttributeWriteBurst:             spec.WriteBurst,
	})
	if spec.SupportedCodecs != nil {
		values[ydbtopic.AttributeSupportedCodecs] = strings.Join(*spec.SupportedCodecs, ",")
	}
	return values
}

func (spec topicConsumerSpec) values(name string) map[string]string {
	values := present(map[string]*yamlvalue.Scalar{
		ydbtopic.AttributeImportant:          spec.Important,
		ydbtopic.AttributeReadFrom:           spec.ReadFrom,
		ydbtopic.AttributeAvailabilityPeriod: spec.AvailabilityPeriod,
	})
	values[ydbtopic.AttributeName] = name
	if spec.SupportedCodecs != nil {
		values[ydbtopic.AttributeSupportedCodecs] = strings.Join(*spec.SupportedCodecs, ",")
	}
	return values
}

// decodeTopics reads the document's topics, each checked by the rules the
// annotation reads a topic with.
func decodeTopics(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]topicSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		spec := specs[key]
		topic, err := ydbtopic.ParseTopic(spec.values())
		if err != nil {
			return nil, fmt.Errorf("topic %q: %w", key, err)
		}
		for _, entry := range spec.Consumers {
			consumer, err := ydbtopic.ParseConsumer(entry.Value.values(entry.Name))
			if err != nil {
				return nil, fmt.Errorf("topic %q, consumer %q: %w", key, entry.Name, err)
			}
			topic.Consumers = append(topic.Consumers, consumer)
		}
		if objects, err = ydbtopic.Declare(objects, string(spec.Schema), entryName(spec.Name, key), "", topic); err != nil {
			return nil, fmt.Errorf("topic %q: %w", key, err)
		}
	}
	return objectsOf(objects)
}

// resourcePoolSpec is one YDB resource pool, keyed by its name, with its
// settings spelled as YDB spells them.
type resourcePoolSpec struct {
	ConcurrentQueryLimit           *yamlvalue.Scalar `yaml:"concurrent_query_limit"`
	QueueSize                      *yamlvalue.Scalar `yaml:"queue_size"`
	DatabaseLoadCPUThreshold       *yamlvalue.Scalar `yaml:"database_load_cpu_threshold"`
	QueryMemoryLimitPercentPerNode *yamlvalue.Scalar `yaml:"query_memory_limit_percent_per_node"`
	QueryCPULimitPercentPerNode    *yamlvalue.Scalar `yaml:"query_cpu_limit_percent_per_node"`
	TotalCPULimitPercentPerNode    *yamlvalue.Scalar `yaml:"total_cpu_limit_percent_per_node"`
	ResourceWeight                 *yamlvalue.Scalar `yaml:"resource_weight"`
}

// resourcePoolClassifierSpec is one YDB resource pool classifier, keyed by
// its name.
type resourcePoolClassifierSpec struct {
	ResourcePool yamlvalue.Scalar  `yaml:"resource_pool"`
	MemberName   yamlvalue.Scalar  `yaml:"member_name"`
	Rank         *yamlvalue.Scalar `yaml:"rank"`
}

// decodePools reads the document's resource pools.
func decodePools(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]resourcePoolSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		spec := specs[key]
		values := present(map[string]*yamlvalue.Scalar{
			ydbworkload.AttributeConcurrentQueryLimit:           spec.ConcurrentQueryLimit,
			ydbworkload.AttributeQueueSize:                      spec.QueueSize,
			ydbworkload.AttributeDatabaseLoadCPUThreshold:       spec.DatabaseLoadCPUThreshold,
			ydbworkload.AttributeQueryMemoryLimitPercentPerNode: spec.QueryMemoryLimitPercentPerNode,
			ydbworkload.AttributeQueryCPULimitPercentPerNode:    spec.QueryCPULimitPercentPerNode,
			ydbworkload.AttributeTotalCPULimitPercentPerNode:    spec.TotalCPULimitPercentPerNode,
			ydbworkload.AttributeResourceWeight:                 spec.ResourceWeight,
		})
		values[ydbworkload.AttributeName] = key
		name, pool, err := ydbworkload.ParsePool(values)
		if err != nil {
			return nil, fmt.Errorf("resource pool %q: %w", key, err)
		}
		if objects, err = objects.With(ydbworkload.DesiredPoolObject(name, "", pool)); err != nil {
			return nil, err
		}
	}
	return objectsOf(objects)
}

// decodeClassifiers reads the document's resource pool classifiers.
func decodeClassifiers(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]resourcePoolClassifierSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		spec := specs[key]
		values := map[string]string{
			ydbworkload.AttributeName:         key,
			ydbworkload.AttributeResourcePool: string(spec.ResourcePool),
			ydbworkload.AttributeMemberName:   string(spec.MemberName),
		}
		if spec.Rank != nil {
			values[ydbworkload.AttributeRank] = string(*spec.Rank)
		}
		name, classifier, err := ydbworkload.ParseClassifier(values)
		if err != nil {
			return nil, fmt.Errorf("resource pool classifier %q: %w", key, err)
		}
		if objects, err = objects.With(ydbworkload.DesiredClassifierObject(name, "", classifier)); err != nil {
			return nil, err
		}
	}
	return objectsOf(objects)
}

// connectionSpec is how a YDB async replication or transfer reaches another
// database.
type connectionSpec struct {
	ConnectionString   *yamlvalue.Scalar `yaml:"connection_string"`
	TokenSecretName    *yamlvalue.Scalar `yaml:"token_secret_name"`
	TokenSecretPath    *yamlvalue.Scalar `yaml:"token_secret_path"`
	User               *yamlvalue.Scalar `yaml:"user"`
	PasswordSecretName *yamlvalue.Scalar `yaml:"password_secret_name"`
	PasswordSecretPath *yamlvalue.Scalar `yaml:"password_secret_path"`
}

func (spec connectionSpec) values() map[string]string {
	return present(map[string]*yamlvalue.Scalar{
		ydbreplication.AttributeConnectionString:   spec.ConnectionString,
		ydbreplication.AttributeTokenSecretName:    spec.TokenSecretName,
		ydbreplication.AttributeTokenSecretPath:    spec.TokenSecretPath,
		ydbreplication.AttributeUser:               spec.User,
		ydbreplication.AttributePasswordSecretName: spec.PasswordSecretName,
		ydbreplication.AttributePasswordSecretPath: spec.PasswordSecretPath,
	})
}

// asyncReplicationSpec is one YDB async replication: its directory, its
// connection, its consistency and its items in the order they are written.
type asyncReplicationSpec struct {
	connectionSpec   `yaml:",inline"`
	Name             yamlvalue.Scalar      `yaml:"name"`
	Schema           yamlvalue.Scalar      `yaml:"schema"`
	ConsistencyLevel *yamlvalue.Scalar     `yaml:"consistency_level"`
	CommitInterval   *yamlvalue.Scalar     `yaml:"commit_interval"`
	Items            []replicationItemSpec `yaml:"items"`
}

// replicationItemSpec is one table, or directory of tables, a replication
// copies.
type replicationItemSpec struct {
	Source yamlvalue.Scalar `yaml:"source"`
	Target yamlvalue.Scalar `yaml:"target"`
}

// transferSpec is one YDB transfer.
type transferSpec struct {
	connectionSpec `yaml:",inline"`
	Name           yamlvalue.Scalar  `yaml:"name"`
	Schema         yamlvalue.Scalar  `yaml:"schema"`
	Source         yamlvalue.Scalar  `yaml:"source"`
	Target         yamlvalue.Scalar  `yaml:"target"`
	Using          yamlvalue.Scalar  `yaml:"using"`
	Consumer       *yamlvalue.Scalar `yaml:"consumer"`
	BatchSizeBytes *yamlvalue.Scalar `yaml:"batch_size_bytes"`
	FlushInterval  *yamlvalue.Scalar `yaml:"flush_interval"`
}

// decodeReplications reads the document's async replications.
func decodeReplications(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]asyncReplicationSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		spec := specs[key]
		values := spec.values()
		maps.Copy(values, present(map[string]*yamlvalue.Scalar{
			ydbreplication.AttributeConsistencyLevel: spec.ConsistencyLevel,
			ydbreplication.AttributeCommitInterval:   spec.CommitInterval,
		}))
		replication, err := ydbreplication.ParseReplication(values)
		if err != nil {
			return nil, fmt.Errorf("async replication %q: %w", key, err)
		}
		if len(spec.Items) == 0 {
			return nil, fmt.Errorf("async replication %q: declares no item; list the tables it replicates under items", key)
		}
		for index, entry := range spec.Items {
			item, err := ydbreplication.ParseItem(map[string]string{
				ydbreplication.AttributeSource: string(entry.Source),
				ydbreplication.AttributeTarget: string(entry.Target),
			})
			if err != nil {
				return nil, fmt.Errorf("async replication %q, item %d: %w", key, index+1, err)
			}
			replication.Items = append(replication.Items, item)
		}
		if objects, err = ydbreplication.DeclareReplication(objects, string(spec.Schema), entryName(spec.Name, key), "", replication); err != nil {
			return nil, fmt.Errorf("async replication %q: %w", key, err)
		}
	}
	return objectsOf(objects)
}

// decodeTransfers reads the document's transfers.
func decodeTransfers(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]transferSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		spec := specs[key]
		values := spec.values()
		values[ydbreplication.AttributeSource] = string(spec.Source)
		values[ydbreplication.AttributeTarget] = string(spec.Target)
		values[ydbreplication.AttributeUsing] = string(spec.Using)
		maps.Copy(values, present(map[string]*yamlvalue.Scalar{
			ydbreplication.AttributeConsumer:       spec.Consumer,
			ydbreplication.AttributeBatchSizeBytes: spec.BatchSizeBytes,
			ydbreplication.AttributeFlushInterval:  spec.FlushInterval,
		}))
		transfer, err := ydbreplication.ParseTransfer(values)
		if err != nil {
			return nil, fmt.Errorf("transfer %q: %w", key, err)
		}
		if objects, err = ydbreplication.DeclareTransfer(objects, string(spec.Schema), entryName(spec.Name, key), "", transfer); err != nil {
			return nil, fmt.Errorf("transfer %q: %w", key, err)
		}
	}
	return objectsOf(objects)
}

// coordinationNodeSpec declares a YDB coordination node. The settings are
// keyed as the annotation keys them.
type coordinationNodeSpec struct {
	StructName              yamlvalue.Scalar  `yaml:"struct_name"`
	Name                    yamlvalue.Scalar  `yaml:"name"`
	Schema                  yamlvalue.Scalar  `yaml:"schema"`
	SelfCheckPeriod         *yamlvalue.Scalar `yaml:"self_check_period"`
	SessionGracePeriod      *yamlvalue.Scalar `yaml:"session_grace_period"`
	ReadConsistencyMode     *yamlvalue.Scalar `yaml:"read_consistency_mode"`
	AttachConsistencyMode   *yamlvalue.Scalar `yaml:"attach_consistency_mode"`
	RateLimiterCountersMode *yamlvalue.Scalar `yaml:"rate_limiter_counters_mode"`
}

// decodeCoordinationNodes reads the document's coordination nodes.
func decodeCoordinationNodes(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]coordinationNodeSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		spec := specs[key]
		name := entryName(spec.Name, key)
		if err := ydbcoordination.RefuseName(string(spec.Schema), name); err != nil {
			return nil, fmt.Errorf("coordination node %q: %w", key, err)
		}
		settings, err := ydbcoordination.ParseDeclaration(present(map[string]*yamlvalue.Scalar{
			ydbcoordination.SettingSelfCheckPeriod:         spec.SelfCheckPeriod,
			ydbcoordination.SettingSessionGracePeriod:      spec.SessionGracePeriod,
			ydbcoordination.SettingReadConsistencyMode:     spec.ReadConsistencyMode,
			ydbcoordination.SettingAttachConsistencyMode:   spec.AttachConsistencyMode,
			ydbcoordination.SettingRateLimiterCountersMode: spec.RateLimiterCountersMode,
		}))
		if err != nil {
			return nil, fmt.Errorf("coordination node %q: %w", key, err)
		}
		object := ydbcoordination.DesiredObject(string(spec.Schema), name, string(spec.StructName), settings)
		if objects, err = objects.With(object); err != nil {
			return nil, fmt.Errorf("coordination node %q: %w", key, err)
		}
	}
	return objectsOf(objects)
}

// secretSpec is one YDB secret: its directory and the environment variable
// its value comes from. Value is read only so that a document that writes the
// value is refused by name, rather than answered with the decoder's
// unknown-field error, and it is never kept or printed.
type secretSpec struct {
	Name     yamlvalue.Scalar  `yaml:"name"`
	Schema   yamlvalue.Scalar  `yaml:"schema"`
	ValueEnv yamlvalue.Scalar  `yaml:"value_env"`
	Value    *yamlvalue.Scalar `yaml:"value"`
}

// decodeSecrets reads the document's secrets.
func decodeSecrets(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]secretSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		spec := specs[key]
		values := map[string]string{ydbsecret.AttributeValueEnv: string(spec.ValueEnv)}
		if spec.Value != nil {
			values[ydbsecret.AttributeValue] = ""
		}
		valueEnv, err := ydbsecret.ParseValueEnv(values)
		if err != nil {
			return nil, fmt.Errorf("secret %q: %w", key, err)
		}
		if objects, err = ydbsecret.Declare(objects, string(spec.Schema), entryName(spec.Name, key), "", valueEnv); err != nil {
			return nil, fmt.Errorf("secret %q: %w", key, err)
		}
	}
	return objectsOf(objects)
}

type streamingQuerySpec struct {
	Name            yamlvalue.Scalar `yaml:"name"`
	Schema          yamlvalue.Scalar `yaml:"schema"`
	Text            yamlvalue.Scalar `yaml:"text"`
	Run             *bool            `yaml:"run"`
	ResourcePool    yamlvalue.Scalar `yaml:"resource_pool"`
	AllowStateReset bool             `yaml:"allow_state_reset"`
}

// decodeStreamingQueries reads the document's streaming queries.
func decodeStreamingQueries(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]streamingQuerySpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		entry := specs[key]
		name := entryName(entry.Name, key)
		spec := ydbstreaming.Spec{Text: string(entry.Text), Run: entry.Run, ResourcePool: string(entry.ResourcePool)}
		if err := ydbstreaming.Validate(spec); err != nil {
			return nil, fmt.Errorf("streaming query %q: %w", name, err)
		}
		object := ydbstreaming.DesiredObject(string(entry.Schema), name, "", spec, entry.AllowStateReset)
		if err := ydbstreaming.ValidateIdentity(object.Ref); err != nil {
			return nil, err
		}
		var err error
		if objects, err = objects.With(object); err != nil {
			return nil, err
		}
	}
	return objectsOf(objects)
}

// externalDataSourceSpec is one YDB external data source.
type externalDataSourceSpec struct {
	Name       yamlvalue.Scalar            `yaml:"name"`
	Schema     yamlvalue.Scalar            `yaml:"schema"`
	SourceType yamlvalue.Scalar            `yaml:"source_type"`
	Location   yamlvalue.Scalar            `yaml:"location"`
	AuthMethod yamlvalue.Scalar            `yaml:"auth_method"`
	Options    map[string]yamlvalue.Scalar `yaml:"options"`
}

// externalTableSpec is one YDB external table.
type externalTableSpec struct {
	Name       yamlvalue.Scalar            `yaml:"name"`
	Schema     yamlvalue.Scalar            `yaml:"schema"`
	DataSource yamlvalue.Scalar            `yaml:"data_source"`
	Location   yamlvalue.Scalar            `yaml:"location"`
	Columns    []externalColumnSpec        `yaml:"columns"`
	Options    map[string]yamlvalue.Scalar `yaml:"options"`
}

// externalColumnSpec is one column of an external table.
type externalColumnSpec struct {
	Name    yamlvalue.Scalar `yaml:"name"`
	Type    yamlvalue.Scalar `yaml:"type"`
	NotNull bool             `yaml:"not_null"`
}

// decodeExternalDataSources reads the document's external data sources.
func decodeExternalDataSources(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]externalDataSourceSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		spec := specs[key]
		options, err := ydbexternal.CheckOptions(scalarMap(spec.Options), ydbexternal.DataSourceReserved...)
		if err != nil {
			return nil, fmt.Errorf("external data source %q: %w", key, err)
		}
		name, err := externalObjectName(spec.Name, key)
		if err != nil {
			return nil, fmt.Errorf("external data source %q: %w", key, err)
		}
		objects, err = ydbexternal.DeclareSource(objects, string(spec.Schema), name, "", ydbexternal.DataSource{
			SourceType: strings.TrimSpace(string(spec.SourceType)),
			Location:   strings.TrimSpace(string(spec.Location)),
			AuthMethod: strings.TrimSpace(string(spec.AuthMethod)),
			Options:    options,
		})
		if err != nil {
			return nil, fmt.Errorf("external data source %q: %w", key, err)
		}
	}
	return objectsOf(objects)
}

// decodeExternalTables reads the document's external tables.
func decodeExternalTables(decode func(any) error, _ yamlext.Tables) ([]yamlext.Contribution, error) {
	var specs map[string]externalTableSpec
	if err := decode(&specs); err != nil {
		return nil, err
	}
	var objects schemaext.Objects
	for _, key := range sortedKeys(specs) {
		var err error
		if objects, err = specs[key].declare(objects, key); err != nil {
			return nil, fmt.Errorf("external table %q: %w", key, err)
		}
	}
	return objectsOf(objects)
}

// declare adds one external table to objects, named key unless it names
// itself.
func (spec externalTableSpec) declare(objects schemaext.Objects, key string) (schemaext.Objects, error) {
	options, err := ydbexternal.CheckOptions(scalarMap(spec.Options), ydbexternal.TableReserved...)
	if err != nil {
		return objects, err
	}
	name, err := externalObjectName(spec.Name, key)
	if err != nil {
		return objects, err
	}
	columns := make([]ydbexternal.Column, 0, len(spec.Columns))
	for _, column := range spec.Columns {
		columns = append(columns, ydbexternal.Column{
			Name: strings.TrimSpace(string(column.Name)), Type: strings.TrimSpace(string(column.Type)),
			NotNull: column.NotNull,
		})
	}
	if err := ydbexternal.CheckColumns(columns); err != nil {
		return objects, err
	}
	return ydbexternal.DeclareTable(objects, string(spec.Schema), name, "", ydbexternal.Table{
		DataSource: strings.TrimSpace(string(spec.DataSource)),
		Location:   strings.TrimSpace(string(spec.Location)),
		Columns:    columns,
		Options:    options,
	})
}

// externalObjectName is the name an external object's entry gives, or its
// key, and one path segment either way.
func externalObjectName(written yamlvalue.Scalar, key string) (string, error) {
	name := entryName(written, key)
	if strings.Contains(name, "/") {
		return "", &ydbexternal.DeclarationError{Attribute: ydbexternal.AttributeName,
			Reason: fmt.Sprintf("%q holds a slash; name the directory with %s", name, ydbexternal.AttributeSchema)}
	}
	return name, nil
}

// scalarMap is values with plain strings.
func scalarMap(values map[string]yamlvalue.Scalar) map[string]string {
	if values == nil {
		return nil
	}
	plain := make(map[string]string, len(values))
	for name, value := range values {
		plain[name] = string(value)
	}
	return plain
}
