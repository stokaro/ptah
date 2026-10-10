package yamlschema

import (
	"fmt"

	"ptah.run/core/schemamodel"
	"ptah.run/dialect/ydb/ydbreplication"
)

// connectionSpec is how a YDB async replication or transfer reaches another
// database. Each value is a pointer, so a setting the document leaves out is
// told from one written empty, which is refused rather than read as no
// setting.
type connectionSpec struct {
	ConnectionString   *stringScalar `yaml:"connection_string"`
	TokenSecretName    *stringScalar `yaml:"token_secret_name"`
	TokenSecretPath    *stringScalar `yaml:"token_secret_path"`
	User               *stringScalar `yaml:"user"`
	PasswordSecretName *stringScalar `yaml:"password_secret_name"`
	PasswordSecretPath *stringScalar `yaml:"password_secret_path"`
}

// values are the connection's settings keyed by attribute name.
func (spec connectionSpec) values() map[string]string {
	values := make(map[string]string)
	for attribute, value := range map[string]*stringScalar{
		ydbreplication.AttributeConnectionString:   spec.ConnectionString,
		ydbreplication.AttributeTokenSecretName:    spec.TokenSecretName,
		ydbreplication.AttributeTokenSecretPath:    spec.TokenSecretPath,
		ydbreplication.AttributeUser:               spec.User,
		ydbreplication.AttributePasswordSecretName: spec.PasswordSecretName,
		ydbreplication.AttributePasswordSecretPath: spec.PasswordSecretPath,
	} {
		if value != nil {
			values[attribute] = string(*value)
		}
	}
	return values
}

// asyncReplicationSpec is one YDB async replication in a YAML document: its
// directory, its connection, its consistency and its items in the order they
// are written.
type asyncReplicationSpec struct {
	connectionSpec   `yaml:",inline"`
	Name             stringScalar          `yaml:"name"`
	Schema           stringScalar          `yaml:"schema"`
	ConsistencyLevel *stringScalar         `yaml:"consistency_level"`
	CommitInterval   *stringScalar         `yaml:"commit_interval"`
	Items            []replicationItemSpec `yaml:"items"`
}

// replicationItemSpec is one table, or directory of tables, a replication
// copies.
type replicationItemSpec struct {
	Source stringScalar `yaml:"source"`
	Target stringScalar `yaml:"target"`
}

// transferSpec is one YDB transfer in a YAML document.
type transferSpec struct {
	connectionSpec `yaml:",inline"`
	Name           stringScalar  `yaml:"name"`
	Schema         stringScalar  `yaml:"schema"`
	Source         stringScalar  `yaml:"source"`
	Target         stringScalar  `yaml:"target"`
	Using          stringScalar  `yaml:"using"`
	Consumer       *stringScalar `yaml:"consumer"`
	BatchSizeBytes *stringScalar `yaml:"batch_size_bytes"`
	FlushInterval  *stringScalar `yaml:"flush_interval"`
}

// addAsyncReplications reads the document's async replications, each checked
// by the rules the annotation parser reads one with.
func (d document) addAsyncReplications(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.AsyncReplications) {
		spec := d.AsyncReplications[key]
		values := spec.values()
		for attribute, value := range map[string]*stringScalar{
			ydbreplication.AttributeConsistencyLevel: spec.ConsistencyLevel,
			ydbreplication.AttributeCommitInterval:   spec.CommitInterval,
		} {
			if value != nil {
				values[attribute] = string(*value)
			}
		}
		replication, err := ydbreplication.ParseReplication(values)
		if err != nil {
			return fmt.Errorf("async replication %q: %w", key, err)
		}
		if len(spec.Items) == 0 {
			return fmt.Errorf("async replication %q: declares no item; list the tables it replicates under items", key)
		}
		for index, entry := range spec.Items {
			item, err := ydbreplication.ParseItem(map[string]string{
				ydbreplication.AttributeSource: string(entry.Source),
				ydbreplication.AttributeTarget: string(entry.Target),
			})
			if err != nil {
				return fmt.Errorf("async replication %q, item %d: %w", key, index+1, err)
			}
			replication.Items = append(replication.Items, item)
		}
		objects, err := ydbreplication.DeclareReplication(db.FeatureObjects, string(spec.Schema), valueOrDefault(spec.Name, key), "", replication)
		if err != nil {
			return fmt.Errorf("async replication %q: %w", key, err)
		}
		db.FeatureObjects = objects
	}
	return nil
}

// addTransfers reads the document's transfers, each checked by the rules the
// annotation parser reads one with.
func (d document) addTransfers(db *schemamodel.Database) error {
	for _, key := range sortedKeys(d.Transfers) {
		spec := d.Transfers[key]
		values := spec.values()
		values[ydbreplication.AttributeSource] = string(spec.Source)
		values[ydbreplication.AttributeTarget] = string(spec.Target)
		values[ydbreplication.AttributeUsing] = string(spec.Using)
		for attribute, value := range map[string]*stringScalar{
			ydbreplication.AttributeConsumer:       spec.Consumer,
			ydbreplication.AttributeBatchSizeBytes: spec.BatchSizeBytes,
			ydbreplication.AttributeFlushInterval:  spec.FlushInterval,
		} {
			if value != nil {
				values[attribute] = string(*value)
			}
		}
		transfer, err := ydbreplication.ParseTransfer(values)
		if err != nil {
			return fmt.Errorf("transfer %q: %w", key, err)
		}
		objects, err := ydbreplication.DeclareTransfer(db.FeatureObjects, string(spec.Schema), valueOrDefault(spec.Name, key), "", transfer)
		if err != nil {
			return fmt.Errorf("transfer %q: %w", key, err)
		}
		db.FeatureObjects = objects
	}
	return nil
}
