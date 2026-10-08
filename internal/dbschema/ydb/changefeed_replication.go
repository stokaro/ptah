package ydb

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbschema"
)

// The controller writes the replica-table path and target item number, not its
// own object path. It can run on a different cluster; a local directory scan
// cannot prove its absence. Do not classify a stream by its generated name.
func changefeedReplication(attributes map[string]string) (*ydbschema.ReplicationBinding, bool) {
	if len(attributes) == 0 {
		return nil, true
	}
	raw, found := attributes["__async_replication"]
	if !found || len(attributes) != 1 {
		return nil, false
	}
	value, err := schemaext.DecodeJSON[struct {
		Path             string `json:"path"`
		ID               string `json:"id"`
		Autopartitioning *bool  `json:"supports_topic_autopartitioning"`
	}]([]byte(raw))
	if err != nil || value.Autopartitioning == nil {
		return nil, false
	}
	binding := &ydbschema.ReplicationBinding{DestinationPath: value.Path, ItemID: value.ID, SupportsTopicAutopartitioning: *value.Autopartitioning}
	if err := binding.Validate(); err != nil {
		return nil, false
	}
	return binding, true
}
