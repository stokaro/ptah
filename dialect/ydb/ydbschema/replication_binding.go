package ydbschema

import (
	"fmt"
	"strconv"
	"strings"

	"ptah.run/core/schemaext"
)

// ReplicationBinding records the target item named by a changefeed's
// __async_replication attribute. DestinationPath names the replica table, not
// the replication object. That object may live in another database or cluster;
// this observation does not invent its identity or authorize changes to it.
type ReplicationBinding struct {
	DestinationPath               string `json:"destination_path"`
	ItemID                        string `json:"item_id"`
	SupportsTopicAutopartitioning bool   `json:"supports_topic_autopartitioning"`
}

// Clone returns an independent binding, preserving a nil observation.
func (b *ReplicationBinding) Clone() *ReplicationBinding {
	if b == nil {
		return nil
	}
	return new(*b)
}

// Validate checks the observed binding without resolving its destination or
// treating its item number as the identity of a replication object.
func (b *ReplicationBinding) Validate() error {
	if b == nil {
		return nil
	}
	if !strings.HasPrefix(b.DestinationPath, "/") || b.DestinationPath == "/" || strings.ContainsRune(b.DestinationPath, 0) {
		return fmt.Errorf("%w: replication binding requires an absolute destination path", schemaext.ErrInvalidValue)
	}
	id, err := strconv.ParseUint(b.ItemID, 10, 64)
	if err != nil || id == 0 || strconv.FormatUint(id, 10) != b.ItemID {
		return fmt.Errorf("%w: replication binding requires a canonical positive item ID", schemaext.ErrInvalidValue)
	}
	return nil
}

// UnmarshalJSON refuses incomplete or unknown binding fields. In particular,
// an omitted autopartitioning observation is not an observed false value.
func (b *ReplicationBinding) UnmarshalJSON(data []byte) error {
	value, err := schemaext.DecodeJSON[*struct {
		DestinationPath  string `json:"destination_path"`
		ItemID           string `json:"item_id"`
		Autopartitioning *bool  `json:"supports_topic_autopartitioning"`
	}](data)
	if err != nil {
		return err
	}
	if b == nil || value == nil || value.Autopartitioning == nil {
		return fmt.Errorf("%w: incomplete replication binding", schemaext.ErrInvalidValue)
	}
	decoded := ReplicationBinding{DestinationPath: value.DestinationPath, ItemID: value.ItemID, SupportsTopicAutopartitioning: *value.Autopartitioning}
	if err := decoded.Validate(); err != nil {
		return err
	}
	*b = decoded
	return nil
}

func equalBinding(a, b *ReplicationBinding) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}
