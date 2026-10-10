package ydbast

import (
	"ptah.run/core/schemaext"
	"ptah.run/dialect/ydb/ydbdiff"
)

// CoordinationCodec records the explicit standalone operation wire. It carries
// all operands and no capability grants, database handles, or host callbacks.
func CoordinationCodec() schemaext.Codec {
	return pathChangeCodec("coordination", &CoordinationNode{}, ydbdiff.CoordinationCodec(),
		func(value *CoordinationNode) (string, string, *ydbdiff.CoordinationNode) {
			return value.Schema, value.Name, &value.Change
		},
		func(schema, name string, change ydbdiff.CoordinationNode) *CoordinationNode {
			return &CoordinationNode{Schema: schema, Name: name, Change: change}
		})
}
