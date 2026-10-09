package schemaproperties

import (
	"context"

	"ptah.run/core/schemaext"
	"ptah.run/core/schemamodel"
)

// DecodeIndexes replaces claimed source properties with desired index facets.
// It consumes Index.Type only when a selected definition owns the type key;
// otherwise that common declaration stays unchanged. Target property groups
// follow DecodeTables' ownership, alias, conflict, and error rules. An index
// with no properties gains no intent. Index data is copied; other schema data
// stays shared and read-only. Errors and cancellation return no partial schema.
func DecodeIndexes(ctx context.Context, db *schemamodel.Database, target string, runtime Runtime) (*schemamodel.Database, error) {
	return decode(ctx, db, target, schemaext.IndexPlatformProperties, runtime)
}

// EncodeIndexes exports index facets as properties bound to the selected target.
// It preserves intent without restoring settings to common fields. Unsupported
// models, exclusions, foreign bindings, mixed declarations, and empty fragments
// are refused as in EncodeTables. Index data is copied and other data remains
// shared read-only. A nil schema is invalid; errors return no partial schema.
func EncodeIndexes(ctx context.Context, db *schemamodel.Database, target string, runtime Runtime) (*schemamodel.Database, error) {
	return encode(ctx, db, target, schemaext.IndexPlatformProperties, runtime)
}

// Decode lowers the selected target's table and then index source properties
// with the rules of DecodeTables and DecodeIndexes. Every path that renders,
// validates, or compares a declaration for a target uses it, so a declared
// property means the same thing on each. Errors and cancellation return no
// schema.
func Decode(ctx context.Context, db *schemamodel.Database, target string, runtime Runtime) (*schemamodel.Database, error) {
	tables, err := DecodeTables(ctx, db, target, runtime)
	if err != nil {
		return nil, err
	}
	return DecodeIndexes(ctx, tables, target, runtime)
}
