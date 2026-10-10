package chschema

import (
	_ "embed" // Embed the exact definition bound to refresh schedules.
	"encoding/json"
	"fmt"
	"slices"

	"ptah.run/core/schemaext"
)

//go:embed refresh-codecs.json
var refreshDefinition []byte

// refreshFields are the schedule's wire properties; mode and interval are
// required in both representations.
var refreshFields = []string{"mode", "interval", "offset", "randomize", "depends_on", "append"}

// RefreshWireDefinition returns an independent description of both refresh
// models.
func RefreshWireDefinition() json.RawMessage { return slices.Clone(refreshDefinition) }

// RefreshCodecs returns strict versioned codecs for desired and observed
// refresh schedules. Registration understands the model but grants no target
// or server support.
func RefreshCodecs() []schemaext.Codec {
	return []schemaext.Codec{
		valueCodec(&DesiredRefresh{}, schemaext.Desired, RefreshWireDefinition(), decodeDesiredRefresh, validateDesiredRefreshPayload),
		valueCodec(&ObservedRefresh{}, schemaext.Observed, RefreshWireDefinition(), decodeObservedRefresh, validateObservedRefreshPayload),
	}
}

// Owner is the provider identity under which the bundled runtime registers
// the ClickHouse codecs. Coverage built here names it, so a runtime that
// registers the codecs under another identity must build its own coverage.
const Owner = "ptah.run/clickhouse"

// RefreshCoverage records knowledge of refresh schedules for one
// representation: the whole kind, and any subject that departs from it.
func RefreshCoverage(representation schemaext.Representation, knowledge schemaext.Knowledge, subjects []schemaext.SubjectCoverage) (schemaext.Coverage, error) {
	owned := make([]schemaext.OwnedCodec, 0, 2)
	for _, codec := range RefreshCodecs() {
		owned = append(owned, schemaext.OwnedCodec{Owner: Owner, Codec: codec})
	}
	registry, err := schemaext.NewRegistry(owned...)
	if err != nil {
		return schemaext.Coverage{}, err
	}
	for _, model := range registry.Definitions() {
		if model.Kind == RefreshKind && model.Representation == representation {
			return schemaext.NewCoverage(representation, []schemaext.KindCoverage{{Model: model, Knowledge: knowledge}}, subjects)
		}
	}
	return schemaext.Coverage{}, fmt.Errorf("%w: refresh coverage requires a schema representation", schemaext.ErrInvalidValue)
}

func validateDesiredRefreshPayload(payload schemaext.Payload) error {
	v, ok := payload.(*DesiredRefresh)
	if !ok {
		return fmt.Errorf("%w: expected a desired ClickHouse refresh schedule, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateDesiredRefresh(v)
}

func validateObservedRefreshPayload(payload schemaext.Payload) error {
	v, ok := payload.(*ObservedRefresh)
	if !ok {
		return fmt.Errorf("%w: expected an observed ClickHouse refresh schedule, got %T", schemaext.ErrInvalidValue, payload)
	}
	return ValidateObservedRefresh(v)
}

func decodeDesiredRefresh(data json.RawMessage) (schemaext.Payload, error) {
	if _, err := wireObject(data, "refresh", refreshFields, []string{"mode", "interval"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*DesiredRefresh](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateDesiredRefresh(value); err != nil {
		return nil, err
	}
	return value, nil
}

func decodeObservedRefresh(data json.RawMessage) (schemaext.Payload, error) {
	if _, err := wireObject(data, "refresh", refreshFields, []string{"mode", "interval"}); err != nil {
		return nil, err
	}
	value, err := schemaext.DecodeJSON[*ObservedRefresh](data)
	if err != nil {
		return nil, err
	}
	if err := ValidateObservedRefresh(value); err != nil {
		return nil, err
	}
	return value, nil
}
