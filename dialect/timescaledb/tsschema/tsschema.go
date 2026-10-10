// Package tsschema owns the TimescaleDB schema model: the hypertable settings a
// table carries as a facet, and the continuous aggregates a schema holds as
// named objects, each in a desired and an observed representation. It contains
// no provider selection, database access or SQL.
//
// TimescaleDB is an extension of PostgreSQL rather than a target of its own,
// so its models attach to PostgreSQL-family objects and are selected for those
// targets by explicit composition. A hypertable is a property of an existing
// table: it has no name, and `timescaledb_information.hypertables` is keyed by
// the relation. A continuous aggregate is independently addressable: it has its
// own name in the relation namespace and depends on the hypertable it reads.
package tsschema

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// Owner is the provider identity that owns every TimescaleDB codec and service.
// It is separate from the PostgreSQL target the services are registered for.
const Owner = "ptah.run/timescaledb"

const (
	// HypertableKind identifies the partitioning settings of one table.
	HypertableKind schemaext.Kind = "ptah.run/timescaledb/hypertable"
	// ContinuousAggregateKind identifies one continuous aggregate.
	ContinuousAggregateKind schemaext.Kind = "ptah.run/timescaledb/continuous-aggregate"
)

// validText refuses text a statement or a catalog cannot carry faithfully.
func validText(field, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: TimescaleDB %s is not valid UTF-8", schemaext.ErrInvalidValue, field)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: TimescaleDB %s contains a NUL byte", schemaext.ErrInvalidValue, field)
	}
	return nil
}

// validTexts checks field/value pairs in order, so the first invalid field is
// the one a refusal names.
func validTexts(pairs ...string) error {
	for i := 0; i+1 < len(pairs); i += 2 {
		if err := validText(pairs[i], pairs[i+1]); err != nil {
			return err
		}
	}
	return nil
}

// wireObject decodes one JSON object and checks its keys exactly. JSON null is
// neither an omitted value nor an observed absence, and encoding/json would
// otherwise accept a key in any letter case.
func wireObject(data json.RawMessage, model string, allowed, required []string) (map[string]json.RawMessage, error) {
	fields, err := schemaext.DecodeJSON[map[string]json.RawMessage](data)
	if err != nil {
		return nil, err
	}
	if fields == nil {
		return nil, fmt.Errorf("%w: expected a non-null TimescaleDB %s object", schemaext.ErrInvalidValue, model)
	}
	for name, value := range fields {
		if !slices.Contains(allowed, name) {
			return nil, fmt.Errorf("%w: unknown TimescaleDB %s property %q", schemaext.ErrInvalidValue, model, name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return nil, fmt.Errorf("%w: TimescaleDB %s property %q cannot be null", schemaext.ErrInvalidValue, model, name)
		}
	}
	for _, name := range required {
		if _, found := fields[name]; !found {
			return nil, fmt.Errorf("%w: missing TimescaleDB %s property %q", schemaext.ErrInvalidValue, model, name)
		}
	}
	return fields, nil
}
