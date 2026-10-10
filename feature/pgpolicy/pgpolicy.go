// Package pgpolicy owns the PostgreSQL row-security model: a policy as a
// feature object identified by its table and name, and a table's row-security
// switches as a facet of that table, each in a desired and an observed
// representation. It contains no provider selection, database access or SQL.
//
// The model keeps PostgreSQL's semantics rather than a shape shared with other
// engines (ADR 0020). A policy's command, role selectors, USING and WITH CHECK
// expressions and permissive or restrictive composition decide what it admits,
// and ENABLE and FORCE ROW LEVEL SECURITY are two independent flags of the
// table. Another PostgreSQL-wire target may select this model only when its
// adapter implements the same contract.
package pgpolicy

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// Owner is the provider identity that owns every row-security codec and
// service. It is separate from the targets the services are registered for.
const Owner = "ptah.run/pgpolicy"

const (
	// PolicyKind identifies one policy on one table.
	PolicyKind schemaext.Kind = "ptah.run/pgpolicy/policy"
	// TableStateKind identifies a table's row-security switches.
	TableStateKind schemaext.Kind = "ptah.run/pgpolicy/table-state"
)

// validText refuses text a statement or a catalog cannot carry faithfully.
func validText(field, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: row-security %s is not valid UTF-8", schemaext.ErrInvalidValue, field)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: row-security %s contains a NUL byte", schemaext.ErrInvalidValue, field)
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

// modelError names the model and representation a refusal is about.
func modelError(kind schemaext.Kind, representation schemaext.Representation, err error) error {
	return fmt.Errorf("%s model %q: %w", representation, kind, err)
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
		return nil, fmt.Errorf("%w: expected a non-null row-security %s object", schemaext.ErrInvalidValue, model)
	}
	for _, name := range slices.Sorted(maps.Keys(fields)) {
		if !slices.Contains(allowed, name) {
			return nil, fmt.Errorf("%w: unknown row-security %s property %q", schemaext.ErrInvalidValue, model, name)
		}
		if bytes.Equal(bytes.TrimSpace(fields[name]), []byte("null")) {
			return nil, fmt.Errorf("%w: row-security %s property %q cannot be null", schemaext.ErrInvalidValue, model, name)
		}
	}
	for _, name := range required {
		if _, found := fields[name]; !found {
			return nil, fmt.Errorf("%w: missing row-security %s property %q", schemaext.ErrInvalidValue, model, name)
		}
	}
	return fields, nil
}
