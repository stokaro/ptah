// Package mssqlschema owns the SQL Server security policy model: one
// schema-scoped feature object holding the policy's state and every predicate
// it binds to a table, in a desired and an observed representation. It
// contains no provider selection, database access or SQL.
//
// The model keeps SQL Server's semantics rather than a shape shared with other
// engines (ADR 0020). A security policy belongs to a schema, not to a table:
// one policy may bind predicates to several tables, each through an inline
// table-valued function, so it is captured once with no table parent. Its
// state, schema binding and replication behavior are its own, and each binding
// keeps its table, its function invocation, whether it filters reads or
// blocks writes, and the exact operation a block predicate is evaluated for.
package mssqlschema

import (
	"encoding/json"
	"fmt"
	"strings"
	"unicode/utf8"

	"ptah.run/core/schemaext"
)

// Owner is the provider identity that owns every security policy codec and
// service. It is separate from the targets the services are registered for.
const Owner = "ptah.run/mssql"

// SecurityPolicyKind identifies one security policy of one schema.
const SecurityPolicyKind schemaext.Kind = "ptah.run/mssql/security-policy"

// validText refuses text a statement or a catalog cannot carry faithfully.
func validText(field, value string) error {
	if !utf8.ValidString(value) {
		return fmt.Errorf("%w: security policy %s is not valid UTF-8", schemaext.ErrInvalidValue, field)
	}
	if strings.ContainsRune(value, '\x00') {
		return fmt.Errorf("%w: security policy %s contains a NUL byte", schemaext.ErrInvalidValue, field)
	}
	return nil
}

// modelError names the model and representation a refusal is about, as the
// typed error the codec boundary and the census recognize.
func modelError(representation schemaext.Representation, err error) error {
	return &schemaext.InvalidModelError{Kind: SecurityPolicyKind, Representation: representation, Message: err.Error()}
}

// decodeObject decodes one strict object and refuses a present key whose
// string value is empty: the model spells an absent value by omitting the key,
// so an empty one cannot mean anything the definition allows.
func decodeObject(data json.RawMessage, shape schemaext.ObjectShape, nonEmpty ...string) (map[string]json.RawMessage, error) {
	fields, err := schemaext.DecodeObject(data, shape)
	if err != nil {
		return nil, err
	}
	for _, key := range nonEmpty {
		if string(fields[key]) == `""` {
			return nil, fmt.Errorf("%w: security %s property %q cannot be empty; omit it instead", schemaext.ErrInvalidValue, shape.Name, key)
		}
	}
	return fields, nil
}
