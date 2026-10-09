// Package chresolve resolves ClickHouse storage intent against captured
// state and creation rules. It neither inspects a server nor changes a source.
package chresolve

import (
	"errors"
	"fmt"
	"strings"

	"ptah.run/core/schemaext"
	"ptah.run/dialect/clickhouse/chschema"
)

// ErrUnknownCurrent means an omitted setting needs an unavailable observation.
// It is not a successful no-op or permission to select a creation default.
var ErrUnknownCurrent = errors.New("ClickHouse setting was not inspected")

// ErrMissingSortingKey means creation rules cannot supply a MergeTree ORDER BY.
// An explicit empty sorting key is valid and does not produce this error.
var ErrMissingSortingKey = errors.New("ClickHouse MergeTree requires a sorting key")

// Origin identifies the evidence used for one prepared property. Defaults are
// Ptah's creation rules, not an assertion about a server's configuration.
type Origin string

const (
	// Declaration means the author supplied an explicit value, including empty.
	Declaration Origin = "declaration"
	// Observation means an omitted setting retained captured current state.
	Observation Origin = "observation"
	// CreationRule means omitted creation intent or an explicit default request
	// selected the same value a new object would receive.
	CreationRule Origin = "creation-rule"
)

// Origins records the source of every prepared property. Its fields correspond
// to chschema.DesiredTable; a successful resolution sets every field.
type Origins struct {
	Engine, OrderBy, PrimaryKey, PartitionBy, SampleBy, TTL, Settings Origin
}

// Request resolves a declaration for creation or an existing table. Current is
// a complete, usable observation; callers must check its source coverage before
// supplying it. Nil means unavailable, never an inspected empty table. Creating
// establishes that no current table exists and therefore forbids Current.
// CommonKey contains the ordered SQL column expressions supplied by the common
// declaration for creation's sorting-key fallback. Inputs remain unchanged.
type Request struct {
	Desired   *chschema.DesiredTable
	Current   *chschema.ObservedTable
	Creating  bool
	CommonKey []string
	// BaseEngine is the common table engine declaration. Target-specific engine
	// intent takes precedence; an unspecified engine retains this declaration.
	BaseEngine string
}

// Result keeps authored intent separate from fully explicit prepared intent.
// Prepared is planning input, not a claim that a table exists or was inspected.
// All fields are values with no mutable aliases to the request. SQL expressions
// retain their spelling apart from surrounding whitespace; no SQL equivalence
// or capability check is implied by a successful resolution.
type Result struct {
	Declared chschema.DesiredTable
	Prepared chschema.DesiredTable
	Origins  Origins
}

// Table resolves every property atomically. Unspecified settings retain current
// state on existing tables and use creation rules on new ones. Default always
// selects a creation rule, including primary-key inheritance from ORDER BY.
// An explicit empty key remains empty. Optional defaults remove the table's
// explicit clause; Settings does not enumerate inherited server configuration.
// Nil or malformed declarations and contradictory creation inputs wrap
// schemaext.ErrInvalidValue. Missing evidence wraps ErrUnknownCurrent; a missing
// creation sorting key wraps ErrMissingSortingKey. Any error returns zero Result.
func Table(request Request) (Result, error) {
	if err := chschema.ValidateDesired(request.Desired); err != nil {
		return Result{}, err
	}
	if request.Creating && request.Current != nil {
		return Result{}, fmt.Errorf("%w: a new ClickHouse table cannot have current state", schemaext.ErrInvalidValue)
	}
	if request.Current != nil {
		if err := chschema.ValidateObserved(request.Current); err != nil {
			return Result{}, err
		}
	}
	current := chschema.ObservedTable{}
	if request.Current != nil {
		current = *request.Current
	}
	result := Result{Declared: *request.Desired}
	p, o := &result.Prepared, &result.Origins
	engine := request.Desired.Engine
	if engine.State == chschema.Unspecified && request.BaseEngine != "" {
		engine = chschema.Setting{State: chschema.Explicit, Value: request.BaseEngine}
	}
	properties := []property{
		{"engine", engine, &current.Engine, "MergeTree", &p.Engine, &o.Engine},
		{"order_by", request.Desired.OrderBy, &current.OrderBy, "", &p.OrderBy, &o.OrderBy},
		{"primary_key", request.Desired.PrimaryKey, &current.PrimaryKey, "", &p.PrimaryKey, &o.PrimaryKey},
		{"partition_by", request.Desired.PartitionBy, &current.PartitionBy, "", &p.PartitionBy, &o.PartitionBy},
		{"sample_by", request.Desired.SampleBy, &current.SampleBy, "", &p.SampleBy, &o.SampleBy},
		{"ttl", request.Desired.TTL, &current.TTL, "", &p.TTL, &o.TTL},
		{"settings", request.Desired.Settings, &current.Settings, "", &p.Settings, &o.Settings},
	}
	for _, property := range properties {
		if request.Current == nil {
			property.current = nil
		}
		if err := resolveProperty(property, request.Creating); err != nil {
			return Result{}, err
		}
	}
	if IsMergeTree(p.Engine.Value) {
		if o.OrderBy == CreationRule {
			p.OrderBy.Value = strings.Join(request.CommonKey, ", ")
			if strings.TrimSpace(p.OrderBy.Value) == "" {
				return Result{}, ErrMissingSortingKey
			}
		}
		if o.PrimaryKey == CreationRule {
			p.PrimaryKey.Value = p.OrderBy.Value
		}
	}
	// Common-key expressions enter through the target context rather than the
	// facet. They must satisfy the same text contract as declared properties.
	if err := chschema.ValidateDesired(p); err != nil {
		return Result{}, err
	}
	return result, nil
}

type property struct {
	name     string
	desired  chschema.Setting
	current  *string
	fallback string
	value    *chschema.Setting
	origin   *Origin
}

func resolveProperty(property property, creating bool) error {
	property.value.State = chschema.Explicit
	switch {
	case property.desired.State == chschema.Explicit:
		property.value.Value = strings.TrimSpace(property.desired.Value)
		*property.origin = Declaration
	case property.desired.State == chschema.Unspecified && !creating:
		if property.current == nil {
			return fmt.Errorf("%w: %s", ErrUnknownCurrent, property.name)
		}
		property.value.Value = *property.current
		*property.origin = Observation
	default:
		property.value.Value = property.fallback
		*property.origin = CreationRule
	}
	return nil
}

// IsMergeTree reports whether engine names a MergeTree-family engine, ignoring
// surrounding whitespace, case, and its parameter list. It does not validate
// that the engine is installed or that its parameters are supported.
func IsMergeTree(engine string) bool {
	name, _, _ := strings.Cut(engine, "(")
	return strings.HasSuffix(strings.ToUpper(strings.TrimSpace(name)), "MERGETREE")
}

// StorageOptionKeys returns the option names owned by ClickHouse table storage.
// The independent result uses AST spelling; source overrides use lowercase.
// Rendering and preparation share this vocabulary so duplicate declarations
// cannot be accepted by one stage and silently ignored by another.
func StorageOptionKeys() []string {
	return []string{"ENGINE", "ORDER_BY", "PARTITION_BY", "PRIMARY_KEY", "SAMPLE_BY", "SETTINGS", "TTL"}
}
