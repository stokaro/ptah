package crdbschema

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"ptah.run/core/schemaext"
)

// The parameter names, spelled exactly as CockroachDB spells them. A second
// spelling of any of these is a silent no-op on the server rather than an
// error, so every consumer names them through these constants.
const (
	// MarkerParameter is ttl, the derived marker. It is never declared or
	// compared. `RESET (ttl)` is how a whole configuration is removed.
	MarkerParameter = "ttl"

	ExpirationExpressionParameter         = "ttl_expiration_expression"
	ExpireAfterParameter                  = "ttl_expire_after"
	RowStatsPollIntervalParameter         = "ttl_row_stats_poll_interval"
	JobCronParameter                      = "ttl_job_cron"
	SelectBatchSizeParameter              = "ttl_select_batch_size"
	DeleteBatchSizeParameter              = "ttl_delete_batch_size"
	SelectRateLimitParameter              = "ttl_select_rate_limit"
	DeleteRateLimitParameter              = "ttl_delete_rate_limit"
	PauseParameter                        = "ttl_pause"
	LabelMetricsParameter                 = "ttl_label_metrics"
	DisableChangefeedReplicationParameter = "ttl_disable_changefeed_replication"
)

// ValueKind says how a parameter value is spelled in a statement.
type ValueKind string

const (
	// TextValue is a string the statement quotes.
	TextValue ValueKind = "text"
	// CountValue is a decimal integer.
	CountValue ValueKind = "count"
	// FlagValue is the boolean true; a false flag is never written.
	FlagValue ValueKind = "flag"
)

// Parameter is one set storage parameter in its unquoted form.
type Parameter struct {
	Name  string
	Value string
	Kind  ValueKind
}

type textField struct {
	name  string
	value *string
}

type countField struct {
	name  string
	value **int64
}

type flagField struct {
	name  string
	value *bool
}

// The order of each list is the order Parameters returns, which is the order
// a statement names the parameters. It is fixed so that the statement text is
// a function of the two states alone; the migration layer fingerprints it.
// The enablers come first because the others modify them.
func (p *Policy) texts() []textField {
	return []textField{
		{ExpirationExpressionParameter, &p.ExpirationExpression},
		{ExpireAfterParameter, &p.ExpireAfter},
		{RowStatsPollIntervalParameter, &p.RowStatsPollInterval},
		{JobCronParameter, &p.JobCron},
	}
}

func (p *Policy) counts() []countField {
	return []countField{
		{SelectBatchSizeParameter, &p.SelectBatchSize},
		{DeleteBatchSizeParameter, &p.DeleteBatchSize},
		{SelectRateLimitParameter, &p.SelectRateLimit},
		{DeleteRateLimitParameter, &p.DeleteRateLimit},
	}
}

func (p *Policy) flags() []flagField {
	return []flagField{
		{PauseParameter, &p.Pause},
		{LabelMetricsParameter, &p.LabelMetrics},
		{DisableChangefeedReplicationParameter, &p.DisableChangefeedReplication},
	}
}

// ManagedParameters names every parameter the policy models, in statement
// order. Each call returns a fresh slice.
func ManagedParameters() []string {
	var p Policy
	var names []string
	for _, text := range p.texts() {
		names = append(names, text.name)
	}
	for _, count := range p.counts() {
		names = append(names, count.name)
	}
	for _, flag := range p.flags() {
		names = append(names, flag.name)
	}
	return names
}

// Parameters returns the set parameters in statement order. A policy that
// sets nothing returns nil.
func (p Policy) Parameters() []Parameter {
	var result []Parameter
	for _, text := range p.texts() {
		if *text.value != "" {
			result = append(result, Parameter{Name: text.name, Value: *text.value, Kind: TextValue})
		}
	}
	for _, count := range p.counts() {
		if *count.value != nil {
			result = append(result, Parameter{Name: count.name, Value: strconv.FormatInt(**count.value, 10), Kind: CountValue})
		}
	}
	for _, flag := range p.flags() {
		if *flag.value {
			result = append(result, Parameter{Name: flag.name, Value: "true", Kind: FlagValue})
		}
	}
	return result
}

// Sets reports whether the policy sets a managed parameter.
func (p Policy) Sets(name string) bool {
	return slices.ContainsFunc(p.Parameters(), func(parameter Parameter) bool { return parameter.Name == name })
}

// DecodeDeclared reads declared parameters into a validated declaration. Every
// name must be a managed parameter. The derived ttl marker is refused with the
// reason the server gives, and other names are refused as unknown. A count
// must be a decimal integer and a flag a boolean; a false flag declares the
// engine's default and decodes to an unset flag. An empty map is invalid,
// because a declaration with no parameters is not a policy. Errors wrap
// schemaext.ErrInvalidValue and name the first offending parameter in sorted
// order, so the same input always reports the same problem.
func DecodeDeclared(parameters map[string]string) (*DesiredRowTTL, error) {
	result := &DesiredRowTTL{}
	for _, name := range slices.Sorted(maps.Keys(parameters)) {
		if err := assignDeclared(&result.Policy, name, parameters[name]); err != nil {
			return nil, err
		}
	}
	if err := ValidateDesired(result); err != nil {
		return nil, err
	}
	return result, nil
}

func assignDeclared(p *Policy, name, value string) error {
	// The marker is refused in any case: naming its lower-case spelling would
	// point at a name refused in turn.
	if strings.EqualFold(name, MarkerParameter) {
		return fmt.Errorf("%w: %s is derived from the other parameters and is refused by the server when it arrives "+
			"alone; declare %s to turn a TTL on, and remove that to turn it off",
			schemaext.ErrInvalidValue, MarkerParameter, ExpirationExpressionParameter)
	}
	for _, text := range p.texts() {
		if text.name == name {
			// An empty text is how a policy says "unset", so storing it would
			// read a declared parameter as one never written. Refused like the
			// blank text ValidateDesired refuses and the wire codec's empty
			// string.
			if value == "" {
				return fmt.Errorf("%w: %s is empty; remove the parameter to leave it unset", schemaext.ErrInvalidValue, name)
			}
			*text.value = value
			return nil
		}
	}
	for _, count := range p.counts() {
		if count.name == name {
			parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
			if err != nil {
				return fmt.Errorf("%w: %s = %q, which is not an integer", schemaext.ErrInvalidValue, name, value)
			}
			*count.value = &parsed
			return nil
		}
	}
	for _, flag := range p.flags() {
		if flag.name == name {
			parsed, err := strconv.ParseBool(strings.TrimSpace(value))
			if err != nil {
				return fmt.Errorf("%w: %s = %q, which is not true or false", schemaext.ErrInvalidValue, name, value)
			}
			*flag.value = parsed
			return nil
		}
	}
	if lower := strings.ToLower(name); lower != name && slices.Contains(ManagedParameters(), lower) {
		return fmt.Errorf("%w: unknown row-level TTL parameter %q: parameter names are lower case, as %q",
			schemaext.ErrInvalidValue, name, lower)
	}
	return fmt.Errorf("%w: unknown row-level TTL parameter %q: Ptah manages %s",
		schemaext.ErrInvalidValue, name, strings.Join(ManagedParameters(), ", "))
}

// DecodeStored reads the unquoted storage parameters a table read returned.
// Names the policy does not model are ignored, because a server that adds a
// storage parameter must not break a read: v26.2 adds schema_locked to every
// table and v25.4 adds nothing. A count that is not an integer is left unset
// rather than recorded as a zero nobody stored, and the server never stores a
// false flag. It returns nil when no managed parameter is set, which is a table
// without a TTL. The result is not validated; callers validate it before use.
func DecodeStored(parameters map[string]string) *ObservedRowTTL {
	var policy Policy
	for _, text := range policy.texts() {
		*text.value = parameters[text.name]
	}
	for _, count := range policy.counts() {
		if parsed, err := strconv.ParseInt(parameters[count.name], 10, 64); err == nil {
			*count.value = &parsed
		}
	}
	for _, flag := range policy.flags() {
		parsed, err := strconv.ParseBool(parameters[flag.name])
		*flag.value = err == nil && parsed
	}
	if policy.IsZero() {
		return nil
	}
	return &ObservedRowTTL{Policy: policy}
}
