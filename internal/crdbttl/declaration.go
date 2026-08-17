package crdbttl

import (
	"fmt"
	"slices"
	"strconv"
	"strings"

	"go.5x5.cz/ptah/core/ast"
)

// AttributePrefix is what every declared TTL attribute starts with. A parser
// meeting an unknown attribute under it has met a parameter this package
// refuses or does not know, which is worth a diagnostic rather than silence.
const AttributePrefix = "ttl_"

// FromAttributes reads a table's TTL declaration out of parsed annotation
// attributes, and returns nil for a table that declares none.
//
// The attribute names are the storage parameter names, so this is a lookup
// rather than a translation: what an author writes, what the statement carries
// and what pg_class.reloptions reports back are one vocabulary. That is worth
// more than a prettier attribute name, because the operator debugging a TTL is
// reading the catalog and the declaration side by side.
//
// A ttl_ attribute this package does not model is an error rather than an
// ignored key. Two of them name real CockroachDB parameters Ptah refuses, and
// an author who wrote one gets the measured reason and the alternative; the
// rest are typos, and a silently ignored typo in a row-expiry policy is a
// policy that does not exist.
func FromAttributes(table string, attributes map[string]string) (*ast.RowTTLSpec, error) {
	declared := make([]string, 0, len(attributes))
	for name := range attributes {
		if strings.HasPrefix(strings.ToLower(name), AttributePrefix) {
			declared = append(declared, name)
		}
	}
	if len(declared) == 0 {
		return nil, nil
	}
	slices.Sort(declared)

	spec := &ast.RowTTLSpec{}
	for _, name := range declared {
		if err := assignAttribute(spec, table, strings.ToLower(name), attributes[name]); err != nil {
			return nil, err
		}
	}
	if spec.IsZero() {
		return nil, nil
	}
	return spec, nil
}

// assignAttribute writes one declared attribute into the spec.
func assignAttribute(spec *ast.RowTTLSpec, table, name, value string) error {
	if reason, refused := RefusalFor(name); refused {
		return fmt.Errorf("table %q declares %s: %s", table, name, reason)
	}
	switch name {
	case ExpirationExpressionParameter:
		spec.ExpirationExpression = value
		return nil
	case JobCronParameter:
		spec.JobCron = value
		return nil
	}
	if assigned, err := assignIntAttribute(spec, table, name, value); assigned {
		return err
	}
	if assigned, err := assignBoolAttribute(spec, table, name, value); assigned {
		return err
	}
	return fmt.Errorf(
		"table %q declares unknown row-level TTL attribute %q: Ptah manages %s",
		table, name, strings.Join(ManagedParameters(), ", "))
}

func assignIntAttribute(spec *ast.RowTTLSpec, table, name, value string) (bool, error) {
	targets := map[string]**int64{
		SelectBatchSizeParameter: &spec.SelectBatchSize,
		DeleteBatchSizeParameter: &spec.DeleteBatchSize,
		SelectRateLimitParameter: &spec.SelectRateLimit,
		DeleteRateLimitParameter: &spec.DeleteRateLimit,
	}
	target, ok := targets[name]
	if !ok {
		return false, nil
	}
	parsed, err := strconv.ParseInt(strings.TrimSpace(value), 10, 64)
	if err != nil {
		return true, fmt.Errorf("table %q declares %s = %q, which is not an integer", table, name, value)
	}
	*target = &parsed
	return true, nil
}

func assignBoolAttribute(spec *ast.RowTTLSpec, table, name, value string) (bool, error) {
	targets := map[string]**bool{
		PauseParameter:                        &spec.Pause,
		LabelMetricsParameter:                 &spec.LabelMetrics,
		DisableChangefeedReplicationParameter: &spec.DisableChangefeedReplication,
	}
	target, ok := targets[name]
	if !ok {
		return false, nil
	}
	parsed, err := strconv.ParseBool(strings.TrimSpace(value))
	if err != nil {
		return true, fmt.Errorf("table %q declares %s = %q, which is not true or false", table, name, value)
	}
	if !parsed {
		// A false boolean is normalized to "not declared", because on the
		// server those are the SAME STATE rather than two states that happen to
		// behave alike. Measured on v26.2.5: `WITH (..., ttl_pause = false)` is
		// accepted and pg_class.reloptions then holds no ttl_pause row at all,
		// and `ALTER TABLE ... SET (ttl_pause = false)` erases an existing
		// ttl_pause = true exactly as `RESET (ttl_pause)` does. All three
		// booleans behave identically.
		//
		// Keeping the false would make the declaration structurally different
		// from every read of the table it describes, so the comparison would
		// find a difference on every run and the plan would never empty. The
		// author loses nothing: false is the engine's default, and the
		// statement Ptah emits to reach it is the same either way.
		return true, nil
	}
	*target = &parsed
	return true, nil
}

// ManagedParameters names every parameter Ptah models, in the order it renders
// them. It is here so a diagnostic can list the surface rather than a reader
// having to find it.
func ManagedParameters() []string {
	return []string{
		ExpirationExpressionParameter,
		JobCronParameter,
		SelectBatchSizeParameter,
		DeleteBatchSizeParameter,
		SelectRateLimitParameter,
		DeleteRateLimitParameter,
		PauseParameter,
		LabelMetricsParameter,
		DisableChangefeedReplicationParameter,
	}
}
