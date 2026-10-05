// Package ydbcoordination holds what Ptah knows about a YDB coordination node
// without a connection: the defaults and limits of a node's configuration,
// the attributes that declare one, and the statement Ptah writes for a change.
//
// YQL has no statement for a coordination node. Measured on YDB 25.1.4.7 and
// 26.2.1.14, `CREATE COORDINATION NODE`, `ALTER COORDINATION NODE` and `DROP
// COORDINATION NODE` are parse errors (`no viable alternative at input 'CREATE
// COORDINATION'` on 26.2, `Unexpected token 'CREATE'` on 25.1), and `CREATE
// OBJECT ... (TYPE COORDINATION_NODE)` is refused at execution. The `ydb` CLI
// has no command for one either. A node is created, changed, described and
// dropped through the coordination service's CreateNode, AlterNode,
// DescribeNode and DropNode calls.
//
// So Ptah writes a statement of its own, in YQL's shape, and Ptah's YDB
// connection runs it through the coordination service instead of sending it
// to the server (see [Recognize]). A plan, a plan file and a migration file
// then carry a coordination node change as text, like every other step. The
// statement is Ptah's, not YDB's: another client sending it to YDB gets the
// parse error above.
//
// This package is free of the YDB SDK, so the renderer and the planner, which
// read it, link no driver.
package ydbcoordination

import (
	"fmt"
	"path"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/core/platform"
	"ptah.run/core/platform/capability"
	"ptah.run/core/ptaherr"
	"ptah.run/core/schemamodel"
)

// LockNode is the coordination node, at the database root, whose semaphores
// are Ptah's own locks (see internal/dblock). The schema reader leaves it out
// of every read, a declaration may not name it, and Ptah's statements refuse
// to touch it.
const LockNode = "ptah_locks"

// The consistency modes a node reads and attaches in, and the counter modes
// of its rate limiter, as a declaration and Ptah's statement spell them.
const (
	ConsistencyStrict  = "strict"
	ConsistencyRelaxed = "relaxed"
	CountersAggregated = "aggregated"
	CountersDetailed   = "detailed"
)

// The limits within which the node honors its periods.
//
// YDB stores whatever a client sends: measured on 25.1.4.7 and 26.2.1.14, a
// self-check period of 1 ms or of 4294967295 ms, and a consistency mode of 7,
// are accepted and read back as sent. The node then runs with the value
// clamped: its tablet takes the self-check period between 500 ms and 10 s,
// and the session grace period between the self-check period plus one second
// and 30 s (MIN_SELF_CHECK_PERIOD, MAX_SELF_CHECK_PERIOD,
// MIN_SESSION_GRACE_PERIOD and MAX_SESSION_GRACE_PERIOD in
// ydb/core/kesus/tablet/tablet_impl.h, the same at the 25.1.4.7 and 26.2.1.14
// tags). Ptah refuses a value the node would not run with, so a declaration
// never reads back as one thing and runs as another.
const (
	MinSelfCheckPeriodMillis    = 500
	MaxSelfCheckPeriodMillis    = 10_000
	MinSessionGraceMarginMillis = 1_000
	MaxSessionGracePeriodMillis = 30_000
)

// Defaults is the configuration a node runs with for every setting nobody
// set: a self-check every second, a ten-second grace period, relaxed reads,
// strict attaches and aggregated counters. They are the tablet's own defaults
// (ydb/core/kesus/tablet/tablet_impl.cpp, the same at the 25.1.4.7 and
// 26.2.1.14 tags). DescribeNode does not report them: a node created without
// a setting reads back with the field unset.
func Defaults() ast.CoordinationNodeSpec {
	return ast.CoordinationNodeSpec{
		SelfCheckPeriodMillis:    1_000,
		SessionGracePeriodMillis: 10_000,
		ReadConsistencyMode:      ConsistencyRelaxed,
		AttachConsistencyMode:    ConsistencyStrict,
		RateLimiterCountersMode:  CountersAggregated,
	}
}

// Effective is spec with every setting it leaves unset taken from [Defaults]:
// the configuration the node runs with.
func Effective(spec ast.CoordinationNodeSpec) ast.CoordinationNodeSpec {
	defaults := Defaults()
	if spec.SelfCheckPeriodMillis == 0 {
		spec.SelfCheckPeriodMillis = defaults.SelfCheckPeriodMillis
	}
	if spec.SessionGracePeriodMillis == 0 {
		spec.SessionGracePeriodMillis = defaults.SessionGracePeriodMillis
	}
	if spec.ReadConsistencyMode == "" {
		spec.ReadConsistencyMode = defaults.ReadConsistencyMode
	}
	if spec.AttachConsistencyMode == "" {
		spec.AttachConsistencyMode = defaults.AttachConsistencyMode
	}
	if spec.RateLimiterCountersMode == "" {
		spec.RateLimiterCountersMode = defaults.RateLimiterCountersMode
	}
	return spec
}

// Changes is what an ALTER has to send to take a node configured as current
// to desired: each setting whose value the node runs with differs, at its
// desired value, and nothing else. Both sides are read through [Effective],
// so a setting left unset on one side and set to its default on the other is
// no change. The zero spec means there is nothing to change.
//
// Naming only what changed is safe: measured on 25.1.4.7 and 26.2.1.14, an
// AlterNode that sets one setting keeps every other one as it was, and an
// empty one changes nothing.
func Changes(desired, current ast.CoordinationNodeSpec) ast.CoordinationNodeSpec {
	want, have := Effective(desired), Effective(current)
	var changes ast.CoordinationNodeSpec
	if want.SelfCheckPeriodMillis != have.SelfCheckPeriodMillis {
		changes.SelfCheckPeriodMillis = want.SelfCheckPeriodMillis
	}
	if want.SessionGracePeriodMillis != have.SessionGracePeriodMillis {
		changes.SessionGracePeriodMillis = want.SessionGracePeriodMillis
	}
	if want.ReadConsistencyMode != have.ReadConsistencyMode {
		changes.ReadConsistencyMode = want.ReadConsistencyMode
	}
	if want.AttachConsistencyMode != have.AttachConsistencyMode {
		changes.AttachConsistencyMode = want.AttachConsistencyMode
	}
	if want.RateLimiterCountersMode != have.RateLimiterCountersMode {
		changes.RateLimiterCountersMode = want.RateLimiterCountersMode
	}
	return changes
}

// Merge is current with every setting changes names replaced: the
// configuration a node holds after an ALTER that sends changes.
func Merge(current, changes ast.CoordinationNodeSpec) ast.CoordinationNodeSpec {
	if changes.SelfCheckPeriodMillis != 0 {
		current.SelfCheckPeriodMillis = changes.SelfCheckPeriodMillis
	}
	if changes.SessionGracePeriodMillis != 0 {
		current.SessionGracePeriodMillis = changes.SessionGracePeriodMillis
	}
	if changes.ReadConsistencyMode != "" {
		current.ReadConsistencyMode = changes.ReadConsistencyMode
	}
	if changes.AttachConsistencyMode != "" {
		current.AttachConsistencyMode = changes.AttachConsistencyMode
	}
	if changes.RateLimiterCountersMode != "" {
		current.RateLimiterCountersMode = changes.RateLimiterCountersMode
	}
	return current
}

// SettingError is a setting whose value a node cannot take. Its message names
// the setting and the value.
type SettingError struct {
	// Setting is the setting's name, one of [Settings].
	Setting string
	// Message is the whole message.
	Message string
}

func (e *SettingError) Error() string { return e.Message }

// settingError builds a [SettingError] for setting.
func settingError(setting, format string, args ...any) *SettingError {
	return &SettingError{Setting: setting, Message: fmt.Sprintf(format, args...)}
}

// Validate reports a configuration the node would not run as written: a mode
// outside the ones above, or a period outside the limits the node clamps it
// to. The grace period is checked against the self-check period the node runs
// with, so a self-check period of 10 s with no grace period declared is
// refused: the default grace of 10 s would be raised to 11 s.
func Validate(spec ast.CoordinationNodeSpec) error {
	for _, mode := range []struct{ setting, value string }{
		{SettingReadConsistencyMode, spec.ReadConsistencyMode},
		{SettingAttachConsistencyMode, spec.AttachConsistencyMode},
	} {
		if mode.value != "" && mode.value != ConsistencyStrict && mode.value != ConsistencyRelaxed {
			return settingError(mode.setting, "%s %q: the mode is %q or %q", mode.setting, mode.value,
				ConsistencyStrict, ConsistencyRelaxed)
		}
	}
	if counters := spec.RateLimiterCountersMode; counters != "" && counters != CountersAggregated &&
		counters != CountersDetailed {
		return settingError(SettingRateLimiterCountersMode, "%s %q: the mode is %q or %q",
			SettingRateLimiterCountersMode, counters, CountersAggregated, CountersDetailed)
	}
	effective := Effective(spec)
	if check := effective.SelfCheckPeriodMillis; check < MinSelfCheckPeriodMillis || check > MaxSelfCheckPeriodMillis {
		return settingError(SettingSelfCheckPeriod, "%s %s: YDB runs a node's self-check every %s to %s and "+
			"moves a period outside that range into it", SettingSelfCheckPeriod, PeriodText(check),
			PeriodText(MinSelfCheckPeriodMillis), PeriodText(MaxSelfCheckPeriodMillis))
	}
	grace, least := effective.SessionGracePeriodMillis, effective.SelfCheckPeriodMillis+MinSessionGraceMarginMillis
	if grace < least || grace > MaxSessionGracePeriodMillis {
		return settingError(SettingSessionGracePeriod, "%s %s: YDB runs a node with a grace period from the "+
			"self-check period plus %s (%s here) to %s and moves a period outside that range into it",
			SettingSessionGracePeriod,
			PeriodText(grace), PeriodText(MinSessionGraceMarginMillis), PeriodText(least),
			PeriodText(MaxSessionGracePeriodMillis))
	}
	return nil
}

// RefuseName refuses a node Ptah must not write: Ptah's own lock node at the
// database root, and a path with a segment that starts with a dot, which
// belongs to the server. schema is the directory, relative to the database
// root, and name the node's name in it.
func RefuseName(schema, name string) error {
	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("a coordination node needs a name")
	}
	written := strings.Trim(schema, "/") + "/" + name
	for segment := range strings.SplitSeq(strings.TrimPrefix(written, "/"), "/") {
		if strings.HasPrefix(segment, ".") {
			return fmt.Errorf("coordination node %s has the path segment %q; a name that starts with a dot "+
				"belongs to the server, and Ptah never writes one", strings.TrimPrefix(written, "/"), segment)
		}
	}
	if path.Clean(strings.TrimPrefix(written, "/")) == LockNode {
		return fmt.Errorf("coordination node %s at the database root holds Ptah's own locks, "+
			"and Ptah never creates, changes or drops it for a schema; name the node differently", LockNode)
	}
	return nil
}

// ValidateDeclared refuses the coordination nodes a schema declares where
// they cannot be created as written: every node on a target without
// [capability.CoordinationNodes], with a [ptaherr.CapabilityError] naming the
// key, and on a target with it a node [RefuseName] refuses or whose
// configuration [Validate] refuses.
//
// The renderer and the comparison both run it before they emit or compare
// anything, so `schema render` and a plan refuse the same declaration with
// the same words, and neither leaves a node out in silence.
func ValidateDeclared(dialect string, caps capability.Capabilities, nodes []schemamodel.CoordinationNode) error {
	if len(nodes) == 0 {
		return nil
	}
	if !caps.Has(capability.CoordinationNodes) {
		normalized := platform.NormalizeDialect(dialect)
		return &ptaherr.CapabilityError{
			Dialect: normalized,
			Feature: string(capability.CoordinationNodes),
			Err:     ptaherr.ErrUnsupportedFeature,
			Message: fmt.Sprintf("coordination node %s, which requires target capability %s, unavailable on this "+
				"%s target: a coordination node is a YDB object", nodes[0].QualifiedName(),
				capability.CoordinationNodes, normalized),
		}
	}
	for _, node := range nodes {
		if err := RefuseName(node.Schema, node.Name); err != nil {
			return fmt.Errorf("%w: %w", ptaherr.ErrUnsupportedFeature, err)
		}
		if err := Validate(node.Spec); err != nil {
			return fmt.Errorf("%w: coordination node %s: %w", ptaherr.ErrUnsupportedFeature, node.QualifiedName(), err)
		}
	}
	return nil
}
