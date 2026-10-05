package ydbcoordination

import (
	"fmt"
	"math"
	"strings"

	"ptah.run/core/ast"
	"ptah.run/internal/ydbtype"
)

// The settings of a coordination node, as a declaration's attributes and
// Ptah's statement name them: the coordination service's field names, with
// the periods named without their unit, because a declaration writes them as
// durations.
const (
	SettingSelfCheckPeriod         = "self_check_period"
	SettingSessionGracePeriod      = "session_grace_period"
	SettingReadConsistencyMode     = "read_consistency_mode"
	SettingAttachConsistencyMode   = "attach_consistency_mode"
	SettingRateLimiterCountersMode = "rate_limiter_counters_mode"
)

// Settings lists the settings in the order a statement writes them and a
// declaration's errors are reported.
func Settings() []string {
	return []string{
		SettingSelfCheckPeriod, SettingSessionGracePeriod, SettingReadConsistencyMode,
		SettingAttachConsistencyMode, SettingRateLimiterCountersMode,
	}
}

// ParseDeclaration reads a coordination node's settings out of values, keyed
// by setting name, and ignores every other key. A period is an ISO 8601
// duration such as PT1S or PT0.5S, a whole number of milliseconds, since the
// node keeps milliseconds; a mode is one of the names above, in either case.
// The result is checked by [Validate]. A refusal is a [*SettingError] naming
// the setting it is about.
func ParseDeclaration(values map[string]string) (ast.CoordinationNodeSpec, error) {
	var spec ast.CoordinationNodeSpec
	for _, setting := range Settings() {
		value, present := values[setting]
		if !present {
			continue
		}
		if err := set(&spec, setting, value); err != nil {
			return ast.CoordinationNodeSpec{}, err
		}
	}
	if err := Validate(spec); err != nil {
		return ast.CoordinationNodeSpec{}, err
	}
	return spec, nil
}

// set writes one setting's declared value into spec.
func set(spec *ast.CoordinationNodeSpec, setting, value string) error {
	if strings.TrimSpace(value) == "" {
		// An empty value would read as no declaration at all.
		return settingError(setting, "%s is empty: leave the setting out to keep the node's default", setting)
	}
	switch setting {
	case SettingSelfCheckPeriod, SettingSessionGracePeriod:
		millis, err := ParsePeriod(value)
		if err != nil {
			return settingError(setting, "%s %q: %v", setting, value, err)
		}
		if setting == SettingSelfCheckPeriod {
			spec.SelfCheckPeriodMillis = millis
		} else {
			spec.SessionGracePeriodMillis = millis
		}
	case SettingReadConsistencyMode:
		spec.ReadConsistencyMode = strings.ToLower(strings.TrimSpace(value))
	case SettingAttachConsistencyMode:
		spec.AttachConsistencyMode = strings.ToLower(strings.TrimSpace(value))
	case SettingRateLimiterCountersMode:
		spec.RateLimiterCountersMode = strings.ToLower(strings.TrimSpace(value))
	default:
		return settingError(setting, "unknown coordination node setting %q", setting)
	}
	return nil
}

// ParsePeriod reads an ISO 8601 duration into whole milliseconds. It refuses
// text that is not a duration, a negative or zero one, a fraction of a
// millisecond, which the node would not keep, and one too long for the
// node's field.
func ParsePeriod(text string) (uint32, error) {
	micros, ok := ydbtype.ParseInterval(text)
	switch {
	case !ok:
		return 0, fmt.Errorf("a period is an ISO 8601 duration such as PT1S or PT0.5S")
	case micros <= 0:
		return 0, fmt.Errorf("a period is longer than zero")
	case micros%1_000 != 0:
		return 0, fmt.Errorf("the node keeps a period in whole milliseconds")
	case micros/1_000 > math.MaxUint32:
		return 0, fmt.Errorf("the node keeps a period of at most %d milliseconds", uint32(math.MaxUint32))
	}
	return uint32(micros / 1_000), nil
}

// PeriodText writes a period of millis milliseconds as the ISO 8601 duration
// a declaration and Ptah's statement spell it: 1000 is PT1S, 500 is PT0.5S.
func PeriodText(millis uint32) string {
	return ydbtype.IntervalText(int64(millis) * 1_000)
}

// Attributes is spec as a declaration's attributes, in the order of
// [Settings], with the settings it leaves unset left out. It is what a
// description read from a database is written back as.
func Attributes(spec ast.CoordinationNodeSpec) [][2]string {
	var attributes [][2]string
	if spec.SelfCheckPeriodMillis != 0 {
		attributes = append(attributes, [2]string{SettingSelfCheckPeriod, PeriodText(spec.SelfCheckPeriodMillis)})
	}
	if spec.SessionGracePeriodMillis != 0 {
		attributes = append(attributes,
			[2]string{SettingSessionGracePeriod, PeriodText(spec.SessionGracePeriodMillis)})
	}
	if spec.ReadConsistencyMode != "" {
		attributes = append(attributes, [2]string{SettingReadConsistencyMode, spec.ReadConsistencyMode})
	}
	if spec.AttachConsistencyMode != "" {
		attributes = append(attributes, [2]string{SettingAttachConsistencyMode, spec.AttachConsistencyMode})
	}
	if spec.RateLimiterCountersMode != "" {
		attributes = append(attributes, [2]string{SettingRateLimiterCountersMode, spec.RateLimiterCountersMode})
	}
	return attributes
}
