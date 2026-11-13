package migrationrollout

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// ErrInvalidDefinition is the sentinel every refusal of a rollout definition
// wraps, whether the YAML could not be read or a rule was broken.
var ErrInvalidDefinition = errors.New("invalid rollout definition")

// Definition is a rollout definition as written: the target inventory and the
// stages the targets are grouped in. [Definition.Resolve] validates it.
type Definition struct {
	// Targets is the inventory. Every target belongs to exactly one stage.
	Targets []TargetDefinition `yaml:"targets"`
	// Stages lists the stages. Their execution order follows DependsOn, with
	// the order written here deciding between stages that are ready together.
	Stages []StageDefinition `yaml:"stages"`
}

// TargetDefinition is one database the release is rolled out to.
type TargetDefinition struct {
	// Name identifies the target in every report and in the state file.
	Name string `yaml:"name"`
	// URL is the database URL. Exactly one of URL and URLEnv is set.
	URL string `yaml:"url"`
	// URLEnv names the environment variable holding the database URL, so a
	// definition kept in a repository carries no credentials.
	URLEnv string `yaml:"url_env"`
	// Config is a project config file for this target's run, relative to the
	// definition file. Empty leaves the run to find ./ptah.yaml as
	// `ptah migrations up` does.
	Config string `yaml:"config"`
	// ExpectSequence is this target's approved migration sequence, relative to
	// the definition file. The target's run refuses any other selection under
	// its migration lock.
	ExpectSequence string `yaml:"expect_sequence"`
	// AllowDestructive lets this target's run apply migrations the destructive
	// lint gate would refuse. It is per target on purpose: there is no setting
	// that waives the gate for a whole rollout.
	AllowDestructive bool `yaml:"allow_destructive"`
}

// StageDefinition is one group of targets that runs as a unit.
type StageDefinition struct {
	// Name identifies the stage.
	Name string `yaml:"name"`
	// Canary marks the stage every other stage must depend on.
	Canary bool `yaml:"canary"`
	// Targets names the members, in the order they start.
	Targets []string `yaml:"targets"`
	// DependsOn names the stages that must meet their requirement before this
	// one starts.
	DependsOn []string `yaml:"depends_on"`
	// Concurrency is how many members run at once. Zero means one.
	Concurrency int `yaml:"concurrency"`
	// OnFailure is "stop" (the default) or "continue".
	OnFailure string `yaml:"on_failure"`
	// Require is how many members must succeed. The zero value means all.
	Require Requirement `yaml:"require"`
}

// Decode reads a rollout definition. Decoding is strict: an unknown key, a
// repeated key, a value of the wrong type and a second document are refused,
// because a setting this build does not read would be a constraint the author
// believes is in force.
func Decode(data []byte) (Definition, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var definition Definition
	if err := decoder.Decode(&definition); err != nil {
		if errors.Is(err, io.EOF) {
			return Definition{}, fmt.Errorf("%w: the file holds no document", ErrInvalidDefinition)
		}
		return Definition{}, fmt.Errorf("%w: %w", ErrInvalidDefinition, err)
	}
	var next yaml.Node
	if err := decoder.Decode(&next); !errors.Is(err, io.EOF) {
		return Definition{}, fmt.Errorf("%w: the file holds more than one document", ErrInvalidDefinition)
	}
	return definition, nil
}

// requirementKind says how a [Requirement] counts.
type requirementKind int

const (
	requireAll requirementKind = iota
	requireCount
	requirePercent
)

// Requirement is how many of a stage's targets must succeed: all of them, a
// count, or a percentage of the members rounded up. Its zero value is all.
type Requirement struct {
	kind  requirementKind
	value int
}

// RequireAll is the requirement every member must meet.
func RequireAll() Requirement { return Requirement{} }

// RequireCount is the requirement that at least n members succeed.
func RequireCount(n int) Requirement { return Requirement{kind: requireCount, value: n} }

// RequirePercent is the requirement that at least percent of the members
// succeed, rounded up to a whole target.
func RequirePercent(percent int) Requirement {
	return Requirement{kind: requirePercent, value: percent}
}

// String spells the requirement as a definition writes it: "all", a count, or
// a percentage such as "50%".
func (r Requirement) String() string {
	switch r.kind {
	case requireCount:
		return strconv.Itoa(r.value)
	case requirePercent:
		return strconv.Itoa(r.value) + "%"
	default:
		return "all"
	}
}

// UnmarshalYAML reads "all", a positive integer, or a percentage from 1% to
// 100%.
func (r *Requirement) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind != yaml.ScalarNode {
		return fmt.Errorf("line %d: require is all, a count of targets, or a percentage such as 50%%", node.Line)
	}
	parsed, err := parseRequirement(node.Value)
	if err != nil {
		return fmt.Errorf("line %d: %w", node.Line, err)
	}
	*r = parsed
	return nil
}

func parseRequirement(value string) (Requirement, error) {
	text := strings.TrimSpace(value)
	if text == "all" {
		return RequireAll(), nil
	}
	if digits, ok := strings.CutSuffix(text, "%"); ok {
		percent, err := strconv.Atoi(digits)
		if err != nil || percent < 1 || percent > 100 {
			return Requirement{}, fmt.Errorf("require %q is not a percentage from 1%% to 100%%", value)
		}
		return RequirePercent(percent), nil
	}
	count, err := strconv.Atoi(text)
	if err != nil || count < 1 {
		return Requirement{}, fmt.Errorf("require %q is not all, a positive count of targets, or a percentage", value)
	}
	return RequireCount(count), nil
}

// successesNeeded is how many of members targets must succeed.
func (r Requirement) successesNeeded(members int) int {
	switch r.kind {
	case requireCount:
		return r.value
	case requirePercent:
		return (r.value*members + 99) / 100
	default:
		return members
	}
}
