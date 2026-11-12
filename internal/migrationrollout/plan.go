package migrationrollout

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"ptah.run/internal/atlasurl"
	"ptah.run/internal/dburldisplay"
)

// PlanContractVersion is the version of the plan document `ptah migrations
// rollout plan --json` prints. It rises when a field changes meaning or leaves.
const PlanContractVersion = 1

// FailurePolicy is what a stage does once one of its targets fails.
type FailurePolicy string

const (
	// FailureStop starts no further target of the stage after a failure. The
	// targets already running finish, and their results are kept.
	FailureStop FailurePolicy = "stop"
	// FailureContinue starts every target of the stage whatever the others
	// report.
	FailureContinue FailurePolicy = "continue"
)

// namePattern is what a target or a stage may be called. A name prefixes every
// line a target's run writes, so it holds no space and no character a shell or
// a log reader would read as syntax.
var namePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// envNamePattern is a portable environment variable name.
var envNamePattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// ResolveOptions are the inputs [Definition.Resolve] reads from outside the
// definition.
type ResolveOptions struct {
	// BaseDir is the directory relative paths in the definition are read
	// from: the definition file's own.
	BaseDir string
	// LookupEnv reads the variables url_env names. Nil reads the process
	// environment.
	LookupEnv func(string) (string, bool)
}

// Plan is a validated definition: the stages in the order they run, each with
// its members in the order they start.
type Plan struct {
	// Digest identifies the plan: a SHA-256 over every stage and target
	// setting, with each database URL in its display form. A credential can
	// change between a run and its resumption; a host, a database, a member or
	// a stage setting cannot.
	Digest string
	// Stages holds the stages in execution order.
	Stages []Stage
}

// Stage is one resolved stage.
type Stage struct {
	Name        string
	Canary      bool
	DependsOn   []string
	Concurrency int
	OnFailure   FailurePolicy
	Require     Requirement
	// Required is how many members must succeed for the stage to succeed.
	Required int
	Targets  []Target
}

// Target is one resolved target.
type Target struct {
	TargetRecord
	// URL is the database URL, credentials included. It never reaches a
	// report: TargetRecord.Database is its display form.
	URL string
	// ConfigPath and ExpectSequencePath are Config and ExpectSequence
	// resolved against the definition's directory.
	ConfigPath         string
	ExpectSequencePath string
}

// TargetRecord is what every report says about a target's settings. It holds
// no credential.
type TargetRecord struct {
	Name             string `json:"name"`
	Database         string `json:"database"`
	URLEnv           string `json:"url_env,omitempty"`
	Config           string `json:"config,omitempty"`
	ExpectSequence   string `json:"expect_sequence,omitempty"`
	AllowDestructive bool   `json:"allow_destructive,omitempty"`
}

// StageRecord is what every report says about a stage's settings.
type StageRecord struct {
	Name        string         `json:"name"`
	Canary      bool           `json:"canary,omitempty"`
	DependsOn   []string       `json:"depends_on,omitempty"`
	Concurrency int            `json:"concurrency"`
	OnFailure   FailurePolicy  `json:"on_failure"`
	Require     string         `json:"require"`
	Required    int            `json:"required"`
	Targets     []TargetRecord `json:"targets"`
}

// PlanDocument is the plan as `ptah migrations rollout plan --json` prints it.
type PlanDocument struct {
	ContractVersion int           `json:"contract_version"`
	Digest          string        `json:"digest"`
	Stages          []StageRecord `json:"stages"`
}

// Document returns the plan as a document.
func (p Plan) Document() PlanDocument {
	return PlanDocument{ContractVersion: PlanContractVersion, Digest: p.Digest, Stages: p.records()}
}

func (p Plan) records() []StageRecord {
	records := make([]StageRecord, 0, len(p.Stages))
	for _, stage := range p.Stages {
		records = append(records, stage.record())
	}
	return records
}

func (s Stage) record() StageRecord {
	targets := make([]TargetRecord, 0, len(s.Targets))
	for _, target := range s.Targets {
		targets = append(targets, target.TargetRecord)
	}
	return StageRecord{
		Name:        s.Name,
		Canary:      s.Canary,
		DependsOn:   slices.Clone(s.DependsOn),
		Concurrency: s.Concurrency,
		OnFailure:   s.OnFailure,
		Require:     s.Require.String(),
		Required:    s.Required,
		Targets:     targets,
	}
}

// problems collects every rule a definition breaks, so one read reports them
// all rather than one per edit.
type problems []string

func (p *problems) addf(format string, args ...any) {
	*p = append(*p, fmt.Sprintf(format, args...))
}

func (p problems) err() error {
	if len(p) == 0 {
		return nil
	}
	return fmt.Errorf("%w:\n- %s", ErrInvalidDefinition, strings.Join(p, "\n- "))
}

// Resolve validates the definition and returns its plan. It contacts no
// database. Every broken rule is reported in one error wrapping
// [ErrInvalidDefinition]:
//
//   - a target or stage name that is empty, repeated, or not a plain word;
//   - a target with both or neither of url and url_env, an unset variable, a
//     config or expect_sequence file that does not exist;
//   - two targets that address one database endpoint with the same connection
//     options and config, whatever credentials they carry;
//   - a stage naming an unknown target or stage, a target in two stages or in
//     none, a stage with no targets, a dependency cycle;
//   - a concurrency below one, an unknown on_failure, a requirement above the
//     member count;
//   - more than one canary stage, a canary that depends on a stage, or a stage
//     that does not depend on the canary directly or through other stages.
func (d Definition) Resolve(opts ResolveOptions) (Plan, error) {
	if opts.LookupEnv == nil {
		opts.LookupEnv = os.LookupEnv
	}
	var found problems
	targets := resolveTargets(d.Targets, opts, &found)
	stages := resolveStages(d.Stages, targets, &found)
	order := orderStages(stages, &found)
	checkCanary(stages, &found)
	if err := found.err(); err != nil {
		return Plan{}, err
	}
	plan := Plan{Stages: make([]Stage, 0, len(order))}
	for _, index := range order {
		plan.Stages = append(plan.Stages, stages[index])
	}
	digest, err := planDigest(plan)
	if err != nil {
		return Plan{}, err
	}
	plan.Digest = digest
	return plan, nil
}

func planDigest(plan Plan) (string, error) {
	encoded, err := json.Marshal(plan.records())
	if err != nil {
		return "", fmt.Errorf("encode rollout plan: %w", err)
	}
	sum := sha256.Sum256(encoded)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

func resolveTargets(definitions []TargetDefinition, opts ResolveOptions, found *problems) map[string]Target {
	if len(definitions) == 0 {
		found.addf("targets: the inventory names no target")
	}
	targets := make(map[string]Target, len(definitions))
	resolved := make([]Target, 0, len(definitions))
	for index, definition := range definitions {
		target, ok := resolveTarget(index, definition, opts, found)
		if !ok {
			continue
		}
		if _, repeated := targets[target.Name]; repeated {
			found.addf("target %q is declared more than once", target.Name)
			continue
		}
		targets[target.Name] = target
		resolved = append(resolved, target)
	}
	checkDuplicateEndpoints(resolved, found)
	return targets
}

func resolveTarget(index int, definition TargetDefinition, opts ResolveOptions, found *problems) (Target, bool) {
	if !namePattern.MatchString(definition.Name) {
		found.addf("targets[%d]: name %q is not a word of letters, digits, '.', '_' or '-', starting with a letter or digit", index, definition.Name)
		return Target{}, false
	}
	label := fmt.Sprintf("target %q", definition.Name)
	target := Target{TargetRecord: TargetRecord{
		Name:             definition.Name,
		URLEnv:           definition.URLEnv,
		Config:           definition.Config,
		ExpectSequence:   definition.ExpectSequence,
		AllowDestructive: definition.AllowDestructive,
	}}
	switch {
	case definition.URL != "" && definition.URLEnv != "":
		found.addf("%s: set url or url_env, not both", label)
	case definition.URL != "":
		target.URL = definition.URL
	case definition.URLEnv == "":
		found.addf("%s: set url or url_env", label)
	case !envNamePattern.MatchString(definition.URLEnv):
		found.addf("%s: url_env %q is not an environment variable name", label, definition.URLEnv)
	default:
		value, ok := opts.LookupEnv(definition.URLEnv)
		if !ok || strings.TrimSpace(value) == "" {
			found.addf("%s: the variable %s that url_env names is not set", label, definition.URLEnv)
			value = ""
		}
		target.URL = value
	}
	target.Database = dburldisplay.Format(target.URL)
	target.ConfigPath = resolveFile(opts.BaseDir, label, "config", definition.Config, found)
	target.ExpectSequencePath = resolveFile(opts.BaseDir, label, "expect_sequence", definition.ExpectSequence, found)
	return target, true
}

// resolveFile reads a path the definition names against the definition's
// directory and refuses one that is not a file there.
func resolveFile(baseDir, label, key, value string, found *problems) string {
	if value == "" {
		return ""
	}
	path := filepath.FromSlash(value)
	if !filepath.IsAbs(path) {
		path = filepath.Join(baseDir, path)
	}
	info, err := os.Stat(path)
	switch {
	case err != nil:
		found.addf("%s: %s %q cannot be read: %v", label, key, value, err)
	case info.IsDir():
		found.addf("%s: %s %q is a directory", label, key, value)
	}
	return path
}

// checkDuplicateEndpoints refuses two targets that are one target written
// twice: the same database endpoint with the same connection options and the
// same config. Credentials are ignored, so the same database listed under two
// accounts is still one target.
//
// The options and the config take part because one database can hold several
// targets: a schema-per-tenant layout selects a tenant with a search_path in
// the URL, or with the revision table a config names, and each such target
// keeps its own history.
func checkDuplicateEndpoints(targets []Target, found *problems) {
	for i, left := range targets {
		for _, right := range targets[i+1:] {
			if sameTarget(left, right) {
				found.addf("targets %q and %q address one database with the same options and config", left.Name, right.Name)
			}
		}
	}
}

func sameTarget(left, right Target) bool {
	if left.URL == "" || right.URL == "" || left.Config != right.Config {
		return false
	}
	if left.URL == right.URL {
		return true
	}
	leftScheme, _, leftOK := strings.Cut(left.URL, "://")
	rightScheme, _, rightOK := strings.Cut(right.URL, "://")
	if !leftOK || !rightOK || !strings.EqualFold(leftScheme, rightScheme) {
		return false
	}
	leftURL, leftErr := url.Parse(left.URL)
	rightURL, rightErr := url.Parse(right.URL)
	if leftErr != nil || rightErr != nil || leftURL.RawQuery != rightURL.RawQuery {
		return false
	}
	same, err := atlasurl.SameDatabaseEndpoint(left.URL, right.URL)
	return err == nil && same
}

func resolveStages(definitions []StageDefinition, targets map[string]Target, found *problems) []Stage {
	if len(definitions) == 0 {
		found.addf("stages: the definition names no stage")
	}
	stages := make([]Stage, 0, len(definitions))
	seen := make(map[string]bool, len(definitions))
	memberOf := make(map[string]string, len(targets))
	for index, definition := range definitions {
		if !namePattern.MatchString(definition.Name) {
			found.addf("stages[%d]: name %q is not a word of letters, digits, '.', '_' or '-', starting with a letter or digit", index, definition.Name)
			continue
		}
		if seen[definition.Name] {
			found.addf("stage %q is declared more than once", definition.Name)
			continue
		}
		seen[definition.Name] = true
		stages = append(stages, resolveStage(definition, targets, memberOf, found))
	}
	for _, stage := range stages {
		for _, dependency := range stage.DependsOn {
			if !seen[dependency] {
				found.addf("stage %q depends on %q, which is not a stage", stage.Name, dependency)
			}
		}
	}
	for name := range targets {
		if _, ok := memberOf[name]; !ok {
			found.addf("target %q belongs to no stage, so the rollout would never reach it", name)
		}
	}
	return stages
}

func resolveStage(definition StageDefinition, targets map[string]Target, memberOf map[string]string, found *problems) Stage {
	label := fmt.Sprintf("stage %q", definition.Name)
	stage := Stage{
		Name:        definition.Name,
		Canary:      definition.Canary,
		DependsOn:   slices.Clone(definition.DependsOn),
		Concurrency: definition.Concurrency,
		OnFailure:   FailurePolicy(definition.OnFailure),
		Require:     definition.Require,
	}
	if stage.Concurrency == 0 {
		stage.Concurrency = 1
	}
	if stage.Concurrency < 0 {
		found.addf("%s: concurrency %d is below one", label, definition.Concurrency)
	}
	if stage.OnFailure == "" {
		stage.OnFailure = FailureStop
	}
	if stage.OnFailure != FailureStop && stage.OnFailure != FailureContinue {
		found.addf("%s: on_failure %q is not stop or continue", label, definition.OnFailure)
	}
	if len(definition.Targets) == 0 {
		found.addf("%s: names no target", label)
	}
	for _, name := range definition.Targets {
		target, ok := targets[name]
		if !ok {
			found.addf("%s: target %q is not in the inventory", label, name)
			continue
		}
		if owner, taken := memberOf[name]; taken {
			found.addf("%s: target %q is already a member of stage %q", label, name, owner)
			continue
		}
		memberOf[name] = definition.Name
		stage.Targets = append(stage.Targets, target)
	}
	for _, dependency := range definition.DependsOn {
		if dependency == definition.Name {
			found.addf("%s: depends on itself", label)
		}
	}
	stage.Required = stage.Require.successesNeeded(len(stage.Targets))
	if stage.Required > len(stage.Targets) && len(stage.Targets) > 0 {
		found.addf("%s: require %s is more than its %d targets", label, stage.Require, len(stage.Targets))
	}
	return stage
}

// orderStages returns the stage indexes in execution order: a stage runs after
// every stage it depends on, and between stages that are ready together the
// one declared first runs first. A cycle is reported with its path.
func orderStages(stages []Stage, found *problems) []int {
	index := make(map[string]int, len(stages))
	for i, stage := range stages {
		index[stage.Name] = i
	}
	placed := make([]bool, len(stages))
	order := make([]int, 0, len(stages))
	for len(order) < len(stages) {
		next := -1
		for i, stage := range stages {
			if !placed[i] && dependenciesPlaced(stage, index, placed) {
				next = i
				break
			}
		}
		if next < 0 {
			found.addf("stages form a dependency cycle: %s", describeCycle(stages, index, placed))
			return order
		}
		placed[next] = true
		order = append(order, next)
	}
	return order
}

func dependenciesPlaced(stage Stage, index map[string]int, placed []bool) bool {
	for _, dependency := range stage.DependsOn {
		position, known := index[dependency]
		if known && !placed[position] {
			return false
		}
	}
	return true
}

// describeCycle walks unplaced dependencies from the first unplaced stage until
// a stage repeats, and spells the loop it found.
func describeCycle(stages []Stage, index map[string]int, placed []bool) string {
	start := slices.Index(placed, false)
	visited := make(map[int]int, len(stages))
	path := make([]string, 0, len(stages))
	current := start
	for {
		if at, seen := visited[current]; seen {
			loop := append(slices.Clone(path[at:]), stages[current].Name)
			return strings.Join(loop, " -> ")
		}
		visited[current] = len(path)
		path = append(path, stages[current].Name)
		current = firstUnplacedDependency(stages[current], index, placed)
	}
}

func firstUnplacedDependency(stage Stage, index map[string]int, placed []bool) int {
	for _, dependency := range stage.DependsOn {
		if position, known := index[dependency]; known && !placed[position] {
			return position
		}
	}
	// Unreachable for a stage orderStages could not place: it has an unplaced
	// dependency by construction.
	return index[stage.Name]
}

// checkCanary holds a canary stage to what the mark promises: one canary, run
// first, and nothing that can start before it has met its requirement.
func checkCanary(stages []Stage, found *problems) {
	var canaries []string
	for _, stage := range stages {
		if stage.Canary {
			canaries = append(canaries, stage.Name)
		}
	}
	if len(canaries) == 0 {
		return
	}
	if len(canaries) > 1 {
		found.addf("stages %s are each marked canary; a rollout has at most one canary", strings.Join(canaries, ", "))
		return
	}
	canary := canaries[0]
	byName := make(map[string]Stage, len(stages))
	for _, stage := range stages {
		byName[stage.Name] = stage
	}
	if len(byName[canary].DependsOn) > 0 {
		found.addf("canary stage %q depends on another stage; the canary runs first", canary)
	}
	for _, stage := range stages {
		if stage.Name != canary && !reaches(stage.Name, canary, byName, make(map[string]bool, len(stages))) {
			found.addf("stage %q does not depend on the canary stage %q, so it could start before the canary succeeds", stage.Name, canary)
		}
	}
}

// reaches reports whether stage from depends on stage to, directly or through
// other stages.
func reaches(from, to string, byName map[string]Stage, visited map[string]bool) bool {
	if visited[from] {
		return false
	}
	visited[from] = true
	for _, dependency := range byName[from].DependsOn {
		if dependency == to || reaches(dependency, to, byName, visited) {
			return true
		}
	}
	return false
}
