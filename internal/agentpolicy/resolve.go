package agentpolicy

import (
	"fmt"
	"slices"
	"sort"

	"go.5x5.cz/ptah/internal/agentdiag"
)

// ErrHardDenied reports a configuration that tried to grant a capability no
// layer may grant.
var ErrHardDenied = agentdiag.Sentinel(agentdiag.CodeCapabilityDenied, "capability cannot be granted")

// Defaults is the builtin policy, and it is the answer to "what does Ptah do
// when nobody configured anything".
//
// Every entry is stated, including the ones a reader would guess, because a
// capability absent from this table would resolve to the zero [Verdict] --
// [VerdictDeny] -- and be indistinguishable from a deliberate refusal. The
// guard test over [Capabilities] fails when a capability is missing here, so
// adding one to the list above forces a decision about it rather than letting
// it default in silence.
//
// The shape of the table: reading a declaration is allowed, reading a live
// database is asked about because it opens a connection to something the
// operator may not have meant, writing a file is asked about, and everything
// that mutates a database or reaches past the workspace is refused.
func Defaults() []Rule {
	return []Rule{
		{Capability: ProjectRead, Verdict: VerdictAllow},
		{Capability: SchemaValidate, Verdict: VerdictAllow},
		{Capability: SchemaRender, Verdict: VerdictAllow},
		{Capability: SchemaLineage, Verdict: VerdictAllow},
		// Resolving a desired schema that runs repository-controlled code is
		// not reading the repository. The default refuses rather than asks,
		// because the question "may Ptah run this project's loader" is one the
		// operator answers when they configure the workspace, not one to put in
		// front of somebody mid-conversation who has no way to see what the
		// command does.
		{Capability: SchemaExternalExecute, Verdict: VerdictDeny},

		// Inspecting a database the operator classified is a question worth
		// asking; inspecting one nobody classified, which includes every URL
		// that arrived as a tool argument, is refused.
		{Capability: DatabaseInspect, Database: ClassEphemeral, Verdict: VerdictAllow},
		{Capability: DatabaseInspect, Database: ClassDev, Verdict: VerdictAsk},
		{Capability: DatabaseInspect, Database: ClassTarget, Verdict: VerdictAsk},
		{Capability: DatabaseInspect, Database: ClassProduction, Verdict: VerdictDeny},
		{Capability: DatabaseInspect, Database: ClassUnclassified, Verdict: VerdictDeny},

		{Capability: DatabaseReadRows, Verdict: VerdictDeny},
		{Capability: DatabaseExecuteSQL, Verdict: VerdictDeny},
		{Capability: MigrationApply, Verdict: VerdictDeny},

		{Capability: ArtifactRead, Verdict: VerdictAllow},
		{Capability: ArtifactWrite, Verdict: VerdictAsk},
		{Capability: ArtifactDelete, Verdict: VerdictDeny},

		{Capability: FilesystemArbitraryRead, Verdict: VerdictDeny},
		{Capability: FilesystemArbitraryWrite, Verdict: VerdictDeny},
		{Capability: ShellExecute, Verdict: VerdictDeny},
		{Capability: NetworkArbitrary, Verdict: VerdictDeny},
	}
}

// LayerRules is one layer's contribution to the assembled policy.
type LayerRules struct {
	Layer Layer
	// Source names where the rules were read from, for a diagnostic that sends
	// the operator to the file rather than to the layer's abstract name.
	Source string
	Rules  []Rule
}

// cell identifies one resolved capability-and-scope pair.
type cell struct {
	Capability Capability
	Artifact   ArtifactClass
	Database   DatabaseClass
}

// Entry is one row of the resolved policy.
type Entry struct {
	Capability Capability    `json:"capability"`
	Artifact   ArtifactClass `json:"artifact,omitempty"`
	Database   DatabaseClass `json:"database,omitempty"`
	Verdict    string        `json:"verdict"`
	// DecidedBy names the layer whose rule produced the verdict, so an operator
	// asking "why can it not write" is answered with a place to edit.
	DecidedBy string `json:"decided_by"`
	Source    string `json:"source,omitempty"`
}

// Ignored is a rule that had no effect, and why.
//
// It is reported rather than dropped because the two rules that reach it are a
// repository trying to grant itself something and an operator writing a line
// that does nothing, and both are worth seeing.
type Ignored struct {
	Layer   string `json:"layer"`
	Source  string `json:"source,omitempty"`
	Rule    string `json:"rule"`
	Reason  string `json:"reason"`
	Applied string `json:"applied_verdict"`
}

// Policy is a fully resolved decision table.
//
// It is resolved once, before the model is reached, and never consults anything
// afterwards. That is what makes "the model cannot escalate" a property of the
// program rather than a promise: there is no code path from a tool argument to
// this table.
type Policy struct {
	verdicts map[cell]Verdict
	decided  map[cell]LayerRules
	ignored  []Ignored
}

// Assemble resolves the layers into one policy.
//
// Layers are applied in the order given after being sorted by [Layer], so a
// caller cannot change the outcome by passing them in a different order --
// which is the property that makes the precedence rule checkable rather than a
// convention every call site has to remember.
func Assemble(layers ...LayerRules) (*Policy, error) {
	policy := &Policy{
		verdicts: make(map[cell]Verdict),
		decided:  make(map[cell]LayerRules),
	}
	builtin := LayerRules{Layer: LayerBuiltin, Source: "builtin", Rules: Defaults()}
	if err := policy.applyLayer(builtin); err != nil {
		return nil, err
	}
	if err := policy.assertComplete(); err != nil {
		return nil, err
	}

	ordered := slices.Clone(layers)
	sort.SliceStable(ordered, func(i, j int) bool { return ordered[i].Layer < ordered[j].Layer })
	for _, layer := range ordered {
		if layer.Layer == LayerBuiltin {
			return nil, fmt.Errorf("layer %q is this package's own; it cannot be supplied", layer.Layer)
		}
		if err := policy.applyLayer(layer); err != nil {
			return nil, err
		}
	}
	return policy, nil
}

// applyLayer folds one layer in, most specific rules last so a scoped rule wins
// over the unscoped rule in the same file whatever order they were written in.
func (p *Policy) applyLayer(layer LayerRules) error {
	rules := slices.Clone(layer.Rules)
	sort.SliceStable(rules, func(i, j int) bool { return rules[i].specificity() < rules[j].specificity() })
	for _, rule := range rules {
		if err := rule.validate(); err != nil {
			return fmt.Errorf("%s policy: %w", layer.Layer, err)
		}
		if IsHardDenied(rule.Capability) && rule.Verdict != VerdictDeny {
			return fmt.Errorf("%s policy: %q: %w", layer.Layer, rule, ErrHardDenied)
		}
		p.applyRule(layer, rule)
	}
	return nil
}

// applyRule writes the rule into every cell it governs.
func (p *Policy) applyRule(layer LayerRules, rule Rule) {
	touched := false
	for _, target := range cellsFor(rule.Capability) {
		if !rule.matches(target.request()) {
			continue
		}
		touched = true
		current, seen := p.verdicts[target]
		if !seen || layer.Layer.mayWiden() {
			p.verdicts[target] = rule.Verdict
			p.decided[target] = layer
			continue
		}
		narrowed := atMost(current, rule.Verdict)
		if narrowed == current && rule.Verdict != current {
			p.ignored = append(p.ignored, Ignored{
				Layer:   layer.Layer.String(),
				Source:  layer.Source,
				Rule:    rule.String(),
				Applied: current.String(),
				Reason: fmt.Sprintf(
					"a %s policy may only narrow, and %s is already in effect",
					layer.Layer, current),
			})
			continue
		}
		p.verdicts[target] = narrowed
		p.decided[target] = layer
	}
	if !touched {
		p.ignored = append(p.ignored, Ignored{
			Layer:   layer.Layer.String(),
			Source:  layer.Source,
			Rule:    rule.String(),
			Applied: "none",
			Reason:  "the rule governs no capability and scope this build knows",
		})
	}
}

// request is the cell read as the request that would land in it.
func (c cell) request() Request {
	return Request{Capability: c.Capability, Artifact: c.Artifact, Database: c.Database}
}

// cellsFor enumerates every scope a capability is decided over.
func cellsFor(capability Capability) []cell {
	switch capabilityScope[capability] {
	case scopeArtifact:
		cells := make([]cell, 0, len(ArtifactClasses()))
		for _, class := range ArtifactClasses() {
			cells = append(cells, cell{Capability: capability, Artifact: class})
		}
		return cells
	case scopeDatabase:
		cells := make([]cell, 0, len(DatabaseClasses()))
		for _, class := range DatabaseClasses() {
			cells = append(cells, cell{Capability: capability, Database: class})
		}
		return cells
	case scopeNone:
		return []cell{{Capability: capability}}
	}
	return nil
}

// Scopes enumerates every scope a capability is decided over, as the requests
// that would land in each cell.
//
// It is exported because two callers need the same enumeration and neither
// should rebuild it: the test that asserts the builtin table decides every
// cell, and the surface that renders what a session may do. A hand-written
// second list is what stops matching the first.
func Scopes(capability Capability) []Request {
	targets := cellsFor(capability)
	requests := make([]Request, 0, len(targets))
	for _, target := range targets {
		requests = append(requests, target.request())
	}
	return requests
}

// assertComplete refuses a builtin table that left a cell undecided.
//
// A missing cell would resolve to [VerdictDeny] through the zero value and read
// exactly like a deliberate refusal, which is the failure this check exists to
// make impossible: a capability added to [Capabilities] without a default is a
// build that fails here rather than a surface that quietly refuses something it
// was meant to allow.
func (p *Policy) assertComplete() error {
	for _, capability := range Capabilities() {
		for _, target := range cellsFor(capability) {
			if _, decided := p.verdicts[target]; !decided {
				return fmt.Errorf("builtin policy states no default for %q", target.request())
			}
		}
	}
	return nil
}

// Decide answers one request from the resolved table.
//
// It reads nothing but the request's capability and scope. The paths and the
// reason the request carries are for the person being asked and for the audit
// record; deciding from them would be deciding from text the model chose.
func (p *Policy) Decide(req Request) (Decision, error) {
	if err := req.validate(); err != nil {
		return Decision{}, err
	}
	target := cell{Capability: req.Capability, Artifact: req.Artifact, Database: req.Database}
	verdict, decided := p.verdicts[target]
	if !decided {
		// Unreachable while assertComplete holds, and stated anyway: an
		// undecided cell must refuse rather than fall through to the zero
		// value's meaning by accident.
		return Decision{
			Verdict: VerdictDeny,
			Layer:   LayerBuiltin,
			Reason:  fmt.Sprintf("no policy decides %q", req),
		}, nil
	}
	layer := p.decided[target]
	return Decision{
		Verdict: verdict,
		Layer:   layer.Layer,
		Source:  layer.Source,
		Reason:  fmt.Sprintf("%q is %s by %s policy", req, verdict, layer.Layer),
	}, nil
}

// Entries renders the whole resolved table, sorted for a stable diff.
//
// The whole table rather than the grants: an operator checking what an agent
// session can do needs to see the refusals too, and a report that listed only
// what is allowed would answer "nothing was granted" the same way as "the
// report is broken".
func (p *Policy) Entries() []Entry {
	entries := make([]Entry, 0, len(p.verdicts))
	for _, capability := range Capabilities() {
		for _, target := range cellsFor(capability) {
			layer := p.decided[target]
			entries = append(entries, Entry{
				Capability: target.Capability,
				Artifact:   target.Artifact,
				Database:   target.Database,
				Verdict:    p.verdicts[target].String(),
				DecidedBy:  layer.Layer.String(),
				Source:     layer.Source,
			})
		}
	}
	return entries
}

// Ignored lists the rules that had no effect.
func (p *Policy) Ignored() []Ignored {
	return slices.Clone(p.ignored)
}

// Decision is what the broker concluded, and enough of why to print it.
type Decision struct {
	Verdict Verdict
	Layer   Layer
	Source  string
	Reason  string
}
