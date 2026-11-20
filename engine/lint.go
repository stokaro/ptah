package engine

import (
	"cmp"
	"context"
	"fmt"
	"slices"

	"ptah.run/core/lintext"
	"ptah.run/core/ptaherr"
)

// Lint publishes the migration lint rules one owner checks and the service
// that checks them. A rule names the targets it judges, each one a target
// the runtime registers. Several providers may publish one name, which then
// stands for one hazard checked once per engine: their targets must not
// overlap and their severities must match.
type Lint struct {
	// Rules are the rules the service checks, as data.
	Rules []lintext.Rule
	// Service checks them, once per lint run.
	Service lintext.Service
}

// ownedLintRule is a published rule and the service that checks it.
type ownedLintRule struct {
	owner   string
	rule    lintext.Rule
	service int
}

func (r *Runtime) registerLint(owner string, declaration Lint) error {
	if declaration.Service == nil || nilService(declaration.Service) || len(declaration.Rules) == 0 {
		return fmt.Errorf("%w: provider %q publishes lint rules without a service, or a service without rules", ErrInvalidRegistration, owner)
	}
	service := len(r.lintServices)
	r.lintServices = append(r.lintServices, declaration.Service)
	for _, rule := range declaration.Rules {
		if err := rule.Validate(); err != nil {
			return fmt.Errorf("%w: provider %q: %w", ErrInvalidRegistration, owner, err)
		}
		published := rule.Clone()
		for i, name := range published.Targets {
			selected, found := r.lookup(name)
			if !found {
				return fmt.Errorf("%w: lint rule %s judges unregistered target %q", ErrInvalidRegistration, rule.Name, name)
			}
			published.Targets[i] = selected.name
		}
		for _, existing := range r.lintRules[rule.Name] {
			if existing.rule.Severity != published.Severity {
				return fmt.Errorf("%w: lint rule %s is published by %q at severity %s and by %q at severity %s; one name has one severity",
					ErrInvalidRegistration, rule.Name, existing.owner, existing.rule.Severity, owner, published.Severity)
			}
			if shared := sharedTarget(existing.rule.Targets, published.Targets); shared != "" {
				return fmt.Errorf("%w: lint rule %s is published by %q and %q for the same target %q",
					ErrInvalidRegistration, rule.Name, existing.owner, owner, shared)
			}
		}
		r.lintRules[rule.Name] = append(r.lintRules[rule.Name], ownedLintRule{owner: owner, rule: published, service: service})
	}
	return nil
}

// sharedTarget returns a target both lists name, or the empty string.
func sharedTarget(a, b []string) string {
	for _, target := range a {
		if slices.Contains(b, target) {
			return target
		}
	}
	return ""
}

// judging returns the published entries of a rule name that judge target:
// all of them for a run that names no target.
func (r *Runtime) judging(ruleName, target string) []ownedLintRule {
	if r == nil {
		return nil
	}
	var found []ownedLintRule
	for _, owned := range r.lintRules[ruleName] {
		if owned.rule.Judges(target) {
			found = append(found, owned)
		}
	}
	return found
}

// LintRules returns the published rules that judge target, every published
// rule for an empty target, sorted by name. A name several owners publish is
// returned once per owner, each naming that owner's targets. The rules are
// copies, each naming canonical targets. An unregistered target wraps
// ptaherr.ErrUnsupportedDialect.
func (r *Runtime) LintRules(target string) ([]lintext.Rule, error) {
	name, err := r.lintTarget(target)
	if err != nil {
		return nil, err
	}
	var rules []lintext.Rule
	if r != nil {
		for _, entries := range r.lintRules {
			for _, owned := range entries {
				if owned.rule.Judges(name) {
					rules = append(rules, owned.rule.Clone())
				}
			}
		}
	}
	slices.SortFunc(rules, func(a, b lintext.Rule) int {
		return cmp.Or(cmp.Compare(a.Name, b.Name), slices.Compare(a.Targets, b.Targets))
	})
	return rules, nil
}

// SplitStatements splits a lint run's sources with the splitter of the
// request's target, under its canonical name. A target that is not
// registered or registers no splitter wraps ptaherr.ErrUnsupportedDialect, a
// reply that does not answer the request wraps lintext.ErrInvalid, and an
// error returns no result.
func (r *Runtime) SplitStatements(ctx context.Context, request lintext.SplitRequest) (lintext.SplitResult, error) {
	if ctx == nil {
		return lintext.SplitResult{}, fmt.Errorf("%w: splitting requires a context", lintext.ErrInvalid)
	}
	selected, found := r.lookup(request.Target)
	if !found || selected.splitter == nil {
		return lintext.SplitResult{}, fmt.Errorf("%w: no statement splitter for target %q", ptaherr.ErrUnsupportedDialect, request.Target)
	}
	request.Target = selected.name
	request.Sources = slices.Clone(request.Sources)
	result, err := selected.splitter.SplitStatements(ctx, request)
	if err != nil {
		return lintext.SplitResult{}, err
	}
	if err := lintext.ValidateSplit(request, result); err != nil {
		return lintext.SplitResult{}, fmt.Errorf("statement splitter of %q: %w", selected.owner, err)
	}
	return result, ctx.Err()
}

// CheckMigrations sends each of the request's rule names to the owners that
// publish it for the request's target, one call per owner, and joins the
// findings, sorted by file, statement and name, and the baseline subjects,
// sorted by file and name. A name no owner publishes for the target, and an
// unregistered target, are refused before any owner is called; a reply that
// does not answer its request, or names baseline subjects for a rule whose
// input reads no baseline, wraps lintext.ErrInvalid. An error returns no
// result.
func (r *Runtime) CheckMigrations(ctx context.Context, request lintext.CheckRequest) (lintext.CheckResult, error) {
	if ctx == nil {
		return lintext.CheckResult{}, fmt.Errorf("%w: checking requires a context", lintext.ErrInvalid)
	}
	name, err := r.lintTarget(request.Target.Name)
	if err != nil {
		return lintext.CheckResult{}, err
	}
	request.Target.Name = name
	batches := make(map[int][]string)
	var order []int
	for i, ruleName := range request.Names {
		entries := r.judging(ruleName, name)
		if len(entries) == 0 || slices.Contains(request.Names[:i], ruleName) {
			return lintext.CheckResult{}, fmt.Errorf("%w: no published lint rule %s judges target %q, or it is asked for twice", lintext.ErrInvalid, ruleName, name)
		}
		for _, owned := range entries {
			if _, seen := batches[owned.service]; !seen {
				order = append(order, owned.service)
			}
			if !slices.Contains(batches[owned.service], ruleName) {
				batches[owned.service] = append(batches[owned.service], ruleName)
			}
		}
	}
	result := lintext.CheckResult{Complete: true}
	for _, service := range order {
		if err := ctx.Err(); err != nil {
			return lintext.CheckResult{}, err
		}
		batch := request
		batch.Names = batches[service]
		reply, err := r.lintServices[service].CheckMigrations(ctx, batch)
		if err != nil {
			return lintext.CheckResult{}, err
		}
		owner := r.lintOwner(service)
		if err := lintext.ValidateCheck(batch, reply); err != nil {
			return lintext.CheckResult{}, fmt.Errorf("lint rules of %q: %w", owner, err)
		}
		for _, subject := range reply.BaselineSubjects {
			if !r.readsBaseline(subject.Name, service) {
				return lintext.CheckResult{}, fmt.Errorf("lint rules of %q: %w: rule %s names baseline subjects but reads no baseline",
					owner, lintext.ErrInvalid, subject.Name)
			}
		}
		result.Findings = append(result.Findings, reply.Findings...)
		result.BaselineSubjects = append(result.BaselineSubjects, reply.BaselineSubjects...)
	}
	slices.SortStableFunc(result.Findings, func(a, b lintext.Finding) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Statement, b.Statement), cmp.Compare(a.Name, b.Name))
	})
	slices.SortStableFunc(result.BaselineSubjects, func(a, b lintext.BaselineSubject) int {
		return cmp.Or(cmp.Compare(a.File, b.File), cmp.Compare(a.Name, b.Name))
	})
	return result, ctx.Err()
}

// lintOwner returns the provider that registered a lint service.
func (r *Runtime) lintOwner(service int) string {
	for _, entries := range r.lintRules {
		for _, owned := range entries {
			if owned.service == service {
				return owned.owner
			}
		}
	}
	return ""
}

// readsBaseline reports whether the rule a service publishes under name reads
// a baseline.
func (r *Runtime) readsBaseline(ruleName string, service int) bool {
	for _, owned := range r.lintRules[ruleName] {
		if owned.service == service {
			return owned.rule.Input.ReadsBaseline()
		}
	}
	return false
}

// AnnotateStatements asks every target that registers an annotator, in
// target name order, for the facts of the request's statements, one call per
// annotator, and joins the answers: each statement's effects in that order,
// and every statement an annotator declined with the target that declined it.
// A request whose sources and statement lists do not pair up, and a reply that
// does not answer its request, wrap lintext.ErrInvalid; an error returns no
// result.
func (r *Runtime) AnnotateStatements(ctx context.Context, request lintext.AnnotateRequest) (lintext.AnnotateResult, error) {
	if ctx == nil {
		return lintext.AnnotateResult{}, fmt.Errorf("%w: annotating requires a context", lintext.ErrInvalid)
	}
	if err := lintext.ValidateAnnotateRequest(request); err != nil {
		return lintext.AnnotateResult{}, err
	}
	result := lintext.AnnotateResult{Files: make([][]lintext.Annotation, len(request.Files))}
	for i, statements := range request.Files {
		result.Files[i] = make([]lintext.Annotation, len(statements))
	}
	for _, selected := range r.annotators() {
		if err := ctx.Err(); err != nil {
			return lintext.AnnotateResult{}, err
		}
		reply, err := selected.annotator.AnnotateStatements(ctx, request)
		if err != nil {
			return lintext.AnnotateResult{}, err
		}
		if err := lintext.ValidateAnnotate(request, reply); err != nil {
			return lintext.AnnotateResult{}, fmt.Errorf("statement annotator of %q: %w", selected.owner, err)
		}
		for i, annotations := range reply.Files {
			for j, annotation := range annotations {
				joined := &result.Files[i][j]
				joined.Effects = append(joined.Effects, annotation.Effects...)
				if annotation.Declined {
					joined.Declined = true
					result.Declined = append(result.Declined, lintext.Declined{Target: selected.name, File: i, Statement: j})
				}
			}
		}
	}
	return result, ctx.Err()
}

// annotators are the targets that register an annotator, each once, in name
// order.
func (r *Runtime) annotators() []target {
	if r == nil {
		return nil
	}
	var found []target
	for name, selected := range r.targets {
		if selected.annotator != nil && name == selected.name {
			found = append(found, selected)
		}
	}
	slices.SortFunc(found, func(a, b target) int { return cmp.Compare(a.name, b.name) })
	return found
}

// lintTarget resolves the target a lint call names to its canonical name. An
// empty name stays empty: a run that names no target.
func (r *Runtime) lintTarget(name string) (string, error) {
	if name == "" {
		return "", nil
	}
	selected, found := r.lookup(name)
	if !found {
		return "", fmt.Errorf("%w: lint target %q is not registered", ptaherr.ErrUnsupportedDialect, name)
	}
	return selected.name, nil
}
